//! Encrypted storage for credential values, in the API data directory, under
//! a passphrase the operator supplies. Built on the repository's
//! `codex-secrets` file store, whose OS-keyring lookup is replaced by that
//! passphrase because servers usually lack a keyring.
use crate::ApiError;
use axum::http::StatusCode;
use codex_keyring_store::CredentialStoreError;
use codex_keyring_store::KeyringStore;
use codex_secrets::LocalSecretsBackend;
use codex_secrets::SecretName;
use codex_secrets::SecretScope;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

/// Supplies the operator's passphrase to the secrets store. It is never
/// persisted.
#[derive(Debug)]
struct OperatorPassphrase(String);

impl KeyringStore for OperatorPassphrase {
    fn load(&self, _service: &str, _account: &str) -> Result<Option<String>, CredentialStoreError> {
        Ok(Some(self.0.clone()))
    }

    // The store saves a key only when loading finds none, which never happens.
    fn save(
        &self,
        _service: &str,
        _account: &str,
        _value: &str,
    ) -> Result<(), CredentialStoreError> {
        Ok(())
    }

    fn delete(&self, _service: &str, _account: &str) -> Result<bool, CredentialStoreError> {
        Ok(false)
    }
}

/// The store and the values already written or read. Every write re-encrypts
/// the whole file with a deliberately slow key derivation, so cached values let
/// turn starts read their session's snapshots without decrypting again.
struct Store {
    backend: LocalSecretsBackend,
    cache: HashMap<String, Option<Value>>,
}

/// Unconfigured until the operator supplies a passphrase. Operations run on a
/// blocking thread, one at a time, because each write rewrites the file.
#[derive(Default)]
pub(crate) struct Secrets(OnceLock<Arc<Mutex<Store>>>);

impl Secrets {
    /// Enable the store once the passphrase is shown to read any secrets
    /// already stored, so a wrong passphrase stops startup instead of failing
    /// every later request. Only the first accepted passphrase takes effect.
    pub(crate) async fn configure(
        &self,
        directory: std::path::PathBuf,
        passphrase: String,
    ) -> anyhow::Result<()> {
        let secrets = directory.join("secrets");
        let backend = LocalSecretsBackend::new(directory, Arc::new(OperatorPassphrase(passphrase)));
        let backend = tokio::task::spawn_blocking(move || {
            backend.list(/*scope_filter*/ None).map(|_| backend)
        })
        .await?
        .map_err(|error| {
            error.context(format!(
                "the vault passphrase cannot read the secrets stored in {}",
                secrets.display()
            ))
        })?;
        let _ = self.0.set(Arc::new(Mutex::new(Store {
            backend,
            cache: HashMap::new(),
        })));
        Ok(())
    }

    async fn with<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Store) -> anyhow::Result<T> + Send + 'static,
    ) -> Result<T, ApiError> {
        let store = Arc::clone(self.0.get().ok_or_else(|| {
            ApiError(
                StatusCode::NOT_IMPLEMENTED,
                "vault credentials require the operator to configure a vault passphrase".into(),
            )
        })?);
        Ok(
            tokio::task::spawn_blocking(move || operation(&mut crate::lock(&store)))
                .await
                .map_err(anyhow::Error::from)??,
        )
    }

    pub(crate) async fn set(&self, name: String, value: Value) -> Result<(), ApiError> {
        self.with(move |store| {
            store.backend.set(
                &SecretScope::Global,
                &SecretName::new(&name)?,
                &value.to_string(),
            )?;
            store.cache.insert(name, Some(value));
            Ok(())
        })
        .await
    }

    pub(crate) async fn get(&self, name: String) -> Result<Option<Value>, ApiError> {
        self.with(move |store| {
            if let Some(value) = store.cache.get(&name) {
                return Ok(value.clone());
            }
            let value = store
                .backend
                .get(&SecretScope::Global, &SecretName::new(&name)?)?
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            store.cache.insert(name, value.clone());
            Ok(value)
        })
        .await
    }

    pub(crate) async fn delete(&self, names: Vec<String>) -> Result<(), ApiError> {
        self.with(move |store| {
            for name in names {
                store
                    .backend
                    .delete(&SecretScope::Global, &SecretName::new(&name)?)?;
                store.cache.insert(name, None);
            }
            Ok(())
        })
        .await
    }
}

/// The secrets-store name holding a vault credential's values.
pub(crate) fn credential_name(credential_id: &str) -> String {
    format!("VAULT_{}", credential_id.to_ascii_uppercase())
}
