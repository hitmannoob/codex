use std::time::Duration;

use anyhow::Result;
use app_test_support::TestAppServer;
use codex_app_server_protocol::EnvironmentAddResponse;
use codex_app_server_protocol::EnvironmentRemoveResponse;
use codex_app_server_protocol::EnvironmentStatusKind;
use codex_app_server_protocol::EnvironmentStatusResponse;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::header;
use wiremock::matchers::method;
use wiremock::matchers::path;

const RPC_TIMEOUT: Duration = Duration::from_secs(10);

async fn call(app_server: &mut TestAppServer, method: &str, params: Value) -> Result<Value> {
    let request_id = app_server.send_raw_request(method, Some(params)).await?;
    let response: JSONRPCResponse = timeout(
        RPC_TIMEOUT,
        app_server.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    Ok(response.result)
}

async fn rejected(app_server: &mut TestAppServer, method: &str, params: Value) -> Result<String> {
    let request_id = app_server.send_raw_request(method, Some(params)).await?;
    let error = timeout(
        RPC_TIMEOUT,
        app_server.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    Ok(error.error.message)
}

#[tokio::test]
async fn noise_registry_environments_connect_through_the_registry_and_can_be_removed() -> Result<()>
{
    // A registry with no executor connected yet: each connection asks it for
    // rendezvous credentials, authenticated with the harness token.
    let registry = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/cloud/environment/executor-1/connect"))
        .and(header("authorization", "Bearer harness-token"))
        .respond_with(ResponseTemplate::new(409).set_body_json(
            json!({"error":{"code":"environment_offline","message":"no executor is connected"}}),
        ))
        .mount(&registry)
        .await;
    let codex_home = TempDir::new()?;
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build()
        .await?;
    timeout(RPC_TIMEOUT, app_server.initialize()).await??;

    // Exactly one connection method is accepted.
    for params in [
        json!({"environmentId":"session-env"}),
        json!({"environmentId":"session-env","execServerUrl":"ws://127.0.0.1:1",
            "noiseRegistry":{"url":registry.uri(),"environmentId":"executor-1","authToken":"harness-token"}}),
    ] {
        assert_eq!(
            rejected(&mut app_server, "environment/add", params).await?,
            "exactly one of execServerUrl and noiseRegistry is required"
        );
    }

    let added = call(&mut app_server, "environment/add", json!({"environmentId":"session-env",
        "noiseRegistry":{"url":registry.uri(),"environmentId":"executor-1","authToken":"harness-token"}})).await?;
    let _: EnvironmentAddResponse = serde_json::from_value(added)?;

    // The environment connects through the registry with the harness's Noise key.
    let connect = timeout(RPC_TIMEOUT, async {
        loop {
            let requests = registry.received_requests().await.unwrap_or_default();
            if let Some(request) = requests
                .into_iter()
                .find(|request| request.url.path().ends_with("/connect"))
            {
                break request;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let body: Value = serde_json::from_slice(&connect.body)?;
    assert_eq!(
        body["harness_public_key"]["suite"],
        json!("Noise_hybridIK_X25519+MLKEM768_AESGCM_SHA256")
    );
    let status: EnvironmentStatusResponse = serde_json::from_value(
        call(
            &mut app_server,
            "environment/status",
            json!({"environmentId":"session-env"}),
        )
        .await?,
    )?;
    assert_ne!(status.status, EnvironmentStatusKind::Unknown);

    // Removal forgets the environment; the reserved local id cannot be removed.
    let removed: EnvironmentRemoveResponse = serde_json::from_value(
        call(
            &mut app_server,
            "environment/remove",
            json!({"environmentId":"session-env"}),
        )
        .await?,
    )?;
    assert_eq!(removed, EnvironmentRemoveResponse { removed: true });
    let status: EnvironmentStatusResponse = serde_json::from_value(
        call(
            &mut app_server,
            "environment/status",
            json!({"environmentId":"session-env"}),
        )
        .await?,
    )?;
    assert_eq!(status.status, EnvironmentStatusKind::Unknown);
    let again: EnvironmentRemoveResponse = serde_json::from_value(
        call(
            &mut app_server,
            "environment/remove",
            json!({"environmentId":"session-env"}),
        )
        .await?,
    )?;
    assert_eq!(again, EnvironmentRemoveResponse { removed: false });
    assert_eq!(
        rejected(
            &mut app_server,
            "environment/remove",
            json!({"environmentId":"local"})
        )
        .await?,
        "exec-server protocol error: environment id `local` is reserved for EnvironmentManager"
    );
    Ok(())
}
