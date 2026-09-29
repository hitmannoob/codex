use super::*;
use axum::response::IntoResponse;
use pretty_assertions::assert_eq;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

const PASSPHRASE: &str = "operator-vault-passphrase-for-tests-0123456789";

/// A minimal HTTP MCP server recording the Authorization header of each tool call.
async fn mcp_server() -> anyhow::Result<(
    String,
    Arc<Mutex<Vec<Option<String>>>>,
    tokio::task::JoinHandle<std::io::Result<()>>,
)> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&calls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/mcp", listener.local_addr()?);
    let router = axum::Router::new().route("/mcp", axum::routing::post(move |headers: axum::http::HeaderMap, axum::Json(message): axum::Json<Value>| {
        let recorded = Arc::clone(&recorded);
        async move {
            let result = match message["method"].as_str() {
                Some("initialize") => json!({"protocolVersion": "2025-06-18", "capabilities": {"tools": {}}, "serverInfo": {"name": "fixture", "version": "1"}}),
                Some("notifications/initialized") => return axum::http::StatusCode::ACCEPTED.into_response(),
                Some("tools/list") => json!({"tools": [{"name": "lookup", "description": "Lookup", "inputSchema": {"type": "object", "properties": {}}}]}),
                Some("tools/call") => {
                    let auth = headers.get("authorization").and_then(|value| value.to_str().ok()).map(str::to_owned);
                    recorded.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(auth);
                    json!({"content": [{"type": "text", "text": "looked-up"}]})
                }
                _ => json!({}),
            };
            axum::Json(json!({"jsonrpc": "2.0", "id": message["id"], "result": result})).into_response()
        }
    }));
    Ok((
        url,
        calls,
        tokio::spawn(async move { axum::serve(listener, router).await }),
    ))
}

fn files_containing(directory: &Path, needle: &str) -> anyhow::Result<Vec<String>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            found.extend(files_containing(&path, needle)?);
        } else if std::fs::read(&path)?
            .windows(needle.len())
            .any(|window| window == needle.as_bytes())
        {
            found.push(path.display().to_string());
        }
    }
    Ok(found)
}

#[tokio::test]
async fn vault_credentials_authenticate_mcp_as_session_snapshots() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let (mcp_url, calls, mcp_task) = mcp_server().await?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        // Every user turn asks for one MCP call; call IDs stay unique.
        let counter = Arc::new(AtomicUsize::new(0));
        Mock::given(|request: &wiremock::Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
            body["input"].as_array().and_then(|items| items.last()).is_some_and(|item| item.to_string().contains("call-mcp"))
        })
        .respond_with(move |_: &wiremock::Request| {
            let id = format!("mcp-call-{}", counter.fetch_add(1, Ordering::SeqCst));
            ResponseTemplate::new(/*s*/ 200).set_body_raw(capabilities::call("mcp__warehouse.lookup", &id), "text/event-stream")
        })
        .with_priority(/*p*/ 1).mount(&provider).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let (home_path, data_path) = (home.path().to_path_buf(), data.path().to_path_buf());
        let start = |configured: bool| {
            let (home, data) = (home_path.clone(), data_path.clone());
            async move {
                let api = AgentsApi::new(backend(&home).await?, AbsolutePathBuf::from_absolute_path(&data)?, TOKEN.into()).await?;
                api.allow_mcp_hosts(["127.0.0.1".to_string()]);
                if configured {
                    api.configure_vault(PASSPHRASE.into()).await?;
                }
                Ok::<_, anyhow::Error>(api)
            }
        };
        let api = start(false).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::builder().default_headers([("openai-beta".parse()?, "agents=v1".parse()?)].into_iter().collect()).build()?;
        let mut responses = Vec::new();

        // Vault metadata works without a passphrase; credentials do not.
        let vault = request(&client, reqwest::Method::POST, &format!("{base}/vaults"), json!({"name":" team ","metadata":{"app":"billing"}})).await?;
        assert_eq!(json!({"object":vault["object"],"name":vault["name"],"metadata":vault["metadata"]}), json!({"object":"vault","name":"team","metadata":{"app":"billing"}}));
        let vault_url = format!("{base}/vaults/{}", vault["id"].as_str().context("vault id")?);
        let bearer = |token: &str| json!({"name":"warehouse","auth":{"type":"static_bearer","token":token,"mcp_server_url":mcp_url}});
        let response = client.post(format!("{vault_url}/credentials")).bearer_auth(TOKEN).json(&bearer("secret-token-1")).send().await?;
        assert_eq!(response.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
        server.abort();
        api.shutdown().await?;
        let api = start(true).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let vault_url = format!("{base}/vaults/{}", vault["id"].as_str().context("vault id")?);

        for invalid in [
            json!({"name":"x","auth":{"type":"environment_variable","secret_name":"KEY","secret_value":"v","networking":{"type":"unrestricted"}}}),
            json!({"name":"x","auth":{"type":"mcp_oauth","access_token":"t","mcp_server_url":mcp_url,"refresh":{"client_id":"c","refresh_token":"r","token_endpoint":"https://example.com/token","token_endpoint_auth":{"type":"none"}}}}),
            json!({"name":"x","auth":{"type":"static_bearer","token":"","mcp_server_url":mcp_url}}),
            json!({"name":"x","auth":{"type":"static_bearer","token":"t","mcp_server_url":"http://93.184.216.34/mcp"}}),
            json!({"name":"","auth":{"type":"static_bearer","token":"t","mcp_server_url":mcp_url}}),
        ] {
            let response = client.post(format!("{vault_url}/credentials")).bearer_auth(TOKEN).json(&invalid).send().await?;
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST, "{invalid}");
        }
        let credential = request(&client, reqwest::Method::POST, &format!("{vault_url}/credentials"), bearer("secret-token-1")).await?;
        let credential_url = format!("{vault_url}/credentials/{}", credential["id"].as_str().context("credential id")?);
        assert_eq!(credential["auth"], json!({"type":"static_bearer","mcp_server_url":mcp_url}));
        responses.push(credential.clone());
        responses.push(request(&client, reqwest::Method::GET, &credential_url, Value::Null).await?);
        responses.push(request(&client, reqwest::Method::GET, &format!("{vault_url}/credentials"), Value::Null).await?);

        let sessions = format!("{base}/agents/sessions");
        let warehouse = json!([{"type":"mcp","server_label":"warehouse","transport":{"type":"http","server_url":mcp_url},"allowed_tools":["lookup"]}]);
        let create = |input: &str| json!({"agent":{"model":"mock-model","tools":warehouse},"environment":{"type":"none"},"vault_ids":[vault["id"]],"input":input});
        let auths = || calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();

        // A session snapshots the credential and presents it to the server.
        let first = request(&client, reqwest::Method::POST, &sessions, create("call-mcp first")).await?;
        let first_url = format!("{sessions}/{}", first["id"].as_str().context("first id")?);
        assert_eq!(idle(&client, &first_url, /*expected_turns*/ 1).await?["vault_ids"], json!([vault["id"]]));
        assert_eq!(auths(), vec![Some("Bearer secret-token-1".to_owned())]);

        // Rotation reaches new sessions only; deletion leaves running ones alone.
        let rotated = request(&client, reqwest::Method::POST, &credential_url, json!({"auth":{"type":"static_bearer","token":"secret-token-2"}})).await?;
        assert_eq!(rotated["id"], credential["id"]);
        responses.push(rotated);
        let second = request(&client, reqwest::Method::POST, &sessions, create("call-mcp second")).await?;
        let second_url = format!("{sessions}/{}", second["id"].as_str().context("second id")?);
        idle(&client, &second_url, /*expected_turns*/ 1).await?;
        request(&client, reqwest::Method::DELETE, &credential_url, Value::Null).await?;
        let follow_up = |url: String, text: &'static str| {
            let client = client.clone();
            async move {
                let response = client.post(format!("{url}/events")).bearer_auth(TOKEN)
                    .json(&json!({"events":[{"type":"agent.session.input.message","input":text}]})).send().await?;
                anyhow::ensure!(response.status() == reqwest::StatusCode::ACCEPTED, "follow-up rejected");
                Ok::<_, anyhow::Error>(())
            }
        };
        follow_up(first_url.clone(), "call-mcp again").await?;
        idle(&client, &first_url, /*expected_turns*/ 2).await?;
        let third = request(&client, reqwest::Method::POST, &sessions, create("call-mcp third")).await?;
        idle(&client, &format!("{sessions}/{}", third["id"].as_str().context("third id")?), /*expected_turns*/ 1).await?;
        assert_eq!(auths(), vec![
            Some("Bearer secret-token-1".to_owned()),
            Some("Bearer secret-token-2".to_owned()),
            Some("Bearer secret-token-1".to_owned()),
            None,
        ]);

        // Credential choice is validated before a session exists.
        let a = request(&client, reqwest::Method::POST, &format!("{vault_url}/credentials"), bearer("secret-token-3")).await?;
        request(&client, reqwest::Method::POST, &format!("{vault_url}/credentials"), bearer("secret-token-4")).await?;
        let ambiguous = client.post(&sessions).bearer_auth(TOKEN).json(&create("call-mcp ambiguous")).send().await?;
        assert_eq!(ambiguous.status(), reqwest::StatusCode::BAD_REQUEST);
        let mut foreign = create("call-mcp foreign");
        foreign["agent"]["tools"][0]["credential_id"] = json!("cred_missing");
        assert_eq!(client.post(&sessions).bearer_auth(TOKEN).json(&foreign).send().await?.status(), reqwest::StatusCode::BAD_REQUEST);
        let mut explicit = create("call-mcp explicit");
        explicit["agent"]["tools"][0]["credential_id"] = a["id"].clone();
        request(&client, reqwest::Method::POST, &sessions, explicit).await?;

        // A different passphrase cannot read the stored secrets, so it is
        // refused at startup rather than failing each later request.
        server.abort();
        api.shutdown().await?;
        let api = start(false).await?;
        let refused = api.configure_vault("a-different-operator-passphrase-0123456789".into()).await.err().context("a different passphrase was accepted")?;
        assert_eq!(refused.to_string(), format!("the vault passphrase cannot read the secrets stored in {}", data_path.join("secrets").display()));
        api.shutdown().await?;

        // Snapshots survive a restart under the same passphrase.
        let api = start(true).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let second_url = format!("{base}/agents/sessions/{}", second["id"].as_str().context("second id")?);
        follow_up(second_url.clone(), "call-mcp after restart").await?;
        idle(&client, &second_url, /*expected_turns*/ 2).await?;
        assert_eq!(auths().last().cloned().flatten(), Some("Bearer secret-token-2".to_owned()));

        // No secret reaches responses, the database, worker files, or the model.
        let captures = provider.received_requests().await.context("captures")?;
        for token in ["secret-token-1", "secret-token-2", "secret-token-3", "secret-token-4"] {
            assert!(!responses.iter().any(|response| response.to_string().contains(token)), "{token} in a response");
            assert_eq!(files_containing(data.path(), token)?, Vec::<String>::new(), "{token} in the data directory");
            assert_eq!(files_containing(home.path(), token)?, Vec::<String>::new(), "{token} in the worker home");
            assert!(!captures.iter().any(|request| String::from_utf8_lossy(&request.body).contains(token)), "{token} sent to the model");
        }

        // Deleting the vault deletes its credentials.
        let vault_url = format!("{base}/vaults/{}", vault["id"].as_str().context("vault id")?);
        assert_eq!(request(&client, reqwest::Method::DELETE, &vault_url, Value::Null).await?, json!({"id":vault["id"],"object":"vault.deleted","deleted":true}));
        assert_eq!(client.get(&vault_url).bearer_auth(TOKEN).send().await?.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(client.get(format!("{vault_url}/credentials/{}", a["id"].as_str().unwrap_or_default())).bearer_auth(TOKEN).send().await?.status(), reqwest::StatusCode::NOT_FOUND);
        server.abort();
        api.shutdown().await?;
        mcp_task.abort();
        Ok::<_, anyhow::Error>(())
    }).await?
}
