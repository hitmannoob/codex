use super::*;
use app_test_support::MockResponsesConfig;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_agents_api::AgentsApi;
use codex_app_server_client::AppServerClient;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

const TOKEN: &str = "test-token-for-agents-api-worker-loss";

async fn request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    body: Value,
) -> anyhow::Result<Value> {
    let response = client
        .request(method, url)
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let body = response.bytes().await?;
    anyhow::ensure!(
        status.is_success(),
        "{url}: {status}: {}",
        String::from_utf8_lossy(&body)
    );
    if body.is_empty() {
        return Ok(Value::Null);
    }
    Ok(serde_json::from_slice(&body)?)
}

async fn completed_turns(
    client: &reqwest::Client,
    url: &str,
    count: usize,
) -> anyhow::Result<Value> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let turns = request(
                client,
                reqwest::Method::GET,
                &format!("{url}/turns"),
                Value::Null,
            )
            .await?;
            let data = turns["data"].as_array().context("turns")?;
            if data.len() == count && data.iter().all(|turn| turn["status"] == "completed") {
                return anyhow::Ok(turns);
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        }
    })
    .await?
}

async fn message(client: &reqwest::Client, url: &str, input: &str) -> anyhow::Result<Value> {
    request(
        client,
        reqwest::Method::POST,
        &format!("{url}/events"),
        json!({"events":[{"type":"agent.session.input.message","input":input}]}),
    )
    .await
}

#[tokio::test]
async fn store_reads_survive_worker_loss_and_reconnect_restores_service() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let model = create_mock_responses_server_repeating_assistant("finished").await;
        MockResponsesConfig::new(&model.uri())
            .with_root_config("features.plugins = false")
            .write(home.path())?;
        let first = worker(home.path()).await?;
        let pid = i32::try_from(first.id().context("worker PID")?)?;
        let client1 = runtime::connect(first.socket().clone(), DEADLINE).await?;
        let api = AgentsApi::new(
            AppServerClient::Remote(client1),
            AbsolutePathBuf::from_absolute_path(data.path())?,
            TOKEN.into(),
        )
        .await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}/v1", listener.local_addr()?);
        let router = api.router();
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let client = reqwest::Client::new();
        let session = request(
            &client,
            reqwest::Method::POST,
            &format!("{base}/agents/sessions"),
            json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":"Remember orange-731"}),
        )
        .await?;
        let url = format!(
            "{base}/agents/sessions/{}",
            session["id"].as_str().context("session id")?
        );
        let saved = completed_turns(&client, &url, /*count*/ 1).await?;
        // Kill the worker out from under the API.
        // SAFETY: this PID belongs to the live worker spawned above.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
        // Mutations report the documented recovery error once the loss is observed.
        tokio::time::timeout(DEADLINE, async {
            loop {
                let response = client
                    .post(format!("{url}/events"))
                    .bearer_auth(TOKEN)
                    .json(&json!({"events":[{"type":"agent.session.input.message","input":"Still there?"}]}))
                    .send()
                    .await?;
                if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                    let error: Value = response.json().await?;
                    assert_eq!(error["error"]["message"], json!("app-server disconnected"));
                    return anyhow::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
            }
        })
        .await??;
        // Durable retrieval keeps serving; completed history is unchanged.
        let after_loss = request(
            &client,
            reqwest::Method::GET,
            &format!("{url}/turns"),
            Value::Null,
        )
        .await?;
        assert_eq!(after_loss, saved);
        request(&client, reqwest::Method::GET, &url, Value::Null).await?;
        // Attach a replacement worker; the saved session continues with context.
        first.shutdown().await?;
        let second = worker(home.path()).await?;
        let client2 = runtime::connect(second.socket().clone(), DEADLINE).await?;
        api.reconnect(AppServerClient::Remote(client2)).await?;
        message(&client, &url, "What code did I ask you to remember?").await?;
        completed_turns(&client, &url, /*count*/ 2).await?;
        let requests = model.received_requests().await.context("model requests")?;
        let last: Value = serde_json::from_slice(&requests.last().context("last request")?.body)?;
        assert!(last["input"].to_string().contains("orange-731"));
        server.abort();
        api.shutdown().await?;
        second.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
