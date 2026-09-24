use super::*;
use app_test_support::MockResponsesConfig;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_agents_api::AgentsApi;
use codex_app_server_client::AppServerClient;
use codex_state::SqliteConfig;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

const TOKEN: &str = "test-token-for-agents-api-reconcile";

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
    Ok(serde_json::from_slice(&body)?)
}

async fn completed_turn_id(client: &reqwest::Client, url: &str) -> anyhow::Result<String> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let turns = request(
                client,
                reqwest::Method::GET,
                &format!("{url}/turns"),
                Value::Null,
            )
            .await?;
            if let Some(turn) = turns["data"].as_array().and_then(|data| data.first())
                && turn["status"] == "completed"
            {
                return anyhow::Ok(turn["id"].as_str().context("turn id")?.to_owned());
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn reconcile_recovers_completed_turn_lost_to_disconnect() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let model = create_mock_responses_server_repeating_assistant("finished").await;
        MockResponsesConfig::new(&model.uri())
            .with_root_config("features.plugins = false")
            .write(home.path())?;
        let first = worker(home.path()).await?;
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
        let http = reqwest::Client::new();
        let session = request(
            &http,
            reqwest::Method::POST,
            &format!("{base}/agents/sessions"),
            json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":"Remember orange-731"}),
        )
        .await?;
        let session_id = session["id"].as_str().context("session id")?.to_owned();
        let url = format!("{base}/agents/sessions/{session_id}");
        // The turn completes on the worker, so the rollout records it completed.
        let turn_id = completed_turn_id(&http, &url).await?;

        // Simulate a lost completion notification handled by a disconnect:
        // mark the already-completed public turn provisionally failed exactly as
        // records::disconnected would, writing through the approved sqlite shim.
        let pool = SqliteConfig::from_sqlite_home(AbsolutePathBuf::from_absolute_path(data.path())?)
            .open_read_write_pool(data.path().join("agents-api.sqlite").as_path())
            .await?;
        sqlx::query("UPDATE public_records SET data = json_set(data, '$.status', 'failed', '$.error', json(?)) WHERE session_id = ? AND kind = 'turn' AND id = ?")
            .bind(json!({"code":"connection_failed","message":"backend connection lost"}).to_string())
            .bind(&session_id)
            .bind(&turn_id)
            .execute(&pool)
            .await?;
        sqlx::query("UPDATE public_sessions SET data = json_set(data, '$.status', 'failed', '$.error', 'backend connection lost') WHERE id = ?")
            .bind(&session_id)
            .execute(&pool)
            .await?;
        pool.close().await;
        let provisional = request(&http, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(provisional["data"][0]["status"], json!("failed"));

        // Reconnect a replacement worker on the same home; reconciliation runs
        // and corrects the turn against the authoritative rollout history.
        first.shutdown().await?;
        let second = worker(home.path()).await?;
        let client2 = runtime::connect(second.socket().clone(), DEADLINE).await?;
        api.reconnect(AppServerClient::Remote(client2)).await?;

        let recovered = request(&http, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(recovered["data"][0]["id"], json!(turn_id));
        assert_eq!(recovered["data"][0]["status"], json!("completed"));
        assert_eq!(recovered["data"][0]["error"], Value::Null);
        let session_after = request(&http, reqwest::Method::GET, &url, Value::Null).await?;
        assert_eq!(session_after["status"], json!("idle"));

        server.abort();
        api.shutdown().await?;
        second.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
