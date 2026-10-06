use super::*;
use pretty_assertions::assert_eq;

fn message(text: &str) -> Value {
    json!({"type":"agent.session.input.message","input":text})
}

async fn submit(
    client: &reqwest::Client,
    url: &str,
    key: &str,
    events: Value,
) -> anyhow::Result<reqwest::Response> {
    Ok(client
        .post(format!("{url}/events"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", key)
        .json(&json!({"events": events}))
        .send()
        .await?)
}

#[tokio::test]
async fn concurrent_first_inputs_share_one_thread() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        MockResponsesConfig::new(&provider.uri())
            .with_root_config("features.plugins = false")
            .write(home.path())?;
        let api = AgentsApi::new(
            backend(home.path()).await?,
            AbsolutePathBuf::from_absolute_path(data.path())?,
            TOKEN.into(),
        )
        .await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let agent = request(
            &client,
            reqwest::Method::POST,
            &format!("{base}/agents"),
            json!({"model":"mock-model"}),
        )
        .await?;
        let session = request(
            &client,
            reqwest::Method::POST,
            &format!("{base}/sessions"),
            json!({"agentId":agent["id"],"environment":{"type":"none"}}),
        )
        .await?;
        let url = format!(
            "{base}/sessions/{}",
            session["id"].as_str().context("session id")?
        );
        // Inputs racing to bootstrap an empty session are admitted one at a
        // time, so every turn they start or steer belongs to one Codex thread.
        let responses = futures::future::try_join_all((0..4).map(|n| {
            let (client, input_url) = (&client, format!("{url}/input"));
            async move {
                request(
                    client,
                    reqwest::Method::POST,
                    &input_url,
                    json!({"input":format!("concurrent-{n}")}),
                )
                .await
            }
        }))
        .await?;
        let mut expected = responses
            .iter()
            .map(|response| {
                response["turn"]["id"]
                    .as_str()
                    .map(str::to_owned)
                    .context("turn id")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        expected.sort();
        expected.dedup();
        loop {
            let turns = request(
                &client,
                reqwest::Method::GET,
                &format!("{url}/turns?limit=100"),
                Value::Null,
            )
            .await?;
            let data = turns["data"].as_array().context("turns")?;
            let completed = expected.iter().all(|id| {
                data.iter()
                    .any(|turn| turn["id"] == *id && turn["status"] == "completed")
            });
            if completed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        }
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn keyed_batches_run_once_and_interrupted_keys_report_unknown() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        Mock::given(body_string_contains("call-lookup"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(capabilities::call("lookup", "batch-call"), "text/event-stream"))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        // Stall only the request that continues after the tool result, so the
        // cancel always finds the turn running. Later turns carry the same
        // output in history, so match on the final input item.
        Mock::given(|request: &wiremock::Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
            body["input"].as_array().and_then(|items| items.last()).is_some_and(|item| item["type"] == "function_call_output")
        })
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_delay(Duration::from_secs(/*secs*/ 60)))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let session = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({
            "agent":{"model":"mock-model","tools":[{"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object","properties":{}}}]},
            "environment":{"type":"none"},"input":"call-lookup"
        })).await?;
        let id = session["id"].as_str().context("session id")?.to_owned();
        let url = format!("{base}/agents/sessions/{id}");
        let pending = loop {
            let session = request(&client, reqwest::Method::GET, &url, Value::Null).await?;
            if session["status"] == "requires_action" {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        };
        let action = &pending["required_actions"][0];

        // Events run in order: the result resumes the waiting turn, then the
        // cancel ends the turn it resumed.
        let batch = json!([
            {"type":"agent.session.input.tool_result","turn_id":action["turn_id"],"call_id":action["call_id"],"success":true,"output":"batched-result"},
            {"type":"agent.session.input.cancel"},
        ]);
        assert_eq!(submit(&client, &url, "batch", batch).await?.status(), reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 1).await?;
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(turns["data"][0]["status"], "cancelled");
        let items = request(&client, reqwest::Method::GET, &format!("{url}/items"), Value::Null).await?;
        assert!(items["data"].as_array().context("items")?.iter()
            .any(|item| item["type"] == "function_call_output" && item["output"] == "batched-result"), "{items}");

        // An identical keyed retry replays acceptance without a second turn;
        // reusing the key for different input is rejected.
        assert_eq!(submit(&client, &url, "message", json!([message("keyed-once")])).await?.status(), reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 2).await?;
        assert_eq!(submit(&client, &url, "message", json!([message("keyed-once")])).await?.status(), reqwest::StatusCode::ACCEPTED);
        assert_eq!(submit(&client, &url, "message", json!([message("keyed-twice")])).await?.status(), reqwest::StatusCode::BAD_REQUEST);
        tokio::time::sleep(Duration::from_millis(/*millis*/ 300)).await;
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(turns["data"].as_array().context("turns")?.len(), 2);
        let captures = provider.received_requests().await.context("captures")?;
        assert_eq!(captures.iter().filter(|r| String::from_utf8_lossy(&r.body).contains("keyed-once")).count(), 1);
        server.abort();
        api.shutdown().await?;

        // Leave the state an API crash would while a keyed request executed:
        // its outcome is unknown, so the key never runs it again.
        let pool = codex_state::SqliteConfig::from_sqlite_home(AbsolutePathBuf::from_absolute_path(data.path())?)
            .open_read_write_pool(data.path().join("agents-api.sqlite").as_path()).await?;
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
        sqlx::query("INSERT INTO input_requests (session_id, key, request, state, created_at) VALUES (?, 'interrupted', ?, 'pending', ?)")
            .bind(&id).bind(json!({"events":[message("maybe-ran")]}).to_string()).bind(i64::try_from(now)?)
            .execute(&pool).await?;
        pool.close().await;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let url = format!("{base}/agents/sessions/{id}");
        let unknown = submit(&client, &url, "interrupted", json!([message("maybe-ran")])).await?;
        assert_eq!(unknown.status(), reqwest::StatusCode::CONFLICT);
        assert_eq!(unknown.json::<Value>().await?, json!({"error":{
            "message":"the outcome of the request with this Idempotency-Key is unknown; read the session and submit again with a new key",
            "type":"invalid_request_error","param":null,"code":null}}));
        assert_eq!(submit(&client, &url, "interrupted", json!([message("other")])).await?.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(submit(&client, &url, "fresh", json!([message("maybe-ran")])).await?.status(), reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 3).await?;
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}

#[tokio::test]
async fn cancel_right_after_creation_stops_the_first_turn() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 45), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        // The first turn's model response takes a minute.
        Mock::given(body_string_contains("cancel-me-at-once"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_delay(Duration::from_secs(/*secs*/ 60)))
            .with_priority(/*p*/ 1)
            .mount(&provider)
            .await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        // The session reports in_progress before Codex's turn is recorded; a
        // cancel then must still stop that turn.
        let session = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"),
            json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":"cancel-me-at-once"})).await?;
        assert_eq!(session["status"], "in_progress");
        let url = format!("{base}/agents/sessions/{}", session["id"].as_str().context("session id")?);
        let response = client.post(format!("{url}/events")).bearer_auth(TOKEN)
            .json(&json!({"events":[{"type":"agent.session.input.cancel"}]})).send().await?;
        let status = response.status();
        assert_eq!(status, reqwest::StatusCode::ACCEPTED, "{}", response.text().await?);
        idle(&client, &url, /*expected_turns*/ 1).await?;
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(turns["data"][0]["status"], "cancelled");
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
