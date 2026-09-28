use super::*;
use app_test_support::create_final_assistant_message_sse_response;
use codex_utils_cargo_bin::find_resource;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn saved_settings_reset_and_survive_agent_deletion_and_cold_resume() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        Mock::given(body_string_contains("slow-first"))
            .respond_with(ResponseTemplate::new(/*s*/ 200)
                .set_body_raw(create_final_assistant_message_sse_response("finished")?, "text/event-stream")
                .set_delay(Duration::from_secs(/*secs*/ 1)))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        let catalog = home.path().join("models.json");
        std::fs::copy(find_resource!("tests/fixtures/model_catalog.json")?, &catalog)?;
        MockResponsesConfig::new(&provider.uri()).with_root_config(&format!(
            "features.plugins = false\nmodel_catalog_json = {}\nmodel_reasoning_effort = \"xhigh\"\nmodel_reasoning_summary = \"concise\"\nmodel_verbosity = \"low\"\nservice_tier = \"flex\"", serde_json::to_string(&catalog)?
        )).write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("openai-beta", "agents=v1".parse()?);
        let client = reqwest::Client::builder().default_headers(headers).build()?;
        let schema = json!({"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false});
        let agent = request(&client, reqwest::Method::POST, &format!("{base}/agents"), json!({
            "model":"mock-model","name":"saved-name","instructions":"saved-instructions",
            "reasoning":{"effort":"high","summary":"detailed"},"service_tier":"flex",
            "text":{"format":{"type":"json_schema","schema":schema},"verbosity":"high"}
        })).await?;
        let agent_url = format!("{base}/agents/{}", agent["id"].as_str().context("agent id")?);
        let first = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({
            "agent_id":agent["id"],"environment":{"type":"none"},"input":"slow-first"
        })).await?;
        let reset = request(&client, reqwest::Method::POST, &agent_url, json!({
            "instructions":null,"reasoning":null,"service_tier":null,"text":null,"name":null
        })).await?;
        assert_eq!(reset["instructions"], Value::Null);
        let second = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({
            "agent_id":agent["id"],"agent":{"name":"inline-name"},"environment":{"type":"none"},"input":"reset-second"
        })).await?;
        let priority = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({
            "agent_id":agent["id"],"agent":{"name":"priority-name","service_tier":"fast"},"environment":{"type":"none"},"input":"priority-third"
        })).await?;
        let default_tier = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({
            "agent_id":agent["id"],"agent":{"name":"default-name","service_tier":"default"},"environment":{"type":"none"},"input":"default-fourth"
        })).await?;
        request(&client, reqwest::Method::DELETE, &agent_url, Value::Null).await?;
        let mut snapshots = Vec::new();
        for (session, name) in [(&first, "saved-name"), (&second, "inline-name"), (&priority, "priority-name"), (&default_tier, "default-name")] {
            let id = session["id"].as_str().context("session id")?;
            let saved = idle(&client, &format!("{base}/agents/sessions/{id}"), /*expected_turns*/ 1).await?;
            assert_eq!(saved["agent"]["name"], json!(name));
            assert_eq!(saved["agent"], session["agent"]);
            snapshots.push((id.to_owned(), saved["agent"].clone()));
        }
        server.abort();
        api.shutdown().await?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        for (id, snapshot) in snapshots {
            let url = format!("{base}/agents/sessions/{id}");
            let response = client.post(format!("{url}/events")).bearer_auth(TOKEN).json(&json!({"events":[{"type":"agent.session.input.message","input":"Continue after restart"}]})).send().await?;
            assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
            assert_eq!(idle(&client, &url, /*expected_turns*/ 2).await?["agent"], snapshot);
        }
        let captures = provider.received_requests().await.context("captures")?;
        let requests = captures.iter().filter(|r| r.url.path().ends_with("/responses")).map(|r| serde_json::from_slice::<Value>(&r.body)).collect::<Result<Vec<_>, _>>()?;
        assert_eq!(requests.len(), 8);
        for request in requests {
            let expected = if request["input"].to_string().contains("slow-first") {
                json!({"reasoning":{"effort":"high","summary":"detailed"},"tier":"flex",
                    "text":{"verbosity":"high","format":{"type":"json_schema","name":"codex_output_schema","strict":true,"schema":schema}}})
            } else if request["input"].to_string().contains("priority-third") {
                json!({"reasoning":{"effort":"medium"},"tier":"priority","text":{"verbosity":"medium"}})
            } else if request["input"].to_string().contains("default-fourth") {
                json!({"reasoning":{"effort":"medium"},"tier":"default","text":{"verbosity":"medium"}})
            } else {
                json!({"reasoning":{"effort":"medium"},"tier":null,"text":{"verbosity":"medium"}})
            };
            assert_eq!(json!({"reasoning":request["reasoning"],"tier":request["service_tier"],"text":request["text"]}), expected);
        }
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}
