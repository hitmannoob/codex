use super::*;
use pretty_assertions::assert_eq;

fn answer(id: &str, usage: Option<[i64; 4]>) -> String {
    let mut completed = json!({"type":"response.completed","response":{"id":id}});
    if let Some([input, cached, output, reasoning]) = usage {
        completed["response"]["usage"] = json!({"input_tokens":input,"input_tokens_details":{"cached_tokens":cached},
            "output_tokens":output,"output_tokens_details":{"reasoning_tokens":reasoning},"total_tokens":input + output});
    }
    sse(&[
        json!({"type":"response.created","response":{"id":id}}),
        json!({"type":"response.output_item.done","item":{"type":"message","role":"assistant","id":format!("msg_{id}"),"content":[{"type":"output_text","text":"answered"}]}}),
        completed,
    ])
}

fn usage([input, cached, output, reasoning]: [i64; 4]) -> Value {
    json!({"input_tokens":input,"input_tokens_details":{"cached_tokens":cached},
        "output_tokens":output,"output_tokens_details":{"reasoning_tokens":reasoning},"total_tokens":input + output})
}

#[tokio::test]
async fn usage_sums_each_response_once_across_turns_and_restart() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        // Each marker answers once; later requests also carry earlier markers.
        for (marker, reported) in [
            ("usage-first", Some([10, 4, 6, 2])),
            ("usage-second", Some([20, 5, 7, 3])),
            ("usage-third", Some([30, 6, 8, 4])),
            ("usage-missing", None),
        ] {
            Mock::given(body_string_contains(marker))
                .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(answer(marker, reported), "text/event-stream"))
                .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        }
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let session = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"),
            json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":"usage-first"})).await?;
        let id = session["id"].as_str().context("session id")?.to_owned();
        let send = |base: String, text: &'static str| {
            let client = client.clone();
            let id = id.clone();
            async move {
                let response = client.post(format!("{base}/agents/sessions/{id}/events")).bearer_auth(TOKEN)
                    .json(&json!({"events":[{"type":"agent.session.input.message","input":text}]})).send().await?;
                anyhow::ensure!(response.status() == reqwest::StatusCode::ACCEPTED, "input rejected");
                Ok::<_, anyhow::Error>(())
            }
        };
        let url = format!("{base}/agents/sessions/{id}");
        assert_eq!(idle(&client, &url, /*expected_turns*/ 1).await?["usage"], usage([10, 4, 6, 2]));
        send(base.clone(), "usage-second").await?;
        assert_eq!(idle(&client, &url, /*expected_turns*/ 2).await?["usage"], usage([30, 9, 13, 5]));
        server.abort();
        api.shutdown().await?;

        // A cold resume adds only the new response.
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let url = format!("{base}/agents/sessions/{id}");
        send(base.clone(), "usage-third").await?;
        assert_eq!(idle(&client, &url, /*expected_turns*/ 3).await?["usage"], usage([60, 15, 21, 9]));
        send(base.clone(), "usage-missing").await?;
        let session = idle(&client, &url, /*expected_turns*/ 4).await?;
        assert_eq!(session["usage"], usage([60, 15, 21, 9]));
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns?order=asc"), Value::Null).await?;
        let recorded = turns["data"].as_array().context("turns")?.iter().map(|turn| turn["usage"].clone()).collect::<Vec<_>>();
        assert_eq!(recorded, vec![usage([10, 4, 6, 2]), usage([20, 5, 7, 3]), usage([30, 6, 8, 4]), Value::Null]);
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}

#[tokio::test]
async fn reconnected_stream_merges_with_saved_items() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        Mock::given(body_string_contains("merge-call"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(capabilities::call("lookup", "merge-call"), "text/event-stream"))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let mut first = client.post(format!("{base}/agents/sessions")).bearer_auth(TOKEN).json(&json!({
            "agent":{"model":"mock-model","tools":[{"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object","properties":{}}}]},
            "environment":{"type":"none"},"input":"merge-call","stream":true,
        })).send().await?.error_for_status()?;
        let created = event(&mut first, "agent.session.created").await?;
        let url = format!("{base}/agents/sessions/{}", created["session"]["id"].as_str().context("session id")?);
        event(&mut first, "agent.session.requires_action").await?;
        drop(first);

        // Documented recovery: subscribe first, read saved state, then apply
        // live updates by item ID.
        let mut stream = client.get(format!("{url}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
        let saved = request(&client, reqwest::Method::GET, &format!("{url}/items?order=asc&limit=100"), Value::Null).await?;
        let mut merged: std::collections::BTreeMap<String, Value> = saved["data"].as_array().context("items")?.iter()
            .map(|item| (item["id"].as_str().unwrap_or_default().to_owned(), item.clone())).collect();
        let action = request(&client, reqwest::Method::GET, &url, Value::Null).await?["required_actions"][0].clone();
        let response = client.post(format!("{url}/events")).bearer_auth(TOKEN).json(&json!({"events":[{
            "type":"agent.session.input.tool_result","turn_id":action["turn_id"],"call_id":action["call_id"],"success":true,"output":"merged"}]})).send().await?;
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        for event in until_idle(&mut stream).await? {
            if matches!(event["type"].as_str(), Some("agent.session.turn.item.added" | "agent.session.turn.item.done")) {
                merged.insert(event["item"]["id"].as_str().unwrap_or_default().to_owned(), event["item"].clone());
            }
        }
        let latest = request(&client, reqwest::Method::GET, &format!("{url}/items?order=asc&limit=100"), Value::Null).await?;
        let latest: std::collections::BTreeMap<String, Value> = latest["data"].as_array().context("items")?.iter()
            .map(|item| (item["id"].as_str().unwrap_or_default().to_owned(), item.clone())).collect();
        assert_eq!(merged, latest);
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}

#[tokio::test]
async fn failed_turn_reports_its_error_and_closes_open_items() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        Mock::given(body_string_contains("fail-midway"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(sse(&[
                json!({"type":"response.created","response":{"id":"failing"}}),
                json!({"type":"response.output_item.added","item":{"type":"message","role":"assistant","id":"msg_partial","content":[]}}),
                json!({"type":"response.output_text.delta","delta":"partial"}),
                json!({"type":"response.failed","response":{"id":"failing","error":{"code":"context_length_exceeded","message":"context window exceeded"}}}),
            ]), "text/event-stream"))
            .with_priority(/*p*/ 1).mount(&provider).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let mut stream = client.post(format!("{base}/agents/sessions")).bearer_auth(TOKEN)
            .json(&json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":"fail-midway","stream":true}))
            .send().await?.error_for_status()?;
        let events = until_idle(&mut stream).await?;
        let id = events[0]["session"]["id"].as_str().context("session id")?;
        let url = format!("{base}/agents/sessions/{id}");

        // The streamed part of the message is closed as incomplete before the
        // turn fails, and the failure is reported with its documented code.
        let items = request(&client, reqwest::Method::GET, &format!("{url}/items?order=asc"), Value::Null).await?;
        let message = items["data"].as_array().context("items")?.iter()
            .find(|item| item["role"] == "assistant").context("assistant item")?.clone();
        assert_eq!(message["status"], "incomplete");
        let lifecycle = events.iter().filter(|event| {
            event["item_id"] == message["id"] || event["item"]["id"] == message["id"]
                || matches!(event["type"].as_str(), Some("error" | "agent.session.turn.failed" | "agent.session.idle"))
        }).map(|event| (event["type"].as_str().unwrap_or_default(), event["delta"].as_str().or(event["item"]["status"].as_str())))
            .collect::<Vec<_>>();
        assert_eq!(lifecycle, vec![
            ("agent.session.turn.item.added", Some("in_progress")),
            ("agent.session.turn.content_part.added", None),
            ("agent.session.turn.output_text.delta", Some("partial")),
            ("error", None),
            ("agent.session.turn.item.done", Some("incomplete")),
            ("agent.session.turn.failed", None),
            ("agent.session.idle", None),
        ]);
        let error = events.iter().find(|event| event["type"] == "error").context("error event")?;
        let turn = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?["data"][0].clone();
        assert_eq!(turn["error"]["code"], "context_length_exceeded");
        assert_eq!(error["error"], json!({"type":"error","code":"context_length_exceeded","message":turn["error"]["message"],"param":null}));
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}
