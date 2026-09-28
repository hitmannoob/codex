use super::*;
use pretty_assertions::assert_eq;

fn last_input(request: &wiremock::Request) -> String {
    let body: Value = serde_json::from_slice(&request.body).unwrap_or_default();
    body["input"]
        .as_array()
        .and_then(|items| items.last())
        .map(Value::to_string)
        .unwrap_or_default()
}

fn tools(request: &Value) -> Vec<String> {
    request["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|tool| {
            tool["name"]
                .as_str()
                .or(tool["type"].as_str())
                .unwrap_or_default()
                .to_owned()
        })
        .collect()
}

#[tokio::test]
async fn subagents_are_registered_attributed_and_kept_apart() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        // The root agent spawns one subagent; the subagent answers its task.
        Mock::given(|request: &wiremock::Request| last_input(request).contains("delegate-work"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(sse(&[
                json!({"type":"response.created","response":{"id":"spawn"}}),
                json!({"type":"response.output_item.done","item":{"type":"function_call","call_id":"spawn-call","namespace":"collaboration","name":"spawn_agent",
                    "arguments":json!({"message":"child-task","task_name":"researcher","fork_turns":"none"}).to_string()}}),
                json!({"type":"response.completed","response":{"id":"spawn","status":"completed","output":[]}}),
            ]), "text/event-stream"))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        Mock::given(|request: &wiremock::Request| last_input(request).contains("NEW_TASK"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(sse(&[
                json!({"type":"response.created","response":{"id":"child"}}),
                json!({"type":"response.output_item.done","item":{"type":"message","role":"assistant","id":"msg_child","content":[{"type":"output_text","text":"child-report"}]}}),
                json!({"type":"response.completed","response":{"id":"child","usage":{"input_tokens":20,"input_tokens_details":{"cached_tokens":0},
                    "output_tokens":5,"output_tokens_details":{"reasoning_tokens":1},"total_tokens":25}}}),
            ]), "text/event-stream"))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let mut stream = client.post(format!("{base}/agents/sessions")).bearer_auth(TOKEN).json(&json!({
            "agent":{"model":"mock-model","multi_agent":{"enabled":true,"max_concurrent_subagents":2},
                "tools":[{"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object","properties":{}}}]},
            "environment":{"type":"none"},"input":"delegate-work","stream":true,
        })).send().await?.error_for_status()?;

        // Read until both the root turn has idled and the subagent's turn ended.
        let mut buffer = String::new();
        let mut events: Vec<Value> = Vec::new();
        while !(events.iter().any(|e| e["type"] == "agent.session.idle")
            && events.iter().any(|e| e["type"] == "agent.session.turn.completed" && e["turn"]["subagent_id"].is_string()))
        {
            match buffer.find("\n\n") {
                Some(end) => {
                    let block: String = buffer.drain(..end + 2).collect();
                    if let Some(data) = block.lines().find_map(|line| line.strip_prefix("data: ")) {
                        events.push(serde_json::from_str(data)?);
                    }
                }
                None => buffer.push_str(&String::from_utf8_lossy(&stream.chunk().await?.context("stream ended")?)),
            }
        }
        let session = events[0]["session"].clone();
        let url = format!("{base}/agents/sessions/{}", session["id"].as_str().context("session id")?);

        // The subagent is registered once, owned by the root agent.
        let listed = request(&client, reqwest::Method::GET, &format!("{url}/subagents"), Value::Null).await?;
        let subagent = listed["data"][0].clone();
        let sub_id = subagent["id"].as_str().context("subagent id")?.to_owned();
        assert_eq!(listed["data"].as_array().map(Vec::len), Some(1));
        assert_eq!(request(&client, reqwest::Method::GET, &format!("{url}/subagents/{sub_id}"), Value::Null).await?, subagent);
        let mut expected = subagent.clone();
        for (field, value) in [("object", json!("agent.session.subagent")), ("session_id", session["id"].clone()),
            ("parent_agent_id", session["agent"]["id"].clone()), ("name", json!("researcher")),
            ("instructions", Value::Null), ("closed_at", Value::Null), ("status", json!("active"))] {
            expected[field] = value;
        }
        assert_eq!(subagent, expected);
        let created = events.iter().filter(|e| e["type"] == "agent.session.subagent.created").collect::<Vec<_>>();
        assert_eq!(created.iter().map(|e| e["subagent"].clone()).collect::<Vec<_>>(), vec![subagent.clone()]);

        // Root history holds only the spawn; subagent work stays under the subagent.
        let root_items = request(&client, reqwest::Method::GET, &format!("{url}/items?order=asc"), Value::Null).await?;
        let root_items = root_items["data"].as_array().context("items")?;
        let spawn = root_items.iter().find(|item| item["type"] == "create_subagent_call").context("spawn item")?;
        assert_eq!(json!({"agent_id":spawn["agent_id"],"content":spawn["content"],"model":spawn["model"],"reasoning_effort":spawn["reasoning_effort"],"status":spawn["status"]}),
            json!({"agent_id":sub_id,"content":[],"model":null,"reasoning_effort":null,"status":"completed"}));
        assert!(!root_items.iter().any(|item| item.to_string().contains("child-report")), "{root_items:?}");
        let root_turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(root_turns["data"].as_array().map(Vec::len), Some(1));
        assert_eq!(root_turns["data"][0]["subagent_id"], Value::Null);
        let sub_turns = request(&client, reqwest::Method::GET, &format!("{url}/subagents/{sub_id}/turns"), Value::Null).await?;
        let sub_turn = sub_turns["data"][0].clone();
        let child_usage = json!({"input_tokens":20,"input_tokens_details":{"cached_tokens":0},"output_tokens":5,"output_tokens_details":{"reasoning_tokens":1},"total_tokens":25});
        assert_eq!(json!({"count":sub_turns["data"].as_array().map(Vec::len),"status":sub_turn["status"],"subagent_id":sub_turn["subagent_id"],"agent_id":sub_turn["agent_id"],"usage":sub_turn["usage"]}),
            json!({"count":1,"status":"completed","subagent_id":sub_id,"agent_id":sub_id,"usage":child_usage}));
        let sub_turn_id = sub_turn["id"].as_str().context("subagent turn id")?;
        assert_eq!(request(&client, reqwest::Method::GET, &format!("{url}/subagents/{sub_id}/turns/{sub_turn_id}"), Value::Null).await?, sub_turn);
        let turn_items = request(&client, reqwest::Method::GET, &format!("{url}/subagents/{sub_id}/turns/{sub_turn_id}/items"), Value::Null).await?;
        let sub_items = request(&client, reqwest::Method::GET, &format!("{url}/subagents/{sub_id}/items"), Value::Null).await?;
        assert_eq!(turn_items["data"], sub_items["data"]);
        assert!(sub_items["data"].to_string().contains("child-report"), "{sub_items}");
        // Subagent turns stream on the session, tagged; only root turns idle it.
        let completions = events.iter().filter(|e| e["type"] == "agent.session.turn.completed")
            .map(|e| e["turn"]["subagent_id"].clone()).collect::<Vec<_>>();
        assert_eq!(completions.iter().filter(|id| **id == json!(sub_id)).count(), 1);
        assert_eq!(completions.iter().filter(|id| id.is_null()).count(), 1);
        // Session usage includes the subagent's.
        assert_eq!(request(&client, reqwest::Method::GET, &url, Value::Null).await?["usage"], child_usage);

        // Subagents get no function tools, and Codex goal tools stay off.
        let captures = provider.received_requests().await.context("captures")?;
        let bodies = captures.iter().filter_map(|r| serde_json::from_slice::<Value>(&r.body).ok()).collect::<Vec<_>>();
        let root = bodies.iter().find(|body| body.to_string().contains("delegate-work") && !body.to_string().contains("NEW_TASK")).context("root request")?;
        let child = bodies.iter().find(|body| body.to_string().contains("NEW_TASK")).context("child request")?;
        assert!(tools(root).contains(&"lookup".to_owned()) && tools(root).contains(&"collaboration".to_owned()), "{:?}", tools(root));
        assert!(!tools(child).contains(&"lookup".to_owned()), "{:?}", tools(child));
        assert!(!bodies.iter().any(|body| tools(body).iter().any(|name| name.contains("goal"))));

        // Unknown or mismatched IDs are not found; deletion removes the subagent.
        for path in ["subagents/missing", &format!("subagents/{sub_id}/turns/{}", root_turns["data"][0]["id"].as_str().unwrap_or_default())] {
            assert_eq!(client.get(format!("{url}/{path}")).bearer_auth(TOKEN).send().await?.status(), reqwest::StatusCode::NOT_FOUND, "{path}");
        }
        request(&client, reqwest::Method::DELETE, &url, Value::Null).await?;
        assert_eq!(client.get(format!("{url}/subagents")).bearer_auth(TOKEN).send().await?.status(), reqwest::StatusCode::NOT_FOUND);

        // A session whose subagent is still working cannot be deleted, even
        // though its own turn is over.
        let stalled = Mock::given(|request: &wiremock::Request| last_input(request).contains("NEW_TASK"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_delay(Duration::from_secs(/*secs*/ 30)))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).expect(/*r*/ 1).mount_as_scoped(&provider).await;
        Mock::given(|request: &wiremock::Request| last_input(request).contains("delegate-again"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(sse(&[
                json!({"type":"response.created","response":{"id":"spawn-again"}}),
                json!({"type":"response.output_item.done","item":{"type":"function_call","call_id":"spawn-again","namespace":"collaboration","name":"spawn_agent",
                    "arguments":json!({"message":"slow-task","task_name":"slow","fork_turns":"none"}).to_string()}}),
                json!({"type":"response.completed","response":{"id":"spawn-again","status":"completed","output":[]}}),
            ]), "text/event-stream"))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        let busy = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({
            "agent":{"model":"mock-model","multi_agent":{"enabled":true}},"environment":{"type":"none"},"input":"delegate-again",
        })).await?;
        let busy_url = format!("{base}/agents/sessions/{}", busy["id"].as_str().context("busy id")?);
        idle(&client, &busy_url, /*expected_turns*/ 1).await?;
        stalled.wait_until_satisfied().await;
        assert_eq!(client.delete(&busy_url).bearer_auth(TOKEN).send().await?.status(), reqwest::StatusCode::CONFLICT);
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}
