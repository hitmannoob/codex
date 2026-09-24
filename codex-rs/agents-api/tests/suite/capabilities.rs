use super::*;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use axum::response::IntoResponse;
use pretty_assertions::assert_eq;

pub(super) fn call(name: &str, id: &str) -> String {
    let (namespace, name) = name
        .split_once('.')
        .map(|(namespace, name)| (Some(namespace), name))
        .unwrap_or((None, name));
    [
        json!({"type": "response.created", "response": {"id": id}}),
        json!({"type": "response.output_item.done", "item": {"type": "function_call", "call_id": id, "namespace": namespace, "name": name, "arguments": "{}"}}),
        json!({"type": "response.completed", "response": {"id": id, "status": "completed", "output": []}}),
    ].iter().map(|event| format!("event: {}\ndata: {event}\n\n", event["type"].as_str().unwrap_or_default())).collect()
}

pub(super) async fn pending(client: &reqwest::Client, url: &str) -> anyhow::Result<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(/*secs*/ 10);
    loop {
        let session = request(client, reqwest::Method::GET, url, Value::Null).await?;
        if !session["requiredActions"]
            .as_array()
            .context("actions")?
            .is_empty()
        {
            return Ok(session);
        }
        if tokio::time::Instant::now() >= deadline {
            let turns = request(
                client,
                reqwest::Method::GET,
                &format!("{url}/turns"),
                Value::Null,
            )
            .await?;
            anyhow::bail!("no pending call; session={session}; turns={turns}");
        }
        tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
    }
}

pub(super) async fn serve(
    api: &AgentsApi,
) -> anyhow::Result<(String, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/v1", listener.local_addr()?);
    let router = api.router();
    Ok((
        url,
        tokio::spawn(async move { axum::serve(listener, router).await }),
    ))
}

#[tokio::test]
async fn isolated_capabilities_and_function_results_survive_session_resume() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let final_message = create_final_assistant_message_sse_response("Done")?;
        let model = create_mock_responses_server_sequence_unchecked(vec![
            call("lookup_order", "a-call"), call("get_invoice", "b-call"),
            call("mcp__warehouse.lookup", "mcp-call"), final_message.clone(), final_message.clone(),
            call("lookup_order", "resumed-call"), final_message,
            call("lookup_order", "cancelled-call"), call("lookup_order", "lost-call"),
        ]).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mcp_url = format!("http://{}/mcp", listener.local_addr()?);
        let mcp = axum::Router::new().route("/mcp", axum::routing::post(|axum::Json(message): axum::Json<Value>| async move {
            let result = match message["method"].as_str() {
                Some("initialize") => json!({"protocolVersion": "2025-06-18", "capabilities": {"tools": {}}, "serverInfo": {"name": "fixture", "version": "1"}}),
                Some("notifications/initialized") => return axum::http::StatusCode::ACCEPTED.into_response(),
                Some("tools/list") => json!({"tools": [
                    {"name": "lookup", "description": "Allowed lookup", "inputSchema": {"type": "object", "properties": {}}, "annotations": {"readOnlyHint": true}},
                    {"name": "secret", "description": "Forbidden lookup", "inputSchema": {"type": "object", "properties": {}}}
                ]}),
                Some("tools/call") => json!({"content": [{"type": "text", "text": "mcp-result-731"}]}),
                _ => json!({}),
            };
            axum::Json(json!({"jsonrpc": "2.0", "id": message["id"], "result": result})).into_response()
        }));
        let mcp_task = tokio::spawn(async move { axum::serve(listener, mcp).await });
        MockResponsesConfig::new(&model.uri()).with_root_config("features.plugins = false").with_extra_config(&format!(
            "[mcp_servers.warehouse]\nurl = {mcp_url:?}\nstartup_timeout_sec = 2\ntool_timeout_sec = 2\ndefault_tools_approval_mode = \"approve\"\n[mcp_servers.billing]\nurl = {mcp_url:?}\nstartup_timeout_sec = 2\ntool_timeout_sec = 2\ndefault_tools_approval_mode = \"approve\"\n[mcp_servers.unselected]\nurl = {mcp_url:?}\nstartup_timeout_sec = 2\ntool_timeout_sec = 2\ndefault_tools_approval_mode = \"approve\"\n"
        )).write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = serve(&api).await?;
        let client = reqwest::Client::new();
        for invalid in [
            json!({"tools": [{"name": "bad name", "description": "x", "parameters": {"type": "object"}}]}),
            json!({"tools": [{"name": "x", "description": "x", "parameters": {"type": "array"}}]}),
            json!({"reasoning": {"effort": "unsupported"}}),
            json!({"mcpServers": [{"server": "warehouse", "allowedTools": ["lookup", "lookup"]}]}),
        ] {
            let mut body = json!({"model": "mock-model", "instructions": "x"});
            body.as_object_mut().unwrap().extend(invalid.as_object().unwrap().clone());
            assert_eq!(client.post(format!("{base}/agents")).bearer_auth(TOKEN).json(&body).send().await?.status(), reqwest::StatusCode::BAD_REQUEST);
        }
        let mut sessions = Vec::new();
        for (function, mcp_server) in [("lookup_order", "warehouse"), ("get_invoice", "billing")] {
            let agent = request(&client, reqwest::Method::POST, &format!("{base}/agents"), json!({
                "model": "mock-model", "instructions": "Use tools.", "reasoning": {"effort": "low"},
                "tools": [{"name": function, "description": "Lookup", "parameters": {"type": "object", "properties": {}}}],
                "mcpServers": [{"server": mcp_server, "allowedTools": ["lookup"]}]
            })).await?;
            sessions.push(request(&client, reqwest::Method::POST, &format!("{base}/sessions"), json!({"agentId": agent["id"], "environment": {"type": "none"}})).await?);
        }
        let first_id = sessions[0]["id"].as_str().context("first id")?;
        let first = format!("{base}/sessions/{first_id}");
        let second = format!("{base}/sessions/{}", sessions[1]["id"].as_str().context("second id")?);
        let mut runs = Vec::new();
        for (url, call_id, success) in [(&first, "a-call", true), (&second, "b-call", false)] {
            let mut events = client.get(format!("{url}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
            let started = request(&client, reqwest::Method::POST, &format!("{url}/input"), json!({"input": "Run lookup"})).await?;
            let turn_id = started["turn"]["id"].as_str().context("turn")?;
            let waiting = pending(&client, url).await?;
            let function = if success { "lookup_order" } else { "get_invoice" };
            assert_eq!(waiting["requiredActions"], json!([{"turnId": turn_id, "callId": call_id, "name": function, "arguments": {}}]));
            if success {
                let action_event = event(&mut events, "session.requires_action").await?;
                assert_eq!(action_event["params"]["action"], waiting["requiredActions"][0]);
            }
            runs.push((url, call_id, success, started, events));
        }
        // Both sessions now have outstanding calls. Results must remain isolated.
        for (url, call_id, success, started, mut events) in runs {
            let turn_id = started["turn"]["id"].as_str().context("turn")?;
            let result = json!({"callId": call_id, "success": success, "output": {"message": "application-result-731"}});
            assert_eq!(client.post(format!("{url}/turns/wrong-turn/tool-results")).bearer_auth(TOKEN).json(&result).send().await?.status(), reqwest::StatusCode::NOT_FOUND);
            let wrong = if success { &second } else { &first };
            assert_eq!(client.post(format!("{wrong}/turns/{turn_id}/tool-results")).bearer_auth(TOKEN).json(&result).send().await?.status(), reqwest::StatusCode::NOT_FOUND);
            let result_url = format!("{url}/turns/{turn_id}/tool-results");
            let receipt = request(&client, reqwest::Method::POST, &result_url, result.clone()).await?;
            assert_eq!(receipt, json!({"turnId": turn_id, "callId": call_id, "status": "submitted"}));
            assert_eq!(request(&client, reqwest::Method::POST, &result_url, result).await?, receipt);
            assert_eq!(client.post(&result_url).bearer_auth(TOKEN).json(&json!({"callId": call_id, "success": true, "output": "different"})).send().await?.status(), reqwest::StatusCode::CONFLICT);
            let outcome = completed(&mut events).await?;
            assert_eq!(outcome["params"]["turn"]["id"], json!(turn_id));
            assert_eq!(outcome["params"]["turn"]["status"], json!("completed"));
            assert_eq!(request(&client, reqwest::Method::GET, url, Value::Null).await?["requiredActions"], json!([]));
        }
        let requests = model.received_requests().await.context("captures")?;
        let requests: Vec<Value> = requests.iter().filter(|r| r.url.path().ends_with("/responses")).map(|r| serde_json::from_slice(&r.body)).collect::<Result<_, _>>()?;
        assert_eq!(requests.len(), 5);
        for (index, own, other, function, absent) in [(0, "warehouse", "billing", "lookup_order", "get_invoice"), (1, "billing", "warehouse", "get_invoice", "lookup_order")] {
            let tools = requests[index]["tools"].to_string();
            assert!(tools.contains(own), "{tools}");
            assert!(tools.contains(function), "{tools}");
            for forbidden in [other, absent, "secret", "unselected"] { assert!(!tools.contains(forbidden), "{tools}"); }
            assert_eq!(requests[index]["reasoning"]["effort"], json!("low"));
        }
        assert!(requests[2]["input"].to_string().contains("application-result-731"));
        assert!(requests[3]["input"].to_string().contains("mcp-result-731"));
        assert!(requests[4]["input"].to_string().contains("application-result-731"));
        server.abort();
        api.shutdown().await?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = serve(&api).await?;
        let first = format!("{base}/sessions/{first_id}");
        let mut events = client.get(format!("{first}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
        for call_id in ["resumed-call", "cancelled-call", "lost-call"] {
            let started = request(&client, reqwest::Method::POST, &format!("{first}/input"), json!({"input": "Lookup again"})).await?;
            let turn_id = started["turn"]["id"].as_str().context("turn")?;
            let waiting = pending(&client, &first).await?;
            assert_eq!(waiting["requiredActions"][0]["callId"], json!(call_id));
            assert_eq!(waiting["agent"], sessions[0]["agent"]);
            if call_id == "lost-call" { break; }
            if call_id == "cancelled-call" {
                request(&client, reqwest::Method::POST, &format!("{first}/turns/{turn_id}/cancel"), Value::Null).await?;
            } else {
                request(&client, reqwest::Method::POST, &format!("{first}/turns/{turn_id}/tool-results"), json!({"callId": call_id, "success": true, "output": "resumed-result"})).await?;
            }
            let outcome = completed(&mut events).await?;
            let expected = if call_id == "cancelled-call" { "interrupted" } else { "completed" };
            assert_eq!(outcome["params"]["turn"]["status"], json!(expected));
            if call_id == "cancelled-call" {
                assert_eq!(client.post(format!("{first}/turns/{turn_id}/tool-results")).bearer_auth(TOKEN).json(&json!({"callId": call_id, "success": true, "output": "late"})).send().await?.status(), reqwest::StatusCode::CONFLICT);
            }
        }
        drop(events);
        let captures = model.received_requests().await.context("resumed captures")?;
        let resumed = captures.iter().filter(|request| request.url.path().ends_with("/responses")).nth(5).context("resumed request")?;
        let resumed: Value = serde_json::from_slice(&resumed.body)?;
        assert_eq!(resumed["tools"], requests[0]["tools"]);
        server.abort();
        api.shutdown().await?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = serve(&api).await?;
        let restored = request(&client, reqwest::Method::GET, &format!("{base}/sessions/{first_id}"), Value::Null).await?;
        assert_eq!(restored["requiredActions"], json!([]));
        assert_eq!(restored["unresolvedActions"][0]["callId"], json!("lost-call"));
        server.abort();
        api.shutdown().await?;
        mcp_task.abort();
        Ok::<_, anyhow::Error>(())
    }).await?
}
