use super::*;
use pretty_assertions::assert_eq;

/// A span attribute's value, as JSON: strings as strings, integers as numbers.
pub(super) fn attribute(span: &Value, key: &str) -> Value {
    let Some(value) = span["attributes"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|attribute| attribute["key"] == key)
        .map(|attribute| &attribute["value"])
    else {
        return Value::Null;
    };
    match (&value["stringValue"], &value["intValue"]) {
        (Value::String(text), _) => json!(text),
        (_, Value::String(number)) => json!(number.parse::<i64>().unwrap_or_default()),
        _ => value.clone(),
    }
}

pub(super) fn spans(trace: &Value) -> Vec<Value> {
    trace["otlp"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

fn nanos(span: &Value, field: &str) -> u128 {
    span[field]
        .as_str()
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

#[tokio::test]
async fn finished_turns_export_as_otlp_traces() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        // Generations are recorded from each response's reported usage.
        let usage = json!({"input_tokens":12,"input_tokens_details":null,"output_tokens":3,"output_tokens_details":null,"total_tokens":15});
        let respond = |item: Value| sse(&[
            json!({"type":"response.created","response":{"id":"traced"}}),
            json!({"type":"response.output_item.done","item":item}),
            json!({"type":"response.completed","response":{"id":"traced","usage":usage}}),
        ]);
        Mock::given(body_string_contains("call-lookup"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(respond(json!({"type":"function_call","call_id":"trace-call","name":"lookup","arguments":"{}"})), "text/event-stream"))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        Mock::given(wiremock::matchers::method("POST"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(respond(json!({"type":"message","role":"assistant","id":"answer","content":[{"type":"output_text","text":"finished"}]})), "text/event-stream"))
            .with_priority(/*p*/ 2).mount(&provider).await;
        Mock::given(body_string_contains("stall-turn"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_delay(Duration::from_secs(/*secs*/ 60)))
            .with_priority(/*p*/ 1).mount(&provider).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let sessions = format!("{base}/agents/sessions");
        let session = request(&client, reqwest::Method::POST, &sessions, json!({
            "agent":{"model":"mock-model","instructions":"Look things up.","tools":[{"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object","properties":{}}}]},
            "environment":{"type":"none"},"input":"call-lookup"})).await?;
        let id = session["id"].as_str().context("session id")?.to_owned();
        let url = format!("{sessions}/{id}");
        let pending = loop {
            let session = request(&client, reqwest::Method::GET, &url, Value::Null).await?;
            if session["status"] == "requires_action" {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        };
        let action = &pending["required_actions"][0];
        let send = |events: Value| client.post(format!("{url}/events")).bearer_auth(TOKEN).json(&json!({"events":events})).send();
        assert_eq!(send(json!([{"type":"agent.session.input.tool_result","turn_id":action["turn_id"],"call_id":action["call_id"],"success":true,"output":"found-it"}])).await?.status(), reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 1).await?;
        assert_eq!(send(json!([{"type":"agent.session.input.message","input":"second-turn"}])).await?.status(), reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 2).await?;
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns?order=asc"), Value::Null).await?;
        let first_turn = turns["data"][0]["id"].as_str().context("first turn")?.to_owned();
        let second_turn = turns["data"][1]["id"].as_str().context("second turn")?.to_owned();

        // A running turn has no trace yet.
        assert_eq!(send(json!([{"type":"agent.session.input.message","input":"stall-turn"}])).await?.status(), reqwest::StatusCode::ACCEPTED);
        let traces_url = format!("{url}/traces");
        let running = loop {
            let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
            if turns["data"].as_array().is_some_and(|turns| turns.len() == 3) {
                break request(&client, reqwest::Method::GET, &format!("{traces_url}?order=asc"), Value::Null).await?;
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        };
        let ids = |page: &Value| page["data"].as_array().into_iter().flatten().map(|trace| trace["id"].clone()).collect::<Vec<_>>();
        let (first, second) = (format!("trace_{first_turn}"), format!("trace_{second_turn}"));
        assert_eq!((ids(&running), running["has_more"].clone()), (vec![json!(first), json!(second)], json!(false)));

        // The function-calling turn: the root agent with its two model
        // responses and the tool call, which carries the submitted result.
        let trace = running["data"][0].clone();
        let trace_id = uuid::Uuid::parse_str(&first_turn)?.simple().to_string();
        assert_eq!(json!({"id":trace["id"],"object":trace["object"],"session_id":trace["session_id"],"turn_id":trace["turn_id"],
            "resource":trace["otlp"]["resourceSpans"][0]["resource"]["attributes"]}),
            json!({"id":first,"object":"agent.session.trace","session_id":id,"turn_id":first_turn,"resource":[
                {"key":"service.name","value":{"stringValue":"codex-agents-api"}},
                {"key":"openai.agents.session_id","value":{"stringValue":id}},
                {"key":"openai.agents.turn_id","value":{"stringValue":first_turn}}]}));
        let recorded = spans(&trace);
        let root = recorded.iter().find(|span| span["parentSpanId"] == "").context("root span")?;
        assert_eq!(json!({"name":root["name"],"status":root["status"],"type":attribute(root, "openai.agents.agent_type"),
            "agent":attribute(root, "gen_ai.agent.id"),"instructions":attribute(root, "gen_ai.system_instructions")}),
            json!({"name":"invoke_agent root","status":{"code":1},"type":"root","agent":session["agent"]["id"],"instructions":"Look things up."}));
        let children = recorded.iter().filter(|span| span["parentSpanId"] == root["spanId"]).collect::<Vec<_>>();
        assert_eq!(children.iter().map(|span| span["name"].clone()).collect::<Vec<_>>(), vec![json!("chat"), json!("chat"), json!("execute_tool lookup")]);
        let tool = children[2];
        let result: Value = serde_json::from_str(attribute(tool, "openai.agents.tool.result").as_str().context("tool result")?)?;
        assert_eq!(json!({"status":tool["status"],"call":attribute(tool, "gen_ai.tool.call.id"),"type":attribute(tool, "gen_ai.tool.type"),"output":result["output"]}),
            json!({"status":{"code":1},"call":"trace-call","type":"function_call","output":"found-it"}));
        let answer: Value = serde_json::from_str(attribute(children[1], "gen_ai.output.messages").as_str().context("output")?)?;
        assert_eq!(answer.as_array().and_then(|items| items.last()).map(|item| item["content"][0]["text"].clone()), Some(json!("finished")));
        let asked: Value = serde_json::from_str(attribute(children[1], "gen_ai.input.messages").as_str().context("input")?)?;
        assert_eq!(asked.as_array().map(|items| items.iter().map(|item| item["type"].clone()).collect::<Vec<_>>()), Some(vec![json!("function_call_output")]));
        assert!(children.iter().all(|span| attribute(span, "gen_ai.usage.input_tokens").is_i64() || span["name"] != "chat"));
        // Every span belongs to the trace, has a unique ID, and lies within
        // the root agent's span.
        let mut span_ids = recorded.iter().map(|span| span["spanId"].clone()).collect::<Vec<_>>();
        span_ids.dedup();
        assert_eq!(span_ids.len(), recorded.len());
        for span in &recorded {
            assert_eq!(span["traceId"], json!(trace_id));
            assert!(nanos(span, "startTimeUnixNano") <= nanos(span, "endTimeUnixNano"), "{span}");
            assert!(nanos(root, "startTimeUnixNano") <= nanos(span, "startTimeUnixNano") && nanos(span, "endTimeUnixNano") <= nanos(root, "endTimeUnixNano"), "{span}");
        }

        // Pages follow the turns, newest first by default; exports are stable.
        let newest = request(&client, reqwest::Method::GET, &format!("{traces_url}?limit=1"), Value::Null).await?;
        assert_eq!((ids(&newest), newest["has_more"].clone()), (vec![json!(second)], json!(true)));
        let older = request(&client, reqwest::Method::GET, &format!("{traces_url}?limit=1&after={second}"), Value::Null).await?;
        assert_eq!(older["data"][0], trace);
        for (query, message) in [("order=up", "order must be asc or desc"), ("after=trace_missing", "invalid after cursor"), ("cursor=x", "unknown query parameter cursor")] {
            let response = client.get(format!("{traces_url}?{query}")).bearer_auth(TOKEN).send().await?;
            assert_eq!((response.status(), response.json::<Value>().await?["error"]["message"].clone()), (reqwest::StatusCode::BAD_REQUEST, json!(message)));
        }
        let missing = client.get(format!("{sessions}/missing/traces")).bearer_auth(TOKEN).send().await?;
        assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);

        // A cancelled turn is exported once it ends, without an OK status.
        assert_eq!(send(json!([{"type":"agent.session.input.cancel"}])).await?.status(), reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 3).await?;
        let latest = request(&client, reqwest::Method::GET, &format!("{traces_url}?limit=1"), Value::Null).await?;
        let cancelled = spans(&latest["data"][0]);
        let root = cancelled.iter().find(|span| span["parentSpanId"] == "").context("cancelled root")?;
        assert_eq!(json!({"status":root["status"],"outcome":attribute(root, "openai.agents.status")}), json!({"status":{"code":0},"outcome":"cancelled"}));

        // Deleting the session removes every record it left, traces included.
        request(&client, reqwest::Method::DELETE, &url, Value::Null).await?;
        server.abort();
        api.shutdown().await?;
        assert_eq!(leftover_rows(data.path(), &id).await?, Vec::<(String, i64)>::new());
        Ok::<_, anyhow::Error>(())
    }).await?
}
