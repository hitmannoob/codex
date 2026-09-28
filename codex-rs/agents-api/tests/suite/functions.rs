use super::*;
use pretty_assertions::assert_eq;

// A 1x1 transparent PNG.
const PIXEL: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=";

fn result(action: &Value, fields: Value) -> Value {
    let mut event = json!({"type":"agent.session.input.tool_result","turn_id":action["turn_id"],"call_id":action["call_id"]});
    if let (Some(event), Value::Object(fields)) = (event.as_object_mut(), fields) {
        event.extend(fields);
    }
    json!({"events":[event]})
}

async fn submit(
    client: &reqwest::Client,
    url: &str,
    body: &Value,
) -> anyhow::Result<reqwest::StatusCode> {
    Ok(client
        .post(format!("{url}/events"))
        .bearer_auth(TOKEN)
        .json(body)
        .send()
        .await?
        .status())
}

/// Start a session whose first turn waits on one `lookup` call.
async fn pending(
    client: &reqwest::Client,
    base: &str,
    marker: &str,
) -> anyhow::Result<(String, Value)> {
    let session = request(client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({
        "agent":{"model":"mock-model","tools":[{"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object","properties":{}}}]},
        "environment":{"type":"none"},"input":marker,
    })).await?;
    let url = format!(
        "{base}/agents/sessions/{}",
        session["id"].as_str().context("session id")?
    );
    loop {
        let session = request(client, reqwest::Method::GET, &url, Value::Null).await?;
        if session["status"] == "requires_action" {
            return Ok((url, session["required_actions"][0].clone()));
        }
        tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
    }
}

async fn output_item(client: &reqwest::Client, url: &str) -> anyhow::Result<Value> {
    let items = request(
        client,
        reqwest::Method::GET,
        &format!("{url}/items?order=asc"),
        Value::Null,
    )
    .await?;
    items["data"]
        .as_array()
        .context("items")?
        .iter()
        .find(|item| item["type"] == "function_call_output")
        .cloned()
        .context("function output item")
}

// Image preparation in the in-process worker blocks in place, which requires
// the multi-threaded runtime the API binary uses.
#[tokio::test(flavor = "multi_thread")]
async fn function_results_keep_their_content_limits_and_semantics() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        for marker in ["array-output", "boundary-output", "failed-output", "racing-output", "cancelled-output"] {
            Mock::given(body_string_contains(marker))
                .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(capabilities::call("lookup", &format!("{marker}-call")), "text/event-stream"))
                .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        }
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();

        // Content parts reach the model as parts and are saved as submitted.
        let (url, action) = pending(&client, &base, "array-output").await?;
        let parts = json!([{"type":"input_text","text":"part-one"},{"type":"input_image","image_url":PIXEL}]);
        assert_eq!(submit(&client, &url, &result(&action, json!({"success":true,"output":parts}))).await?, reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 1).await?;
        let saved = output_item(&client, &url).await?;
        assert_eq!(json!({"status":saved["status"],"output":saved["output"],"error":saved["error"]}), json!({"status":"completed","output":parts,"error":null}));
        let captures = provider.received_requests().await.context("captures")?;
        let sent = captures.iter().filter_map(|request| serde_json::from_slice::<Value>(&request.body).ok())
            .flat_map(|body| body["input"].as_array().cloned().unwrap_or_default())
            .find(|item| item["type"] == "function_call_output" && item["call_id"] == "array-output-call")
            .context("function output sent to the model")?;
        let kinds = sent["output"].as_array().context("content parts")?.iter()
            .map(|part| (part["type"].clone(), part["text"].clone())).collect::<Vec<_>>();
        assert_eq!(kinds, vec![(json!("input_text"), json!("part-one")), (json!("input_image"), Value::Null)]);
        let array_url = url;

        // Text is limited to 10,000 UTF-8 bytes; a rejected result leaves the
        // call pending, and the boundary itself is accepted.
        let (url, action) = pending(&client, &base, "boundary-output").await?;
        let too_long = "é".repeat(5_001);
        assert_eq!(submit(&client, &url, &result(&action, json!({"success":true,"output":too_long}))).await?, reqwest::StatusCode::BAD_REQUEST);
        for invalid in [
            json!({"success":true,"error":"both"}),
            json!({"success":false,"output":"only-for-success"}),
            json!({"success":true,"output":[{"type":"input_image","image_url":"https://example.com/remote.png"}]}),
            json!({"success":true,"output":[{"type":"output_text","text":"wrong part"}]}),
            json!({"success":true,"output":{"object":"serialize it first"}}),
        ] {
            assert_eq!(submit(&client, &url, &result(&action, invalid.clone())).await?, reqwest::StatusCode::BAD_REQUEST, "{invalid}");
        }
        assert_eq!(request(&client, reqwest::Method::GET, &url, Value::Null).await?["status"], "requires_action");
        let boundary = "é".repeat(5_000);
        assert_eq!(submit(&client, &url, &result(&action, json!({"success":true,"output":boundary}))).await?, reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 1).await?;
        assert_eq!(output_item(&client, &url).await?["output"], json!(boundary));

        // A failure keeps its message as the error.
        let (url, action) = pending(&client, &base, "failed-output").await?;
        assert_eq!(submit(&client, &url, &result(&action, json!({"success":false,"error":"lookup failed ☃"}))).await?, reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 1).await?;
        let saved = output_item(&client, &url).await?;
        assert_eq!(json!({"status":saved["status"],"output":saved["output"],"error":saved["error"]}), json!({"status":"failed","output":null,"error":"lookup failed ☃"}));

        // A call belongs to its own session, and racing results resolve it once:
        // the winner's retry gets its receipt while the loser conflicts.
        let (url, action) = pending(&client, &base, "racing-output").await?;
        let other_session = result(&action, json!({"success":true,"output":"misrouted"}));
        assert_eq!(submit(&client, &array_url, &other_session).await?, reqwest::StatusCode::BAD_REQUEST);
        let first = result(&action, json!({"success":true,"output":"first"}));
        let second = result(&action, json!({"success":true,"output":"second"}));
        let (a, b) = tokio::join!(submit(&client, &url, &first), submit(&client, &url, &second));
        let mut statuses = vec![a?, b?];
        statuses.sort();
        assert_eq!(statuses, vec![reqwest::StatusCode::ACCEPTED, reqwest::StatusCode::CONFLICT]);
        idle(&client, &url, /*expected_turns*/ 1).await?;
        let winner = output_item(&client, &url).await?["output"].clone();
        let (winner, loser) = if winner == "first" { (first, second) } else { (second, first) };
        assert_eq!(submit(&client, &url, &winner).await?, reqwest::StatusCode::ACCEPTED);
        assert_eq!(submit(&client, &url, &loser).await?, reqwest::StatusCode::CONFLICT);

        // Cancelling the turn resolves its pending call; a late result conflicts.
        let (url, action) = pending(&client, &base, "cancelled-output").await?;
        assert_eq!(submit(&client, &url, &json!({"events":[{"type":"agent.session.input.cancel"}]})).await?, reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 1).await?;
        let late = result(&action, json!({"success":true,"output":"too late"}));
        assert_eq!(submit(&client, &url, &late).await?, reqwest::StatusCode::CONFLICT);
        assert_eq!(request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?["data"][0]["status"], "cancelled");
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}
