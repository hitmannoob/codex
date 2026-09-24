use super::*;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn replacement_backend_fences_stale_function_calls_and_serves_new_work() -> anyhow::Result<()>
{
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let model = create_mock_responses_server_sequence_unchecked(vec![
            capabilities::call("lookup_order", "held-call"),
            create_final_assistant_message_sse_response("Done")?,
        ])
        .await;
        MockResponsesConfig::new(&model.uri())
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
            json!({
                "model": "mock-model", "instructions": "Use tools.",
                "tools": [{"name": "lookup_order", "description": "Lookup", "parameters": {"type": "object", "properties": {}}}]
            }),
        )
        .await?;
        let held = request(
            &client,
            reqwest::Method::POST,
            &format!("{base}/sessions"),
            json!({"agentId": agent["id"], "environment": {"type": "none"}}),
        )
        .await?;
        let held_url = format!("{base}/sessions/{}", held["id"].as_str().context("held id")?);
        let mut events = client
            .get(format!("{held_url}/events"))
            .bearer_auth(TOKEN)
            .send()
            .await?
            .error_for_status()?;
        let started = request(
            &client,
            reqwest::Method::POST,
            &format!("{held_url}/input"),
            json!({"input": "Run lookup"}),
        )
        .await?;
        let turn_id = started["turn"]["id"].as_str().context("turn")?.to_owned();
        capabilities::pending(&client, &held_url).await?;
        // Replace the live backend while a function waiter is outstanding.
        api.reconnect(backend(home.path()).await?).await?;
        event(&mut events, "stream/disconnected").await?;
        // The waiter belonged to the replaced connection and cannot be recreated.
        let session = request(&client, reqwest::Method::GET, &held_url, Value::Null).await?;
        assert_eq!(session["requiredActions"], json!([]));
        assert_eq!(session["unresolvedActions"][0]["callId"], json!("held-call"));
        let stale = client
            .post(format!("{held_url}/turns/{turn_id}/tool-results"))
            .bearer_auth(TOKEN)
            .json(&json!({"callId": "held-call", "success": true, "output": "late"}))
            .send()
            .await?;
        assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
        // The replacement generation serves new sessions end to end.
        let fresh = request(
            &client,
            reqwest::Method::POST,
            &format!("{base}/sessions"),
            json!({"agentId": agent["id"], "environment": {"type": "none"}}),
        )
        .await?;
        let fresh_url = format!("{base}/sessions/{}", fresh["id"].as_str().context("fresh id")?);
        let mut fresh_events = client
            .get(format!("{fresh_url}/events"))
            .bearer_auth(TOKEN)
            .send()
            .await?
            .error_for_status()?;
        request(
            &client,
            reqwest::Method::POST,
            &format!("{fresh_url}/input"),
            json!({"input": "Say done."}),
        )
        .await?;
        let outcome = completed(&mut fresh_events).await?;
        assert_eq!(outcome["params"]["turn"]["status"], json!("completed"));
        drop(events);
        drop(fresh_events);
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
