use super::*;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use codex_state::SqliteConfig;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn reconcile_recovers_submitted_tool_result_lost_to_disconnect() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let model = create_mock_responses_server_sequence_unchecked(vec![
            capabilities::call("lookup_order", "c1"),
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
            json!({"model":"mock-model","instructions":"Use tools.","tools":[{"name":"lookup_order","description":"Lookup","parameters":{"type":"object","properties":{}}}]}),
        )
        .await?;
        let session = request(
            &client,
            reqwest::Method::POST,
            &format!("{base}/sessions"),
            json!({"agentId": agent["id"], "environment": {"type": "none"}}),
        )
        .await?;
        let session_id = session["id"].as_str().context("session id")?.to_owned();
        let url = format!("{base}/sessions/{session_id}");
        let mut events = client
            .get(format!("{url}/events"))
            .bearer_auth(TOKEN)
            .send()
            .await?
            .error_for_status()?;
        let started = request(
            &client,
            reqwest::Method::POST,
            &format!("{url}/input"),
            json!({"input": "Run lookup"}),
        )
        .await?;
        let turn_id = started["turn"]["id"].as_str().context("turn")?.to_owned();
        capabilities::pending(&client, &url).await?;
        // Submit the result; the turn resumes and completes on the worker.
        let result = json!({"callId": "c1", "success": true, "output": "done"});
        let receipt = request(
            &client,
            reqwest::Method::POST,
            &format!("{url}/turns/{turn_id}/tool-results"),
            result.clone(),
        )
        .await?;
        assert_eq!(receipt["status"], json!("submitted"));
        let outcome = completed(&mut events).await?;
        assert_eq!(outcome["params"]["turn"]["status"], json!("completed"));
        drop(events);

        // Simulate a crash that lost the delivery acknowledgment: the tool call
        // reverts to unavailable and its turn to a provisional connection-loss
        // failure, exactly as the startup/disconnect path leaves them, written
        // through the approved codex-state sqlite shim.
        let pool = SqliteConfig::from_sqlite_home(AbsolutePathBuf::from_absolute_path(data.path())?)
            .open_read_write_pool(data.path().join("agents-api.sqlite").as_path())
            .await?;
        sqlx::query("UPDATE tool_calls SET status = 'unavailable' WHERE session_id = ? AND call_id = 'c1'")
            .bind(&session_id)
            .execute(&pool)
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
        let disrupted = request(&client, reqwest::Method::GET, &url, Value::Null).await?;
        assert_eq!(disrupted["unresolvedActions"][0]["callId"], json!("c1"));

        // Reconnect on the same home; reconciliation recognizes the completed
        // turn, so the call delivered even though the acknowledgment was lost.
        api.reconnect(backend(home.path()).await?).await?;
        let recovered = request(&client, reqwest::Method::GET, &url, Value::Null).await?;
        assert_eq!(recovered["unresolvedActions"], json!([]));
        assert_eq!(recovered["requiredActions"], json!([]));
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(turns["data"][0]["status"], json!("completed"));
        // The recovered receipt makes an identical resubmit idempotent, not a
        // conflict, and never re-delivers the result to the model.
        let resubmit = request(
            &client,
            reqwest::Method::POST,
            &format!("{url}/turns/{turn_id}/tool-results"),
            result,
        )
        .await?;
        assert_eq!(resubmit["status"], json!("submitted"));

        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
