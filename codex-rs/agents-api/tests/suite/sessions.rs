use super::*;
use codex_utils_cargo_bin::find_resource;
use pretty_assertions::assert_eq;

fn message(text: &str) -> Value {
    json!({"type":"agent.session.input.message","input":text})
}

async fn send(
    client: &reqwest::Client,
    url: &str,
    event: Value,
) -> anyhow::Result<reqwest::StatusCode> {
    Ok(client
        .post(format!("{url}/events"))
        .bearer_auth(TOKEN)
        .json(&json!({"events":[event]}))
        .send()
        .await?
        .status())
}

fn rollouts(directory: &Path) -> anyhow::Result<usize> {
    let mut count = 0;
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            count += rollouts(&path)?;
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("rollout-"))
        {
            count += 1;
        }
    }
    Ok(count)
}

#[tokio::test]
async fn session_updates_apply_next_turn_and_deletion_waits_then_cleans_up() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        let slow = Mock::given(body_string_contains("slow-turn"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_delay(Duration::from_secs(/*secs*/ 60)))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).expect(/*r*/ 1).mount_as_scoped(&provider).await;
        let mut catalog: Value = serde_json::from_str(&std::fs::read_to_string(find_resource!("tests/fixtures/model_catalog.json")?)?)?;
        let mut other = catalog["models"][0].clone();
        other["slug"] = json!("other-model");
        catalog["models"].as_array_mut().context("models")?.push(other);
        let catalog_path = home.path().join("models.json");
        std::fs::write(&catalog_path, catalog.to_string())?;
        MockResponsesConfig::new(&provider.uri()).with_root_config(&format!(
            "features.plugins = false\nmodel_catalog_json = {}", serde_json::to_string(&catalog_path)?
        )).write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let sessions = format!("{base}/agents/sessions");
        let create = |input: &str| json!({"agent":{"model":"mock-model","reasoning":{"effort":"high","summary":"detailed"},"service_tier":"flex"},
            "environment":{"type":"none"},"input":input});
        let kept = request(&client, reqwest::Method::POST, &sessions, create("kept-session")).await?;
        let session = request(&client, reqwest::Method::POST, &sessions, create("first-turn")).await?;
        let kept_url = format!("{sessions}/{}", kept["id"].as_str().context("kept id")?);
        let url = format!("{sessions}/{}", session["id"].as_str().context("session id")?);
        idle(&client, &kept_url, /*expected_turns*/ 1).await?;
        idle(&client, &url, /*expected_turns*/ 1).await?;

        // Settings change for later turns; metadata and unrelated settings persist.
        let updated = request(&client, reqwest::Method::POST, &url, json!({
            "agent":{"reasoning":{"effort":"low"},"service_tier":"priority"},"metadata":{"team":"a"}
        })).await?;
        assert_eq!(request(&client, reqwest::Method::GET, &url, Value::Null).await?, updated);
        assert_eq!(send(&client, &url, message("second-turn")).await?, reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 2).await?;
        request(&client, reqwest::Method::POST, &url, json!({"agent":{"model":"other-model"}})).await?;
        assert_eq!(send(&client, &url, message("third-turn")).await?, reqwest::StatusCode::ACCEPTED);
        let after = idle(&client, &url, /*expected_turns*/ 3).await?;
        let mut expected = updated.clone();
        expected["agent"]["model"] = json!("other-model");
        assert_eq!(
            json!({"agent":after["agent"],"metadata":after["metadata"]}),
            json!({"agent":expected["agent"],"metadata":{"team":"a"}})
        );
        let captures = provider.received_requests().await.context("captures")?;
        let turns = captures.iter().filter(|r| r.url.path().ends_with("/responses"))
            .map(|r| serde_json::from_slice::<Value>(&r.body)).collect::<Result<Vec<_>, _>>()?
            .into_iter().filter_map(|request| {
                let last = request["input"].as_array()?.iter().rev().find(|item| item["role"] == "user")?.to_string();
                let turn = ["first-turn", "second-turn", "third-turn"].into_iter().find(|turn| last.contains(turn))?;
                Some(json!({"turn":turn,"model":request["model"],"reasoning":request["reasoning"],"tier":request["service_tier"]}))
            }).collect::<Vec<_>>();
        assert_eq!(turns, vec![
            json!({"turn":"first-turn","model":"mock-model","reasoning":{"effort":"high","summary":"detailed"},"tier":"flex"}),
            json!({"turn":"second-turn","model":"mock-model","reasoning":{"effort":"low","summary":"detailed"},"tier":"priority"}),
            json!({"turn":"third-turn","model":"other-model","reasoning":{"effort":"low","summary":"detailed"},"tier":"priority"}),
        ]);

        // Running execution blocks deletion until it is cancelled.
        let mut stream = client.get(format!("{url}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
        assert_eq!(send(&client, &url, message("slow-turn")).await?, reqwest::StatusCode::ACCEPTED);
        slow.wait_until_satisfied().await;
        while request(&client, reqwest::Method::GET, &url, Value::Null).await?["status"] != "in_progress" {
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        }
        assert_eq!(client.delete(&url).bearer_auth(TOKEN).send().await?.status(), reqwest::StatusCode::CONFLICT);
        assert_eq!(send(&client, &url, json!({"type":"agent.session.input.cancel"})).await?, reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 4).await?;
        let session_dir = home.path().join("sessions");
        assert_eq!(rollouts(&session_dir)?, 2);
        assert_eq!(
            request(&client, reqwest::Method::DELETE, &url, Value::Null).await?,
            json!({"id":session["id"],"object":"agent.session.deleted","deleted":true})
        );

        // Deletion ends the live stream, hides every session route, and removes
        // only this session's thread from the worker.
        tokio::time::timeout(Duration::from_secs(/*secs*/ 10), async {
            while stream.chunk().await?.is_some() {}
            Ok::<_, anyhow::Error>(())
        }).await??;
        for (method, suffix, body) in [
            (reqwest::Method::GET, "", Value::Null),
            (reqwest::Method::DELETE, "", Value::Null),
            (reqwest::Method::POST, "", json!({"metadata":{}})),
            (reqwest::Method::GET, "/items", Value::Null),
            (reqwest::Method::POST, "/events", json!({"events":[message("gone")]})),
        ] {
            let response = client.request(method, format!("{url}{suffix}")).bearer_auth(TOKEN).json(&body).send().await?;
            assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND, "{suffix}");
        }
        tokio::time::timeout(Duration::from_secs(/*secs*/ 10), async {
            while rollouts(&session_dir)? != 1 {
                tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await??;
        let kept = request(&client, reqwest::Method::GET, &kept_url, Value::Null).await?;
        assert_eq!(request(&client, reqwest::Method::GET, &sessions, Value::Null).await?["data"], json!([kept]));
        server.abort();
        api.shutdown().await?;

        // Leave the state an API crash would between committing a deletion and
        // removing its thread; the queued cleanup finishes on the next start.
        let pool = codex_state::SqliteConfig::from_sqlite_home(AbsolutePathBuf::from_absolute_path(data.path())?)
            .open_read_write_pool(data.path().join("agents-api.sqlite").as_path()).await?;
        let kept_id = kept["id"].as_str().context("kept id")?;
        let mut tx = pool.begin().await?;
        for statement in [
            "INSERT INTO session_cleanup (session_id, thread_id) SELECT id, thread_id FROM sessions WHERE id = ?",
            "DELETE FROM public_records WHERE session_id = ?",
            "DELETE FROM public_sessions WHERE id = ?",
            "DELETE FROM sessions WHERE id = ?",
        ] {
            sqlx::query(statement).bind(kept_id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        pool.close().await;
        let restarted = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        assert_eq!(rollouts(&session_dir)?, 0);
        restarted.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}
