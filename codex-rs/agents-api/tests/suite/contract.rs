use super::*;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use codex_utils_cargo_bin::find_resource;
use pretty_assertions::assert_eq;

#[test]
#[ignore = "requires CODEX_AGENTS_API_SDK_PYTHON pointing to a Python with openai==3.17.0"]
fn pinned_sdk_matches_operation_inventory() -> anyhow::Result<()> {
    let python = std::env::var("CODEX_AGENTS_API_SDK_PYTHON")?;
    let script = find_resource!("tests/sdk_inventory.py")?;
    let inventory = find_resource!("CONTRACT_INVENTORY.json")?;
    let output = std::process::Command::new(python)
        .arg(script)
        .arg(inventory)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "SDK inventory check failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires CODEX_AGENTS_API_SDK_PYTHON pointing to a Python with openai==3.17.0"]
async fn official_sdk_session_lifecycle() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 45), async {
        let python = std::env::var("CODEX_AGENTS_API_SDK_PYTHON")?;
        let script = find_resource!("tests/sdk_lifecycle.py")?;
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let done = create_final_assistant_message_sse_response("Done")?;
        let model = create_mock_responses_server_sequence_unchecked(vec![
            capabilities::call("lookup", "sdk-call"),
            done.clone(),
            capabilities::call("lookup", "sdk-error"),
            done.clone(),
            done.clone(),
            done,
        ])
        .await;
        Mock::given(body_string_contains("sdk-cancel-input"))
            .respond_with(
                ResponseTemplate::new(/*s*/ 200).set_delay(Duration::from_secs(/*secs*/ 60)),
            )
            .with_priority(/*p*/ 1)
            .up_to_n_times(/*n*/ 1)
            .mount(&model)
            .await;
        MockResponsesConfig::new(&model.uri())
            .with_root_config("features.plugins = false")
            .write(home.path())?;
        let mut session_id = None;
        for _ in 0..2 {
            let api = AgentsApi::new(
                backend(home.path()).await?,
                AbsolutePathBuf::from_absolute_path(data.path())?,
                TOKEN.into(),
            )
            .await?;
            let (base, server) = capabilities::serve(&api).await?;
            let mut command = tokio::process::Command::new(&python);
            command.arg(&script).arg(&base);
            if let Some(id) = &session_id {
                command.arg(id);
            }
            command
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::inherit());
            let output = tokio::time::timeout(
                Duration::from_secs(/*secs*/ 30),
                command
                    .kill_on_drop(/*kill_on_drop*/ true)
                    .spawn()?
                    .wait_with_output(),
            )
            .await;
            if output.is_err() {
                let requests = model.received_requests().await.context("model captures")?;
                eprintln!(
                    "SDK timeout: {} model requests; restarted={}",
                    requests.len(),
                    session_id.is_some()
                );
            }
            let output = output??;
            server.abort();
            api.shutdown().await?;
            anyhow::ensure!(
                output.status.success(),
                "SDK failed:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let receipt: Value = serde_json::from_slice(&output.stdout)?;
            session_id = Some(
                receipt["session_id"]
                    .as_str()
                    .context("session id")?
                    .to_owned(),
            );
        }
        let requests = model.received_requests().await.context("requests")?;
        let last: Value = serde_json::from_slice(&requests.last().context("last request")?.body)?;
        assert!(last["input"].to_string().contains("orange-731"));
        assert!(last["input"].to_string().contains("sdk-result-731"));
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn public_sessions_save_normalized_history_and_validate_inputs() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 30), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let model = create_mock_responses_server_repeating_assistant("finished").await;
        MockResponsesConfig::new(&model.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let body = json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":"Remember orange-731","stream":true});
        let mut stream = client.post(format!("{base}/agents/sessions")).bearer_auth(TOKEN).json(&body).send().await?.error_for_status()?;
        let created = event(&mut stream,"agent.session.created").await?;
        let id = created["session"]["id"].as_str().context("id")?;
        let url = format!("{base}/agents/sessions/{id}");
        // Drop the stream immediately; execution and saved history belong to the service.
        drop(stream);
        let turn = loop {
            let turns = request(&client,reqwest::Method::GET,&format!("{url}/turns"),Value::Null).await?;
            if turns["data"][0]["status"] == "completed" { break turns["data"][0].clone(); }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        };
        assert_eq!(request(&client,reqwest::Method::GET,&format!("{url}/turns/{}",turn["id"].as_str().unwrap()),Value::Null).await?,turn);
        let items = request(&client,reqwest::Method::GET,&format!("{url}/items?order=asc&limit=1"),Value::Null).await?;
        assert_eq!(items["data"][0]["role"],"user");
        assert_eq!(items["has_more"],true);
        let next = request(&client,reqwest::Method::GET,&format!("{url}/items?order=asc&after={}",items["last_id"].as_str().unwrap()),Value::Null).await?;
        assert!(next["data"].as_array().unwrap().iter().any(|i| i["role"] == "assistant" && i["status"] == "completed"));
        for suffix in ["items?after=another-session", "turns?limit=0"] {
            assert_eq!(client.get(format!("{url}/{suffix}")).bearer_auth(TOKEN).send().await?.status(),reqwest::StatusCode::BAD_REQUEST);
        }
        let response = client.post(format!("{url}/events")).bearer_auth(TOKEN).json(&json!({"events":[{"type":"agent.session.input.message","input":[{"role":"user","content":[{"type":"input_text","text":"Follow up"}]}]}]})).send().await?;
        assert_eq!(response.status(),reqwest::StatusCode::ACCEPTED);
        assert!(response.bytes().await?.is_empty());
        server.abort();
        api.shutdown().await?;
        Ok::<_,anyhow::Error>(())
    }).await?
}
