use anyhow::Context;
use app_test_support::MockResponsesConfig;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_agents_api::AgentsApi;
use codex_app_server_client::AppServerClient;
use codex_app_server_client::EnvironmentManager;
use codex_app_server_client::InProcessAppServerClient;
use codex_app_server_client::InProcessClientStartArgs;
use codex_app_server_client::legacy_core::config::ConfigBuilder;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_feedback::CodexFeedback;
use codex_protocol::protocol::SessionSource;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use wiremock::Mock;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_string_contains;

const TOKEN: &str = "test-token-for-the-local-agents-api";

async fn backend(home: &Path) -> anyhow::Result<AppServerClient> {
    let loader_overrides = LoaderOverrides::without_managed_config_for_tests();
    let config = ConfigBuilder::default()
        .codex_home(home.to_path_buf())
        .fallback_cwd(Some(home.to_path_buf()))
        .loader_overrides(loader_overrides.clone())
        .build()
        .await?;
    let state_db = codex_rollout::state_db::try_init(&config).await?;
    let environment_manager = Arc::new(EnvironmentManager::without_environments(
        config.http_client_factory(),
    ));
    Ok(AppServerClient::InProcess(
        InProcessAppServerClient::start(InProcessClientStartArgs {
            arg0_paths: Arg0DispatchPaths::default(),
            config: Arc::new(config),
            cli_overrides: vec![],
            loader_overrides,
            strict_config: false,
            cloud_config_bundle: CloudConfigBundleLoader::default(),
            feedback: CodexFeedback::new(),
            log_db: None,
            state_db: Some(state_db),
            environment_manager,
            config_warnings: vec![],
            session_source: SessionSource::Cli,
            enable_codex_api_key_env: false,
            client_name: "codex_agents_api_test".into(),
            client_version: "0.0.0".into(),
            experimental_api: true,
            mcp_server_openai_form_elicitation: false,
            opt_out_notification_methods: vec![],
            channel_capacity: 128,
        })
        .await?,
    ))
}

async fn request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    body: Value,
) -> anyhow::Result<Value> {
    let response = client
        .request(method, url)
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let body: Value = response.json().await?;
    anyhow::ensure!(status.is_success(), "{url}: {status}: {body}");
    Ok(body)
}

async fn completed(events: &mut reqwest::Response) -> anyhow::Result<Value> {
    event(events, "turn/completed").await
}

async fn event(events: &mut reqwest::Response, name: &str) -> anyhow::Result<Value> {
    let mut received = String::new();
    let marker = format!("event: {name}\n");
    loop {
        let chunk = events.chunk().await?.context("event stream ended")?;
        received.push_str(&String::from_utf8_lossy(&chunk));
        if let Some(start) = received.find(&marker)
            && let Some(end) = received[start..].find("\n\n")
        {
            let data = received[start..start + end]
                .lines()
                .find_map(|line| line.strip_prefix("data: "))
                .context("event data")?;
            return Ok(serde_json::from_str(data)?);
        }
    }
}

#[tokio::test]
async fn sessions_run_follow_up_and_retrieve_independent_turns() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let model = create_mock_responses_server_repeating_assistant("finished").await;
        MockResponsesConfig::new(&model.uri())
            .with_root_config("features.plugins = false")
            .write(home.path())?;
        let api = AgentsApi::new(
            backend(home.path()).await?,
            AbsolutePathBuf::from_absolute_path(data.path())?,
            TOKEN.into(),
        )
        .await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}/v1", listener.local_addr()?);
        let router = api.router();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stop_rx.await;
                })
                .await
        });
        let client = reqwest::Client::new();
        assert_eq!(
            client
                .get(format!("{base}/agents/unknown"))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        let agent = request(
            &client,
            reqwest::Method::POST,
            &format!("{base}/agents"),
            json!({"model": "mock-model", "instructions": "Be concise."}),
        )
        .await?;
        let mut sessions = vec![];
        for _ in 0..2 {
            sessions.push(
                request(
                    &client,
                    reqwest::Method::POST,
                    &format!("{base}/sessions"),
                    json!({"agentId": agent["id"], "environment": {"type": "none"}}),
                )
                .await?,
            );
        }
        let first_id = sessions[0]["id"].as_str().context("session id")?;
        let first = format!("{base}/sessions/{first_id}");
        let mut events = client
            .get(format!("{first}/events"))
            .bearer_auth(TOKEN)
            .send()
            .await?
            .error_for_status()?;
        for input in [
            "Remember the code orange-731.",
            "What code did I ask you to remember?",
        ] {
            let response = request(
                &client,
                reqwest::Method::POST,
                &format!("{first}/input"),
                json!({"input": input}),
            )
            .await?;
            let turn_id = response["turn"]["id"].as_str().context("turn id")?;
            let outcome = completed(&mut events).await?;
            assert_eq!(&outcome["params"]["turn"]["id"], &json!(turn_id));
            assert_eq!(&outcome["params"]["turn"]["status"], &json!("completed"));
        }
        let second_id = sessions[1]["id"].as_str().context("second session id")?;
        let empty = request(
            &client,
            reqwest::Method::GET,
            &format!("{base}/sessions/{second_id}/turns"),
            Value::Null,
        )
        .await?;
        assert_eq!(
            empty,
            json!({"data": [], "nextCursor": null, "backwardsCursor": null})
        );
        let second = format!("{base}/sessions/{second_id}");
        let mut second_events = client
            .get(format!("{second}/events"))
            .bearer_auth(TOKEN)
            .send()
            .await?
            .error_for_status()?;
        request(
            &client,
            reqwest::Method::POST,
            &format!("{second}/input"),
            json!({"input": "An independent conversation."}),
        )
        .await?;
        completed(&mut second_events).await?;
        let turns = request(
            &client,
            reqwest::Method::GET,
            &format!("{first}/turns?limit=1"),
            Value::Null,
        )
        .await?;
        assert_eq!(turns["data"].as_array().context("turns")?.len(), 1);
        assert_eq!(&turns["data"][0]["status"], &json!("completed"));
        assert!(turns["nextCursor"].is_string());
        let captures = model.received_requests().await.context("model requests")?;
        let responses: Vec<Value> = captures
            .iter()
            .filter(|request| request.url.path().ends_with("/responses"))
            .map(|request| serde_json::from_slice(&request.body))
            .collect::<Result<_, _>>()?;
        assert_eq!(responses.len(), 3);
        assert!(responses[1]["input"].to_string().contains("orange-731"));
        assert!(!responses[2]["input"].to_string().contains("orange-731"));
        let tools = responses[0]["tools"].to_string();
        assert!(!tools.contains("exec_command"));
        assert!(!tools.contains("apply_patch"));
        let pending = Mock::given(body_string_contains("pending-input"))
            .respond_with(
                ResponseTemplate::new(/*s*/ 200).set_delay(Duration::from_secs(/*secs*/ 60)),
            )
            .with_priority(/*p*/ 1)
            .expect(/*r*/ 1)
            .mount_as_scoped(&model)
            .await;
        let running = request(
            &client,
            reqwest::Method::POST,
            &format!("{second}/input"),
            json!({"input": "pending-input"}),
        )
        .await?;
        pending.wait_until_satisfied().await;
        let steered = request(
            &client,
            reqwest::Method::POST,
            &format!("{second}/input"),
            json!({"input": "Use concise output."}),
        )
        .await?;
        assert_eq!(running["turn"]["id"], steered["turn"]["id"]);
        let turn_id = running["turn"]["id"].as_str().context("running turn id")?;
        request(
            &client,
            reqwest::Method::POST,
            &format!("{second}/turns/{turn_id}/cancel"),
            Value::Null,
        )
        .await?;
        let interrupted = completed(&mut second_events).await?;
        assert_eq!(
            &interrupted["params"]["turn"]["status"],
            &json!("interrupted")
        );
        drop(events);
        drop(second_events);
        let _ = stop_tx.send(());
        server.await??;
        api.shutdown().await?;
        let restarted = AgentsApi::new(
            backend(home.path()).await?,
            AbsolutePathBuf::from_absolute_path(data.path())?,
            TOKEN.into(),
        )
        .await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}/v1/sessions/{first_id}", listener.local_addr()?);
        let router = restarted.router();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stop_rx.await;
                })
                .await
        });
        let restored = request(&client, reqwest::Method::GET, &base, Value::Null).await?;
        assert_eq!(restored["agent"], agent);
        let mut events = client
            .get(format!("{base}/events"))
            .bearer_auth(TOKEN)
            .send()
            .await?
            .error_for_status()?;
        request(
            &client,
            reqwest::Method::POST,
            &format!("{base}/input"),
            json!({"input": "Continue after restart."}),
        )
        .await?;
        let outcome = completed(&mut events).await?;
        assert_eq!(&outcome["params"]["turn"]["status"], &json!("completed"));
        let captures = model
            .received_requests()
            .await
            .context("requests after restart")?;
        let last = captures
            .iter()
            .rev()
            .find(|request| request.url.path().ends_with("/responses"))
            .context("resumed model request")?;
        assert!(String::from_utf8_lossy(&last.body).contains("orange-731"));
        drop(events);
        let _ = stop_tx.send(());
        server.await??;
        restarted.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[path = "suite/capabilities.rs"]
mod capabilities;

#[path = "suite/contract.rs"]
mod contract;
