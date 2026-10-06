use super::*;
use app_test_support::create_command_execution_sse_response;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use codex_api::AuthProvider;
use codex_exec_server::ExecServerRuntimePaths;
use codex_exec_server::RemoteEnvironmentConfig;
use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use pretty_assertions::assert_eq;

const ENVIRONMENT_KEY: &str = "test-environment-key-for-self-hosted-executors";

/// Presents the environment key, as `CODEX_API_KEY` does for `codex exec-server`.
struct EnvironmentKey;

impl AuthProvider for EnvironmentKey {
    fn add_auth_headers(&self, headers: &mut http::HeaderMap) {
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("Bearer test-environment-key-for-self-hosted-executors"),
        );
    }
}

/// Run an executor for the environment in this process, as
/// `codex exec-server --remote <remote_url> --environment-id <id>` would.
fn executor(remote_url: &str, environment_id: &str) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let config = RemoteEnvironmentConfig::new(
        remote_url.to_owned(),
        environment_id.to_owned(),
        Arc::new(EnvironmentKey),
        HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
    )?;
    let paths = ExecServerRuntimePaths::new(
        std::env::current_exe()?,
        /*codex_linux_sandbox_exe*/ None,
    )?;
    Ok(tokio::spawn(async move {
        let _ = codex_exec_server::run_remote_environment(config, paths).await;
    }))
}

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

fn lifecycle(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| event["type"].as_str())
        .filter(|kind| {
            kind.starts_with("agent.session.environment.")
                || matches!(
                    *kind,
                    "agent.session.requires_action"
                        | "agent.session.in_progress"
                        | "agent.session.idle"
                )
        })
        .map(str::to_owned)
        .collect()
}

#[tokio::test]
async fn self_hosted_input_waits_for_the_executor_and_runs_in_its_workspace() -> anyhow::Result<()>
{
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let workspace = tempfile::tempdir()?;
        let workspace_path = workspace.path().canonicalize()?;
        let workspace_directory = workspace_path.to_str().context("workspace path")?;
        let command = if cfg!(windows) {
            vec!["cmd.exe".to_owned(), "/d".to_owned(), "/c".to_owned(), "cd > where.txt".to_owned()]
        } else {
            vec!["/bin/sh".to_owned(), "-c".to_owned(), "pwd > where.txt".to_owned()]
        };
        let provider = create_mock_responses_server_sequence_unchecked(vec![
            create_command_execution_sse_response(command, /*workdir*/ None, Some(/*timeout_ms*/ 10_000), "where-call")?,
            create_final_assistant_message_sse_response("done")?,
        ]).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let origin = base.trim_end_matches("/v1").to_owned();
        let client = reqwest::Client::new();
        let sessions = format!("{base}/agents/sessions");
        let create = json!({"agent":{"model":"mock-model"},
            "environment":{"type":"self_hosted","workspace_directory":workspace_directory}});

        // Self-hosted environments need the operator's environment key.
        let disabled = client.post(&sessions).bearer_auth(TOKEN).json(&create).send().await?;
        assert_eq!(disabled.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
        api.configure_environments(ENVIRONMENT_KEY.into(), format!("{origin}/registry"))?;

        // Without input, the session starts idle and reports where to connect.
        let session = request(&client, reqwest::Method::POST, &sessions, create).await?;
        let environment_id = session["environment"]["id"].as_str().context("environment id")?.to_owned();
        let environment = json!({"id":environment_id,"type":"self_hosted","workspace_directory":workspace_directory,
            "capability_directories":[],"remote_url":format!("{origin}/registry")});
        assert_eq!((&session["status"], &session["environment"]), (&json!("idle"), &environment));
        assert_eq!(
            request(&client, reqwest::Method::GET, &format!("{base}/agents/environments/{environment_id}"), Value::Null).await?,
            environment
        );

        // Input waits behind an environment_connection action until the
        // executor connects, then runs its command in the workspace.
        let url = format!("{sessions}/{}", session["id"].as_str().context("session id")?);
        let mut stream = client.get(format!("{url}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
        assert_eq!(send(&client, &url, message("where are you?")).await?, reqwest::StatusCode::ACCEPTED);
        let waiting = request(&client, reqwest::Method::GET, &url, Value::Null).await?;
        assert_eq!(
            (&waiting["status"], &waiting["required_actions"]),
            (&json!("requires_action"), &json!([{"type":"environment_connection","environment_id":environment_id}]))
        );
        let running = executor(&format!("{origin}/registry"), &environment_id)?;
        assert_eq!(lifecycle(&until_idle(&mut stream).await?), vec![
            "agent.session.requires_action", "agent.session.environment.connected",
            "agent.session.in_progress", "agent.session.idle",
        ]);
        let reported = std::fs::read_to_string(workspace_path.join("where.txt"))?;
        assert_eq!(std::fs::canonicalize(reported.trim())?, workspace_path);

        // Losing the executor is reported; later input waits again and can be
        // cancelled without starting a turn.
        running.abort();
        until_event(&mut stream, "agent.session.environment.disconnected").await?;
        assert_eq!(send(&client, &url, message("still there?")).await?, reqwest::StatusCode::ACCEPTED);
        assert_eq!(send(&client, &url, json!({"type":"agent.session.input.cancel"})).await?, reqwest::StatusCode::ACCEPTED);
        let cancelled = idle(&client, &url, /*expected_turns*/ 1).await?;
        assert_eq!(cancelled["required_actions"], json!([]));
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(turns["data"].as_array().map(Vec::len), Some(1));

        // Deleting the session forgets the environment; the caller's compute is
        // the caller's to stop.
        let session_id = session["id"].as_str().context("session id")?;
        request(&client, reqwest::Method::DELETE, &url, Value::Null).await?;
        let gone = client.get(format!("{base}/agents/environments/{environment_id}")).bearer_auth(TOKEN).send().await?;
        assert_eq!(gone.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(leftover_rows(data.path(), session_id).await?, Vec::new());
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
