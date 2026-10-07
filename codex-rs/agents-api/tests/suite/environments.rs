use super::*;
use app_test_support::create_command_execution_sse_response;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use codex_api::AuthProvider;
use codex_exec_server::ExecServerRuntimeOptions;
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
    let paths = ExecServerRuntimeOptions::new(
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

/// The public `command_execution` item for a model call.
async fn command_item(client: &reqwest::Client, url: &str, call_id: &str) -> anyhow::Result<Value> {
    let items = request(
        client,
        reqwest::Method::GET,
        &format!("{url}/items?limit=100"),
        Value::Null,
    )
    .await?;
    items["data"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|item| {
            item["type"] == "command_execution"
                && item["id"].as_str().is_some_and(|id| id.ends_with(call_id))
        })
        .cloned()
        .context("command item")
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
            vec!["cmd.exe".to_owned(), "/d".to_owned(), "/c".to_owned(), "cd > where.txt & echo written".to_owned()]
        } else {
            vec!["/bin/sh".to_owned(), "-c".to_owned(), "pwd > where.txt; echo written".to_owned()]
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
        // A skill in a capability directory on the executor reaches the model.
        let capabilities_path = workspace_path.join("capabilities");
        std::fs::create_dir_all(capabilities_path.join("workspace-demo"))?;
        std::fs::write(capabilities_path.join("workspace-demo/SKILL.md"),
            "---\nname: workspace-demo\ndescription: Demonstrates capability directories\n---\nUse it.\n")?;
        let capability_directory = capabilities_path.to_str().context("capabilities path")?;
        // So does a plugin's skill, from a plugin root.
        let plugin_path = workspace_path.join("plugin-demo");
        std::fs::create_dir_all(plugin_path.join(".codex-plugin"))?;
        std::fs::create_dir_all(plugin_path.join("skills/plugin-skill"))?;
        std::fs::write(plugin_path.join(".codex-plugin/plugin.json"), r#"{"name":"plugin-demo","interface":{"displayName":"Plugin Demo"}}"#)?;
        std::fs::write(plugin_path.join("skills/plugin-skill/SKILL.md"),
            "---\nname: plugin-only-skill\ndescription: Comes from a plugin root\n---\nUse it.\n")?;
        let plugin_directory = plugin_path.to_str().context("plugin path")?;
        let create = json!({"agent":{"model":"mock-model"},
            "environment":{"type":"self_hosted","workspace_directory":workspace_directory,"capability_directories":[capability_directory, plugin_directory]}});

        // Self-hosted environments need the operator's environment key.
        let disabled = client.post(&sessions).bearer_auth(TOKEN).json(&create).send().await?;
        assert_eq!(disabled.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
        api.configure_environments(ENVIRONMENT_KEY.into(), format!("{origin}/registry"))?;

        // Without input, the session starts idle and reports where to connect.
        let session = request(&client, reqwest::Method::POST, &sessions, create).await?;
        let environment_id = session["environment"]["id"].as_str().context("environment id")?.to_owned();
        let environment = json!({"id":environment_id,"type":"self_hosted","workspace_directory":workspace_directory,
            "capability_directories":[capability_directory, plugin_directory],"remote_url":format!("{origin}/registry")});
        assert_eq!((&session["status"], &session["environment"]), (&json!("idle"), &environment));
        let info = |status: &str| json!({"id":environment_id,"object":"agent.environment","type":"self_hosted",
            "status":status,"files":[],"plugins":[],"skills":[]});
        let environment_url = format!("{base}/agents/environments/{environment_id}");
        let retrieve = || request(&client, reqwest::Method::GET, &environment_url, Value::Null);
        assert_eq!(retrieve().await?, info("pending"));

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
        assert_eq!(retrieve().await?, info("connected"));
        let first = provider.received_requests().await.context("requests")?.into_iter().next().context("model request")?;
        let offered = String::from_utf8_lossy(&first.body).into_owned();
        assert!(offered.contains("workspace-demo"), "the skill was not offered");
        assert!(offered.contains("plugin-only-skill"), "the plugin's skill was not offered");
        let command = command_item(&client, &url, "where-call").await?;
        assert_eq!(
            (&command["status"], &command["exit_code"], &command["cwd"], command["output"].as_str().map(str::trim)),
            (&json!("completed"), &json!(0), &json!(workspace_directory), Some("written"))
        );

        // Losing the executor is reported; later input waits again and can be
        // cancelled without starting a turn.
        running.abort();
        until_event(&mut stream, "agent.session.environment.disconnected").await?;
        assert_eq!(retrieve().await?, info("disconnected"));
        assert_eq!(send(&client, &url, message("still there?")).await?, reqwest::StatusCode::ACCEPTED);
        assert_eq!(send(&client, &url, json!({"type":"agent.session.input.cancel"})).await?, reqwest::StatusCode::ACCEPTED);
        let cancelled = idle(&client, &url, /*expected_turns*/ 1).await?;
        assert_eq!(cancelled["required_actions"], json!([]));
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(turns["data"].as_array().map(Vec::len), Some(1));

        // Self-hosted files are never published as artifacts.
        assert_eq!(
            request(&client, reqwest::Method::GET, &format!("{url}/artifacts"), Value::Null).await?,
            json!({"object":"list","first_id":null,"last_id":null,"data":[],"has_more":false})
        );
        let artifact = client.get(format!("{url}/artifacts/artifact_1")).bearer_auth(TOKEN).send().await?;
        assert_eq!(artifact.status(), reqwest::StatusCode::NOT_FOUND);

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

fn shell(script: &str) -> Vec<String> {
    if cfg!(windows) {
        vec![
            "cmd.exe".to_owned(),
            "/d".to_owned(),
            "/c".to_owned(),
            script.to_owned(),
        ]
    } else {
        vec!["/bin/sh".to_owned(), "-c".to_owned(), script.to_owned()]
    }
}

#[tokio::test]
async fn executor_and_worker_loss_never_replay_commands_and_recover() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let workspace = tempfile::tempdir()?;
        let workspace_path = workspace.path().canonicalize()?;
        let workspace_directory = workspace_path.to_str().context("workspace path")?;
        let slow = if cfg!(windows) { "echo run>> runs.txt & ping -n 60 127.0.0.1 > nul" } else { "echo run >> runs.txt; sleep 60" };
        let provider = create_mock_responses_server_sequence_unchecked(vec![
            create_command_execution_sse_response(shell(slow), /*workdir*/ None, Some(/*timeout_ms*/ 120_000), "slow-call")?,
            create_final_assistant_message_sse_response("lost")?,
            create_command_execution_sse_response(shell(if cfg!(windows) { "cd > where.txt" } else { "pwd > where.txt" }), /*workdir*/ None, Some(/*timeout_ms*/ 10_000), "where-call")?,
            create_final_assistant_message_sse_response("resumed")?,
            create_final_assistant_message_sse_response("restarted")?,
        ]).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let directory = AbsolutePathBuf::from_absolute_path(data.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, directory.clone(), TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let origin = base.trim_end_matches("/v1").to_owned();
        api.configure_environments(ENVIRONMENT_KEY.into(), format!("{origin}/registry"))?;
        let client = reqwest::Client::new();
        let session = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({"agent":{"model":"mock-model"},
            "environment":{"type":"self_hosted","workspace_directory":workspace_directory}})).await?;
        let session_id = session["id"].as_str().context("session id")?.to_owned();
        let environment_id = session["environment"]["id"].as_str().context("environment id")?.to_owned();
        let url = format!("{base}/agents/sessions/{session_id}");
        let mut stream = client.get(format!("{url}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
        let running = executor(&format!("{origin}/registry"), &environment_id)?;
        until_event(&mut stream, "agent.session.environment.connected").await?;

        // The executor dies mid-command. The turn still ends, with the command
        // reported to the model as failed, and nothing re-runs it later.
        assert_eq!(send(&client, &url, message("start the slow command")).await?, reqwest::StatusCode::ACCEPTED);
        let runs = workspace_path.join("runs.txt");
        while !runs.exists() {
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        }
        running.abort();
        let events = until_idle(&mut stream).await?;
        assert!(lifecycle(&events).contains(&"agent.session.environment.disconnected".to_owned()), "{events:?}");
        let output = provider.received_requests().await.context("requests")?.iter()
            .filter_map(|request| serde_json::from_slice::<Value>(&request.body).ok())
            .flat_map(|body| body["input"].as_array().cloned().unwrap_or_default())
            .find(|item| item["type"] == "function_call_output" && item["call_id"] == "slow-call")
            .context("slow command output reached the model")?;
        let reported = output["output"].as_str().unwrap_or_default();
        assert!(reported.starts_with("exec_command failed"), "{reported}");
        assert_eq!(command_item(&client, &url, "slow-call").await?["status"], "failed");

        // A replacement executor and a replacement worker: the environment is
        // attached again and the thread resumes in the same workspace.
        let running = executor(&format!("{origin}/registry"), &environment_id)?;
        until_event(&mut stream, "agent.session.environment.connected").await?;
        api.reconnect(backend(home.path()).await?).await?;
        let mut stream = client.get(format!("{url}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
        assert_eq!(send(&client, &url, message("where now?")).await?, reqwest::StatusCode::ACCEPTED);
        until_idle(&mut stream).await?;
        let reported = std::fs::read_to_string(workspace_path.join("where.txt"))?;
        assert_eq!(std::fs::canonicalize(reported.trim())?, workspace_path);

        // Input waiting for the executor when the service stops is dropped and
        // fails the session on restart; the restarted service still reports
        // and uses the environment.
        running.abort();
        until_event(&mut stream, "agent.session.environment.disconnected").await?;
        assert_eq!(send(&client, &url, message("waiting at shutdown")).await?, reqwest::StatusCode::ACCEPTED);
        server.abort();
        api.shutdown().await?;
        let api = AgentsApi::new(backend(home.path()).await?, directory, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let origin = base.trim_end_matches("/v1").to_owned();
        api.configure_environments(ENVIRONMENT_KEY.into(), format!("{origin}/registry"))?;
        let url = format!("{base}/agents/sessions/{session_id}");
        let failed = request(&client, reqwest::Method::GET, &url, Value::Null).await?;
        assert_eq!(
            (&failed["status"], &failed["error"], &failed["required_actions"]),
            (&json!("failed"), &json!("the service restarted while input waited for the environment; that input was dropped"), &json!([]))
        );
        let mut stream = client.get(format!("{url}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
        let _running = executor(&format!("{origin}/registry"), &environment_id)?;
        until_event(&mut stream, "agent.session.environment.connected").await?;
        assert_eq!(send(&client, &url, message("after restart")).await?, reqwest::StatusCode::ACCEPTED);
        until_idle(&mut stream).await?;
        assert_eq!(std::fs::read_to_string(&runs)?.lines().count(), 1);
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(turns["data"].as_array().map(Vec::len), Some(3));
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn environment_files_are_written_and_listed_inside_the_workspace() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let workspace = tempfile::tempdir()?;
        let workspace_path = workspace.path().canonicalize()?;
        let workspace_directory = workspace_path.to_str().context("workspace path")?;
        let provider = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let origin = base.trim_end_matches("/v1").to_owned();
        api.configure_environments(ENVIRONMENT_KEY.into(), format!("{origin}/registry"))?;
        let client = reqwest::Client::new();
        let session = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({"agent":{"model":"mock-model"},
            "environment":{"type":"self_hosted","workspace_directory":workspace_directory}})).await?;
        let environment_id = session["environment"]["id"].as_str().context("environment id")?.to_owned();
        let files = format!("{base}/agents/environments/{environment_id}/files");
        let inline = |path: &str, contents: &str| {
            json!({"type":"inline","path":path,"data":base64::Engine::encode(&base64::engine::general_purpose::STANDARD, contents)})
        };
        let inside = |relative: &str| workspace_path.join(relative).to_string_lossy().into_owned();
        let status = |response: reqwest::Response| response.status();

        // Files need a connected executor.
        let offline = client.post(&files).bearer_auth(TOKEN).json(&inline(&inside("early.txt"), "x")).send().await?;
        assert_eq!(status(offline), reqwest::StatusCode::CONFLICT);
        let mut stream = client.get(format!("{base}/agents/sessions/{}/events", session["id"].as_str().context("id")?))
            .bearer_auth(TOKEN).send().await?.error_for_status()?;
        let _running = executor(&format!("{origin}/registry"), &environment_id)?;
        until_event(&mut stream, "agent.session.environment.connected").await?;

        // Writes land in the workspace, creating directories; sources and
        // paths outside the contract are refused.
        let created = request(&client, reqwest::Method::POST, &files, inline(&inside("a/b.txt"), "hello")).await?;
        assert_eq!(created, json!({"object":"agent.environment.file","environment_id":environment_id,"path":inside("a/b.txt"),"size_bytes":5}));
        assert_eq!(std::fs::read_to_string(workspace_path.join("a/b.txt"))?, "hello");
        for (body, message) in [
            (inline(&inside("../escape.txt"), "x"), "path must be an absolute path inside the workspace directory"),
            (inline("relative.txt", "x"), "path must be an absolute path inside the workspace directory"),
            (inline(workspace_directory, "x"), "path must be an absolute path inside the workspace directory"),
            (json!({"type":"inline","path":inside("bad.txt"),"data":"not base64!"}), "data must be standard base64"),
            (json!({"type":"file_id","path":inside("f.txt"),"file_id":"file-missing"}), "file file-missing not found"),
        ] {
            let response = client.post(&files).bearer_auth(TOKEN).header("OpenAI-Beta", "agents=v1").json(&body).send().await?;
            let code = response.status();
            let reply = response.text().await?;
            assert_eq!(code, reqwest::StatusCode::BAD_REQUEST, "{body} {reply}");
            let error = serde_json::from_str::<Value>(&reply)?["error"]["message"].as_str().unwrap_or_default().to_owned();
            assert!(error.starts_with(message), "{body} {reply}");
        }
        assert!(!workspace_path.parent().context("parent")?.join("escape.txt").exists());

        // A Files API upload, larger than one relay frame, arrives whole.
        let large: Vec<u8> = (0..6 * 1024 * 1024).map(|index| (index % 251) as u8).collect();
        let uploaded = files::upload(&client, &base, &[("purpose", "user_data")], "large.bin", &large).await?.error_for_status()?.json::<Value>().await?;
        let copied = request(&client, reqwest::Method::POST, &files, json!({"type":"file_id","path":inside("large.bin"),"file_id":uploaded["id"]})).await?;
        assert_eq!(copied["size_bytes"], json!(large.len()));
        assert!(std::fs::read(workspace_path.join("large.bin"))? == large, "large file contents differ");
        std::fs::remove_file(workspace_path.join("large.bin"))?;

        // Listing orders by path components and pages with an opaque token.
        request(&client, reqwest::Method::POST, &files, inline(&inside("c.txt"), "ccc")).await?;
        request(&client, reqwest::Method::POST, &files, inline(&inside("a/d.txt"), "dd")).await?;
        let paths = |page: &Value| page["data"].as_array().into_iter().flatten()
            .map(|file| (file["path"].as_str().unwrap_or_default().to_owned(), file["size_bytes"].as_u64().unwrap_or_default()))
            .collect::<Vec<_>>();
        let first = request(&client, reqwest::Method::GET, &format!("{files}?order=asc&limit=2"), Value::Null).await?;
        assert_eq!((paths(&first), &first["has_more"]), (vec![(inside("a/b.txt"), 5), (inside("a/d.txt"), 2)], &json!(true)));
        let next = first["next"].as_str().context("next page")?;
        let second = request(&client, reqwest::Method::GET, &format!("{files}?order=asc&limit=2&page={next}"), Value::Null).await?;
        assert_eq!((paths(&second), &second["has_more"], &second["next"]), (vec![(inside("c.txt"), 3)], &json!(false), &Value::Null));
        let descending = request(&client, reqwest::Method::GET, &files, Value::Null).await?;
        assert_eq!(paths(&descending), vec![(inside("c.txt"), 3), (inside("a/d.txt"), 2), (inside("a/b.txt"), 5)]);
        let directory = request(&client, reqwest::Method::GET, &format!("{files}?order=asc&path={}", inside("a")), Value::Null).await?;
        assert_eq!(paths(&directory), vec![(inside("a/b.txt"), 5), (inside("a/d.txt"), 2)]);
        let bad_page = client.get(format!("{files}?page=%%%")).bearer_auth(TOKEN).send().await?;
        assert_eq!(bad_page.status(), reqwest::StatusCode::BAD_REQUEST);
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

/// A minimal stdio MCP server: answers initialize, lists one tool, and reports
/// its working directory and `FIXTURE_SECRET` when called.
const STDIO_MCP: &str = r#"while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"stdio-fixture","version":"1"}}}\n' "$id" ;;
    *'"method":"tools/list"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"where","description":"Report the directory","inputSchema":{"type":"object","properties":{}}}]}}\n' "$id" ;;
    *'"method":"tools/call"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"stdio-in-%s-with-%s"}]}}\n' "$id" "$(pwd)" "$FIXTURE_SECRET" ;;
  esac
done
"#;

#[tokio::test]
async fn mcp_servers_run_on_the_executor() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let workspace = tempfile::tempdir()?;
        let workspace_path = workspace.path().canonicalize()?;
        let workspace_directory = workspace_path.to_str().context("workspace path")?;
        // The stdio fixture is a POSIX shell script.
        let stdio = !cfg!(windows);
        let mut responses = vec![capabilities::call("mcp__warehouse.lookup", "http-call")];
        if stdio {
            responses.push(capabilities::call("mcp__local.where", "stdio-call"));
        }
        responses.push(create_final_assistant_message_sse_response("done")?);
        let provider = create_mock_responses_server_sequence_unchecked(responses).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let (mcp_url, calls, _mcp) = mcp::mcp_server().await?;
        std::fs::write(workspace_path.join("server.sh"), STDIO_MCP)?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let origin = base.trim_end_matches("/v1").to_owned();
        api.configure_environments(ENVIRONMENT_KEY.into(), format!("{origin}/registry"))?;
        let client = reqwest::Client::new();
        let sessions = format!("{base}/agents/sessions");
        // The loopback server is reachable only from the executor's side: the
        // operator never allowed it for service-origin connections.
        let mut tools = vec![json!({"type":"mcp","server_label":"warehouse","connection_origin":"environment",
            "allowed_tools":["lookup"],"transport":{"type":"http","server_url":mcp_url,"headers":{"x-tenant":"acme"}}})];
        if stdio {
            tools.push(json!({"type":"mcp","server_label":"local","allowed_tools":["where"],
                "transport":{"type":"stdio","command":"/bin/sh","args":["server.sh"],"cwd":workspace_directory,
                    "env":{"FIXTURE_SECRET":"s3cret-value"}}}));
        }
        let agent = json!({"model":"mock-model","tools":tools});
        let agents = format!("{base}/agents");
        let beta = |builder: reqwest::RequestBuilder| builder.bearer_auth(TOKEN).header("OpenAI-Beta", "agents=v1");

        // Literal env values are secrets: they need the vault store, are
        // never returned, and travel only to the executor.
        if stdio {
            let disabled = beta(client.post(&agents)).json(&agent).send().await?;
            assert_eq!(disabled.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
            api.configure_vault("test-vault-passphrase-for-stdio-env-values".into()).await?;
        }
        let saved: Value = beta(client.post(&agents)).json(&agent).send().await?.error_for_status()?.json().await?;
        let read: Value = beta(client.get(format!("{agents}/{}", saved["id"].as_str().context("agent id")?))).send().await?.error_for_status()?.json().await?;
        assert!(!read.to_string().contains("s3cret-value") && !saved.to_string().contains("s3cret-value"), "{read}");

        // Executor-run servers need a self-hosted environment.
        let refused = client.post(&sessions).bearer_auth(TOKEN).header("OpenAI-Beta", "agents=v1")
            .json(&json!({"agent":agent,"environment":{"type":"none"},"input":"x"})).send().await?;
        assert_eq!(refused.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(refused.json::<Value>().await?["error"]["message"], "stdio and environment-origin MCP servers need a self_hosted environment");

        let session = request(&client, reqwest::Method::POST, &sessions, json!({"agent_id":saved["id"],
            "environment":{"type":"self_hosted","workspace_directory":workspace_directory}})).await?;
        assert!(!session.to_string().contains("s3cret-value"), "{session}");
        let environment_id = session["environment"]["id"].as_str().context("environment id")?.to_owned();
        let url = format!("{sessions}/{}", session["id"].as_str().context("session id")?);
        let mut stream = client.get(format!("{url}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
        let _running = executor(&format!("{origin}/registry"), &environment_id)?;
        until_event(&mut stream, "agent.session.environment.connected").await?;
        assert_eq!(send(&client, &url, message("use the tools")).await?, reqwest::StatusCode::ACCEPTED);
        until_idle(&mut stream).await?;

        assert_eq!(*calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner), vec![json!({"tool":"lookup","tenant":"acme"})]);
        let outputs: Vec<(String, String)> = provider.received_requests().await.context("requests")?.iter()
            .filter_map(|request| serde_json::from_slice::<Value>(&request.body).ok())
            .flat_map(|body| body["input"].as_array().cloned().unwrap_or_default())
            .filter(|item| item["type"] == "function_call_output")
            .map(|item| (item["call_id"].as_str().unwrap_or_default().to_owned(), item["output"].to_string()))
            .collect();
        let output = |call: &str| outputs.iter().find(|(id, _)| id == call).map(|(_, output)| output.clone()).unwrap_or_default();
        assert!(output("http-call").contains("mcp-result-731"), "{outputs:?}");
        if stdio {
            assert!(output("stdio-call").contains(&format!("stdio-in-{workspace_directory}-with-s3cret-value")), "{outputs:?}");
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

/// The operator's connection wait (one second here, five minutes by default)
/// bounds how long input waits for an executor.
#[tokio::test]
async fn input_waiting_past_the_connection_wait_fails_the_session() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let workspace = tempfile::tempdir()?;
        let workspace_directory = workspace.path().to_str().context("workspace path")?.to_owned();
        let provider = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        api.configure_environments(ENVIRONMENT_KEY.into(), format!("{}/registry", base.trim_end_matches("/v1")))?;
        api.set_environment_connection_wait(Duration::from_secs(/*secs*/ 1));
        let client = reqwest::Client::new();
        let session = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({"agent":{"model":"mock-model"},
            "environment":{"type":"self_hosted","workspace_directory":workspace_directory}})).await?;
        let environment_id = session["environment"]["id"].clone();
        let url = format!("{base}/agents/sessions/{}", session["id"].as_str().context("session id")?);
        let mut stream = client.get(format!("{url}/events")).bearer_auth(TOKEN).send().await?.error_for_status()?;
        let started = std::time::Instant::now();
        assert_eq!(send(&client, &url, message("anyone there?")).await?, reqwest::StatusCode::ACCEPTED);
        let events = until_event(&mut stream, "agent.session.environment.failed").await?;
        assert!(started.elapsed() >= Duration::from_secs(/*secs*/ 1), "{:?}", started.elapsed());
        let timed_out = "the environment did not connect in time; the input waiting for it was dropped";
        assert_eq!(lifecycle(&events), vec!["agent.session.requires_action", "agent.session.environment.failed"]);
        assert_eq!(events.last().map(|event| &event["environment"]), Some(&json!({"id":environment_id,"type":"self_hosted",
            "status":"failed","error":{"type":"environment_error","code":"environment_connection_timeout","message":timed_out}})));
        let failed = request(&client, reqwest::Method::GET, &url, Value::Null).await?;
        assert_eq!((&failed["status"], &failed["error"], &failed["required_actions"]), (&json!("failed"), &json!(timed_out), &json!([])));
        let turns = request(&client, reqwest::Method::GET, &format!("{url}/turns"), Value::Null).await?;
        assert_eq!(turns["data"], json!([]));
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires CODEX_AGENTS_API_SDK_PYTHON pointing to a Python with openai==3.17.0"]
async fn official_sdk_self_hosted_environment() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let python = std::env::var("CODEX_AGENTS_API_SDK_PYTHON")?;
        let script = codex_utils_cargo_bin::find_resource!("tests/sdk_environments.py")?;
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let workspace = tempfile::tempdir()?;
        let workspace_path = workspace.path().canonicalize()?;
        let workspace_directory = workspace_path.to_str().context("workspace path")?;
        let provider = create_mock_responses_server_sequence_unchecked(vec![
            create_command_execution_sse_response(
                shell("echo sdk"),
                /*workdir*/ None,
                Some(/*timeout_ms*/ 10_000),
                "sdk-command",
            )?,
            create_final_assistant_message_sse_response("done")?,
        ])
        .await;
        MockResponsesConfig::new(&provider.uri())
            .with_root_config("features.plugins = false")
            .write(home.path())?;
        let api = AgentsApi::new(
            backend(home.path()).await?,
            AbsolutePathBuf::from_absolute_path(data.path())?,
            TOKEN.into(),
        )
        .await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let origin = base.trim_end_matches("/v1").to_owned();
        api.configure_environments(ENVIRONMENT_KEY.into(), format!("{origin}/registry"))?;
        api.set_environment_connection_wait(Duration::from_secs(/*secs*/ 60));
        let client = reqwest::Client::new();
        let session = request(
            &client,
            reqwest::Method::POST,
            &format!("{base}/agents/sessions"),
            json!({"agent":{"model":"mock-model"},
            "environment":{"type":"self_hosted","workspace_directory":workspace_directory}}),
        )
        .await?;
        let session_id = session["id"].as_str().context("session id")?.to_owned();
        let environment_id = session["environment"]["id"]
            .as_str()
            .context("environment id")?
            .to_owned();
        let mut stream = client
            .get(format!("{base}/agents/sessions/{session_id}/events"))
            .bearer_auth(TOKEN)
            .send()
            .await?
            .error_for_status()?;
        let _running = executor(&format!("{origin}/registry"), &environment_id)?;
        until_event(&mut stream, "agent.session.environment.connected").await?;
        let output = tokio::process::Command::new(&python)
            .arg(&script)
            .arg(&base)
            .arg(&session_id)
            .arg(workspace_directory)
            .kill_on_drop(/*kill_on_drop*/ true)
            .output()
            .await?;
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
