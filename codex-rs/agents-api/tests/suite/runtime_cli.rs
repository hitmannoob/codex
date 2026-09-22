use super::*;
use app_test_support::MockResponsesConfig;
use app_test_support::create_mock_responses_server_repeating_assistant;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::path::Path;
use std::process::Stdio;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Child;
use tokio::process::Command;

const TOKEN: &str = "test-token-for-managed-agents-api";

async fn start_api(
    data: &Path,
    socket: Option<&AbsolutePathBuf>,
) -> anyhow::Result<(Child, String, Option<u32>)> {
    let mut command = Command::new(cargo_bin("codex-agents-api")?);
    command
        .args(["--listen", "127.0.0.1:0", "--data-directory"])
        .arg(data)
        .env("CODEX_AGENTS_API_TOKEN", TOKEN)
        .env("CODEX_APP_SERVER_DISABLE_MANAGED_CONFIG", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    match socket {
        Some(socket) => {
            command.arg("--app-server-socket").arg(socket.as_path());
        }
        None => {
            command
                .arg("--app-server-bin")
                .arg(cargo_bin("codex-app-server")?);
        }
    }
    let mut child = command.spawn()?;
    let mut lines = BufReader::new(child.stderr.take().context("stderr")?).lines();
    let mut worker_pid = None;
    let base = tokio::time::timeout(DEADLINE, async {
        while let Some(line) = lines.next_line().await? {
            if let Some(pid) = line.strip_prefix("agents-api managed worker pid=") {
                worker_pid = Some(pid.parse()?);
            }
            if let Some(address) = line.strip_prefix("agents-api listening on ") {
                return anyhow::Ok(format!("http://{address}/v1"));
            }
            eprintln!("{line}");
        }
        anyhow::bail!("API exited before readiness: {}", child.wait().await?);
    })
    .await??;
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("{line}");
        }
    });
    Ok((child, base, worker_pid))
}

async fn stop_api(mut child: Child) -> anyhow::Result<()> {
    let pid = i32::try_from(child.id().context("API process ID")?)?;
    // SAFETY: this PID belongs to our live, unreaped child process.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let status = tokio::time::timeout(DEADLINE, child.wait()).await??;
    assert!(status.success(), "API shutdown failed: {status}");
    Ok(())
}

async fn turns(client: &reqwest::Client, url: &str, count: usize) -> anyhow::Result<Value> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let response: Value = client
                .get(format!("{url}/turns"))
                .bearer_auth(TOKEN)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let data = response["data"].as_array().context("turns")?;
            if data.len() == count && data.iter().all(|turn| turn["status"] == "completed") {
                return anyhow::Ok(response);
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn cli_manages_worker_and_resumes_saved_session_after_restart() -> anyhow::Result<()> {
    let data = tempfile::tempdir()?;
    let home = data.path().join("codex-home");
    std::fs::create_dir_all(&home)?;
    let model = create_mock_responses_server_repeating_assistant("finished").await;
    MockResponsesConfig::new(&model.uri())
        .with_root_config("features.plugins = false")
        .write(&home)?;
    let client = reqwest::Client::new();
    let (api, base, _) = start_api(data.path(), /*socket*/ None).await?;
    let session: Value = client.post(format!("{base}/agents/sessions")).bearer_auth(TOKEN)
        .json(&json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":"Remember orange-731"}))
        .send().await?.error_for_status()?.json().await?;
    let id = session["id"].as_str().context("session id")?;
    let saved = turns(
        &client,
        &format!("{base}/agents/sessions/{id}"),
        /*count*/ 1,
    )
    .await?;
    stop_api(api).await?;
    let (api, base, _) = start_api(data.path(), /*socket*/ None).await?;
    let url = format!("{base}/agents/sessions/{id}");
    assert_eq!(turns(&client, &url, /*count*/ 1).await?, saved);
    client.post(format!("{url}/events")).bearer_auth(TOKEN)
        .json(&json!({"events":[{"type":"agent.session.input.message", "input":"What code did I ask you to remember?"}]}))
        .send().await?.error_for_status()?;
    turns(&client, &url, /*count*/ 2).await?;
    stop_api(api).await?;
    let requests = model.received_requests().await.context("model requests")?;
    let last: Value = serde_json::from_slice(&requests.last().context("last request")?.body)?;
    assert!(last["input"].to_string().contains("orange-731"));
    Ok(())
}

#[tokio::test]
async fn api_shutdown_preserves_external_worker() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let data = tempfile::tempdir()?;
    let owned = worker(home.path()).await?;
    let (api, _, _) = start_api(data.path(), Some(owned.socket())).await?;
    stop_api(api).await?;
    // A full initialization still succeeds after the API has exited.
    runtime::connect(owned.socket().clone(), DEADLINE)
        .await?
        .shutdown()
        .await?;
    owned.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn worker_crash_stops_the_api() -> anyhow::Result<()> {
    let data = tempfile::tempdir()?;
    let (mut api, _, pid) = start_api(data.path(), /*socket*/ None).await?;
    let pid = i32::try_from(pid.context("managed worker PID")?)?;
    // SAFETY: the API owns this live worker and has not reaped it.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    let status = tokio::time::timeout(DEADLINE, api.wait()).await??;
    assert!(
        !status.success(),
        "worker loss must not report a successful API exit"
    );
    Ok(())
}

#[tokio::test]
async fn unresponsive_worker_is_forced_down_and_reaped() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let owned = worker(home.path()).await?;
    runtime::connect(owned.socket().clone(), DEADLINE)
        .await?
        .shutdown()
        .await?;
    let pid = i32::try_from(owned.id().context("worker PID")?)?;
    // SAFETY: this PID belongs to the live, unreaped worker.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0);
    tokio::time::timeout(DEADLINE, owned.shutdown()).await??;
    // kill(pid, 0) checks existence without delivering a signal; reaping must be complete.
    assert_eq!(
        unsafe {
            libc::kill(pid, /*sig*/ 0)
        },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    Ok(())
}

#[tokio::test]
async fn startup_timeout_reaps_the_owned_process() -> anyhow::Result<()> {
    let data = tempfile::tempdir()?;
    let output = tokio::time::timeout(
        DEADLINE,
        Command::new(cargo_bin("codex-agents-api")?)
            .args([
                "--listen",
                "127.0.0.1:0",
                "--worker-startup-timeout-secs",
                "0",
                "--data-directory",
            ])
            .arg(data.path())
            .arg("--app-server-bin")
            .arg(cargo_bin("codex-app-server")?)
            .env("CODEX_AGENTS_API_TOKEN", TOKEN)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("initialization timed out"), "{stderr}");
    let pid: i32 = stderr
        .lines()
        .find_map(|line| line.strip_prefix("agents-api managed worker pid="))
        .context("worker PID")?
        .parse()?;
    // SAFETY: signal zero only checks process existence.
    assert_eq!(
        unsafe {
            libc::kill(pid, /*sig*/ 0)
        },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    Ok(())
}
