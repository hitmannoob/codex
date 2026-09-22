// Exercise the private process owner without expanding the library's public API.
#[path = "../src/runtime.rs"]
mod runtime;

use anyhow::Context;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_cargo_bin::cargo_bin;
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(/*secs*/ 30);

async fn worker(home: &std::path::Path) -> anyhow::Result<runtime::Worker> {
    runtime::Worker::spawn(
        AbsolutePathBuf::try_from(cargo_bin("codex-app-server")?)?,
        AbsolutePathBuf::from_absolute_path(home)?,
    )
    .await
}

#[tokio::test]
async fn managed_worker_initializes_locks_home_and_cleans_up() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let owned = worker(home.path()).await?;
    assert!(owned.id().is_some());
    let socket = owned.socket().clone();
    let client = runtime::connect(socket.clone(), DEADLINE).await?;
    assert!(client.server_version().is_some());
    assert!(worker(home.path()).await.is_err());
    client.shutdown().await?;
    owned.shutdown().await?;
    assert!(!socket.exists());
    // A subsequent owner can reuse the same persistent home.
    worker(home.path()).await?.shutdown().await?;
    assert!(home.path().exists());
    Ok(())
}

#[tokio::test]
async fn startup_exit_is_observed_and_reaped() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    std::fs::write(home.path().join("config.toml"), "invalid = [")?;
    let mut owned = Some(worker(home.path()).await?);
    let error = tokio::time::timeout(DEADLINE, runtime::worker_exit(&mut owned))
        .await?
        .expect_err("invalid configuration must exit");
    assert!(error.to_string().contains("managed app-server exited"));
    owned.context("owned worker")?.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn readiness_deadline_includes_a_stalled_handshake() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let private = directory.path().join("private");
    codex_uds::prepare_private_socket_directory(&private).await?;
    let socket = AbsolutePathBuf::try_from(private.join("worker.sock"))?;
    let mut listener = codex_uds::UnixListener::bind(&socket).await?;
    let connecting = runtime::connect(socket, Duration::from_millis(/*millis*/ 200));
    let stalled = async {
        let _peer = listener.accept().await?;
        std::future::pending::<std::io::Result<()>>().await
    };
    let result = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), async {
        tokio::select! {
            result = connecting => result.map(|_| ()),
            result = stalled => result.map_err(Into::into),
        }
    })
    .await?;
    assert!(
        result
            .expect_err("initialization must time out")
            .to_string()
            .contains("initialization timed out")
    );
    Ok(())
}

#[tokio::test]
async fn missing_worker_executable_is_reported() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let home = AbsolutePathBuf::from_absolute_path(home.path())?;
    let error = runtime::Worker::spawn(home.join("missing-worker"), home)
        .await
        .err()
        .context("missing executable should fail")?;
    assert!(error.to_string().contains("failed to start app-server"));
    Ok(())
}

#[cfg(unix)]
#[path = "suite/runtime_cli.rs"]
mod runtime_cli;
