mod runtime;

use anyhow::Context;
use clap::Parser;
use codex_agents_api::AgentsApi;
use codex_app_server_client::AppServerClient;
use codex_utils_absolute_path::AbsolutePathBuf;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:4501")]
    listen: SocketAddr,
    /// Connect to a caller-owned worker instead of starting a managed worker.
    #[arg(long, conflicts_with_all = ["app_server_bin", "codex_home"])]
    app_server_socket: Option<PathBuf>,
    /// Managed worker executable; defaults to the sibling codex-app-server binary.
    #[arg(long)]
    app_server_bin: Option<PathBuf>,
    /// Persistent worker configuration/history; defaults to DATA_DIRECTORY/codex-home.
    #[arg(long)]
    codex_home: Option<PathBuf>,
    #[arg(long, default_value_t = 30)]
    worker_startup_timeout_secs: u64,
    #[arg(long)]
    data_directory: PathBuf,
    #[arg(long, env = "CODEX_AGENTS_API_TOKEN", hide_env_values = true)]
    token: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.listen.ip().is_loopback(),
        "initial API supports loopback listeners only"
    );
    anyhow::ensure!(
        args.token.len() >= 32,
        "API token must contain at least 32 bytes"
    );
    let directory = AbsolutePathBuf::relative_to_current_dir(args.data_directory)?;
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    let startup_timeout = Duration::from_secs(args.worker_startup_timeout_secs);
    // Register SIGTERM before starting the child, including during readiness.
    let stopping = shutdown_signal();
    tokio::pin!(stopping);
    let mut worker = None;
    // Managed executable/home are retained so a crashed worker can be respawned;
    // external-socket mode leaves this `None` and is never restarted.
    let mut managed = None;
    let socket = match args.app_server_socket {
        Some(socket) => AbsolutePathBuf::relative_to_current_dir(socket)?,
        None => {
            let executable =
                match args.app_server_bin {
                    Some(path) => AbsolutePathBuf::relative_to_current_dir(path)?,
                    None => AbsolutePathBuf::try_from(std::env::current_exe()?.with_file_name(
                        format!("codex-app-server{}", std::env::consts::EXE_SUFFIX),
                    ))?,
                };
            let home = match args.codex_home {
                Some(path) => AbsolutePathBuf::relative_to_current_dir(path)?,
                None => directory.join("codex-home"),
            };
            // Reclaim a worker orphaned by a prior API-process crash before
            // starting our own, so two app-servers never share this home.
            runtime::reclaim_orphan(&home).await?;
            let owned = runtime::Worker::spawn(executable.clone(), home.clone()).await?;
            eprintln!(
                "agents-api managed worker pid={}",
                owned.id().context("worker process ID")?
            );
            let socket = owned.socket().clone();
            worker = Some(owned);
            managed = Some((executable, home));
            socket
        }
    };
    let serving = async {
        let client = tokio::select! {
            result = runtime::connect(socket, startup_timeout) => result?,
            result = runtime::worker_exit(&mut worker) => return result,
            result = &mut stopping => return result,
        };
        let api = AgentsApi::new(AppServerClient::Remote(client), directory, args.token).await?;
        eprintln!("agents-api listening on {}", listener.local_addr()?);
        let (stop_http, stopping_http) = tokio::sync::oneshot::channel();
        let mut http = tokio::spawn(
            axum::serve(listener, api.router())
                .with_graceful_shutdown(async { let _ = stopping_http.await; })
                .into_future(),
        );
        // Keep serving durable reads while a crashed managed worker is replaced.
        // A lost worker no longer stops the process; only http failure, restart
        // exhaustion, or a shutdown signal ends the loop.
        let http_result = |result: Result<std::io::Result<()>, tokio::task::JoinError>| {
            result
                .context("HTTP server task failed")
                .and_then(|result| result.map_err(Into::into))
        };
        let result: anyhow::Result<()> = loop {
            tokio::select! {
                result = &mut http => break http_result(result),
                result = &mut stopping => break result,
                _ = runtime::worker_exit(&mut worker) => {
                    eprintln!("agents-api: managed worker exited; attempting restart");
                    let restarted = tokio::select! {
                        result = restart_worker(&mut worker, &api, &managed, startup_timeout) => result,
                        result = &mut http => break http_result(result),
                        result = &mut stopping => break result,
                    };
                    if let Err(error) = restarted {
                        break Err(error);
                    }
                }
            }
        };
        let _ = stop_http.send(());
        let cleanup = tokio::time::timeout(Duration::from_secs(/*secs*/ 10), async {
            let cleanup = api.shutdown().await;
            if !http.is_finished() {
                (&mut http).await??;
            }
            cleanup
        }).await.context("API shutdown timed out");
        http.abort();
        let cleanup = cleanup?;
        result.and(cleanup)
    }.await;
    let cleanup = match worker {
        Some(worker) => worker.shutdown().await,
        None => Ok(()),
    };
    // Run both cleanups even when serving or initialization fails.
    if let Err(error) = &cleanup {
        eprintln!("agents-api: worker cleanup failed: {error:#}");
    }
    serving.and(cleanup)
}

/// Bounded restarts prevent an unrecoverable worker from spinning forever;
/// exhaustion ends the process so the failure is visible to an operator.
const WORKER_RESTART_ATTEMPTS: usize = 5;
const WORKER_RESTART_BACKOFF: Duration = Duration::from_millis(/*millis*/ 200);
const WORKER_RESTART_BACKOFF_MAX: Duration = Duration::from_secs(/*secs*/ 5);

/// Reap the crashed worker, then respawn and reattach with bounded backoff.
///
/// The dead worker is dropped first so its home lock and socket are released
/// before a replacement claims them. Each replacement completes the app-server
/// initialization handshake (via `connect`) before `reconnect` routes requests
/// to it. Only managed workers are restarted; an external worker returns an
/// error rather than being respawned.
async fn restart_worker(
    worker: &mut Option<runtime::Worker>,
    api: &AgentsApi,
    managed: &Option<(AbsolutePathBuf, AbsolutePathBuf)>,
    startup_timeout: Duration,
) -> anyhow::Result<()> {
    let (executable, home) = match managed {
        Some(managed) => managed,
        None => anyhow::bail!("cannot restart an externally managed worker"),
    };
    if let Some(dead) = worker.take()
        && let Err(error) = dead.shutdown().await
    {
        eprintln!("agents-api: reaping crashed worker failed: {error:#}");
    }
    let mut backoff = WORKER_RESTART_BACKOFF;
    let mut last_error = None;
    for attempt in 1..=WORKER_RESTART_ATTEMPTS {
        tokio::time::sleep(backoff).await;
        match respawn(executable, home, startup_timeout).await {
            Ok((replacement, client)) => {
                let pid = replacement.id().context("worker process ID")?;
                api.reconnect(client).await?;
                *worker = Some(replacement);
                eprintln!("agents-api managed worker restarted pid={pid} attempt={attempt}");
                return Ok(());
            }
            Err(error) => {
                eprintln!("agents-api: worker restart attempt {attempt} failed: {error:#}");
                last_error = Some(error);
                backoff = (backoff * 2).min(WORKER_RESTART_BACKOFF_MAX);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("worker restart produced no error")))
        .context("managed worker restart attempts exhausted")
}

/// Spawn one replacement worker and connect an initialized client to it,
/// failing fast if the fresh process exits before the handshake completes.
async fn respawn(
    executable: &AbsolutePathBuf,
    home: &AbsolutePathBuf,
    startup_timeout: Duration,
) -> anyhow::Result<(runtime::Worker, AppServerClient)> {
    let mut worker = Some(runtime::Worker::spawn(executable.clone(), home.clone()).await?);
    let socket = match &worker {
        Some(worker) => worker.socket().clone(),
        None => anyhow::bail!("worker missing after spawn"),
    };
    let client = tokio::select! {
        result = runtime::connect(socket, startup_timeout) => result?,
        result = runtime::worker_exit(&mut worker) => {
            return Err(result
                .err()
                .unwrap_or_else(|| anyhow::anyhow!("worker exited during restart"))
                .context("managed worker exited before initialization"));
        }
    };
    match worker {
        Some(worker) => Ok((worker, AppServerClient::Remote(client))),
        None => anyhow::bail!("worker vanished during initialization"),
    }
}

fn shutdown_signal() -> impl Future<Output = anyhow::Result<()>> {
    #[cfg(unix)]
    let termination = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
    #[cfg(unix)]
    let interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt());
    async move {
        #[cfg(unix)]
        {
            let mut termination = termination?;
            let mut interrupt = interrupt?;
            tokio::select! {
                _ = interrupt.recv() => {},
                _ = termination.recv() => {},
            }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await?;
        Ok(())
    }
}
