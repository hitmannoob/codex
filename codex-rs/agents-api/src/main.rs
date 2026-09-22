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
    // Register SIGTERM before starting the child, including during readiness.
    let stopping = shutdown_signal();
    tokio::pin!(stopping);
    let mut worker = None;
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
            let owned = runtime::Worker::spawn(executable, home).await?;
            eprintln!(
                "agents-api managed worker pid={}",
                owned.id().context("worker process ID")?
            );
            let socket = owned.socket().clone();
            worker = Some(owned);
            socket
        }
    };
    let serving = async {
        let client = tokio::select! {
            result = runtime::connect(socket, Duration::from_secs(args.worker_startup_timeout_secs)) => result?,
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
        let result = tokio::select! {
            result = &mut http => result.context("HTTP server task failed").and_then(|result| result.map_err(Into::into)),
            result = runtime::worker_exit(&mut worker) => result,
            result = &mut stopping => result,
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
