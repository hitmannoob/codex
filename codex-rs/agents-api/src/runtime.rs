use anyhow::Context;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_utils_absolute_path::AbsolutePathBuf;
use std::fs::File;
use std::process::ExitStatus;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Child;
use tokio::process::Command;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(/*secs*/ 10);

/// Owns one local harness process, its private socket, and its persistent-home lock.
pub(crate) struct Worker {
    child: Child,
    socket: AbsolutePathBuf,
    _socket_directory: tempfile::TempDir,
    _home_lock: File,
}

impl Worker {
    pub(crate) async fn spawn(
        executable: AbsolutePathBuf,
        home: AbsolutePathBuf,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&home).await?;
        let home_lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(home.join("agents-api-worker.lock"))?;
        home_lock
            .try_lock()
            .context("worker home is already in use")?;
        // Keep the socket path short even when the persistent data path is deep.
        let socket_directory = tempfile::Builder::new().prefix("agents-").tempdir()?;
        let private_directory = socket_directory.path().join("private");
        codex_uds::prepare_private_socket_directory(&private_directory).await?;
        let socket = AbsolutePathBuf::try_from(private_directory.join("worker.sock"))?;
        let mut command = Command::new(executable.as_path());
        command
            .arg("--listen")
            .arg(format!("unix://{}", socket.display()))
            .arg("--strict-config")
            .env("CODEX_HOME", home.as_path())
            .env_remove("CODEX_AGENTS_API_TOKEN")
            .current_dir(home.as_path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        #[cfg(windows)]
        command.env(codex_app_server_transport::DAEMON_SHUTDOWN_SOCKET_ENV, "1");
        let child = command
            .spawn()
            .with_context(|| format!("failed to start app-server at {}", executable.display()))?;
        Ok(Self {
            child,
            socket,
            _socket_directory: socket_directory,
            _home_lock: home_lock,
        })
    }

    pub(crate) fn id(&self) -> Option<u32> {
        self.child.id()
    }

    pub(crate) fn socket(&self) -> &AbsolutePathBuf {
        &self.socket
    }

    pub(crate) async fn wait(&mut self) -> std::io::Result<ExitStatus> {
        self.child.wait().await
    }

    pub(crate) async fn shutdown(mut self) -> anyhow::Result<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }
        let pid = self.child.id().context("worker has no process ID")?;
        let graceful = async {
            #[cfg(unix)]
            {
                let pid = i32::try_from(pid)?;
                // SAFETY: this PID belongs to our live, unreaped Child.
                if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            #[cfg(windows)]
            {
                use futures::SinkExt;
                use futures::StreamExt;
                use tokio_tungstenite::tungstenite::Message;

                let stream = codex_uds::UnixStream::connect(self.socket.as_path()).await?;
                stream.ensure_non_elevated_peer()?;
                let (mut socket, _) =
                    tokio_tungstenite::client_async("ws://localhost/daemon/shutdown", stream)
                        .await?;
                socket.send(Message::Text(pid.to_string().into())).await?;
                let reply = socket
                    .next()
                    .await
                    .context("missing shutdown acknowledgment")??;
                anyhow::ensure!(
                    matches!(reply, Message::Text(ack) if ack == pid.to_string()),
                    "shutdown acknowledgment did not match worker"
                );
                socket.close(None).await?;
            }
            self.child.wait().await?;
            anyhow::Ok(())
        };
        match tokio::time::timeout(SHUTDOWN_TIMEOUT, graceful).await {
            Ok(Ok(())) => Ok(()),
            result => {
                if self.child.try_wait()?.is_some() {
                    return Ok(());
                }
                eprintln!("agents-api: forcing worker shutdown after {result:?}");
                self.child
                    .kill()
                    .await
                    .context("failed to stop and reap worker")
            }
        }
    }
}

pub(crate) async fn connect(
    socket: AbsolutePathBuf,
    deadline: Duration,
) -> anyhow::Result<RemoteAppServerClient> {
    let mut last_error = None;
    let connecting = async {
        loop {
            match RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
                endpoint: RemoteAppServerEndpoint::UnixSocket {
                    socket_path: socket.clone(),
                },
                client_name: "codex_agents_api".into(),
                client_version: env!("CARGO_PKG_VERSION").into(),
                experimental_api: true,
                mcp_server_openai_form_elicitation: false,
                opt_out_notification_methods: vec![],
                channel_capacity: 128,
            })
            .await
            {
                Ok(client) => return client,
                Err(error) => last_error = Some(error),
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 50)).await;
        }
    };
    tokio::time::timeout(deadline, connecting)
        .await
        .with_context(|| {
            format!(
                "app-server initialization timed out at {}: {last_error:?}",
                socket.display()
            )
        })
}

pub(crate) async fn worker_exit(worker: &mut Option<Worker>) -> anyhow::Result<()> {
    match worker {
        Some(worker) => anyhow::bail!("managed app-server exited: {}", worker.wait().await?),
        None => std::future::pending().await,
    }
}
