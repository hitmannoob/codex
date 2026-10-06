"""Run the Agents API in a detached Modal Sandbox behind an encrypted tunnel.

The service, its Codex worker, and the `codex` CLI are built from the
repository's committed `HEAD` into a cached Modal image layer, so later starts
take seconds. The provider key comes from a Modal secret you create; this
script never reads it.

Usage:
  serve.py build STATE_DIR   build (or reuse) the image
  serve.py start STATE_DIR   start the service; prints the endpoint and writes
                             url, token, vault-passphrase, environment-key, and
                             sandbox-id to STATE_DIR
  serve.py logs STATE_DIR    show the service log
  serve.py stop STATE_DIR    terminate the Sandbox
"""

import argparse
import os
import secrets
import subprocess
import sys
import time
import urllib.request

import modal

APP = "agents-api-eval"
PROVIDER_SECRET = "agents-api-openrouter"

CONFIG = """model = "openai/gpt-5-mini"
model_provider = "openrouter"
approval_policy = "never"
sandbox_mode = "read-only"

[features]
plugins = false

[model_providers.openrouter]
name = "openrouter"
base_url = "https://openrouter.ai/api/v1"
env_key = "OPENROUTER_API_KEY"
wire_api = "responses"
"""

# A non-login shell: a login shell resets PATH and loses the Rust toolchain.
BUILD = """
set -euo pipefail
export CARGO_HOME=/usr/local/cargo RUSTUP_HOME=/usr/local/rustup PATH=/usr/local/cargo/bin:$PATH
mkdir -p /build && tar xf /src/codex.tar -C /build
cd /build/codex-rs
cargo build -p codex-agents-api -p codex-app-server -p codex-cli \
  --bin codex-agents-api --bin codex-app-server --bin codex
mkdir -p /opt/agents-api/bin
cp target/debug/codex-agents-api target/debug/codex-app-server target/debug/codex /opt/agents-api/bin/
cd / && rm -rf /build /src/codex.tar /usr/local/cargo/registry
"""

# The service binds loopback only, so a TCP forwarder exposes it on the
# tunnel's port. Forwarding bytes keeps the Host header and websocket upgrades.
FORWARD = """
import asyncio

async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()

async def handle(client_reader, client_writer):
    try:
        service_reader, service_writer = await asyncio.open_connection("127.0.0.1", 4501)
    except OSError:
        client_writer.close()
        return
    await asyncio.gather(pipe(client_reader, service_writer), pipe(service_reader, client_writer),
                         return_exceptions=True)

async def main():
    server = await asyncio.start_server(handle, "0.0.0.0", 8080)
    async with server:
        await server.serve_forever()

asyncio.run(main())
"""

SCRIPT = """
set -euo pipefail
mkdir -p /data/codex-home /data/logs
cat > /data/forward.py <<'PY'
{forward}PY
python3 /data/forward.py &
cat > /data/codex-home/config.toml <<'TOML'
{config}TOML
/opt/agents-api/bin/codex-agents-api --listen 127.0.0.1:4501 --data-directory /data \
  --codex-home /data/codex-home --app-server-bin /opt/agents-api/bin/codex-app-server \
  2>&1 | tee /data/logs/agents-api.log
"""


def build():
    """Image build step: compile the binaries into the image layer."""
    subprocess.run(["bash", "-c", BUILD], check=True)


def archive(state):
    """A `git archive` of the repository's committed HEAD. The same commit gives
    the same archive, so the image layer is reused."""
    root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    path = os.path.join(os.path.abspath(state), "codex.tar")
    subprocess.run(
        ["git", "archive", "--format=tar", "-o", path, "HEAD"], cwd=root, check=True
    )
    return path


def image(source):
    """Rust 1.95 (the repository's toolchain) with the binaries baked in."""
    return (
        modal.Image.from_registry("rust:1.95-bookworm", add_python="3.12")
        .apt_install(
            "pkg-config",
            "libssl-dev",
            "libcap-dev",
            "libdbus-1-dev",
            "clang",
            "cmake",
            "git",
            "python3",
            "procps",
            "curl",
        )
        .add_local_file(source, "/src/codex.tar", copy=True)
        .run_function(build, cpu=16, memory=32768, timeout=2 * 3600)
    )


def start(state, hours):
    values = {
        name: secrets.token_hex(24)
        for name in ("token", "vault-passphrase", "environment-key")
    }
    for name, value in values.items():
        with open(os.path.join(state, name), "w", encoding="utf-8") as handle:
            handle.write(value)
    sandbox = modal.Sandbox.create(
        "bash",
        "-lc",
        SCRIPT.format(config=CONFIG, forward=FORWARD),
        app=modal.App.lookup(APP, create_if_missing=True),
        image=image(archive(state)),
        secrets=[
            modal.Secret.from_name(PROVIDER_SECRET),
            modal.Secret.from_dict(
                {
                    "CODEX_AGENTS_API_TOKEN": values["token"],
                    "CODEX_AGENTS_API_VAULT_PASSPHRASE": values["vault-passphrase"],
                    "CODEX_AGENTS_API_ENVIRONMENT_KEY": values["environment-key"],
                }
            ),
        ],
        encrypted_ports=[8080],
        cpu=4,
        memory=8192,
        timeout=int(hours * 3600),
    )
    url = sandbox.tunnels()[8080].url.rstrip("/")
    for name, value in (("sandbox-id", sandbox.object_id), ("url", url)):
        with open(os.path.join(state, name), "w", encoding="utf-8") as handle:
            handle.write(value)
    deadline = time.monotonic() + 180
    while True:
        try:
            with urllib.request.urlopen(f"{url}/healthz", timeout=10) as response:
                print("healthz", response.status, response.read().decode())
                break
        except Exception as error:  # Not ready yet.
            if sandbox.poll() is not None:
                output = sandbox.stdout.read().splitlines()[-25:]
                shown = [
                    line
                    for line in output
                    if not any(
                        word in line.lower() for word in ("token", "passphrase", "key")
                    )
                ]
                sys.exit(f"sandbox exited: {sandbox.returncode}\n" + "\n".join(shown))
            if time.monotonic() > deadline:
                sys.exit(f"not healthy after 180s: {error}")
            time.sleep(3)
    print("sandbox", sandbox.object_id)
    print("endpoint", f"{url}/v1")


def sandbox_for(state):
    with open(os.path.join(state, "sandbox-id"), encoding="utf-8") as handle:
        return modal.Sandbox.from_id(handle.read().strip())


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("command", choices=("build", "start", "logs", "stop"))
    parser.add_argument(
        "state", help="directory for the endpoint's credentials and IDs"
    )
    parser.add_argument(
        "--hours", type=float, default=3, help="Sandbox lifetime (default 3)"
    )
    args = parser.parse_args()
    os.makedirs(args.state, exist_ok=True)
    if args.command == "build":
        with modal.enable_output():
            image(archive(args.state)).build(
                modal.App.lookup(APP, create_if_missing=True)
            )
        print("image built")
    elif args.command == "start":
        start(args.state, args.hours)
    elif args.command == "logs":
        process = sandbox_for(args.state).exec(
            "tail", "-n", "60", "/data/logs/agents-api.log"
        )
        print(process.stdout.read())
    else:
        sandbox_for(args.state).terminate()
        print("terminated")


if __name__ == "__main__":
    main()
