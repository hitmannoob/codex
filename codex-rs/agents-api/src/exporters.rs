//! Telemetry setup for the CLI: `tracing` output on stderr, and, when the
//! operator names an OTLP collector, export of the API's spans and metrics
//! through `codex-otel`.
use codex_otel::OtelExporter;
use codex_otel::OtelHttpProtocol;
use codex_otel::OtelProvider;
use codex_otel::OtelSettings;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Managed and external worker startup, from launch to a completed
/// initialization handshake, tagged `reason` start or restart.
pub(crate) const WORKER_STARTUP_DURATION: &str = "agents_api.worker.startup.duration_ms";
/// Managed worker restart attempts, tagged `outcome` restarted or failed.
pub(crate) const WORKER_RESTART: &str = "agents_api.worker.restart";

pub(crate) fn count(name: &str, tags: &[(&str, &str)]) {
    if let Some(metrics) = codex_otel::global() {
        let _ = metrics.counter(name, /*inc*/ 1, tags);
    }
}

pub(crate) fn elapsed(name: &str, duration: Duration, tags: &[(&str, &str)]) {
    if let Some(metrics) = codex_otel::global() {
        let _ = metrics.record_duration(name, duration, tags);
    }
}

/// Parse an `--otlp-header` value.
pub(crate) fn header(value: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .filter(|(name, _)| !name.is_empty())
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .ok_or_else(|| "expected NAME=VALUE".to_owned())
}

/// Where and how to export telemetry.
pub(crate) struct Export {
    /// OTLP/HTTP collector base URL; `/v1/traces` and `/v1/metrics` are
    /// appended. Nothing is exported without it.
    pub(crate) endpoint: Option<String>,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) environment: String,
    pub(crate) data_directory: PathBuf,
}

/// Install the stderr log and, when configured, span and metric export.
/// `RUST_LOG` filters the log (default: this crate at info), and
/// `LOG_FORMAT=json` writes it as JSON lines. The returned provider flushes
/// exports on shutdown.
pub(crate) fn install(export: Export) -> anyhow::Result<Option<OtelProvider>> {
    let provider = match &export.endpoint {
        Some(endpoint) => {
            let base = endpoint.trim_end_matches('/');
            let exporter = |signal: &str| OtelExporter::OtlpHttp {
                endpoint: format!("{base}/v1/{signal}"),
                headers: export.headers.iter().cloned().collect::<HashMap<_, _>>(),
                protocol: OtelHttpProtocol::Binary,
                tls: None,
            };
            OtelProvider::try_new(&OtelSettings {
                environment: export.environment.clone(),
                service_name: "codex-agents-api".into(),
                service_version: env!("CARGO_PKG_VERSION").into(),
                codex_home: export.data_directory.clone(),
                exporter: OtelExporter::None,
                trace_exporter: exporter("traces"),
                metrics_exporter: exporter("metrics"),
                runtime_metrics: false,
                span_attributes: BTreeMap::new(),
                tracestate: BTreeMap::new(),
            })
            .map_err(|error| anyhow::anyhow!("telemetry export setup failed: {error}"))?
        }
        None => None,
    };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("codex_agents_api=info"));
    let stderr = if std::env::var("LOG_FORMAT").is_ok_and(|format| format == "json") {
        tracing_subscriber::fmt::layer()
            .json()
            .with_writer(std::io::stderr)
            .with_filter(filter)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_ansi(std::io::stderr().is_terminal())
            .with_filter(filter)
            .boxed()
    };
    tracing_subscriber::registry()
        .with(stderr)
        .with(provider.as_ref().and_then(OtelProvider::tracing_layer))
        .try_init()?;
    Ok(provider)
}
