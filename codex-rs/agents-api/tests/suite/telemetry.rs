use super::*;
use codex_otel::MetricsClient;
use codex_otel::MetricsConfig;
use opentelemetry_sdk::metrics::InMemoryMetricExporter;
use opentelemetry_sdk::metrics::data::AggregatedMetrics;
use opentelemetry_sdk::metrics::data::MetricData;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;

/// Each recorded combination of a counter's tags, with its count, or of a
/// duration histogram's tags, with its number of samples.
fn recorded(snapshot: &ResourceMetrics, name: &str) -> Vec<(BTreeMap<String, String>, u64)> {
    let mut found = Vec::new();
    for scope in snapshot.scope_metrics() {
        for metric in scope.metrics().filter(|metric| metric.name() == name) {
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    found.extend(sum.data_points().map(|point| {
                        (
                            point
                                .attributes()
                                .map(|pair| {
                                    (
                                        pair.key.as_str().to_owned(),
                                        pair.value.as_str().into_owned(),
                                    )
                                })
                                .collect(),
                            point.value(),
                        )
                    }));
                }
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                    found.extend(histogram.data_points().map(|point| {
                        (
                            point
                                .attributes()
                                .map(|pair| {
                                    (
                                        pair.key.as_str().to_owned(),
                                        pair.value.as_str().into_owned(),
                                    )
                                })
                                .collect(),
                            point.count(),
                        )
                    }));
                }
                _ => {}
            }
        }
    }
    found
}

/// Whether `name` recorded at least one sample with exactly these tags,
/// ignoring the resource-wide tags every metric carries. Other tests in this
/// process share the global client, so counts are lower bounds.
fn has(snapshot: &ResourceMetrics, name: &str, expected: &[(&str, &str)]) -> bool {
    recorded(snapshot, name).iter().any(|(tags, count)| {
        *count > 0
            && expected
                .iter()
                .all(|(key, value)| tags.get(*key).map(String::as_str) == Some(*value))
    })
}

#[tokio::test]
async fn requests_turns_and_tool_calls_are_measured_and_correlated() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let metrics = codex_otel::install_global_metrics(MetricsClient::new(
            MetricsConfig::in_memory("test", "codex-agents-api", "0", InMemoryMetricExporter::default())
                .with_runtime_reader(),
        )?);
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        Mock::given(body_string_contains("call-lookup"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(capabilities::call("lookup", "metered-call"), "text/event-stream"))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let sessions = format!("{base}/agents/sessions");

        // Every routed response carries its own request ID.
        let created = client.post(&sessions).bearer_auth(TOKEN).json(&json!({
            "agent":{"model":"mock-model","tools":[{"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object","properties":{}}}]},
            "environment":{"type":"none"},"input":"call-lookup"})).send().await?;
        let request_id = |response: &reqwest::Response| response.headers().get("x-request-id").and_then(|value| value.to_str().ok()).map(str::to_owned);
        let first = request_id(&created).context("request id")?;
        let session: Value = created.json().await?;
        let url = format!("{sessions}/{}", session["id"].as_str().context("session id")?);
        let read = client.get(&url).bearer_auth(TOKEN).send().await?;
        let second = request_id(&read).context("second request id")?;
        assert!(first.starts_with("req_") && second.starts_with("req_") && first != second, "{first} {second}");
        // Errors carry one too: rebuilt error bodies, rejected credentials, and
        // unknown routes.
        let missing = client.get(format!("{sessions}/missing")).bearer_auth(TOKEN).send().await?;
        let unauthorized = client.get(&sessions).send().await?;
        let unrouted = client.get(format!("{base}/nowhere")).bearer_auth(TOKEN).send().await?;
        assert_eq!(
            [&missing, &unauthorized, &unrouted].map(|response| (response.status(), request_id(response).is_some_and(|id| id.starts_with("req_")))),
            [(reqwest::StatusCode::NOT_FOUND, true), (reqwest::StatusCode::UNAUTHORIZED, true), (reqwest::StatusCode::NOT_FOUND, true)]
        );
        // Environment `none` needs initial input, however it is left out.
        for body in [json!({"agent":{"model":"mock-model"},"environment":{"type":"none"}}),
                     json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":null}),
                     json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":[]})] {
            let response = client.post(&sessions).bearer_auth(TOKEN).json(&body).send().await?;
            let has_id = request_id(&response).is_some();
            assert_eq!((response.status(), has_id, response.json::<Value>().await?["error"]["message"].clone()),
                (reqwest::StatusCode::BAD_REQUEST, true, json!("input is required when environment.type is none")), "{body}");
        }

        let pending = loop {
            let session = request(&client, reqwest::Method::GET, &url, Value::Null).await?;
            if session["status"] == "requires_action" {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        };
        let action = &pending["required_actions"][0];
        let submitted = client.post(format!("{url}/events")).bearer_auth(TOKEN).json(&json!({"events":[{"type":"agent.session.input.tool_result",
            "turn_id":action["turn_id"],"call_id":action["call_id"],"success":false,"error":"lookup unavailable"}]})).send().await?;
        assert_eq!(submitted.status(), reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 1).await?;

        // Requests are labeled by route template, turns by outcome, and tool
        // calls by type and result.
        let snapshot = metrics.snapshot()?;
        let checks = [
            has(&snapshot, "agents_api.http.request", &[("method", "POST"), ("route", "/v1/agents/sessions"), ("status", "200")]),
            has(&snapshot, "agents_api.http.request", &[("method", "POST"), ("route", "/v1/agents/sessions/_id_/events"), ("status", "202")]),
            has(&snapshot, "agents_api.http.request.duration_ms", &[("route", "/v1/agents/sessions/_id")]),
            has(&snapshot, "agents_api.turn", &[("status", "completed"), ("agent_type", "root")]),
            has(&snapshot, "agents_api.turn.duration_ms", &[("status", "completed"), ("agent_type", "root")]),
            has(&snapshot, "agents_api.tool.call", &[("type", "function_call"), ("status", "failed")]),
            has(&snapshot, "agents_api.backend.connection", &[("event", "connected")]),
        ];
        assert_eq!(checks, [true; 7], "{snapshot:?}");
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}
