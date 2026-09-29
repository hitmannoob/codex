use super::*;
use axum::response::IntoResponse;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::Mac;
use pretty_assertions::assert_eq;
use std::sync::Mutex;

const PASSPHRASE: &str = "operator-webhook-passphrase-for-tests-0123456789";

/// One request a receiver got: its path, webhook headers, and parsed body.
#[derive(Clone, Debug)]
struct Received {
    path: String,
    id: String,
    timestamp: String,
    signature: String,
    body: String,
}

/// A receiver that records deliveries. `/hook` answers 204, `/fail` 500,
/// `/redirect` redirects to `/hook`, and `/flaky` fails each delivery's first
/// attempt.
async fn receiver() -> anyhow::Result<(
    String,
    Arc<Mutex<Vec<Received>>>,
    tokio::task::JoinHandle<std::io::Result<()>>,
)> {
    let received = Arc::new(Mutex::new(Vec::<Received>::new()));
    let recorded = Arc::clone(&received);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let router = axum::Router::new().fallback(
        move |uri: axum::http::Uri, headers: axum::http::HeaderMap, body: String| {
            let recorded = Arc::clone(&recorded);
            async move {
                let header = |name: &str| {
                    headers
                        .get(name)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned()
                };
                let retry = {
                    let mut recorded = recorded
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let retry = recorded.iter().any(|seen| seen.id == header("webhook-id"));
                    recorded.push(Received {
                        path: uri.path().to_owned(),
                        id: header("webhook-id"),
                        timestamp: header("webhook-timestamp"),
                        signature: header("webhook-signature"),
                        body,
                    });
                    retry
                };
                match uri.path() {
                    "/fail" => axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                    "/flaky" if !retry => {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                    }
                    "/redirect" => axum::response::Redirect::temporary("/hook").into_response(),
                    _ => axum::http::StatusCode::NO_CONTENT.into_response(),
                }
            }
        },
    );
    Ok((
        base,
        received,
        tokio::spawn(async move { axum::serve(listener, router).await }),
    ))
}

/// The Standard Webhooks signature the pinned SDK checks.
fn sign(secret: &str, received: &Received) -> anyhow::Result<String> {
    let key = STANDARD.decode(secret.strip_prefix("whsec_").context("secret prefix")?)?;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&key)?;
    mac.update(format!("{}.{}.{}", received.id, received.timestamp, received.body).as_bytes());
    Ok(format!(
        "v1,{}",
        STANDARD.encode(mac.finalize().into_bytes())
    ))
}

#[tokio::test]
async fn webhook_endpoints_sign_test_deliveries_and_keep_secrets_private() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, server) = capabilities::serve(&api).await?;
        let (hooks, received, receiver_task) = receiver().await?;
        let client = reqwest::Client::new();
        let endpoints = format!("{base}/webhook_endpoints");
        let post = |url: String, body: Value| client.post(url).bearer_auth(TOKEN).json(&body).send();
        let error = |status: reqwest::StatusCode, message: &str| (status, json!({"error":{"message":message,"type":"invalid_request_error","param":null,"code":null}}));
        let outcome = |response: reqwest::Response| async move { anyhow::Ok((response.status(), response.json::<Value>().await?)) };

        // Event types need no passphrase; endpoints, whose secrets are
        // encrypted, do.
        assert_eq!(request(&client, reqwest::Method::GET, &format!("{base}/webhook_event_types"), Value::Null).await?,
            json!({"object":"list","data":["agent.session.created","agent.session.action_required","agent.session.in_progress","agent.session.idle","agent.session.failed"]}));
        let hook = json!({"name":"ops","url":format!("{hooks}/hook"),"event_types":["agent.session.idle","agent.session.idle","agent.session.failed"]});
        let public = json!({"name":"ops","url":"https://93.184.216.34/hook","event_types":["agent.session.idle"]});
        assert_eq!(outcome(post(endpoints.clone(), public).await?).await?.0, reqwest::StatusCode::NOT_IMPLEMENTED);
        api.configure_vault(PASSPHRASE.into()).await?;

        // Receivers must be https on public addresses unless the operator
        // allows the host.
        for (invalid, message) in [
            (json!({"name":"ops","url":format!("{hooks}/hook"),"event_types":["agent.session.idle"]}), "url must be an absolute https URL"),
            (json!({"name":"ops","url":"https://10.0.0.1/hook","event_types":["agent.session.idle"]}), "url resolves to a non-public address; the operator must allow its host"),
            (json!({"name":"ops","url":"https://93.184.216.34/hook","event_types":["response.completed"]}), "event_types accepts only agent.session.created, agent.session.action_required, agent.session.in_progress, agent.session.idle, agent.session.failed"),
            (json!({"name":"ops","url":"https://93.184.216.34/hook","event_types":[]}), "event_types must list at least one event type"),
            (json!({"url":"https://93.184.216.34/hook","event_types":["agent.session.idle"]}), "name must contain 1 to 256 UTF-8 bytes after trimming"),
            (json!({"name":"ops","url":"https://93.184.216.34/hook","event_types":["agent.session.idle"],"secret":"x"}), "unknown field secret"),
        ] {
            assert_eq!(outcome(post(endpoints.clone(), invalid.clone()).await?).await?, error(reqwest::StatusCode::BAD_REQUEST, message), "{invalid}");
        }
        api.allow_webhook_hosts(["127.0.0.1".to_string()]);

        // The secret is returned once; repeated event types collapse.
        let created = request(&client, reqwest::Method::POST, &endpoints, hook).await?;
        let id = created["id"].as_str().context("endpoint id")?.to_owned();
        let secret = created["signing_secret"].as_str().context("signing secret")?.to_owned();
        let endpoint_url = format!("{endpoints}/{id}");
        assert!(id.starts_with("we_") && secret.starts_with("whsec_") && STANDARD.decode(&secret[6..])?.len() == 32, "{created}");
        let mut expected = json!({"id":id,"object":"webhook_endpoint","created_at":created["created_at"],"updated_at":created["created_at"],"name":"ops",
            "url":format!("{hooks}/hook"),"event_types":["agent.session.idle","agent.session.failed"],"signing_secret_hint":format!("whsec_...{}", &secret[secret.len() - 4..])});
        let mut with_secret = expected.clone();
        with_secret["signing_secret"] = json!(secret);
        assert_eq!(created, with_secret);
        assert_eq!(request(&client, reqwest::Method::GET, &endpoint_url, Value::Null).await?, expected);
        // An update that changes nothing keeps `updated_at`.
        assert_eq!(request(&client, reqwest::Method::POST, &endpoint_url, json!({"name":"ops"})).await?, expected);

        // A test delivery is signed and carries a sample event.
        let test = |event_type: &str| post(format!("{endpoint_url}/test"), json!({"event_type":event_type}));
        assert_eq!(outcome(test("agent.session.action_required").await?).await?, (reqwest::StatusCode::OK,
            json!({"object":"webhook_endpoint.test","webhook_endpoint_id":id,"event_type":"agent.session.action_required","status_code":204,"success":true})));
        let delivery = received.lock().unwrap_or_else(std::sync::PoisonError::into_inner).last().cloned().context("delivery")?;
        let body: Value = serde_json::from_str(&delivery.body)?;
        assert_eq!(json!({"path":delivery.path,"object":body["object"],"type":body["type"],"data":body["data"]}),
            json!({"path":"/hook","object":"event","type":"agent.session.action_required","data":{"id":"sess_test","required_action":{"type":"function_call"}}}));
        assert!(delivery.id.starts_with("wh_") && body["id"].as_str().is_some_and(|id| id.starts_with("evt_")), "{delivery:?}");
        assert!(delivery.timestamp.parse::<u64>()?.abs_diff(body["created_at"].as_u64().context("created_at")?) <= 5);
        assert_eq!(delivery.signature, sign(&secret, &delivery)?);

        // Failures are reported as the receiver's status; redirects are not
        // followed, and an unreachable receiver is a gateway error.
        for (path, status) in [("fail", 500), ("redirect", 307)] {
            request(&client, reqwest::Method::POST, &endpoint_url, json!({"url":format!("{hooks}/{path}")})).await?;
            assert_eq!(outcome(test("agent.session.idle").await?).await?.1["status_code"], json!(status));
        }
        assert_eq!(received.lock().unwrap_or_else(std::sync::PoisonError::into_inner).iter().map(|r| r.path.clone()).collect::<Vec<_>>(), vec!["/hook", "/fail", "/redirect"]);
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await?.local_addr()?;
        request(&client, reqwest::Method::POST, &endpoint_url, json!({"url":format!("http://{closed}/hook")})).await?;
        assert_eq!(outcome(test("agent.session.idle").await?).await?.0, reqwest::StatusCode::BAD_GATEWAY);
        let updated = request(&client, reqwest::Method::POST, &endpoint_url, json!({"url":format!("{hooks}/hook"),"event_types":["agent.session.created"]})).await?;
        expected["event_types"] = json!(["agent.session.created"]);
        expected["updated_at"] = updated["updated_at"].clone();
        assert_eq!(updated, expected);

        // Rotation can keep the old secret signing for a day, so receivers
        // holding either secret accept deliveries.
        let rotated = request(&client, reqwest::Method::POST, &format!("{endpoint_url}/rotate_secret"), json!({"keep_old_secret_active_for_24_hours":true})).await?;
        let kept = rotated["signing_secret"].as_str().context("rotated secret")?.to_owned();
        test("agent.session.created").await?;
        let delivery = received.lock().unwrap_or_else(std::sync::PoisonError::into_inner).last().cloned().context("delivery")?;
        assert_eq!(delivery.signature, format!("{} {}", sign(&kept, &delivery)?, sign(&secret, &delivery)?));
        let replaced = request(&client, reqwest::Method::POST, &format!("{endpoint_url}/rotate_secret"), json!({})).await?;
        let current = replaced["signing_secret"].as_str().context("replaced secret")?.to_owned();
        assert_eq!(replaced["signing_secret_hint"], json!(format!("whsec_...{}", &current[current.len() - 4..])));
        test("agent.session.created").await?;
        let delivery = received.lock().unwrap_or_else(std::sync::PoisonError::into_inner).last().cloned().context("delivery")?;
        assert_eq!(delivery.signature, sign(&current, &delivery)?);

        // Listing is newest first; deletion removes the endpoint.
        let second = request(&client, reqwest::Method::POST, &endpoints, json!({"name":"audit","url":format!("{hooks}/hook"),"event_types":["agent.session.failed"]})).await?;
        let page = request(&client, reqwest::Method::GET, &format!("{endpoints}?limit=1"), Value::Null).await?;
        assert_eq!(json!({"ids":page["data"].as_array().context("page")?.iter().map(|e| e["id"].clone()).collect::<Vec<_>>(),"has_more":page["has_more"]}), json!({"ids":[second["id"]],"has_more":true}));
        let rest = request(&client, reqwest::Method::GET, &format!("{endpoints}?after={}", second["id"].as_str().context("second id")?), Value::Null).await?;
        assert_eq!(json!({"ids":rest["data"].as_array().context("rest")?.iter().map(|e| e["id"].clone()).collect::<Vec<_>>(),"has_more":rest["has_more"]}), json!({"ids":[id],"has_more":false}));
        let deleted = client.delete(&endpoint_url).bearer_auth(TOKEN).send().await?;
        assert_eq!(outcome(deleted).await?.1, json!({"id":id,"object":"webhook_endpoint.deleted","deleted":true}));
        let missing = client.get(&endpoint_url).bearer_auth(TOKEN).send().await?;
        assert_eq!(outcome(missing).await?, error(reqwest::StatusCode::NOT_FOUND, "webhook endpoint not found"));

        // No signing secret reaches the database or any other data file.
        let second_secret = second["signing_secret"].as_str().context("second secret")?;
        for needle in [secret.as_str(), kept.as_str(), current.as_str(), second_secret] {
            assert_eq!(vaults::files_containing(data.path(), &needle[6..])?, Vec::<String>::new());
        }
        receiver_task.abort();
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}

/// Wait until the receiver has recorded what `done` looks for.
async fn deliveries(
    received: &Mutex<Vec<Received>>,
    done: impl Fn(&[Received]) -> bool,
) -> Vec<Received> {
    loop {
        let seen = received
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if done(&seen) {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(/*millis*/ 50)).await;
    }
}

fn at<'a>(seen: &'a [Received], path: &str) -> Vec<&'a Received> {
    seen.iter()
        .filter(|received| received.path == path)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn session_events_are_queued_retried_and_survive_restart() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        Mock::given(body_string_contains("call-lookup"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(capabilities::call("lookup", "hook-call"), "text/event-stream"))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        // A turn that is still running when the API stops.
        Mock::given(body_string_contains("stall-turn"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_delay(Duration::from_secs(/*secs*/ 60)))
            .with_priority(/*p*/ 1).mount(&provider).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let (home_path, data_path) = (home.path().to_path_buf(), data.path().to_path_buf());
        let start = || {
            let (home, data) = (home_path.clone(), data_path.clone());
            async move {
                let api = AgentsApi::new(backend(&home).await?, AbsolutePathBuf::from_absolute_path(&data)?, TOKEN.into()).await?;
                api.configure_vault(PASSPHRASE.into()).await?;
                api.allow_webhook_hosts(["127.0.0.1".to_string()]);
                Ok::<_, anyhow::Error>(api)
            }
        };
        let api = start().await?;
        let (base, server) = capabilities::serve(&api).await?;
        let (hooks, received, receiver_task) = receiver().await?;
        let client = reqwest::Client::new();
        let endpoints = format!("{base}/webhook_endpoints");
        let endpoint = |name: &str, url: String, event_types: Value| request(&client, reqwest::Method::POST, &endpoints, json!({"name":name,"url":url,"event_types":event_types}));
        let all = endpoint("all", format!("{hooks}/hook"), json!(["agent.session.created","agent.session.action_required","agent.session.in_progress","agent.session.idle","agent.session.failed"])).await?;
        let flaky = endpoint("flaky", format!("{hooks}/flaky"), json!(["agent.session.idle"])).await?;
        let failures = endpoint("failures", format!("{hooks}/failed"), json!(["agent.session.failed"])).await?;
        let secret = |endpoint: &Value| endpoint["signing_secret"].as_str().map(str::to_owned).context("signing secret");
        let sessions = format!("{base}/agents/sessions");

        // A session that waits on a function result reports every status but
        // failure, in order, to the endpoint subscribed to all of them.
        let session = request(&client, reqwest::Method::POST, &sessions, json!({
            "agent":{"model":"mock-model","tools":[{"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object","properties":{}}}]},
            "environment":{"type":"none"},"input":"call-lookup"})).await?;
        let id = session["id"].as_str().context("session id")?.to_owned();
        let url = format!("{sessions}/{id}");
        let pending = loop {
            let session = request(&client, reqwest::Method::GET, &url, Value::Null).await?;
            if session["status"] == "requires_action" {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
        };
        let action = &pending["required_actions"][0];
        let submitted = client.post(format!("{url}/events")).bearer_auth(TOKEN).json(&json!({"events":[{"type":"agent.session.input.tool_result",
            "turn_id":action["turn_id"],"call_id":action["call_id"],"success":true,"output":"found"}]})).send().await?;
        assert_eq!(submitted.status(), reqwest::StatusCode::ACCEPTED);
        idle(&client, &url, /*expected_turns*/ 1).await?;

        // The flaky receiver's first attempt fails; the retry repeats the same
        // delivery ID and event.
        let seen = deliveries(&received, |seen| at(seen, "/hook").len() == 5 && at(seen, "/flaky").len() == 2).await;
        let events = at(&seen, "/hook").iter().map(|received| serde_json::from_str(&received.body)).collect::<Result<Vec<Value>, _>>()?;
        assert_eq!(events.iter().map(|event| json!([event["type"], event["data"]])).collect::<Vec<_>>(), vec![
            json!(["agent.session.created", {"id":id}]),
            json!(["agent.session.in_progress", {"id":id}]),
            json!(["agent.session.action_required", {"id":id,"required_action":{"type":"function_call"}}]),
            json!(["agent.session.in_progress", {"id":id}]),
            json!(["agent.session.idle", {"id":id}]),
        ]);
        for received in at(&seen, "/hook") {
            assert_eq!(received.signature, sign(&secret(&all)?, received)?);
        }
        let retried = at(&seen, "/flaky");
        assert_eq!((retried[0].id == retried[1].id, retried[0].body == retried[1].body, retried[1].signature == sign(&secret(&flaky)?, retried[1])?),
            (true, true, true));
        // One event reaches every subscribed endpoint under its own delivery ID.
        let idle_event = at(&seen, "/hook")[4];
        assert_eq!((retried[0].body == idle_event.body, retried[0].id != idle_event.id), (true, true));
        assert_eq!(at(&seen, "/failed").len(), 0);

        // Stop with an undeliverable event queued and a turn still running.
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await?.local_addr()?;
        let down = endpoint("down", format!("http://{closed}/hook"), json!(["agent.session.created"])).await?;
        let second = request(&client, reqwest::Method::POST, &sessions, json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":"stall-turn"})).await?;
        let second_id = second["id"].as_str().context("second id")?.to_owned();
        deliveries(&received, |seen| at(seen, "/hook").iter().any(|received| received.body.contains(&second_id) && received.body.contains("agent.session.in_progress"))).await;
        server.abort();
        api.shutdown().await?;

        // After a restart, the queued event reaches the endpoint's new URL, and
        // the interrupted session's failure is delivered.
        let api = start().await?;
        let (base, server) = capabilities::serve(&api).await?;
        request(&client, reqwest::Method::POST, &format!("{base}/webhook_endpoints/{}", down["id"].as_str().context("down id")?), json!({"url":format!("{hooks}/restored")})).await?;
        let seen = deliveries(&received, |seen| !at(seen, "/restored").is_empty() && !at(seen, "/failed").is_empty()).await;
        let restored: Value = serde_json::from_str(&at(&seen, "/restored")[0].body)?;
        let failed: Value = serde_json::from_str(&at(&seen, "/failed")[0].body)?;
        assert_eq!((restored["type"].clone(), restored["data"].clone(), failed["type"].clone(), failed["data"].clone()),
            (json!("agent.session.created"), json!({"id":second_id}), json!("agent.session.failed"), json!({"id":second_id})));
        assert_eq!(at(&seen, "/failed")[0].signature, sign(&secret(&failures)?, at(&seen, "/failed")[0])?);
        receiver_task.abort();
        server.abort();
        api.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    }).await?
}
