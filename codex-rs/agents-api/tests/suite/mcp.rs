use super::*;
use axum::response::IntoResponse;
use pretty_assertions::assert_eq;
use std::sync::Mutex;

/// A minimal HTTP MCP server with an allowed `lookup` tool and a `secret` tool
/// that no session selects. It records each tool call and its tenant header.
async fn mcp_server() -> anyhow::Result<(
    String,
    Arc<Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<std::io::Result<()>>,
)> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&calls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/mcp", listener.local_addr()?);
    let router = axum::Router::new().route("/mcp", axum::routing::post(move |headers: axum::http::HeaderMap, axum::Json(message): axum::Json<Value>| {
        let recorded = Arc::clone(&recorded);
        async move {
            let result = match message["method"].as_str() {
                Some("initialize") => json!({"protocolVersion": "2025-06-18", "capabilities": {"tools": {}}, "serverInfo": {"name": "fixture", "version": "1"}}),
                Some("notifications/initialized") => return axum::http::StatusCode::ACCEPTED.into_response(),
                Some("tools/list") => json!({"tools": [
                    {"name": "lookup", "description": "Allowed lookup", "inputSchema": {"type": "object", "properties": {}}},
                    {"name": "secret", "description": "Unselected lookup", "inputSchema": {"type": "object", "properties": {}}}
                ]}),
                Some("tools/call") => {
                    let tenant = headers.get("x-tenant").and_then(|value| value.to_str().ok()).map(str::to_owned);
                    recorded.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(json!({"tool": message["params"]["name"], "tenant": tenant}));
                    json!({"content": [{"type": "text", "text": "mcp-result-731"}]})
                }
                _ => json!({}),
            };
            axum::Json(json!({"jsonrpc": "2.0", "id": message["id"], "result": result})).into_response()
        }
    }));
    Ok((
        url,
        calls,
        tokio::spawn(async move { axum::serve(listener, router).await }),
    ))
}

fn mcp(label: &str, transport: Value, extra: Value) -> Value {
    let mut tool = json!({"type":"mcp","server_label":label,"transport":transport});
    if let (Some(tool), Value::Object(extra)) = (tool.as_object_mut(), extra) {
        tool.extend(extra);
    }
    tool
}

fn session(tools: Value, input: &str) -> Value {
    json!({"agent":{"model":"mock-model","tools":tools},"environment":{"type":"none"},"input":input})
}

fn tool_names(request: &Value) -> String {
    request["tools"].to_string()
}

#[tokio::test]
async fn public_mcp_servers_are_scoped_filtered_and_egress_checked() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 90), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let (mcp_url, calls, mcp_task) = mcp_server().await?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        Mock::given(body_string_contains("use-mcp"))
            .respond_with(ResponseTemplate::new(/*s*/ 200).set_body_raw(capabilities::call("mcp__warehouse.lookup", "mcp-call"), "text/event-stream"))
            .with_priority(/*p*/ 1).up_to_n_times(/*n*/ 1).mount(&provider).await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").with_extra_config(&format!(
            "[mcp_servers.configured]\nurl = {mcp_url:?}\nenabled = false\n"
        )).write(home.path())?;
        let start = || async {
            let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
            api.allow_mcp_hosts(["127.0.0.1".to_string()]);
            Ok::<_, anyhow::Error>(api)
        };
        let api = start().await?;
        let (base, server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let sessions = format!("{base}/agents/sessions");

        // Unreachable or unsupported servers are rejected before any session exists.
        let http = |url: &str| json!({"type":"http","server_url":url});
        for (tools, reason) in [
            (json!([mcp("internal", http("https://10.0.0.1/mcp"), json!({}))]), "private address"),
            (json!([mcp("metadata", http("https://169.254.169.254/latest"), json!({}))]), "link-local metadata address"),
            (json!([mcp("loopback", http("https://localhost:1/mcp"), json!({}))]), "loopback name"),
            (json!([mcp("plain", http("http://93.184.216.34/mcp"), json!({}))]), "plain http"),
            (json!([mcp("configured", http(&mcp_url), json!({}))]), "worker server label"),
            (json!([mcp("local", json!({"type":"stdio","command":"server","cwd":"/"}), json!({}))]), "stdio"),
            (json!([mcp("remote", http(&mcp_url), json!({"connection_origin":"environment"}))]), "environment origin"),
            (json!([mcp("vaulted", http(&mcp_url), json!({"credential_id":"cred_1"}))]), "vault credential"),
            (json!([mcp("secret", json!({"type":"http","server_url":mcp_url,"headers":{"Authorization":"Bearer x"}}), json!({}))]), "authorization header"),
        ] {
            let response = client.post(&sessions).bearer_auth(TOKEN).json(&session(tools, "rejected")).send().await?;
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST, "{reason}");
        }
        assert_eq!(request(&client, reqwest::Method::GET, &sessions, Value::Null).await?["data"], json!([]));

        // An allowed server exposes only its selected tools, with its headers,
        // and only to the session that configured it.
        let warehouse = json!([mcp("warehouse", json!({"type":"http","server_url":mcp_url,"headers":{"x-tenant":"tenant-a"}}), json!({"allowed_tools":["lookup"]}))]);
        let scoped = request(&client, reqwest::Method::POST, &sessions, session(warehouse, "use-mcp")).await?;
        let plain = request(&client, reqwest::Method::POST, &sessions, session(json!([]), "no-mcp")).await?;
        let scoped_url = format!("{sessions}/{}", scoped["id"].as_str().context("scoped id")?);
        idle(&client, &scoped_url, /*expected_turns*/ 1).await?;
        idle(&client, &format!("{sessions}/{}", plain["id"].as_str().context("plain id")?), /*expected_turns*/ 1).await?;
        assert_eq!(*calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner), vec![json!({"tool":"lookup","tenant":"tenant-a"})]);
        let items = request(&client, reqwest::Method::GET, &format!("{scoped_url}/items?order=asc"), Value::Null).await?;
        let call = items["data"].as_array().context("items")?.iter().find(|item| item["type"] == "mcp_call").context("mcp_call item")?;
        assert_eq!(json!({"server_label":call["server_label"],"name":call["name"],"arguments":call["arguments"],"output":call["output"],"error":call["error"],"status":call["status"]}),
            json!({"server_label":"warehouse","name":"lookup","arguments":{},"output":[{"type":"text","text":"mcp-result-731"}],"error":null,"status":"completed"}));
        let captures = provider.received_requests().await.context("captures")?;
        let bodies = captures.iter().filter_map(|request| serde_json::from_slice::<Value>(&request.body).ok()).collect::<Vec<_>>();
        let with_mcp = bodies.iter().find(|body| body.to_string().contains("use-mcp")).context("scoped request")?;
        let without_mcp = bodies.iter().find(|body| body.to_string().contains("no-mcp")).context("plain request")?;
        assert!(tool_names(with_mcp).contains("lookup") && !tool_names(with_mcp).contains("secret"), "{}", tool_names(with_mcp));
        // Resource tools would reach past `allowed_tools`, so none are offered.
        assert!(!tool_names(with_mcp).contains("mcp_resource"), "{}", tool_names(with_mcp));
        assert!(!tool_names(without_mcp).contains("warehouse"), "{}", tool_names(without_mcp));
        assert!(bodies.iter().any(|body| body["input"].to_string().contains("mcp-result-731")));
        // A required server that cannot initialize fails the turn, not the request.
        let required = json!([mcp("down", json!({"type":"http","server_url":"http://127.0.0.1:1/mcp"}), json!({"required":true}))]);
        let down = request(&client, reqwest::Method::POST, &sessions, session(required, "required-down")).await?;
        let down_url = format!("{sessions}/{}", down["id"].as_str().context("down id")?);
        let turns = request(&client, reqwest::Method::GET, &format!("{down_url}/turns"), Value::Null).await?;
        let turn = &turns["data"][0];
        assert_eq!(json!({"session":down["status"],"turns":turns["data"].as_array().map(Vec::len),"status":turn["status"],"error":turn["error"]}),
            json!({"session":"idle","turns":1,"status":"failed","error":{"code":"connection_failed","message":"a required MCP server failed to initialize"}}));
        server.abort();
        api.shutdown().await?;

        // Cold resume re-establishes the session's server.
        let api = start().await?;
        let (base, server) = capabilities::serve(&api).await?;
        let scoped_url = format!("{base}/agents/sessions/{}", scoped["id"].as_str().context("scoped id")?);
        let response = client.post(format!("{scoped_url}/events")).bearer_auth(TOKEN)
            .json(&json!({"events":[{"type":"agent.session.input.message","input":"after-restart"}]})).send().await?;
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        idle(&client, &scoped_url, /*expected_turns*/ 2).await?;
        let captures = provider.received_requests().await.context("captures")?;
        let resumed = captures.iter().rev().filter_map(|request| serde_json::from_slice::<Value>(&request.body).ok())
            .find(|body| body.to_string().contains("after-restart")).context("resumed request")?;
        assert!(tool_names(&resumed).contains("lookup") && !tool_names(&resumed).contains("secret"), "{}", tool_names(&resumed));
        server.abort();
        api.shutdown().await?;
        mcp_task.abort();
        Ok::<_, anyhow::Error>(())
    }).await?
}
