use super::*;
use codex_utils_cargo_bin::find_resource;
use pretty_assertions::assert_eq;

/// Whether a body is the public error shape the SDK parses.
fn public_error(body: &Value) -> bool {
    let error = &body["error"];
    error["message"]
        .as_str()
        .is_some_and(|message| !message.is_empty())
        && error["type"].is_string()
        && error.get("param").is_some()
        && error.get("code").is_some()
}

#[tokio::test]
async fn every_inventoried_operation_answers_with_the_public_error_shape() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let inventory: Value = serde_json::from_str(&std::fs::read_to_string(find_resource!("CONTRACT_INVENTORY.json")?)?)?;
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("unused").await;
        MockResponsesConfig::new(&provider.uri()).write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let mut observed = Vec::new();
        for operation in inventory["operations"].as_array().context("operations")? {
            if operation["status"] == "missing" {
                continue;
            }
            let method: reqwest::Method = operation["method"].as_str().context("method")?.parse()?;
            let path = operation["path"].as_str().context("path")?;
            let path = path.split('/').map(|segment| if segment.starts_with('{') { "missing-id" } else { segment }).collect::<Vec<_>>().join("/");
            let url = format!("{base}{path}");
            let send = |authorized: bool| {
                let mut builder = client.request(method.clone(), &url).header("OpenAI-Beta", "agents=v1");
                if authorized {
                    builder = builder.bearer_auth(TOKEN);
                }
                if method == reqwest::Method::POST {
                    builder = builder.json(&json!({}));
                }
                builder.send()
            };
            for authorized in [false, true] {
                let response = send(authorized).await?;
                let status = response.status().as_u16();
                let body: Value = response.json().await.unwrap_or(Value::Null);
                observed.push(json!({"id":operation["id"],"authorized":authorized,"status":status,"public":public_error(&body)}));
            }
        }
        // Every operation refuses a missing token, then answers placeholder IDs
        // with 404 and empty bodies with 400; lists succeed. Errors always use
        // the public shape; successes never do.
        let created_from_empty_body = ["VLT-001"];
        // POSTs that validate the body before looking up the resource.
        let validated_first = ["EVT-001", "ENV-002", "CRD-001", "CRD-003", "WHK-007"];
        let expected: Vec<Value> = inventory["operations"].as_array().context("operations")?.iter()
            .filter(|operation| operation["status"] != "missing")
            .flat_map(|operation| {
                let id = operation["id"].as_str().unwrap_or_default();
                let named = operation["path"].as_str().unwrap_or_default().contains('{');
                let status = match (operation["method"].as_str().unwrap_or_default(), named) {
                    ("GET", false) => 200,
                    ("POST", false) if created_from_empty_body.contains(&id) => 200,
                    ("POST", false) => 400,
                    ("POST", true) if validated_first.contains(&id) => 400,
                    _ => 404,
                };
                [json!({"id":id,"authorized":false,"status":401,"public":true}),
                 json!({"id":id,"authorized":true,"status":status,"public":status != 200})]
            })
            .collect();
        assert_eq!(observed, expected);
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn every_list_bounds_its_pagination() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let inventory: Value = serde_json::from_str(&std::fs::read_to_string(find_resource!("CONTRACT_INVENTORY.json")?)?)?;
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let workspace = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("unused").await;
        MockResponsesConfig::new(&provider.uri()).write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        api.configure_environments("an-environment-key-for-pagination-checks".into(), format!("{}/registry", base.trim_end_matches("/v1")))?;
        let client = reqwest::Client::new();
        // Parents that need no model call: an idle self-hosted session and a vault.
        let session = request(&client, reqwest::Method::POST, &format!("{base}/agents/sessions"), json!({"agent":{"model":"mock-model"},
            "environment":{"type":"self_hosted","workspace_directory":workspace.path().to_str().context("workspace")?}})).await?;
        let vault: Value = client.post(format!("{base}/vaults")).bearer_auth(TOKEN).header("OpenAI-Beta", "agents=v1")
            .json(&json!({"name":"pages"})).send().await?.error_for_status()?.json().await?;
        let parent = |name: &str| match name {
            "{session_id}" => session["id"].as_str().map(str::to_owned),
            "{vault_id}" => vault["id"].as_str().map(str::to_owned),
            "{environment_id}" => session["environment"]["id"].as_str().map(str::to_owned),
            _ => Some("missing-id".to_owned()),
        };
        let queries = ["", "?limit=0", "?limit=101", "?order=sideways", "?after=missing-id"];
        let mut observed = Vec::new();
        for operation in inventory["operations"].as_array().context("operations")? {
            let path = operation["path"].as_str().unwrap_or_default();
            if operation["status"] == "missing" || operation["method"] != "GET" || operation["sdk_method"] != "list" {
                continue;
            }
            let path = path.split('/').map(|segment| if segment.starts_with('{') { parent(segment).unwrap_or_default() } else { segment.to_owned() }).collect::<Vec<_>>().join("/");
            for query in queries {
                let response = client.get(format!("{base}{path}{query}")).bearer_auth(TOKEN).header("OpenAI-Beta", "agents=v1").send().await?;
                let status = response.status().as_u16();
                let body: Value = response.json().await.unwrap_or(Value::Null);
                observed.push(json!({"id":operation["id"],"query":query,"status":status,"public":public_error(&body)}));
            }
        }
        // Lists reject out-of-range limits, unknown orders, and unknown
        // cursors, except where the contract says otherwise: vault,
        // credential, and webhook endpoint lists clamp `limit` to 1..=100,
        // file lists allow up to 10,000, and webhook event types take no
        // parameters. Subagent lists answer 404 for the placeholder subagent,
        // and environment files need a connected executor (409).
        let expected: Vec<Value> = observed.iter().map(|row| {
            let id = row["id"].as_str().unwrap_or_default();
            let query = row["query"].as_str().unwrap_or_default();
            let status = match (id, query) {
                ("SUB-003" | "SUB-005" | "SUB-006", _) => 404,
                ("ENV-003", "") => 409,
                ("WHK-008", _) | (_, "") => 200,
                ("VLT-003" | "CRD-004" | "WHK-004", "?limit=0" | "?limit=101") => 200,
                ("FIL-003", "?limit=101") => 200,
                _ => 400,
            };
            json!({"id":id,"query":query,"status":status,"public":status != 200})
        }).collect();
        assert_eq!(observed, expected);
        assert_eq!(observed.len(), 15 * queries.len());
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

/// Builds an operation's request body around some metadata.
type BodyFor<'a> = Box<dyn Fn(&Value) -> Value + 'a>;

#[tokio::test]
async fn metadata_limits_hold_on_every_resource_that_takes_metadata() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let workspace = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("unused").await;
        MockResponsesConfig::new(&provider.uri()).write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        api.configure_environments("an-environment-key-for-metadata-checks".into(), format!("{}/registry", base.trim_end_matches("/v1")))?;
        let client = reqwest::Client::new();
        let send = |method: reqwest::Method, url: String, body: Value| {
            client.request(method, url).bearer_auth(TOKEN).header("OpenAI-Beta", "agents=v1").json(&body).send()
        };
        // Limits count characters, so multi-byte text sits exactly at them.
        let pairs = |count: usize| (0..count).map(|index| (format!("k{index}"), json!("v"))).collect::<serde_json::Map<_, _>>();
        let accepted = [
            Value::Object(pairs(16)),
            json!({"é".repeat(64): "ü".repeat(512)}),
        ];
        let refused = [
            Value::Object(pairs(17)),
            json!({"é".repeat(65): "v"}),
            json!({"k": "ü".repeat(513)}),
            json!({"k": 1}),
        ];
        let workspace_directory = workspace.path().to_str().context("workspace")?;
        let agent: Value = send(reqwest::Method::POST, format!("{base}/agents"), json!({"model":"mock-model"})).await?.json().await?;
        let session: Value = send(reqwest::Method::POST, format!("{base}/agents/sessions"), json!({"agent":{"model":"mock-model"},
            "environment":{"type":"self_hosted","workspace_directory":workspace_directory}})).await?.json().await?;
        let agent_url = format!("{base}/agents/{}", agent["id"].as_str().context("agent id")?);
        let session_url = format!("{base}/agents/sessions/{}", session["id"].as_str().context("session id")?);
        let operations: [(&str, String, BodyFor<'_>); 5] = [
            ("agent create", format!("{base}/agents"), Box::new(|metadata: &Value| json!({"model":"mock-model","metadata":metadata}))),
            ("agent update", agent_url, Box::new(|metadata: &Value| json!({"metadata":metadata}))),
            ("session create", format!("{base}/agents/sessions"), Box::new(move |metadata: &Value| json!({"agent":{"model":"mock-model"},
                "environment":{"type":"self_hosted","workspace_directory":workspace_directory},"metadata":metadata}))),
            ("session update", session_url, Box::new(|metadata: &Value| json!({"metadata":metadata}))),
            ("vault create", format!("{base}/vaults"), Box::new(|metadata: &Value| json!({"name":"bounds","metadata":metadata}))),
        ];
        let mut observed = Vec::new();
        let mut expected = Vec::new();
        for (name, url, body) in &operations {
            for (metadata, status) in accepted.iter().map(|metadata| (metadata, 200)).chain(refused.iter().map(|metadata| (metadata, 400))) {
                let response = send(reqwest::Method::POST, url.clone(), body(metadata)).await?;
                let code = response.status().as_u16();
                let reply: Value = response.json().await?;
                let saved = if code == 200 { reply["metadata"].clone() } else { reply["error"]["message"].clone() };
                observed.push(json!({"operation":name,"status":code,"result":saved}));
                expected.push(json!({"operation":name,"status":status,
                    "result":if status == 200 { metadata.clone() } else if metadata["k"] == 1 { json!("metadata must contain string pairs") } else { json!("metadata exceeds its size limit") }}));
            }
        }
        assert_eq!(observed, expected);
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
