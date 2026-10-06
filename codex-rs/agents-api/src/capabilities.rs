use crate::ApiError;
use crate::State;
use crate::resources::AgentConfig;
use crate::resources::Environment;
use axum::http::StatusCode;
use serde_json::Value;
use serde_json::json;
use std::collections::HashSet;

pub(crate) fn validate(config: &AgentConfig) -> Result<(), ApiError> {
    let invalid = || {
        ApiError(
            StatusCode::BAD_REQUEST,
            "invalid or oversized agent capabilities".into(),
        )
    };
    if config.tools.len() > 16 || config.mcp_servers.len() > 16 {
        return Err(invalid());
    }
    let mut names = HashSet::new();
    for tool in config
        .tools
        .iter()
        .filter_map(crate::agent_tools::Tool::function)
    {
        if !identifier(&tool.name)
            || tool.name == "mcp"
            || tool.name.starts_with("mcp__")
            || !names.insert(&tool.name)
            || serde_json::to_vec(tool).map_err(anyhow::Error::from)?.len() > 1024
            || tool.parameters.get("type") != Some(&json!("object"))
            || codex_tools::parse_tool_input_schema(&tool.parameters).is_err()
        {
            return Err(invalid());
        }
    }
    names.clear();
    for selection in &config.mcp_servers {
        if !identifier(&selection.server)
            || !names.insert(&selection.server)
            || selection.allowed_tools.len() > 32
            || selection.allowed_tools.iter().any(|name| !identifier(name))
            || selection.allowed_tools.iter().collect::<HashSet<_>>().len()
                != selection.allowed_tools.len()
        {
            return Err(invalid());
        }
    }
    crate::mcp::validate(config)?;
    if let Some(effort) = config.reasoning.as_ref().and_then(|r| r.effort.as_deref())
        && !matches!(
            effort,
            "none"
                | "minimal"
                | "low"
                | "medium"
                | "high"
                | "xhigh"
                | "max"
                | "ultra"
                | "persistent"
        )
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub(crate) async fn overrides(
    state: &State,
    config: &AgentConfig,
    environment: &Environment,
    mcp_tokens: &std::collections::BTreeMap<String, String>,
) -> Result<Value, ApiError> {
    if matches!(config.service_tier.as_deref(), Some("priority" | "fast")) {
        let mut cursor = Value::Null;
        let mut supported = false;
        // Bound catalog traversal even if an external worker returns a broken cursor.
        for _ in 0..100 {
            let page = state
                .rpc(
                    "model/list",
                    json!({"cursor":cursor,"limit":100,"includeHidden":true}),
                )
                .await?;
            if let Some(model) = page["data"]
                .as_array()
                .and_then(|models| models.iter().find(|model| model["model"] == config.model))
            {
                supported = model["serviceTiers"]
                    .as_array()
                    .is_some_and(|tiers| tiers.iter().any(|tier| tier["id"] == "priority"));
                break;
            }
            let next = page["nextCursor"].clone();
            if next.is_null() || next == cursor {
                break;
            }
            cursor = next;
        }
        if !supported {
            return Err(crate::contract::invalid(
                "selected model does not advertise priority service tier support",
            ));
        }
    }
    let cwd = match environment {
        // A self-hosted workspace is not on this host.
        Environment::None | Environment::SelfHosted { .. } => None,
        Environment::Local { cwd } => Some(cwd),
    };
    let effective = state.rpc("config/read", json!({"cwd": cwd})).await?;
    let mut servers = effective
        .pointer("/config/mcp_servers")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    crate::mcp::check(state, config, &servers).await?;
    for selection in &config.mcp_servers {
        let Some(server) = servers.get(&selection.server) else {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!("unknown MCP server: {}", selection.server),
            ));
        };
        if server.get("enabled") == Some(&json!(false))
            || selection.allowed_tools.iter().any(|tool| {
                server
                    .get("enabled_tools")
                    .and_then(Value::as_array)
                    .is_some_and(|allowed| !allowed.contains(&json!(tool)))
                    || server
                        .get("disabled_tools")
                        .and_then(Value::as_array)
                        .is_some_and(|denied| denied.contains(&json!(tool)))
            })
        {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "MCP selection exceeds server-side policy".into(),
            ));
        }
    }
    for (name, server) in &mut servers {
        let selection = config
            .mcp_servers
            .iter()
            .find(|selection| selection.server == *name);
        // Override only policy fields. config/read includes nullable transport
        // defaults which are not valid TOML overrides, and may contain secrets.
        *server = json!({"enabled": selection.is_some()});
        if let Some(selection) = selection {
            server["enabled_tools"] = json!(selection.allowed_tools);
        }
    }
    servers.extend(crate::mcp::overrides(config, mcp_tokens));
    // Codex's V2 multi-agent runtime counts the root thread toward its cap, so
    // it gets one more thread than the public subagent limit.
    let multi_agent = match &config.multi_agent {
        Some(settings) if settings.enabled => json!({"enabled": true,
            "max_concurrent_threads_per_session": settings.max_concurrent_subagents.unwrap_or(6) + 1}),
        _ => json!(false),
    };
    // Disable other sources of MCP capabilities; never mutate global config.
    let mut overrides = json!({
        "mcp_servers": servers, "features.plugins": false, "features.apps": false,
        "features.enable_mcp_apps": false, "web_search": "disabled",
        "agents.enabled": false, "features.multi_agent_v2": multi_agent,
        // Codex's goal tools are not an advertised capability, and its MCP
        // resource tools would read any resource of a server, beyond the
        // caller's allowed tools.
        "features.goals": false,
        "features.mcp_resources": false,
        "tools.experimental_request_user_input.enabled": false,
        "model_reasoning_summary": config.reasoning.as_ref().and_then(|r| r.summary.as_deref()).unwrap_or("none"),
        "model_verbosity": config.text.as_ref().and_then(|t| t.verbosity.as_ref()).map_or(json!("medium"), |v| json!(v)),
        "features.fast_mode": true,
        "features.explicit_default_service_tier": true,
    });
    if let Some(effort) = config.reasoning.as_ref().and_then(|r| r.effort.as_ref()) {
        overrides["model_reasoning_effort"] = json!(effort);
    }
    Ok(overrides)
}
