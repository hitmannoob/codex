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
    for tool in &config.tools {
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
    if let Some(reasoning) = &config.reasoning
        && !matches!(
            reasoning.effort.as_str(),
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

fn identifier(value: &str) -> bool {
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
) -> Result<Value, ApiError> {
    let cwd = match environment {
        Environment::None => None,
        Environment::Local { cwd } => Some(cwd),
    };
    let effective = state.rpc("config/read", json!({"cwd": cwd})).await?;
    let mut servers = effective
        .pointer("/config/mcp_servers")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
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
    // Disable other sources of MCP capabilities; never mutate global config.
    let mut overrides = json!({
        "mcp_servers": servers, "features.plugins": false, "features.apps": false,
        "features.enable_mcp_apps": false, "web_search": "disabled",
        "agents.enabled": false, "features.multi_agent_v2": false,
        "tools.experimental_request_user_input.enabled": false,
    });
    if let Some(reasoning) = &config.reasoning {
        overrides["model_reasoning_effort"] = json!(reasoning.effort);
    }
    Ok(overrides)
}
