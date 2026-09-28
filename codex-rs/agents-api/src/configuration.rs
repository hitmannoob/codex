//! Saved and inline agent configuration; shared by creation and replacement updates.
use crate::ApiError;
use crate::agent_tools::Tool;
use crate::contract::invalid;
use crate::resources::Agent;
use crate::resources::AgentConfig;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Text {
    pub format: Option<TextFormat>,
    pub verbosity: Option<crate::agent_tools::Detail>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TextFormat {
    Text,
    JsonSchema {
        schema: serde_json::Map<String, Value>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MultiAgent {
    pub enabled: bool,
    pub max_concurrent_subagents: Option<u32>,
}

pub(crate) fn agent(agent: &Agent) -> Value {
    let config = &agent.config;
    let enabled = config.multi_agent.as_ref().is_some_and(|m| m.enabled);
    json!({"id":agent.id,"model":config.model,"instructions":config.instructions,
        "name":agent.name,"multi_agent":{"enabled":enabled,
            "max_concurrent_subagents":enabled.then(|| config.multi_agent.as_ref().and_then(|m| m.max_concurrent_subagents).unwrap_or(6))},
        "reasoning":{"effort":config.reasoning.as_ref().and_then(|r| r.effort.as_ref()),
            "summary":config.reasoning.as_ref().and_then(|r| r.summary.as_ref())},
        "service_tier":config.service_tier.as_deref().unwrap_or("auto"),
        "text":{"format":config.text.as_ref().and_then(|t| t.format.as_ref()).map_or(json!({"type":"text"}), |f| json!(f)),
            "verbosity":config.text.as_ref().and_then(|t| t.verbosity.as_ref()).map_or(json!("medium"), |v| json!(v))},
        "tools":config.tools.iter().map(|tool| match tool {
            Tool::Function(t) => json!({"type":"function","name":t.name,"description":t.description,"parameters":t.parameters,"defer_loading":t.defer_loading}),
            Tool::Capability(t) => json!(t),
        }).collect::<Vec<_>>()})
}

/// Parse replacement metadata: null clears it, and a supplied map replaces it.
pub(crate) fn metadata(value: Value) -> Result<BTreeMap<String, String>, ApiError> {
    if value.is_null() {
        return Ok(BTreeMap::new());
    }
    let metadata: BTreeMap<String, String> =
        serde_json::from_value(value).map_err(|_| invalid("metadata must contain string pairs"))?;
    if metadata.len() > 16
        || metadata
            .iter()
            .any(|(key, value)| key.chars().count() > 64 || value.chars().count() > 512)
    {
        return Err(invalid("metadata exceeds its size limit"));
    }
    Ok(metadata)
}

pub(crate) fn configure(mut config: AgentConfig, patch: Value) -> Result<AgentConfig, ApiError> {
    let fields = patch
        .as_object()
        .ok_or_else(|| invalid("agent must be an object"))?;
    for (key, value) in fields {
        match key.as_str() {
            "model" => {
                config.model = value
                    .as_str()
                    .ok_or_else(|| invalid("model must be a string"))?
                    .into()
            }
            "instructions" => {
                config.instructions =
                    serde_json::from_value(value.clone()).map_err(|e| invalid(e.to_string()))?
            }
            "tools" => {
                config.tools.clear();
                if !value.is_null() {
                    for tool in value
                        .as_array()
                        .ok_or_else(|| invalid("tools must be an array or null"))?
                    {
                        let mut tool = tool.clone();
                        let kind = tool
                            .get("type")
                            .and_then(Value::as_str)
                            .ok_or_else(|| invalid("tool type is required"))?;
                        if kind == "function" {
                            tool.as_object_mut()
                                .ok_or_else(|| invalid("invalid function"))?
                                .remove("type");
                        }
                        config.tools.push(
                            serde_json::from_value(tool).map_err(|e| invalid(e.to_string()))?,
                        );
                    }
                }
            }
            "reasoning" => {
                config.reasoning =
                    serde_json::from_value(value.clone()).map_err(|e| invalid(e.to_string()))?
            }
            "text" => {
                config.text =
                    serde_json::from_value(value.clone()).map_err(|e| invalid(e.to_string()))?
            }
            "service_tier" => {
                config.service_tier =
                    serde_json::from_value(value.clone()).map_err(|e| invalid(e.to_string()))?;
                if config.service_tier.as_ref().is_some_and(|t| {
                    !matches!(
                        t.as_str(),
                        "auto" | "default" | "flex" | "priority" | "fast"
                    )
                }) {
                    return Err(invalid("unsupported service tier"));
                }
            }
            "multi_agent" => {
                config.multi_agent =
                    serde_json::from_value(value.clone()).map_err(|e| invalid(e.to_string()))?;
                if config
                    .multi_agent
                    .as_ref()
                    .is_some_and(|m| m.max_concurrent_subagents == Some(0))
                {
                    return Err(invalid("max_concurrent_subagents must be positive"));
                }
            }
            _ => return Err(invalid(format!("agent field {key} is not implemented"))),
        }
    }
    if config.model.trim().is_empty()
        || config.model.len() > 256
        || config.instructions.as_deref().unwrap_or_default().len() > 1024
    {
        return Err(invalid(
            "model must be 1-256 bytes; instructions must be at most 1024 bytes",
        ));
    }
    if !config.mcp_servers.is_empty() {
        return Err(invalid(
            "saved prototype MCP selections cannot be used on this API path",
        ));
    }
    if config
        .reasoning
        .as_ref()
        .and_then(|r| r.effort.as_deref())
        .is_some_and(|e| {
            !matches!(
                e,
                "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
            )
        })
    {
        return Err(invalid("unsupported reasoning effort"));
    }
    if config
        .reasoning
        .as_ref()
        .and_then(|r| r.summary.as_deref())
        .is_some_and(|s| !matches!(s, "concise" | "detailed" | "auto"))
    {
        return Err(invalid("unsupported reasoning summary"));
    }
    crate::capabilities::validate(&config)?;
    Ok(config)
}

/// Keep stored configuration separate from capabilities not yet implemented by this service.
pub(crate) fn validate_execution(config: &AgentConfig) -> Result<(), ApiError> {
    use crate::agent_tools::CapabilityTool;
    use crate::agent_tools::SearchMode;
    if config.multi_agent.as_ref().is_some_and(|m| m.enabled) {
        return Err(invalid("multi-agent execution is not implemented"));
    }
    for tool in &config.tools {
        let unsupported = match tool {
            Tool::Function(tool) if tool.defer_loading => Some("deferred function discovery"),
            Tool::Function(_) => None,
            Tool::Capability(CapabilityTool::ProgrammaticToolCalling { enabled: false }) => None,
            Tool::Capability(CapabilityTool::WebSearch {
                mode: SearchMode::Disabled,
                ..
            }) => None,
            Tool::Capability(CapabilityTool::ToolSearch) => Some("tool search"),
            Tool::Capability(CapabilityTool::ProgrammaticToolCalling { enabled: true }) => {
                Some("programmatic tool calling")
            }
            Tool::Capability(tool @ CapabilityTool::Mcp { .. }) => crate::mcp::unsupported(tool),
            Tool::Capability(CapabilityTool::WebSearch { .. }) => Some("web search"),
        };
        if let Some(feature) = unsupported {
            return Err(invalid(format!("{feature} execution is not implemented")));
        }
    }
    Ok(())
}
