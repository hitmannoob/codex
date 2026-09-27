//! Persisted tool configuration. Execution of non-function tools belongs to G06-G08.
use crate::resources::FunctionTool;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

// The untagged function representation preserves existing saved agents and prototype clients.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub(crate) enum Tool {
    Function(FunctionTool),
    Capability(CapabilityTool),
}

impl Tool {
    pub(crate) fn function(&self) -> Option<&FunctionTool> {
        match self {
            Self::Function(tool) => Some(tool),
            Self::Capability(_) => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CapabilityTool {
    ToolSearch,
    ProgrammaticToolCalling {
        #[serde(default = "enabled")]
        enabled: bool,
    },
    Mcp {
        server_label: String,
        transport: McpTransport,
        allowed_tools: Option<Vec<String>>,
        #[serde(default, deserialize_with = "default_if_null")]
        connection_origin: ConnectionOrigin,
        credential_id: Option<String>,
        #[serde(default, deserialize_with = "default_if_null")]
        request_metadata: BTreeMap<String, Value>,
        #[serde(default)]
        required: bool,
    },
    WebSearch {
        allowed_domains: Option<Vec<String>>,
        #[serde(default, deserialize_with = "default_if_null")]
        context_size: Detail,
        location: Option<Location>,
        #[serde(default, deserialize_with = "default_if_null")]
        mode: SearchMode,
    },
}

fn enabled() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum McpTransport {
    Http {
        server_url: String,
        #[serde(default, deserialize_with = "default_if_null")]
        headers: BTreeMap<String, String>,
    },
    Stdio {
        command: String,
        cwd: String,
        #[serde(default, deserialize_with = "default_if_null")]
        args: Vec<String>,
        #[serde(default, deserialize_with = "default_if_null")]
        env_vars: Vec<String>,
    },
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConnectionOrigin {
    #[default]
    Service,
    Environment,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Detail {
    Low,
    #[default]
    Medium,
    High,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SearchMode {
    Disabled,
    Cached,
    #[default]
    Live,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Location {
    city: Option<String>,
    country: Option<String>,
    region: Option<String>,
    timezone: Option<String>,
}

fn default_if_null<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}
