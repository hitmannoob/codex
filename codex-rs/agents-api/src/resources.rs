use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AgentConfig {
    pub model: String,
    pub instructions: Option<String>,
    #[serde(default)]
    pub tools: Vec<crate::agent_tools::Tool>,
    #[serde(default)]
    pub mcp_servers: Vec<McpSelection>,
    pub reasoning: Option<Reasoning>,
    #[serde(default)]
    pub text: Option<crate::configuration::Text>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub multi_agent: Option<crate::configuration::MultiAgent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FunctionTool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
    #[serde(default, rename = "defer_loading", alias = "deferLoading")]
    pub defer_loading: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct McpSelection {
    pub server: String,
    pub allowed_tools: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reasoning {
    pub effort: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequiredAction {
    pub turn_id: String,
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ToolResult {
    pub call_id: String,
    pub success: bool,
    pub output: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Agent {
    pub id: String,
    pub config: AgentConfig,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum Environment {
    None,
    Local {
        cwd: AbsolutePathBuf,
    },
    /// A caller-owned executor; paths are on the executor's OS.
    SelfHosted {
        id: String,
        cwd: String,
        #[serde(default)]
        capability_directories: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Session {
    pub id: String,
    pub agent: Agent,
    pub environment: Environment,
    pub thread_id: Option<String>,
    #[serde(default)]
    pub required_actions: Vec<RequiredAction>,
    #[serde(default)]
    pub unresolved_actions: Vec<RequiredAction>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SessionCreateParams {
    pub agent_id: String,
    pub environment: Environment,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InputParams {
    pub input: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageParams {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}
