use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AgentConfig {
    pub model: String,
    pub instructions: String,
    #[serde(default)]
    pub tools: Vec<FunctionTool>,
    #[serde(default)]
    pub mcp_servers: Vec<McpSelection>,
    pub reasoning: Option<Reasoning>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FunctionTool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
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
    pub effort: String,
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
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum Environment {
    None,
    Local { cwd: AbsolutePathBuf },
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
