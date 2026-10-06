//! Public MCP servers: which configurations can run, where a session may
//! connect, and the session-scoped Codex configuration for each server.
//!
//! Service-origin HTTP servers are reached from the worker host, so their URL
//! must use https and resolve only to public addresses unless the operator
//! allows the host. Stdio servers and environment-origin HTTP connections run on
//! a self-hosted session's executor, on the caller's compute and network, so
//! the service's egress policy does not apply to them. Credentials come from
//! vaults (G11).
use crate::ApiError;
use crate::State;
use crate::agent_tools::CapabilityTool;
use crate::agent_tools::ConnectionOrigin;
use crate::agent_tools::McpTransport;
use crate::agent_tools::Tool;
use crate::contract::invalid;
use crate::resources::AgentConfig;
use axum::http::HeaderName;
use axum::http::HeaderValue;
use axum::http::Uri;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::net::IpAddr;

/// Where an executable MCP server runs.
enum Endpoint<'a> {
    /// HTTP from the worker host, within the service's egress policy.
    Service {
        url: &'a str,
        headers: &'a BTreeMap<String, String>,
    },
    /// HTTP from the session's executor.
    Environment {
        url: &'a str,
        headers: &'a BTreeMap<String, String>,
    },
    /// A process on the session's executor.
    Stdio {
        command: &'a str,
        args: &'a [String],
        cwd: &'a str,
        env_vars: &'a [String],
    },
}

/// An executable public MCP server.
struct Server<'a> {
    label: &'a str,
    endpoint: Endpoint<'a>,
    allowed_tools: Option<&'a Vec<String>>,
    required: bool,
}

fn servers(config: &AgentConfig) -> impl Iterator<Item = Server<'_>> {
    config.tools.iter().filter_map(|tool| match tool {
        Tool::Capability(CapabilityTool::Mcp {
            server_label,
            transport,
            connection_origin,
            allowed_tools,
            required,
            ..
        }) => Some(Server {
            label: server_label,
            endpoint: match (transport, connection_origin) {
                (
                    McpTransport::Http {
                        server_url,
                        headers,
                    },
                    ConnectionOrigin::Service,
                ) => Endpoint::Service {
                    url: server_url,
                    headers,
                },
                (
                    McpTransport::Http {
                        server_url,
                        headers,
                    },
                    ConnectionOrigin::Environment,
                ) => Endpoint::Environment {
                    url: server_url,
                    headers,
                },
                // A stdio server is a process on the executor whatever its origin.
                (
                    McpTransport::Stdio {
                        command,
                        cwd,
                        args,
                        env_vars,
                    },
                    ConnectionOrigin::Service | ConnectionOrigin::Environment,
                ) => Endpoint::Stdio {
                    command,
                    args,
                    cwd,
                    env_vars,
                },
            },
            allowed_tools: allowed_tools.as_ref(),
            required: *required,
        }),
        Tool::Capability(_) | Tool::Function(_) => None,
    })
}

/// Whether an MCP tool runs on the session's executor, which needs a
/// self-hosted environment.
pub(crate) fn needs_environment(tool: &CapabilityTool) -> bool {
    matches!(
        tool,
        CapabilityTool::Mcp {
            transport: McpTransport::Stdio { .. },
            ..
        } | CapabilityTool::Mcp {
            connection_origin: ConnectionOrigin::Environment,
            ..
        }
    )
}

/// Executable servers that may use a vault credential: (label, URL, explicit
/// credential ID).
pub(crate) fn credential_targets(
    config: &AgentConfig,
) -> impl Iterator<Item = (&str, &str, Option<&str>)> {
    config.tools.iter().filter_map(|tool| match tool {
        Tool::Capability(CapabilityTool::Mcp {
            server_label,
            transport: McpTransport::Http { server_url, .. },
            credential_id,
            ..
        }) => Some((
            server_label.as_str(),
            server_url.as_str(),
            credential_id.as_deref(),
        )),
        Tool::Capability(_) | Tool::Function(_) => None,
    })
}

/// Shape checks applied whenever MCP configuration is saved.
pub(crate) fn validate(config: &AgentConfig) -> Result<(), ApiError> {
    let mut labels = HashSet::new();
    for tool in &config.tools {
        let Tool::Capability(CapabilityTool::Mcp {
            server_label,
            transport,
            allowed_tools,
            ..
        }) = tool
        else {
            continue;
        };
        if !crate::capabilities::identifier(server_label) || !labels.insert(server_label) {
            return Err(invalid("MCP server labels must be unique identifiers"));
        }
        if let Some(tools) = allowed_tools
            && (tools.len() > 128
                || tools
                    .iter()
                    .any(|name| !crate::capabilities::identifier(name))
                || tools.iter().collect::<HashSet<_>>().len() != tools.len())
        {
            return Err(invalid(
                "allowed_tools must list at most 128 unique tool names",
            ));
        }
        let (server_url, headers) = match transport {
            McpTransport::Http {
                server_url,
                headers,
            } => (server_url, headers),
            McpTransport::Stdio {
                command,
                cwd,
                args,
                env_vars,
            } => {
                if command.is_empty()
                    || command.len() > 4096
                    || args.len() > 64
                    || args.iter().any(|arg| arg.len() > 4096)
                    || env_vars.len() > 64
                    || !env_vars.iter().all(|name| variable_name(name))
                {
                    return Err(invalid(
                        "stdio MCP servers take a command, at most 64 arguments, and at most 64 environment variable names",
                    ));
                }
                if !crate::environments::absolute(cwd) {
                    return Err(invalid("stdio MCP cwd must be an absolute path"));
                }
                continue;
            }
        };
        let uri: Uri = server_url
            .parse()
            .map_err(|_| invalid("MCP server_url must be an absolute http(s) URL"))?;
        if server_url.len() > 2048
            || uri.host().is_none()
            || !matches!(uri.scheme_str(), Some("https" | "http"))
        {
            return Err(invalid("MCP server_url must be an absolute http(s) URL"));
        }
        if headers.len() > 32 {
            return Err(invalid("MCP transports accept at most 32 headers"));
        }
        for (name, value) in headers {
            let name = HeaderName::try_from(name.as_str())
                .map_err(|_| invalid(format!("invalid MCP header name {name}")))?;
            HeaderValue::try_from(value.as_str())
                .map_err(|_| invalid(format!("invalid value for MCP header {name}")))?;
            // Headers are stored and returned as non-secret configuration.
            if name == axum::http::header::AUTHORIZATION {
                return Err(invalid(
                    "MCP authorization must come from a vault credential, not a header",
                ));
            }
        }
    }
    Ok(())
}

/// Whether `name` is a portable environment variable name.
pub(crate) fn variable_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Why a saved MCP configuration cannot run yet, if it cannot.
pub(crate) fn unsupported(tool: &CapabilityTool) -> Option<&'static str> {
    let CapabilityTool::Mcp {
        request_metadata, ..
    } = tool
    else {
        return None;
    };
    (!request_metadata.is_empty()).then_some("MCP request metadata")
}

/// Check where each server would connect, and that no label shadows a server
/// the worker is configured with. Runs before every turn, since DNS answers
/// and the operator's allowlist can change.
pub(crate) async fn check(
    state: &State,
    config: &AgentConfig,
    configured: &Map<String, Value>,
) -> Result<(), ApiError> {
    for server in servers(config) {
        if configured.contains_key(server.label) {
            return Err(invalid(format!(
                "MCP server label {} is reserved by the worker configuration",
                server.label
            )));
        }
        // Executor connections use the caller's network, not the service's.
        let Endpoint::Service { url, .. } = server.endpoint else {
            continue;
        };
        let uri: Uri = url
            .parse()
            .map_err(|_| invalid("MCP server_url must be an absolute http(s) URL"))?;
        let host = uri
            .host()
            .unwrap_or_default()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase();
        if crate::lock(&state.mcp_hosts).contains(&host) {
            continue;
        }
        if uri.scheme_str() != Some("https") {
            return Err(invalid(format!(
                "MCP server {} must use https unless the operator allows its host",
                server.label
            )));
        }
        let addresses: Vec<IpAddr> = match host.parse() {
            Ok(address) => vec![address],
            Err(_) => tokio::net::lookup_host((host.as_str(), uri.port_u16().unwrap_or(443)))
                .await
                .map_err(|_| invalid(format!("MCP server {} did not resolve", server.label)))?
                .map(|address| address.ip())
                .collect(),
        };
        if addresses.is_empty() || !addresses.iter().all(public) {
            return Err(invalid(format!(
                "MCP server {} resolves to a non-public address; the operator must allow its host",
                server.label
            )));
        }
    }
    Ok(())
}

/// Addresses a service may reach without operator approval.
pub(crate) fn public(address: &IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let [first, second, ..] = address.octets();
            !(address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_broadcast()
                || address.is_multicast()
                || address.is_documentation()
                || first == 0
                // Shared address space (100.64.0.0/10).
                || (first == 100 && (64..128).contains(&second)))
        }
        IpAddr::V6(address) => match address.to_ipv4_mapped() {
            Some(mapped) => public(&IpAddr::V4(mapped)),
            None => {
                !(address.is_loopback()
                    || address.is_unspecified()
                    || address.is_multicast()
                    || address.is_unique_local()
                    || address.is_unicast_link_local())
            }
        },
    }
}

/// Session-scoped Codex server entries. Only these fields are ever set from
/// public configuration, so no public field can reach worker-local options such
/// as header helper commands. Public tool calls need no interactive approval:
/// the caller's `allowed_tools` selection is the approval. `tokens` holds the
/// session's credential snapshots by server label; they travel only in this
/// in-memory thread configuration. Servers that run on the executor name the
/// session's `environment_id`; without one they are left out rather than run
/// on the worker host.
pub(crate) fn overrides(
    config: &AgentConfig,
    tokens: &BTreeMap<String, String>,
    stdio_env: &crate::stdio_env::Values,
    environment_id: Option<&str>,
) -> Map<String, Value> {
    servers(config)
        .filter_map(|server| {
            let mut entry = json!({"enabled": true, "required": server.required,
                "default_tools_approval_mode": "approve"});
            match server.endpoint {
                Endpoint::Service { url, headers } | Endpoint::Environment { url, headers } => {
                    entry["url"] = json!(url);
                    let mut headers = headers.clone();
                    if let Some(token) = tokens.get(server.label) {
                        headers.insert("Authorization".to_owned(), format!("Bearer {token}"));
                    }
                    if !headers.is_empty() {
                        entry["http_headers"] = json!(headers);
                    }
                }
                Endpoint::Stdio {
                    command,
                    args,
                    cwd,
                    env_vars,
                } => {
                    entry["command"] = json!(command);
                    entry["args"] = json!(args);
                    entry["cwd"] = json!(cwd);
                    // Values come from the executor's environment.
                    entry["env_vars"] = env_vars
                        .iter()
                        .map(|name| json!({"name": name, "source": "remote"}))
                        .collect();
                    if let Some(values) = stdio_env.get(server.label) {
                        entry["env"] = json!(values);
                    }
                }
            }
            if !matches!(server.endpoint, Endpoint::Service { .. }) {
                entry["environment_id"] = json!(environment_id?);
            }
            if let Some(tools) = server.allowed_tools {
                entry["enabled_tools"] = json!(tools);
            }
            Some((server.label.to_owned(), entry))
        })
        .collect()
}

#[cfg(test)]
#[path = "mcp_tests.rs"]
mod tests;
