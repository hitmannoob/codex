//! Public MCP servers: which configurations can run, where a session may
//! connect, and the session-scoped Codex configuration for each server.
//!
//! Only HTTP servers reached from the service are executable. Stdio servers and
//! environment-origin connections run inside an execution environment (G09), and
//! credentials come from vaults (G11). The worker host makes each connection, so
//! a server URL must use https and resolve only to public addresses unless the
//! operator allows its host.
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

/// An executable public MCP server.
struct Server<'a> {
    label: &'a str,
    url: &'a str,
    headers: &'a BTreeMap<String, String>,
    allowed_tools: Option<&'a Vec<String>>,
    required: bool,
}

fn servers(config: &AgentConfig) -> impl Iterator<Item = Server<'_>> {
    config.tools.iter().filter_map(|tool| match tool {
        Tool::Capability(CapabilityTool::Mcp {
            server_label,
            transport:
                McpTransport::Http {
                    server_url,
                    headers,
                },
            allowed_tools,
            required,
            ..
        }) => Some(Server {
            label: server_label,
            url: server_url,
            headers,
            allowed_tools: allowed_tools.as_ref(),
            required: *required,
        }),
        Tool::Capability(_) | Tool::Function(_) => None,
    })
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
        let McpTransport::Http {
            server_url,
            headers,
        } = transport
        else {
            continue;
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

/// Why a saved MCP configuration cannot run yet, if it cannot.
pub(crate) fn unsupported(tool: &CapabilityTool) -> Option<&'static str> {
    let CapabilityTool::Mcp {
        transport,
        connection_origin,
        request_metadata,
        ..
    } = tool
    else {
        return None;
    };
    if matches!(transport, McpTransport::Stdio { .. }) {
        Some("stdio MCP server")
    } else if *connection_origin == ConnectionOrigin::Environment {
        Some("environment-origin MCP connection")
    } else if !request_metadata.is_empty() {
        Some("MCP request metadata")
    } else {
        None
    }
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
        let uri: Uri = server
            .url
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
/// in-memory thread configuration.
pub(crate) fn overrides(
    config: &AgentConfig,
    tokens: &BTreeMap<String, String>,
) -> Map<String, Value> {
    servers(config)
        .map(|server| {
            let mut entry = json!({"url": server.url, "enabled": true, "required": server.required,
                "default_tools_approval_mode": "approve"});
            let mut headers = server.headers.clone();
            if let Some(token) = tokens.get(server.label) {
                headers.insert("Authorization".to_owned(), format!("Bearer {token}"));
            }
            if !headers.is_empty() {
                entry["http_headers"] = json!(headers);
            }
            if let Some(tools) = server.allowed_tools {
                entry["enabled_tools"] = json!(tools);
            }
            (server.label.to_owned(), entry)
        })
        .collect()
}

#[cfg(test)]
#[path = "mcp_tests.rs"]
mod tests;
