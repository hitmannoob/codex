//! A streaming `multipart/form-data` reader for file uploads. The `file` part
//! is written to disk as it arrives and hashed on the way; other parts are
//! small form fields kept in memory. Nothing holds a whole upload in memory.
use crate::ApiError;
use crate::contract::invalid;
use axum::body::Body;
use futures::StreamExt;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::path::Path;
use tokio::io::AsyncWriteExt;

/// Bytes of form fields and part headers an upload may carry besides its file.
const MAX_FIELD_BYTES: usize = 64 * 1024;

/// A received upload: its form fields, and the file part's name, size, and
/// SHA-256 digest.
pub(crate) struct Upload {
    pub(crate) fields: BTreeMap<String, String>,
    pub(crate) filename: String,
    pub(crate) size: u64,
    pub(crate) sha256: String,
}

enum Phase {
    /// Before the first delimiter.
    Preamble,
    /// After a delimiter: either the final `--` or a part's headers follow.
    Delimited,
    Headers,
    /// Inside a part: the file, or a field with this name.
    File,
    Field(String, Vec<u8>),
}

/// The boundary of a `multipart/form-data` content type.
pub(crate) fn boundary(content_type: &str) -> Option<String> {
    if !content_type.starts_with("multipart/form-data") {
        return None;
    }
    content_type
        .split(';')
        .map(str::trim)
        .find_map(|parameter| parameter.strip_prefix("boundary="))
        .map(|boundary| boundary.trim_matches('"').to_owned())
        .filter(|boundary| !boundary.is_empty() && boundary.len() <= 70)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Read an upload, writing its `file` part to `destination` (at most
/// `max_file_bytes`). The caller removes `destination` if this fails.
pub(crate) async fn receive(
    body: Body,
    boundary: &str,
    destination: &Path,
    max_file_bytes: u64,
) -> Result<Upload, ApiError> {
    let malformed = || invalid("malformed multipart body");
    // A leading CRLF lets the first delimiter match the same separator as
    // the ones between parts.
    let separator = format!("\r\n--{boundary}").into_bytes();
    let mut buffer = b"\r\n".to_vec();
    let mut chunks = body.into_data_stream();
    let mut phase = Phase::Preamble;
    let mut fields = BTreeMap::new();
    let mut file: Option<(tokio::fs::File, String)> = None;
    let mut file_done = false;
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut side_bytes = 0usize;
    loop {
        // Consume what the buffer holds; `None` asks for more input.
        let progressed = match &mut phase {
            Phase::Preamble => find(&buffer, &separator).map(|at| {
                buffer.drain(..at + separator.len());
                phase = Phase::Delimited;
            }),
            Phase::Delimited if buffer.len() < 2 => None,
            Phase::Delimited => {
                match &buffer[..2] {
                    b"--" => break,
                    b"\r\n" => {
                        buffer.drain(..2);
                        phase = Phase::Headers;
                    }
                    _ => return Err(malformed()),
                }
                Some(())
            }
            Phase::Headers => match find(&buffer, b"\r\n\r\n") {
                None if buffer.len() > MAX_FIELD_BYTES => return Err(malformed()),
                None => None,
                Some(end) => {
                    side_bytes += end;
                    let headers = std::str::from_utf8(&buffer[..end]).map_err(|_| malformed())?;
                    let disposition = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.trim()
                                .eq_ignore_ascii_case("content-disposition")
                                .then_some(value.to_owned())
                        })
                        .ok_or_else(malformed)?;
                    let parameter = |key: &str| {
                        disposition.split(';').map(str::trim).find_map(|parameter| {
                            parameter
                                .strip_prefix(key)
                                .and_then(|rest| rest.strip_prefix('='))
                                .map(|value| value.trim_matches('"').to_owned())
                        })
                    };
                    let name = parameter("name").ok_or_else(malformed)?;
                    buffer.drain(..end + 4);
                    phase = match parameter("filename") {
                        Some(filename) if name == "file" && !file_done && file.is_none() => {
                            let handle = tokio::fs::File::create(destination)
                                .await
                                .map_err(anyhow::Error::from)?;
                            file = Some((handle, filename));
                            Phase::File
                        }
                        _ if name == "file" => return Err(invalid("upload exactly one file")),
                        _ => Phase::Field(name, Vec::new()),
                    };
                    Some(())
                }
            },
            Phase::File | Phase::Field(..) => {
                let (content, end) = match find(&buffer, &separator) {
                    Some(at) => (at, Some(at + separator.len())),
                    // Keep a tail that may begin the separator.
                    None => (buffer.len().saturating_sub(separator.len() - 1), None),
                };
                if content > 0 {
                    match &mut phase {
                        Phase::File => {
                            size += content as u64;
                            if size > max_file_bytes {
                                return Err(invalid(format!(
                                    "files are limited to {max_file_bytes} bytes"
                                )));
                            }
                            hasher.update(&buffer[..content]);
                            if let Some((handle, _)) = &mut file {
                                handle
                                    .write_all(&buffer[..content])
                                    .await
                                    .map_err(anyhow::Error::from)?;
                            }
                        }
                        Phase::Field(_, value) => {
                            side_bytes += content;
                            if side_bytes > MAX_FIELD_BYTES {
                                return Err(invalid("upload form fields are too large"));
                            }
                            value.extend_from_slice(&buffer[..content]);
                        }
                        Phase::Preamble | Phase::Delimited | Phase::Headers => {}
                    }
                    buffer.drain(..content);
                }
                end.map(|end| {
                    buffer.drain(..end - content);
                    match std::mem::replace(&mut phase, Phase::Delimited) {
                        Phase::File => file_done = true,
                        Phase::Field(name, value) => {
                            fields.insert(name, String::from_utf8_lossy(&value).into_owned());
                        }
                        Phase::Preamble | Phase::Delimited | Phase::Headers => {}
                    }
                })
            }
        };
        if progressed.is_some() {
            continue;
        }
        match chunks.next().await {
            Some(chunk) => buffer.extend_from_slice(&chunk.map_err(|_| malformed())?),
            None => return Err(malformed()),
        }
    }
    let (mut handle, filename) = file
        .filter(|_| file_done)
        .ok_or_else(|| invalid("file is required"))?;
    handle.flush().await.map_err(anyhow::Error::from)?;
    handle.sync_all().await.map_err(anyhow::Error::from)?;
    Ok(Upload {
        fields,
        filename,
        size,
        sha256: format!("{:x}", hasher.finalize()),
    })
}

#[cfg(test)]
#[path = "upload_tests.rs"]
mod tests;
