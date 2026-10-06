use super::*;
use pretty_assertions::assert_eq;

const BOUNDARY: &str = "test-boundary";

fn body(parts: &[(&str, Option<&str>, &[u8])]) -> Vec<u8> {
    let mut body = b"preamble".to_vec();
    for (name, filename, value) in parts {
        let filename = filename
            .map(|name| format!("; filename=\"{name}\""))
            .unwrap_or_default();
        body.extend(format!("\r\n--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"{filename}\r\n\r\n").bytes());
        body.extend_from_slice(value);
    }
    body.extend(format!("\r\n--{BOUNDARY}--\r\n").bytes());
    body
}

/// Feed the body in chunks of `size` bytes, so delimiters straddle chunks.
async fn receive_in_chunks(
    body: Vec<u8>,
    size: usize,
    max: u64,
) -> (Result<Upload, ApiError>, Vec<u8>) {
    let directory = tempfile::tempdir().expect("tempdir");
    let destination = directory.path().join("upload");
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
        body.chunks(size).map(|chunk| Ok(chunk.to_vec())).collect();
    let result = receive(
        Body::from_stream(futures::stream::iter(chunks)),
        BOUNDARY,
        &destination,
        max,
    )
    .await;
    let written = std::fs::read(&destination).unwrap_or_default();
    (result, written)
}

#[tokio::test]
async fn uploads_stream_whatever_the_chunking() {
    // Contents that contain a near-miss of the separator.
    let contents = format!("line one\r\n--{}x\r\n--test-boundar", "test-boundar").into_bytes();
    let upload = body(&[
        ("purpose", None, b"user_data"),
        ("file", Some("a.txt"), &contents),
        ("note", None, b"n"),
    ]);
    for size in [1, 3, 7, 64, upload.len()] {
        let (result, written) = receive_in_chunks(upload.clone(), size, /*max*/ 1024).await;
        let received = result.unwrap_or_else(|error| panic!("chunk size {size}: {}", error.1));
        assert_eq!(
            (
                received.fields,
                received.filename,
                received.size,
                received.sha256,
                written
            ),
            (
                BTreeMap::from([
                    ("note".to_owned(), "n".to_owned()),
                    ("purpose".to_owned(), "user_data".to_owned())
                ]),
                "a.txt".to_owned(),
                contents.len() as u64,
                format!("{:x}", Sha256::digest(&contents)),
                contents.clone(),
            ),
            "chunk size {size}"
        );
    }
}

#[tokio::test]
async fn malformed_and_oversized_uploads_are_refused() {
    let messages = |result: Result<Upload, ApiError>| result.err().map(|error| error.1);
    let truncated = body(&[("file", Some("a.txt"), b"abc")]);
    let truncated = truncated[..truncated.len() - 10].to_vec();
    assert_eq!(
        messages(receive_in_chunks(truncated, 4, 1024).await.0),
        Some("malformed multipart body".to_owned())
    );
    let oversized = body(&[("file", Some("a.txt"), &[7; 100])]);
    assert_eq!(
        messages(receive_in_chunks(oversized, 16, 99).await.0),
        Some("files are limited to 99 bytes".to_owned())
    );
    let twice = body(&[("file", Some("a.txt"), b"a"), ("file", Some("b.txt"), b"b")]);
    assert_eq!(
        messages(receive_in_chunks(twice, 5, 1024).await.0),
        Some("upload exactly one file".to_owned())
    );
    let none = body(&[("purpose", None, b"user_data")]);
    assert_eq!(
        messages(receive_in_chunks(none, 5, 1024).await.0),
        Some("file is required".to_owned())
    );
}
