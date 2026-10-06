use super::*;
use pretty_assertions::assert_eq;

const BOUNDARY: &str = "agents-api-test-boundary";

/// A `multipart/form-data` upload body, as the SDK's `files.create` sends.
pub(super) fn upload_body(fields: &[(&str, &str)], filename: &str, contents: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend(
            format!(
                "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .bytes(),
        );
    }
    body.extend(format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n").bytes());
    body.extend(contents);
    body.extend(format!("\r\n--{BOUNDARY}--\r\n").bytes());
    body
}

pub(super) async fn upload(
    client: &reqwest::Client,
    base: &str,
    fields: &[(&str, &str)],
    filename: &str,
    contents: &[u8],
) -> anyhow::Result<reqwest::Response> {
    Ok(client
        .post(format!("{base}/files"))
        .bearer_auth(TOKEN)
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={BOUNDARY}"),
        )
        .body(upload_body(fields, filename, contents))
        .send()
        .await?)
}

#[tokio::test]
async fn files_upload_list_download_and_delete() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 60), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("unused").await;
        MockResponsesConfig::new(&provider.uri()).write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();

        let notes = upload(&client, &base, &[("purpose", "user_data")], "notes.txt", b"first file").await?.error_for_status()?.json::<Value>().await?;
        let id = notes["id"].as_str().context("file id")?.to_owned();
        assert!(id.starts_with("file-"), "{id}");
        assert_eq!(notes, json!({"id":id,"object":"file","bytes":10,"created_at":notes["created_at"],"filename":"notes.txt",
            "purpose":"user_data","status":"processed","expires_at":null,"status_details":null}));
        let expiring = upload(&client, &base, &[("purpose", "assistants"), ("expires_after[anchor]", "created_at"), ("expires_after[seconds]", "3600")],
            "data.bin", &[0, 1, 2, 255]).await?.error_for_status()?.json::<Value>().await?;
        assert_eq!(expiring["expires_at"].as_i64(), expiring["created_at"].as_i64().map(|created| created + 3600));

        // Uploads that break the contract are refused in the public error shape.
        for (fields, message) in [
            (vec![("purpose", "weights")], "purpose must be one of"),
            (vec![], "purpose is required"),
            (vec![("purpose", "user_data"), ("expires_after[anchor]", "created_at"), ("expires_after[seconds]", "60")], "expires_after.seconds must be between"),
        ] {
            let response = upload(&client, &base, &fields, "x.txt", b"x").await?;
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
            let error = response.json::<Value>().await?["error"]["message"].as_str().unwrap_or_default().to_owned();
            assert!(error.starts_with(message), "{error}");
        }

        assert_eq!(request(&client, reqwest::Method::GET, &format!("{base}/files/{id}"), Value::Null).await?, notes);
        let content = client.get(format!("{base}/files/{}/content", expiring["id"].as_str().context("id")?)).bearer_auth(TOKEN).send().await?;
        assert_eq!(content.bytes().await?.to_vec(), vec![0, 1, 2, 255]);
        let ids = |page: &Value| page["data"].as_array().into_iter().flatten().map(|file| file["id"].clone()).collect::<Vec<_>>();
        let newest = request(&client, reqwest::Method::GET, &format!("{base}/files?limit=1"), Value::Null).await?;
        assert_eq!((ids(&newest), &newest["has_more"]), (vec![expiring["id"].clone()], &json!(true)));
        let older = request(&client, reqwest::Method::GET, &format!("{base}/files?after={}", expiring["id"].as_str().context("id")?), Value::Null).await?;
        assert_eq!((ids(&older), &older["has_more"]), (vec![json!(id)], &json!(false)));
        let ascending = request(&client, reqwest::Method::GET, &format!("{base}/files?order=asc"), Value::Null).await?;
        assert_eq!(ids(&ascending), vec![json!(id), expiring["id"].clone()]);
        let user_data = request(&client, reqwest::Method::GET, &format!("{base}/files?purpose=user_data"), Value::Null).await?;
        assert_eq!(ids(&user_data), vec![json!(id)]);

        assert_eq!(request(&client, reqwest::Method::DELETE, &format!("{base}/files/{id}"), Value::Null).await?,
            json!({"id":id,"object":"file","deleted":true}));
        for suffix in ["", "/content"] {
            let gone = client.get(format!("{base}/files/{id}{suffix}")).bearer_auth(TOKEN).send().await?;
            assert_eq!(gone.status(), reqwest::StatusCode::NOT_FOUND);
        }
        assert!(!data.path().join("files").join(&id).exists());
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
