use super::*;
use pretty_assertions::assert_eq;

const SESSIONS: usize = 8;
const TURNS: usize = 3;

/// The texts of a session's user messages, oldest first.
async fn user_inputs(client: &reqwest::Client, url: &str) -> anyhow::Result<Vec<String>> {
    let items = request(
        client,
        reqwest::Method::GET,
        &format!("{url}/items?order=asc&limit=100"),
        Value::Null,
    )
    .await?;
    Ok(items["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| item["role"] == "user")
        .filter_map(|item| item["content"][0]["text"].as_str().map(str::to_owned))
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sessions_stay_apart_while_a_subscriber_stalls() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        MockResponsesConfig::new(&provider.uri()).with_root_config("features.plugins = false").write(home.path())?;
        let api = AgentsApi::new(backend(home.path()).await?, AbsolutePathBuf::from_absolute_path(data.path())?, TOKEN.into()).await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let sessions = format!("{base}/agents/sessions");
        let input = |session: usize, turn: usize| format!("session-{session}-turn-{turn}");

        let created = futures::future::try_join_all((0..SESSIONS).map(|session| {
            let (client, sessions) = (&client, &sessions);
            let body = json!({"agent":{"model":"mock-model"},"environment":{"type":"none"},"input":input(session, 0)});
            async move { request(client, reqwest::Method::POST, sessions, body).await }
        })).await?;
        let urls: Vec<String> = created.iter()
            .map(|session| session["id"].as_str().map(|id| format!("{sessions}/{id}")).context("session id"))
            .collect::<anyhow::Result<_>>()?;
        // A subscriber that never reads must not hold anyone up.
        let stalled = client.get(format!("{}/events", urls[0])).bearer_auth(TOKEN).send().await?.error_for_status()?;

        futures::future::try_join_all(urls.iter().enumerate().map(|(session, url)| {
            let client = &client;
            async move {
                idle(client, url, /*expected_turns*/ 1).await?;
                for turn in 1..TURNS {
                    let body = json!({"events":[{"type":"agent.session.input.message","input":input(session, turn)}]});
                    let status = client.post(format!("{url}/events")).bearer_auth(TOKEN).json(&body).send().await?.status();
                    anyhow::ensure!(status == reqwest::StatusCode::ACCEPTED, "{url}: {status}");
                    idle(client, url, turn + 1).await?;
                }
                Ok::<_, anyhow::Error>(())
            }
        })).await?;
        drop(stalled);

        // Every session ran exactly its own inputs, in order.
        let mut observed = Vec::new();
        for url in &urls {
            observed.push(user_inputs(&client, url).await?);
        }
        let expected: Vec<Vec<String>> = (0..SESSIONS).map(|session| (0..TURNS).map(|turn| input(session, turn)).collect()).collect();
        assert_eq!(observed, expected);

        // Deleting them all concurrently leaves nothing behind.
        futures::future::try_join_all(urls.iter().map(|url| request(&client, reqwest::Method::DELETE, url, Value::Null))).await?;
        for session in &created {
            assert_eq!(leftover_rows(data.path(), session["id"].as_str().context("id")?).await?, Vec::new());
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

/// Input that steers a turn as it ends is answered, never dropped: Codex
/// records input that arrives after a turn's last model request without
/// answering it, and the service then starts a follow-up turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn input_steered_as_a_turn_ends_is_always_answered() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 120), async {
        let home = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let provider = create_mock_responses_server_repeating_assistant("finished").await;
        MockResponsesConfig::new(&provider.uri())
            .with_root_config("features.plugins = false")
            .write(home.path())?;
        let api = AgentsApi::new(
            backend(home.path()).await?,
            AbsolutePathBuf::from_absolute_path(data.path())?,
            TOKEN.into(),
        )
        .await?;
        let (base, _server) = capabilities::serve(&api).await?;
        let client = reqwest::Client::new();
        let sessions = format!("{base}/agents/sessions");
        let send = |url: String, text: String| {
            let client = client.clone();
            async move {
                let body = json!({"events":[{"type":"agent.session.input.message","input":text}]});
                let status = client
                    .post(format!("{url}/events"))
                    .bearer_auth(TOKEN)
                    .json(&body)
                    .send()
                    .await?
                    .status();
                anyhow::ensure!(status == reqwest::StatusCode::ACCEPTED, "{url}: {status}");
                Ok::<_, anyhow::Error>(())
            }
        };
        let urls = futures::future::try_join_all((0..SESSIONS).map(|session| {
            let (client, sessions) = (&client, &sessions);
            async move {
                let created = request(
                    client,
                    reqwest::Method::POST,
                    sessions,
                    json!({"agent":{"model":"mock-model"},
                    "environment":{"type":"none"},"input":format!("session-{session}-start")}),
                )
                .await?;
                let url = format!("{sessions}/{}", created["id"].as_str().context("id")?);
                idle(client, &url, /*expected_turns*/ 1).await?;
                Ok::<_, anyhow::Error>(url)
            }
        }))
        .await?;
        let rounds =
            futures::future::try_join_all(urls.iter().enumerate().map(|(session, url)| {
                let (client, send, provider) = (&client, &send, &provider);
                async move {
                    let mut steered = Vec::new();
                    for round in 0..TURNS {
                        let mut stream = client
                            .get(format!("{url}/events"))
                            .bearer_auth(TOKEN)
                            .send()
                            .await?
                            .error_for_status()?;
                        send(url.clone(), format!("session-{session}-round-{round}")).await?;
                        // Steer the moment the answer is done, as the turn ends.
                        until_assistant_done(&mut stream).await?;
                        let steer = format!("session-{session}-steer-{round}");
                        send(url.clone(), steer.clone()).await?;
                        // Accepted input may not have started a turn yet, so
                        // wait for the model to see it before waiting for idle.
                        let deadline =
                            tokio::time::Instant::now() + Duration::from_secs(/*secs*/ 20);
                        while !provider
                            .received_requests()
                            .await
                            .unwrap_or_default()
                            .iter()
                            .any(|request| {
                                String::from_utf8_lossy(&request.body).contains(steer.as_str())
                            })
                        {
                            anyhow::ensure!(
                                tokio::time::Instant::now() < deadline,
                                "{steer} was never answered"
                            );
                            tokio::time::sleep(Duration::from_millis(/*millis*/ 20)).await;
                        }
                        steered.push(steer);
                        idle(client, url, /*expected_turns*/ 1).await?;
                    }
                    Ok::<_, anyhow::Error>(steered)
                }
            }))
            .await?;
        let steered: Vec<String> = rounds.into_iter().flatten().collect();

        // Every steered input reached the model, and every session ended on
        // an answer.
        let bodies: Vec<String> = provider
            .received_requests()
            .await
            .context("requests")?
            .iter()
            .map(|request| String::from_utf8_lossy(&request.body).into_owned())
            .collect();
        let unanswered: Vec<&String> = steered
            .iter()
            .filter(|text| !bodies.iter().any(|body| body.contains(text.as_str())))
            .collect();
        assert_eq!(unanswered, Vec::<&String>::new());
        for url in &urls {
            let items = request(
                &client,
                reqwest::Method::GET,
                &format!("{url}/items?order=desc&limit=1"),
                Value::Null,
            )
            .await?;
            assert_eq!(items["data"][0]["role"], "assistant", "{url}");
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

/// Read a session stream until an assistant message is done.
async fn until_assistant_done(stream: &mut reqwest::Response) -> anyhow::Result<()> {
    let mut buffer = String::new();
    loop {
        while let Some(end) = buffer.find("\n\n") {
            let block: String = buffer.drain(..end + 2).collect();
            if let Some(data) = block.lines().find_map(|line| line.strip_prefix("data: ")) {
                let event: Value = serde_json::from_str(data)?;
                if event["type"] == "agent.session.turn.item.done"
                    && event["item"]["role"] == "assistant"
                {
                    return Ok(());
                }
            }
        }
        let chunk = stream.chunk().await?.context("event stream ended")?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));
    }
}
