use super::*;
use pretty_assertions::assert_eq;

#[test]
fn provider_rejections_keep_only_what_clients_should_see() {
    let long = "x".repeat(600);
    let cases = [
        // A failure inside the response stream carries the provider's raw
        // body; OpenRouter's names the account's user ID.
        (
            json!({"message":"{\"error\":{\"message\":\"nonexistent/model-xyz is not a valid model ID\",\"code\":400},\"user_id\":\"user_secret\"}","codexErrorInfo":"other"}),
            json!({"code":"invalid_request","message":"nonexistent/model-xyz is not a valid model ID"}),
        ),
        // An HTTP rejection: Codex extracts the message, then appends the
        // provider URL and request ID.
        (
            json!({"message":"unexpected status 404 Not Found: The model `x` does not exist, url: https://api.example.com/v1/responses, request id: req_1","codexErrorInfo":"other"}),
            json!({"code":"invalid_request","message":"The model `x` does not exist"}),
        ),
        // A body without a message, and an HTML error page, say nothing usable.
        (
            json!({"message":"unexpected status 401 Unauthorized: {\"error\":{\"code\":\"invalid_api_key\"}}, url: https://gateway.internal/v1/responses","codexErrorInfo":"other"}),
            json!({"code":"authentication_error","message":"the model provider rejected the request"}),
        ),
        (
            json!({"message":"unexpected status 502 Bad Gateway: <html><body>Bad gateway</body></html>, url: https://gateway.internal/v1/responses, cf-ray: abc","codexErrorInfo":"other"}),
            json!({"code":"server_error","message":"the model provider rejected the request"}),
        ),
        // A provider's friendly message still carries Codex's diagnostics.
        (
            json!({"message":"Your organization must be verified to use this model, url: https://api.example.com/v1/responses, request id: req_2","codexErrorInfo":"other"}),
            json!({"code":"internal_error","message":"Your organization must be verified to use this model"}),
        ),
        (
            json!({"message":format!("unexpected status 400 Bad Request: {long}"),"codexErrorInfo":"other"}),
            json!({"code":"invalid_request","message":format!("{}…", "x".repeat(500))}),
        ),
        // A Codex kind decides the code; the message is still cleaned.
        (
            json!({"message":"{\"error\":{\"message\":\"slow down\",\"code\":429},\"user_id\":\"user_secret\"}","codexErrorInfo":"rateLimitExceeded"}),
            json!({"code":"rate_limit_exceeded","message":"slow down"}),
        ),
        // Codex's own messages are kept.
        (
            json!({"message":"context window exceeded","codexErrorInfo":"contextWindowExceeded"}),
            json!({"code":"context_length_exceeded","message":"context window exceeded"}),
        ),
        (
            json!({"message":"worker failed","codexErrorInfo":"other"}),
            json!({"code":"internal_error","message":"worker failed"}),
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(turn_error(&error), expected, "{error}");
    }
}
