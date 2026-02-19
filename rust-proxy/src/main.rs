use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use futures::stream::StreamExt;
use reqwest::Client;
use std::net::SocketAddr;
use tracing::{debug, error, info, warn};

const WARP_ORIGIN: &str = "https://app.warp.dev";

/// HTTP/1.1 hop-by-hop headers that MUST NOT be forwarded to HTTP/2 upstream.
/// RFC 7540 §8.1.2.2: connection-specific header fields are malformed in HTTP/2.
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,warp_rustls_proxy=debug".parse().unwrap()),
        )
        .init();

    let client = Client::builder()
        .use_rustls_tls()
        .http2_adaptive_window(true)
        .build()
        .expect("Failed to build reqwest client with rustls");

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/{*path}", any(proxy_handler))
        .route("/", any(proxy_handler))
        .with_state(client);

    let port: u16 = std::env::var("RUST_PROXY_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(28887);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    info!("🦀 warp-rustls-proxy listening on http://{}", addr);
    info!("   Target: {}", WARP_ORIGIN);
    info!("   TLS backend: rustls + ring (matching Warp client fingerprint)");
    info!("   Hop-by-hop header stripping: enabled");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "warp-rustls-proxy ok\n")
}

async fn proxy_handler(
    axum::extract::State(client): axum::extract::State<Client>,
    req: Request,
) -> impl IntoResponse {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(|q| format!("?{}", q)).unwrap_or_default();
    let target_url = format!("{}{}{}", WARP_ORIGIN, path, query);

    info!("--> {} {}", method, target_url);

    // Extract headers from incoming request, forward to Warp
    let incoming_headers = req.headers().clone();
    let body_bytes = match axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to read request body: {}", e);
            return (StatusCode::BAD_REQUEST, "Failed to read body").into_response();
        }
    };

    info!("    Body size: {} bytes", body_bytes.len());

    // Build outgoing request
    let mut outgoing = client.request(method, &target_url);

    // Forward headers, stripping HTTP/1.1 hop-by-hop headers that are
    // illegal in HTTP/2 (RFC 7540 §8.1.2.2).
    // Also use a whitelist approach: only forward headers that the real
    // Warp client would send, stripping httpx/curl artifacts like
    // `user-agent: python-httpx/...` that expose the proxy.
    let mut forwarded_count = 0u32;
    for (name, value) in incoming_headers.iter() {
        let name_str = name.as_str().to_lowercase();
        if HOP_BY_HOP_HEADERS.contains(&name_str.as_str()) {
            debug!("    [strip-hop] {}: {}", name_str, value.to_str().unwrap_or("<binary>"));
            continue;
        }
        // Strip client-side artifacts that leak proxy identity
        if name_str == "user-agent" || name_str == "accept-encoding" {
            debug!("    [strip-leak] {}: {}", name_str, value.to_str().unwrap_or("<binary>"));
            continue;
        }
        outgoing = outgoing.header(name.clone(), value.clone());
        forwarded_count += 1;
    }
    info!("    Forwarded {} headers (stripped hop-by-hop + leak)", forwarded_count);

    outgoing = outgoing.body(body_bytes);

    // Send request via rustls
    let resp = match outgoing.send().await {
        Ok(r) => r,
        Err(e) => {
            error!("<-- ERROR: {}", e);
            return (StatusCode::BAD_GATEWAY, format!("Upstream error: {}", e)).into_response();
        }
    };

    let status = resp.status();
    let http_version = resp.version();
    info!("<-- {} {} (via {:?})", status.as_u16(), target_url, http_version);

    // Build response headers
    let mut response_headers = HeaderMap::new();
    for (name, value) in resp.headers().iter() {
        let name_str = name.as_str().to_lowercase();
        if name_str == "transfer-encoding" || name_str == "content-length" {
            continue;
        }
        if let Ok(v) = HeaderValue::from_bytes(value.as_bytes()) {
            response_headers.insert(name.clone(), v);
        }
    }

    // Check if this is a streaming response (SSE)
    let is_stream = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("text/event-stream") || ct.contains("application/grpc"))
        .unwrap_or(false);

    if is_stream {
        info!("    Streaming response detected, proxying as stream...");
        let byte_stream = resp.bytes_stream().map(|result| match result {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                error!("Stream error: {}", e);
                Err(std::io::Error::new(std::io::ErrorKind::Other, e))
            }
        });
        let body = Body::from_stream(byte_stream);
        let mut response = Response::new(body);
        *response.status_mut() = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::OK);
        *response.headers_mut() = response_headers;
        response
    } else {
        // Non-streaming: read full body
        let body_bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to read response body: {}", e);
                return (StatusCode::BAD_GATEWAY, "Failed to read upstream response")
                    .into_response();
            }
        };

        if status.as_u16() >= 400 {
            warn!(
                "    Response body (error): {}",
                String::from_utf8_lossy(&body_bytes[..body_bytes.len().min(500)])
            );
        }

        let mut response = Response::new(Body::from(body_bytes));
        *response.status_mut() = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::OK);
        *response.headers_mut() = response_headers;
        response
    }
}
