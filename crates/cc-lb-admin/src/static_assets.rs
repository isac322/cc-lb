use axum::{
    body::Body,
    extract::Path,
    http::{HeaderMap, Response, StatusCode, header},
    response::IntoResponse,
};
use rust_embed::{EmbeddedFile, RustEmbed};

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct Assets;

const IMMUTABLE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
const REVALIDATE_CACHE_CONTROL: &str = "no-cache, must-revalidate";

pub(crate) async fn serve_index(headers: HeaderMap) -> impl IntoResponse {
    match Assets::get("index.html") {
        Some(content) => embedded_asset_response(&headers, "index.html", content),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub(crate) async fn serve_asset(headers: HeaderMap, Path(file): Path<String>) -> impl IntoResponse {
    if let Some(content) = Assets::get(&file) {
        return embedded_asset_response(&headers, &file, content);
    }
    if file == "admin" || file.starts_with("admin/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    match Assets::get("index.html") {
        Some(content) => embedded_asset_response(&headers, "index.html", content),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn embedded_asset_response(
    headers: &HeaderMap,
    file: &str,
    content: EmbeddedFile,
) -> Response<Body> {
    let content_type = if file == "index.html" {
        "text/html; charset=utf-8".to_owned()
    } else {
        mime_guess::from_path(file)
            .first_or_octet_stream()
            .to_string()
    };
    let cache_control = if file.starts_with("assets/") {
        IMMUTABLE_CACHE_CONTROL
    } else {
        REVALIDATE_CACHE_CONTROL
    };
    let etag = format!("\"{}\"", hex::encode(content.metadata.sha256_hash()));
    let response_headers = [
        (header::CONTENT_TYPE, content_type),
        (header::CACHE_CONTROL, cache_control.to_owned()),
        (header::ETAG, etag.clone()),
    ];
    if if_none_match_matches(headers, &etag) {
        return (StatusCode::NOT_MODIFIED, response_headers).into_response();
    }
    (response_headers, content.data).into_response()
}

fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|token| token == "*" || token == etag)
        })
}
