use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use std::time::Duration;

use crate::AdminState;

pub async fn require_admin_auth(
    State(state): State<AdminState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(expected_token) = &state.admin_token else {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Err(StatusCode::UNAUTHORIZED);
    };

    let auth_header = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    let Some(auth_header) = auth_header else {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Err(StatusCode::UNAUTHORIZED);
    };

    if !auth_header.starts_with("Bearer ") {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token = &auth_header["Bearer ".len()..];
    if token != expected_token {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(req).await)
}
