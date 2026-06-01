use axum::{
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use std::{net::SocketAddr, sync::Arc};

use crate::state::app_state::AppState;

pub async fn auth_middlware(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let ip = addr.ip().to_string();

    let limiter = state.redis.clone();

    match limiter.check(&ip).await {
        Ok(()) => Ok(next.run(req).await),
        Err(retry_after) => {
            tracing::warn!(ip = %ip, retry_after = %retry_after, "ip rate limit exceeded");
            Err(StatusCode::TOO_MANY_REQUESTS)
        }
    }
}
