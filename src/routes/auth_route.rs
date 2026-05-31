use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use std::sync::{Arc};
use tokio::sync::mpsc;

use crate::{manager::redis::traits::RedisAdapter, state::app_state::AppState};

#[derive(Deserialize)]
pub struct SetValue {
    pub value: String,
    pub exp: u64,
}

#[derive(Deserialize)]
pub struct Publisher {
    pub channel: String,
    pub message: String,
}

pub async fn set_key(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Json(payload): Json<SetValue>,
) -> Result<Json<()>, StatusCode> {
    let redis = RedisAdapter::set(&state.redis, &key, payload.value, payload.exp).await?;
    Ok(Json(redis))
}

pub async fn get_key(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<Option<String>>, StatusCode> {
    let redis = RedisAdapter::get(&state.redis, &key).await?;
    Ok(Json(redis))
}

pub async fn publish(
    State(state): State<Arc<AppState>>,
    Path(payload): Path<Publisher>,
) -> Result<Json<i32>, StatusCode> {
    let redis = RedisAdapter::publish(&state.redis, &payload.channel, &payload.message).await?;
    Ok(Json(redis))
}

pub async fn subscribe(
    State(state): State<Arc<AppState>>,
    Path(channel): Path<String>,
) -> Result<Json<String>, StatusCode> {
    let (tx, mut rx) = mpsc::channel::<String>(32);
    RedisAdapter::subscribe(&state.redis, &channel, tx).await?;

    let msg = rx.recv().await.unwrap_or_default();
    Ok(Json(msg))
}
