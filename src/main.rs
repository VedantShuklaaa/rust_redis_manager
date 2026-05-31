mod config;
mod manager;
mod routes;
mod state;

use std::sync::Arc;

use crate::{
    config::config::{HOST, PORT, REDIS_URL},
    manager::redis::{connection_manager::RedisManager, traits::RedisAdapter},
    routes::auth_route::{get_key, publish, set_key, subscribe},
    state::app_state::AppState,
};
use axum::{
    Router,
    routing::{get, post},
    serve,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let redis_cfg = RedisManager::new(REDIS_URL).unwrap();

    let state = Arc::new(AppState { redis: redis_cfg });

    let app = Router::new()
        .route("/set/{key}", post(set_key))
        .route("/get/{key}", get(get_key))
        .route("/publisher", get(publish))
        .route("/subscriber", get(subscribe))
        .with_state(state);

    let addr = format!("{}:{}", HOST, PORT);
    let listener = TcpListener::bind(&addr).await.unwrap();
    println!("server is currently running on port: {}", PORT);
    serve(listener, app).await.unwrap()
}
