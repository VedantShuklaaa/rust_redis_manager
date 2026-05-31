use anyhow::Result;
use axum::http::StatusCode;
use redis::Client;

pub trait RedisAdapter: Sized {
    fn new(redis_url: &str) -> Result<Self>;
    async fn set(&self, key: &str, value: String, exp: u64) -> Result<(), StatusCode>;
    async fn get(&self, key: &str) -> Result<Option<String>, StatusCode>;
    async fn publish(&self, channel: &str, message: &str) -> Result<i32, StatusCode>;
	async fn subscribe(&self, channel: &str) -> Result<(), StatusCode>;
}
