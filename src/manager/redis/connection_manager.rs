use axum::http::StatusCode;
use deadpool_redis::{Config, Pool, Runtime};

use crate::manager::redis::traits::RedisAdapter;
use deadpool_redis::redis::AsyncCommands;

#[derive(Clone)]
pub struct RedisManager {
    pub redis_pool: Pool,
}

impl RedisAdapter for RedisManager {
    fn new(redis_url: &str) -> anyhow::Result<Self> {
        let redis_cfg = Config::from_url(redis_url);
        let redis_pool = redis_cfg.create_pool(Some(Runtime::Tokio1))?;

        Ok(Self { redis_pool })
    }

    async fn set(
        &self,
        key: &str,
        value: String,
        exp: u64,
    ) -> anyhow::Result<(), axum::http::StatusCode> {
        let mut redis = self.redis_pool.get().await.map_err(|e| {
            eprintln!("redis error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let _: () = redis.set_ex(key, value, exp).await.map_err(|e| {
            eprintln!("set error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        Ok(())
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<String>, StatusCode> {
        let mut redis = self.redis_pool.get().await.map_err(|e| {
            eprintln!("redis error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let value = redis.get(key).await.map_err(|e| {
            eprintln!("get error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        Ok(value)
    }

    async fn publish(
		&self, 
		channel: &str, 
		message: &str
	) -> anyhow::Result<i32, StatusCode> {
        let mut redis = self.redis_pool.get().await.map_err(|e| {
            eprintln!("redis error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let subscriber = redis.publish(channel, message).await.map_err(|e| {
            eprintln!("publish error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        Ok(subscriber)
    }

}
