use axum::http::StatusCode;
use deadpool_redis::{Config, Pool, Runtime};
use futures_util::StreamExt;

use crate::manager::redis::traits::RedisAdapter;
use deadpool_redis::redis::AsyncCommands;

#[derive(Clone)]
pub struct RedisManager {
    pub redis_pool: Pool,
    redis_url: String,
}

impl RedisAdapter for RedisManager {
    fn new(redis_url: &str) -> anyhow::Result<Self> {
        let redis_cfg = Config::from_url(redis_url);
        let redis_pool = redis_cfg.create_pool(Some(Runtime::Tokio1))?;

        Ok(Self {
            redis_pool,
            redis_url: redis_url.to_string(),
        })
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

        let count = redis.get(key).await.map_err(|e| {
            eprintln!("get error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        Ok(count)
    }

    async fn publish(&self, channel: &str, message: &str) -> anyhow::Result<i32, StatusCode> {
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

    async fn subscribe(
        &self,
        channel: &str,
        tx: tokio::sync::mpsc::Sender<String>,
    ) -> anyhow::Result<(), StatusCode> {
        let client = redis::Client::open(self.redis_url.as_str()).map_err(|e| {
            eprintln!("redis client error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let mut pubsub = client.get_async_pubsub().await.map_err(|e| {
            eprintln!("redis pubsub error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        pubsub.subscribe(channel).await.map_err(|e| {
            eprintln!("subscription error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        tracing::info!(channel = %channel, "subscribed successfully");

        let channel_owned = channel.to_string();
        tokio::spawn(async move {
            let mut stream = pubsub.into_on_message();
            loop {
                match stream.next().await {
                    Some(msg) => {
                        let payload: String = match msg.get_payload() {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("payload error on {}: {}", channel_owned, e);
                                continue;
                            }
                        };
                        if tx.send(payload).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        tracing::warn!(channel = %channel_owned, "pubsub stream ended");
                        break;
                    }
                }
            }
        });

        Ok(())
    }
}
