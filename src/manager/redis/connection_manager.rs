use axum::http::StatusCode;
use deadpool_redis::{Config, Pool, Runtime};
use futures_util::StreamExt;

use crate::manager::redis::traits::RedisAdapter;
use deadpool_redis::redis::AsyncCommands;

#[derive(Clone)]
pub struct RedisManager {
    pub redis_pool: Pool,
    redis_url: String,
    pub limiter: IpRateLimiter,
}

#[derive(Clone)]
pub struct IpRateLimiter {
    pub pool: Pool,
    pub max_requests: u64,
    pub window_secs: u64,
}

impl RedisManager {
    pub fn new(redis_url: &str) -> anyhow::Result<Self> {
        let redis_cfg = Config::from_url(redis_url);
        let redis_pool = redis_cfg.create_pool(Some(Runtime::Tokio1))?;

        Ok(Self {
            redis_pool: redis_pool.clone(),
            redis_url: redis_url.to_string(),
            limiter: IpRateLimiter {
                pool: redis_pool,
                max_requests: 10,
                window_secs: 30,
            },
        })
    }

    pub async fn set(
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

    pub async fn get(&self, key: &str) -> anyhow::Result<Option<String>, StatusCode> {
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

    pub async fn publish(&self, channel: &str, message: &str) -> anyhow::Result<i32, StatusCode> {
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

    pub async fn subscribe(
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

    pub async fn check(&self, ip: &str) -> Result<(), u64> {
        let key = format!("rate_limit:ip:{}", ip);
        let mut conn = self.limiter.pool.get().await.unwrap();

        let count: u64 = conn.incr(&key, 1).await.unwrap();

        if count == 1 {
            let _: () = conn
                .expire(&key, self.limiter.window_secs as i64)
                .await
                .unwrap();
        }

        if count > self.limiter.max_requests {
            let ttl = conn.ttl(&key).await.unwrap();
            return Err(ttl);
        }

        Ok(())
    }
}
