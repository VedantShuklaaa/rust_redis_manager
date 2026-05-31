use crate::{
	manager::redis::connection_manager::RedisManager,
};

#[derive(Clone)]
pub struct AppState {
	pub redis: RedisManager,
}
