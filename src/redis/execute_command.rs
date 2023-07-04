use crate::redis::{commands::Command, error::RedisError};

pub trait ExecuteCommand {
    fn execute_command<T: redis::FromRedisValue>(
        &self,
        command: Command,
    ) -> Result<T, RedisError>;
}
