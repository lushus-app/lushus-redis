#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("Redis connection error: {0}")]
    ConnectionError(String),
    #[error("Redis query error: {0}")]
    QueryError(String),
    #[error("Unable to serialize value for key \"{0}\": {1}")]
    SerializeError(String, String),
    #[error("Unable to deserialize value for key \"{0}\": {1}")]
    DeserializeError(String, String),
}
