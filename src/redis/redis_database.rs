use std::time::Duration;

use lushus_storage::{Storage, StorageRead, StorageTemp, StorageWrite};
use redis::{Client, Connection};
use serde::{de::DeserializeOwned, Serialize};

use crate::redis::{commands::Command, error::RedisError, execute_command::ExecuteCommand};

pub struct RedisDatabase {
    client: Client,
    ttl: Duration,
}

impl RedisDatabase {
    pub fn new(url: &str, ttl: Duration) -> Result<Self, RedisError> {
        let client = Client::open(url)
            .map_err(|e| e.to_string())
            .map_err(RedisError::ConnectionError)?;
        Ok(Self { client, ttl })
    }

    fn connection(&self) -> Result<Connection, RedisError> {
        let connection = self
            .client
            .get_connection()
            .map_err(|e| e.to_string())
            .map_err(RedisError::ConnectionError)?;
        Ok(connection)
    }

    fn _get<T: DeserializeOwned>(&self, key: String) -> Result<Option<T>, RedisError> {
        let command = Command::get(key.clone());
        let data = self.execute_command::<Option<String>>(command)?;
        let value = data
            .map(|v| serde_json::from_str::<T>(&v))
            .transpose()
            .map_err(|e| RedisError::DeserializeError(key, e.to_string()))?;
        Ok(value)
    }
}

impl AsRef<RedisDatabase> for RedisDatabase {
    fn as_ref(&self) -> &RedisDatabase {
        self
    }
}

impl ExecuteCommand for RedisDatabase {
    fn execute_command<T: redis::FromRedisValue>(&self, command: Command) -> Result<T, RedisError> {
        let mut connection = self.connection()?;
        let redis_command: redis::Cmd = command.into();
        let result = redis_command
            .query(&mut connection)
            .map_err(|e| e.to_string())
            .map_err(RedisError::QueryError)?;
        Ok(result)
    }
}

impl Storage for RedisDatabase {
    type Error = RedisError;
}

impl<K: ToString> StorageRead<K> for RedisDatabase {
    fn get<T: DeserializeOwned>(&self, key: &K) -> Result<Option<T>, Self::Error> {
        let key = key.to_string();
        self._get(key)
    }

    fn exists(&self, key: &K) -> Result<bool, Self::Error> {
        let key = key.to_string();
        let command = Command::exists(key);
        let data = self.execute_command::<bool>(command)?;
        Ok(data)
    }
}

impl<K: ToString> StorageWrite<K> for RedisDatabase {
    fn insert<T: Serialize + DeserializeOwned>(
        &mut self,
        key: &K,
        value: &T,
    ) -> Result<Option<T>, Self::Error> {
        let key = key.to_string();
        let previous = self._get(key.clone())?;
        let value = serde_json::to_string(value)
            .map_err(|e| RedisError::SerializeError(key.clone(), e.to_string()))?;
        let ttl = self.ttl;
        let command = Command::set(key, value, ttl);
        self.execute_command(command)?;
        Ok(previous)
    }

    fn remove<T: DeserializeOwned>(&mut self, key: &K) -> Result<Option<T>, Self::Error> {
        let key = key.to_string();
        let previous = self._get(key.clone())?;
        let command = Command::delete(key);
        self.execute_command(command)?;
        Ok(previous)
    }
}

impl<K: ToString> StorageTemp<K> for RedisDatabase {
    fn ttl(&self, key: &K) -> Result<Duration, Self::Error> {
        let key = key.to_string();
        let command = Command::ttl(key);
        let seconds: u64 = self.execute_command(command)?;
        let duration = Duration::from_secs(seconds);
        Ok(duration)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use lushus_storage::{StorageRead, StorageTemp, StorageWrite};

    use super::RedisDatabase;

    const URL: &str = "redis://:password@localhost:6379";

    #[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Foo {
        bar: u64,
    }

    impl Foo {
        fn new(bar: u64) -> Self {
            Self { bar }
        }
    }

    #[test]
    fn test_constructor() {
        let url = "redis://localhost:6379";
        let ttl = Duration::from_secs(1);
        RedisDatabase::new(url, ttl).expect("Unable to connect to Redis");
    }

    #[test]
    fn test_exists_returns_true_when_the_key_value_is_present() {
        let ttl = Duration::from_secs(1);
        let mut redis = RedisDatabase::new(URL, ttl).expect("Unable to connect to Redis");
        let key = "key".to_string();
        let foo = Foo::new(42);
        redis
            .insert(&key, &foo)
            .expect("Failed to insert into Redis");
        let ret = redis.exists(&key).expect("Failed to check key from Redis");
        assert_eq!(ret, true);
    }

    #[test]
    fn test_exists_returns_false_when_the_key_value_is_absent() {
        let ttl = Duration::from_secs(1);
        let mut redis = RedisDatabase::new(URL, ttl).expect("Unable to connect to Redis");
        let key = "key".to_string();
        let foo = Foo::new(42);
        redis
            .insert(&key, &foo)
            .expect("Failed to insert into Redis");
        let key = "bad".to_string();
        let ret = redis.exists(&key).expect("Failed to check key from Redis");
        assert_eq!(ret, false);
    }

    #[test]
    fn test_insert_inserts_the_key_value() {
        let ttl = Duration::from_secs(1);
        let mut redis = RedisDatabase::new(URL, ttl).expect("Unable to connect to Redis");
        let key = "key".to_string();
        let foo = Foo::new(42);
        redis
            .insert(&key, &foo)
            .expect("Failed to insert into Redis");
        let ret = redis
            .get::<Foo>(&key)
            .expect("Failed to get key from Redis");
        assert_eq!(ret, Some(foo));
    }

    #[test]
    fn test_insert_returns_the_previous_value() {
        let ttl = Duration::from_secs(1);
        let mut redis = RedisDatabase::new(URL, ttl).expect("Unable to connect to Redis");
        let key = "key".to_string();
        let foo_a = Foo::new(42);
        redis
            .insert(&key, &foo_a)
            .expect("Failed to insert into Redis");
        let foo_b = Foo::new(69);
        let prev = redis
            .insert(&key, &foo_b)
            .expect("Failed to insert into Redis");
        assert_eq!(prev, Some(foo_a));
    }

    #[test]
    fn test_remove_removes_the_key_value() {
        let ttl = Duration::from_secs(1);
        let mut redis = RedisDatabase::new(URL, ttl).expect("Unable to connect to Redis");
        let key = "key".to_string();
        let foo = Foo::new(42);
        redis
            .insert(&key, &foo)
            .expect("Failed to insert into Redis");
        redis
            .remove::<Foo>(&key)
            .expect("Failed to remove from Redis");
        let ret = redis
            .get::<Foo>(&key)
            .expect("Failed to get key from Redis");
        assert_eq!(ret, None);
    }

    #[test]
    fn test_remove_returns_the_previous_value() {
        let ttl = Duration::from_secs(1);
        let mut redis = RedisDatabase::new(URL, ttl).expect("Unable to connect to Redis");
        let key = "key".to_string();
        let foo = Foo::new(42);
        redis
            .insert(&key, &foo)
            .expect("Failed to insert into Redis");
        let prev = redis.remove(&key).expect("Failed to insert into Redis");
        assert_eq!(prev, Some(foo));
    }

    #[test]
    fn test_ttl_returns_the_expected_ttl_value() {
        let ttl = Duration::from_secs(1);
        let mut redis = RedisDatabase::new(URL, ttl).expect("Unable to connect to Redis");
        let key = "key".to_string();
        let foo = Foo::new(42);
        redis
            .insert(&key, &foo)
            .expect("Failed to insert into Redis");
        let value = redis.ttl(&key).expect("Failed to get TTL for key");
        assert_eq!(value, ttl);
    }
}
