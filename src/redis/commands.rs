use std::time::Duration;

pub enum Command {
    Delete {
        key: String,
    },
    Exists {
        key: String,
    },
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        ttl: Duration,
    },
    TTL {
        key: String,
    },
    Update {
        key: String,
        value: String,
        ttl: Duration,
    },
}

impl Command {
    pub fn delete(key: String) -> Self {
        Self::Delete { key }
    }
    pub fn exists(key: String) -> Self {
        Self::Exists { key }
    }
    pub fn get(key: String) -> Self {
        Self::Get { key }
    }
    pub fn set(key: String, value: String, ttl: Duration) -> Self {
        Self::Set { key, value, ttl }
    }
    pub fn ttl(key: String) -> Self {
        Self::TTL { key }
    }
    pub fn update(key: String, value: String, ttl: Duration) -> Self {
        Self::Update { key, value, ttl }
    }
}

impl From<Command> for redis::Cmd {
    fn from(command: Command) -> Self {
        match command {
            Command::Delete { key } => redis::cmd("DEL").arg(&[&key]).clone(),
            Command::Exists { key } => redis::cmd("EXISTS").arg(&[&key]).clone(),
            Command::Get { key } => redis::cmd("GET").arg(&[&key]).clone(),
            Command::Set { key, value, ttl } => redis::cmd("SET")
                .arg(&[
                    &key,
                    &value,
                    "NX",
                    "EX",
                    format!("{}", ttl.as_secs()).as_ref(),
                ])
                .clone(),
            Command::TTL { key } => redis::cmd("TTL").arg(&[&key]).clone(),
            Command::Update { key, value, ttl } => redis::cmd("SET")
                .arg(&[
                    &key,
                    &value,
                    "XX",
                    "EX",
                    format!("{}", ttl.as_secs()).as_ref(),
                ])
                .clone(),
        }
    }
}
