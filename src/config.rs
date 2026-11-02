//! Configuration loaded from environment variables.
//!
//! See `.env.example` for the full list of supported variables.

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    pub broker_host: String,
    pub broker_port: u16,
    pub broker_tls: bool,
    pub ca_cert: Option<PathBuf>,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: String,
    /// Subscription topic filters. Parsed from `MQ_TOPICS` (comma-separated),
    /// falling back to `MQ_TOPIC`, then to the default.
    pub topics: Vec<String>,
    pub qos: u8,
    /// Application ID used as the default for downlink publishing. Optional;
    /// the downlink CLI falls back to requiring `--application-id`.
    pub application_id: Option<String>,
    pub sqlite_path: PathBuf,
    pub enable_stdout: bool,
    pub enable_sqlite: bool,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid MQ_BROKER_PORT `{0}`: {1}")]
    InvalidPort(String, std::num::ParseIntError),
    #[error("invalid MQ_QOS `{0}` (must be 0, 1 or 2)")]
    InvalidQos(String),
    #[error("invalid boolean for {0}: `{1}`")]
    InvalidBool(&'static str, String),
    #[error("MQ_CLIENT_CERT and MQ_CLIENT_KEY must be provided together")]
    IncompleteClientAuth,
}

type Result<T> = std::result::Result<T, ConfigError>;

impl Config {
    pub fn from_env() -> Result<Self> {
        let broker_port_raw = parse_env("MQ_BROKER_PORT", "1883");
        let broker_port: u16 = broker_port_raw
            .parse()
            .map_err(|e| ConfigError::InvalidPort(broker_port_raw.clone(), e))?;

        let qos_raw = parse_env("MQ_QOS", "0");
        let qos: u8 = qos_raw
            .parse()
            .map_err(|_| ConfigError::InvalidQos(qos_raw.clone()))?;
        if qos > 2 {
            return Err(ConfigError::InvalidQos(qos_raw));
        }

        let client_cert = env_path("MQ_CLIENT_CERT");
        let client_key = env_path("MQ_CLIENT_KEY");
        if client_cert.is_some() != client_key.is_some() {
            return Err(ConfigError::IncompleteClientAuth);
        }

        Ok(Self {
            broker_host: parse_env("MQ_BROKER_HOST", "localhost"),
            broker_port,
            broker_tls: parse_bool("MQ_BROKER_TLS", true)?,
            ca_cert: env_path("MQ_CA_CERT"),
            client_cert,
            client_key,
            username: env_opt("MQ_USERNAME"),
            password: env_opt("MQ_PASSWORD"),
            client_id: parse_env(
                "MQ_CLIENT_ID",
                &format!("mq-subscriber-{}", uuid_suffix()),
            ),
            topics: parse_topics(),
            qos,
            application_id: env_opt("MQ_APPLICATION_ID"),
            sqlite_path: PathBuf::from(parse_env("MQ_SQLITE_PATH", "events.db")),
            enable_stdout: parse_bool("MQ_ENABLE_STDOUT", true)?,
            enable_sqlite: parse_bool("MQ_ENABLE_SQLITE", true)?,
        })
    }
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn parse_env(key: &str, default: &str) -> String {
    env_opt(key).unwrap_or_else(|| default.to_string())
}

fn env_path(key: &str) -> Option<PathBuf> {
    env_opt(key).map(PathBuf::from)
}

/// Parse the subscription topic list: `MQ_TOPICS` (comma-separated) takes
/// precedence over `MQ_TOPIC` (single). Falls back to the default filter.
fn parse_topics() -> Vec<String> {
    const DEFAULT: &str = "application/+/device/+/event/#";

    if let Some(raw) = env_opt("MQ_TOPICS") {
        let topics: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(String::from)
            .collect();
        if !topics.is_empty() {
            return topics;
        }
    }

    match env_opt("MQ_TOPIC") {
        Some(single) => vec![single],
        None => vec![DEFAULT.to_string()],
    }
}

fn parse_bool(key: &'static str, default: bool) -> Result<bool> {
    match env_opt(key) {
        None => Ok(default),
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            _ => Err(ConfigError::InvalidBool(key, v)),
        },
    }
}

/// Small random suffix for the default client id. Avoids pulling in a `uuid`
/// dependency just for a default.
fn uuid_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{}-{:x}", std::process::id(), nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes env-mutating tests so they don't race under parallel runners.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn clear_env() {
        for key in [
            "MQ_BROKER_HOST",
            "MQ_BROKER_PORT",
            "MQ_BROKER_TLS",
            "MQ_CA_CERT",
            "MQ_CLIENT_CERT",
            "MQ_CLIENT_KEY",
            "MQ_USERNAME",
            "MQ_PASSWORD",
            "MQ_CLIENT_ID",
            "MQ_TOPIC",
            "MQ_TOPICS",
            "MQ_QOS",
            "MQ_APPLICATION_ID",
            "MQ_SQLITE_PATH",
            "MQ_ENABLE_STDOUT",
            "MQ_ENABLE_SQLITE",
        ] {
            // SAFETY: only this module mutates these variables, and tests hold
            // ENV_LOCK while doing so.
            unsafe { std::env::remove_var(key) };
        }
    }

    fn set_var(key: &str, value: &str) {
        // SAFETY: see clear_env.
        unsafe { std::env::set_var(key, value) };
    }

    #[test]
    fn defaults_are_sane() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let cfg = Config::from_env().expect("defaults should parse");
        assert_eq!(cfg.broker_host, "localhost");
        assert_eq!(cfg.broker_port, 1883);
        assert!(cfg.broker_tls);
        assert_eq!(cfg.topics, vec!["application/+/device/+/event/#".to_string()]);
        assert_eq!(cfg.qos, 0);
        assert!(cfg.enable_stdout);
        assert!(cfg.enable_sqlite);
        assert!(cfg.username.is_none());
        assert!(cfg.application_id.is_none());
    }

    #[test]
    fn invalid_port_is_rejected() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("MQ_BROKER_PORT", "not-a-number");
        assert!(matches!(Config::from_env(), Err(ConfigError::InvalidPort(_, _))));
    }

    #[test]
    fn invalid_qos_is_rejected() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("MQ_QOS", "7");
        assert!(matches!(Config::from_env(), Err(ConfigError::InvalidQos(_))));
    }

    #[test]
    fn client_cert_requires_key() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("MQ_CLIENT_CERT", "/tmp/client.pem");
        assert!(matches!(
            Config::from_env(),
            Err(ConfigError::IncompleteClientAuth)
        ));
    }

    #[test]
    fn bool_parsing_variants() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("MQ_ENABLE_SQLITE", "off");
        let cfg = Config::from_env().expect("valid bool");
        assert!(!cfg.enable_sqlite);

        clear_env();
        set_var("MQ_ENABLE_STDOUT", "1");
        let cfg = Config::from_env().expect("valid bool");
        assert!(cfg.enable_stdout);
    }

    #[test]
    fn topics_list_parses_comma_separated() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var(
            "MQ_TOPICS",
            "application/app-1/device/+/event/#, application/app-2/device/+/event/#",
        );
        let cfg = Config::from_env().expect("valid topics");
        assert_eq!(
            cfg.topics,
            vec![
                "application/app-1/device/+/event/#".to_string(),
                "application/app-2/device/+/event/#".to_string(),
            ]
        );
    }

    #[test]
    fn topics_list_precedes_single_topic() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("MQ_TOPIC", "application/ignored/device/+/event/#");
        set_var("MQ_TOPICS", "application/wins/device/+/event/#");
        let cfg = Config::from_env().expect("valid topics");
        assert_eq!(cfg.topics, vec!["application/wins/device/+/event/#".to_string()]);
    }

    #[test]
    fn single_topic_is_wrapped_in_list() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("MQ_TOPIC", "application/solo/device/+/event/up");
        let cfg = Config::from_env().expect("valid topic");
        assert_eq!(cfg.topics, vec!["application/solo/device/+/event/up".to_string()]);
    }

    #[test]
    fn empty_topics_list_falls_back_to_single_topic() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("MQ_TOPICS", " , ");
        set_var("MQ_TOPIC", "application/fallback/device/+/event/#");
        let cfg = Config::from_env().expect("valid fallback");
        assert_eq!(cfg.topics, vec!["application/fallback/device/+/event/#".to_string()]);
    }
}
