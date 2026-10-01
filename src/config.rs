use std::env;
use std::time::Duration;

use thiserror::Error;

use crate::db::ExasolDbConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionTarget {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub exasol: ExasolDbConfig,
    pub poll_interval: Duration,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingEnv(String),
    #[error("invalid environment variable {name}: {message}")]
    InvalidEnv { name: String, message: String },
    #[error("invalid argument {name}: {message}")]
    InvalidArg { name: String, message: String },
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_env_and_optional_dsn(None)
    }

    pub fn from_env_and_optional_dsn(cli_dsn: Option<String>) -> Result<Self, ConfigError> {
        let schema = env_or_default("EXA_SCHEMA", "SCHED");
        let tasks_table = env_or_default("EXA_TASKS_TABLE", "SCHED_TASKS");
        let history_table = env_or_default("EXA_HISTORY_TABLE", "SCHED_HISTORY");
        let poll_interval_secs = parse_u64_env("POLL_INTERVAL_SECS", 10)?;
        if poll_interval_secs == 0 {
            return Err(ConfigError::InvalidEnv {
                name: "POLL_INTERVAL_SECS".to_string(),
                message: "must be at least 1 second".to_string(),
            });
        }
        let query_timeout = env::var("EXA_QUERY_TIMEOUT_SECS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                value.parse::<u64>().map_err(|_| ConfigError::InvalidEnv {
                    name: "EXA_QUERY_TIMEOUT_SECS".to_string(),
                    message: format!("expected integer seconds, got '{value}'"),
                })
            })
            .transpose()?;

        let cli_dsn = normalize_cli_dsn(cli_dsn)?;
        let dsn = if let Some(cli_dsn) = cli_dsn {
            cli_dsn
        } else if let Ok(explicit_dsn) = env::var("EXA_DSN") {
            let dsn = explicit_dsn.trim();
            if dsn.is_empty() {
                return Err(ConfigError::InvalidEnv {
                    name: "EXA_DSN".to_string(),
                    message: "must not be empty".to_string(),
                });
            }
            dsn.to_string()
        } else {
            let host = required_env("EXA_HOST")?;
            let port = parse_u16_env("EXA_PORT", 8563)?;
            let user = required_env("EXA_USER")?;
            let password = required_env("EXA_PASSWORD")?;
            let tls = parse_bool_env("EXA_TLS", false)?;
            let validate_server_certificate = parse_bool_env("EXA_VALIDATE_SERVER_CERT", true)?;

            format!(
                "exasol://{}:{}@{}:{}?tls={}&validateservercertificate={}",
                user,
                password,
                host,
                port,
                bool_to_numeric(tls),
                bool_to_numeric(validate_server_certificate),
            )
        };

        let dsn = if let Some(timeout_secs) = query_timeout {
            append_query_param(&dsn, "query_timeout", &timeout_secs.to_string())
        } else {
            dsn
        };
        let schema = if is_env_set("EXA_SCHEMA") {
            schema
        } else {
            schema_from_dsn(&dsn).unwrap_or(schema)
        };

        Ok(Self {
            exasol: ExasolDbConfig {
                dsn,
                schema,
                tasks_table,
                history_table,
            },
            poll_interval: Duration::from_secs(poll_interval_secs),
        })
    }
}

fn required_env(name: &str) -> Result<String, ConfigError> {
    let value = env::var(name).map_err(|_| ConfigError::MissingEnv(name.to_string()))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ConfigError::InvalidEnv {
            name: name.to_string(),
            message: "must not be empty".to_string(),
        });
    }
    Ok(trimmed.to_string())
}

fn is_env_set(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn normalize_cli_dsn(cli_dsn: Option<String>) -> Result<Option<String>, ConfigError> {
    let Some(raw) = cli_dsn else {
        return Ok(None);
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if !trimmed.starts_with("exasol://") {
        return Err(ConfigError::InvalidArg {
            name: "EXASOL_URL".to_string(),
            message: format!("expected exarrow-rs DSN starting with exasol://, got '{trimmed}'"),
        });
    }

    Ok(Some(trimmed.to_string()))
}

fn schema_from_dsn(dsn: &str) -> Option<String> {
    // Reuse exarrow-rs' own parser to stay aligned with DSN format semantics.
    let driver = exarrow::adbc::Driver::new();
    let database = driver.open(dsn).ok()?;
    database.params().schema.clone()
}

pub fn connection_target_from_dsn(dsn: &str) -> Option<ConnectionTarget> {
    // Reuse exarrow-rs' own parser so logging follows the same DSN semantics as connection setup.
    let driver = exarrow::adbc::Driver::new();
    let database = driver.open(dsn).ok()?;
    Some(ConnectionTarget {
        host: database.params().host.clone(),
        port: database.params().port,
    })
}

fn env_or_default(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn parse_u16_env(name: &str, default: u16) -> Result<u16, ConfigError> {
    match env::var(name).ok().filter(|value| !value.trim().is_empty()) {
        Some(value) => value.parse::<u16>().map_err(|_| ConfigError::InvalidEnv {
            name: name.to_string(),
            message: format!("expected integer in [0, 65535], got '{value}'"),
        }),
        None => Ok(default),
    }
}

fn parse_u64_env(name: &str, default: u64) -> Result<u64, ConfigError> {
    match env::var(name).ok().filter(|value| !value.trim().is_empty()) {
        Some(value) => value.parse::<u64>().map_err(|_| ConfigError::InvalidEnv {
            name: name.to_string(),
            message: format!("expected integer seconds, got '{value}'"),
        }),
        None => Ok(default),
    }
}

fn parse_bool_env(name: &str, default: bool) -> Result<bool, ConfigError> {
    match env::var(name).ok().filter(|value| !value.trim().is_empty()) {
        Some(value) => parse_bool(name, &value),
        None => Ok(default),
    }
}

fn parse_bool(name: &str, value: &str) -> Result<bool, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::InvalidEnv {
            name: name.to_string(),
            message: format!("expected boolean (true/false/1/0), got '{value}'"),
        }),
    }
}

fn append_query_param(dsn: &str, key: &str, value: &str) -> String {
    let param_prefix = format!("{key}=");
    if dsn.contains(&param_prefix) {
        return dsn.to_string();
    }

    if dsn.contains('?') {
        format!("{dsn}&{key}={value}")
    } else {
        format!("{dsn}?{key}={value}")
    }
}

fn bool_to_numeric(value: bool) -> u8 {
    if value { 1 } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const CONFIG_ENV_KEYS: [&str; 12] = [
        "EXA_DSN",
        "EXA_HOST",
        "EXA_PORT",
        "EXA_USER",
        "EXA_PASSWORD",
        "EXA_TLS",
        "EXA_VALIDATE_SERVER_CERT",
        "EXA_SCHEMA",
        "EXA_TASKS_TABLE",
        "EXA_HISTORY_TABLE",
        "POLL_INTERVAL_SECS",
        "EXA_QUERY_TIMEOUT_SECS",
    ];

    struct EnvSnapshot {
        values: Vec<(&'static str, Option<String>)>,
    }

    impl EnvSnapshot {
        fn capture() -> Self {
            let values = CONFIG_ENV_KEYS
                .iter()
                .copied()
                .map(|key| (key, env::var(key).ok()))
                .collect();
            Self { values }
        }

        fn clear_all() {
            for key in CONFIG_ENV_KEYS {
                // SAFETY: tests synchronize env mutation through ENV_LOCK.
                unsafe {
                    env::remove_var(key);
                }
            }
        }
    }

    impl Drop for EnvSnapshot {
        fn drop(&mut self) {
            for (key, value) in &self.values {
                match value {
                    Some(value) => {
                        // SAFETY: tests synchronize env mutation through ENV_LOCK.
                        unsafe {
                            env::set_var(key, value);
                        }
                    }
                    None => {
                        // SAFETY: tests synchronize env mutation through ENV_LOCK.
                        unsafe {
                            env::remove_var(key);
                        }
                    }
                }
            }
        }
    }

    fn with_clean_env<T>(f: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        let snapshot = EnvSnapshot::capture();
        EnvSnapshot::clear_all();
        let result = f();
        drop(snapshot);
        result
    }

    fn set_env(name: &str, value: &str) {
        // SAFETY: tests synchronize env mutation through ENV_LOCK.
        unsafe {
            env::set_var(name, value);
        }
    }

    #[test]
    fn from_env_is_alias_for_no_cli_dsn() {
        with_clean_env(|| {
            set_env("EXA_HOST", "db-host");
            set_env("EXA_PORT", "1234");
            set_env("EXA_USER", "scheduler");
            set_env("EXA_PASSWORD", "pw");

            let from_env = AppConfig::from_env().expect("from_env should parse");
            let from_explicit =
                AppConfig::from_env_and_optional_dsn(None).expect("explicit None should parse");

            assert_eq!(from_env.exasol.dsn, from_explicit.exasol.dsn);
            assert_eq!(from_env.exasol.schema, from_explicit.exasol.schema);
            assert_eq!(
                from_env.exasol.tasks_table,
                from_explicit.exasol.tasks_table
            );
            assert_eq!(from_env.poll_interval, from_explicit.poll_interval);
        });
    }

    #[test]
    fn default_schema_is_sched_when_not_configured_elsewhere() {
        with_clean_env(|| {
            set_env("EXA_HOST", "db-host");
            set_env("EXA_USER", "scheduler");
            set_env("EXA_PASSWORD", "pw");

            let config = AppConfig::from_env().expect("config should parse");

            assert_eq!(config.exasol.schema, "SCHED");
        });
    }

    #[test]
    fn from_env_builds_dsn_from_discrete_env_values() {
        with_clean_env(|| {
            set_env("EXA_HOST", "  localhost  ");
            set_env("EXA_PORT", "9999");
            set_env("EXA_USER", "  sys ");
            set_env("EXA_PASSWORD", " exasol ");
            set_env("EXA_TLS", "yes");
            set_env("EXA_VALIDATE_SERVER_CERT", "off");
            set_env("EXA_SCHEMA", "APP");
            set_env("EXA_TASKS_TABLE", "TASKS");
            set_env("POLL_INTERVAL_SECS", "7");

            let config = AppConfig::from_env().expect("env config should parse");
            assert_eq!(
                config.exasol.dsn,
                "exasol://sys:exasol@localhost:9999?tls=1&validateservercertificate=0"
            );
            assert_eq!(config.exasol.schema, "APP");
            assert_eq!(config.exasol.tasks_table, "TASKS");
            assert_eq!(config.poll_interval, Duration::from_secs(7));
        });
    }

    #[test]
    fn cli_dsn_takes_precedence_over_env_dsn() {
        with_clean_env(|| {
            set_env(
                "EXA_DSN",
                "exasol://env:env@localhost:8563/ENV_SCHEMA?tls=0",
            );
            let config = AppConfig::from_env_and_optional_dsn(Some(
                "  exasol://cli:cli@localhost:8563/CLI_SCHEMA?tls=0  ".to_string(),
            ))
            .expect("cli dsn should parse");
            assert!(
                config
                    .exasol
                    .dsn
                    .starts_with("exasol://cli:cli@localhost:8563/CLI_SCHEMA?tls=0"),
                "cli dsn should win precedence"
            );
        });
    }

    #[test]
    fn rejects_empty_env_dsn() {
        with_clean_env(|| {
            set_env("EXA_DSN", "   ");
            let err = AppConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidEnv { name, .. } if name == "EXA_DSN"
            ));
        });
    }

    #[test]
    fn reports_missing_required_env_without_any_dsn() {
        with_clean_env(|| {
            let err = AppConfig::from_env().unwrap_err();
            assert!(matches!(err, ConfigError::MissingEnv(name) if name == "EXA_HOST"));
        });
    }

    #[test]
    fn rejects_invalid_port_value() {
        with_clean_env(|| {
            set_env("EXA_HOST", "localhost");
            set_env("EXA_USER", "sys");
            set_env("EXA_PASSWORD", "exasol");
            set_env("EXA_PORT", "bad");
            let err = AppConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidEnv { name, .. } if name == "EXA_PORT"
            ));
        });
    }

    #[test]
    fn rejects_invalid_poll_interval() {
        with_clean_env(|| {
            set_env("EXA_HOST", "localhost");
            set_env("EXA_USER", "sys");
            set_env("EXA_PASSWORD", "exasol");
            set_env("POLL_INTERVAL_SECS", "abc");
            let err = AppConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidEnv { name, .. } if name == "POLL_INTERVAL_SECS"
            ));
        });
    }

    #[test]
    fn rejects_zero_poll_interval() {
        with_clean_env(|| {
            set_env("EXA_HOST", "localhost");
            set_env("EXA_USER", "sys");
            set_env("EXA_PASSWORD", "exasol");
            set_env("POLL_INTERVAL_SECS", "0");
            let err = AppConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidEnv { name, ref message }
                    if name == "POLL_INTERVAL_SECS" && message.contains("at least 1")
            ));
        });
    }

    #[test]
    fn rejects_invalid_query_timeout() {
        with_clean_env(|| {
            set_env("EXA_DSN", "exasol://sys:exasol@localhost:8563?tls=0");
            set_env("EXA_QUERY_TIMEOUT_SECS", "not-a-number");
            let err = AppConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidEnv { name, .. } if name == "EXA_QUERY_TIMEOUT_SECS"
            ));
        });
    }

    #[test]
    fn rejects_invalid_bool_env_values() {
        with_clean_env(|| {
            set_env("EXA_HOST", "localhost");
            set_env("EXA_USER", "sys");
            set_env("EXA_PASSWORD", "exasol");
            set_env("EXA_TLS", "sometimes");
            let err = AppConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidEnv { name, .. } if name == "EXA_TLS"
            ));
        });
    }

    #[test]
    fn appends_query_timeout_only_once() {
        with_clean_env(|| {
            set_env(
                "EXA_DSN",
                "exasol://sys:exasol@localhost:8563?tls=0&query_timeout=12",
            );
            set_env("EXA_QUERY_TIMEOUT_SECS", "20");
            let config = AppConfig::from_env().expect("config should parse");
            assert!(config.exasol.dsn.contains("query_timeout=12"));
            assert!(!config.exasol.dsn.contains("query_timeout=20"));
        });
    }

    #[test]
    fn normalize_cli_dsn_validation() {
        assert_eq!(normalize_cli_dsn(None).unwrap(), None);
        assert_eq!(normalize_cli_dsn(Some("   ".to_string())).unwrap(), None);

        let err = normalize_cli_dsn(Some("postgres://localhost".to_string())).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidArg { name, .. } if name == "EXASOL_URL"
        ));

        let valid = normalize_cli_dsn(Some(" exasol://sys:pw@localhost:8563 ".to_string()))
            .expect("valid cli dsn should parse");
        assert_eq!(valid, Some("exasol://sys:pw@localhost:8563".to_string()));
    }

    #[test]
    fn parse_bool_accepts_expected_literals() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            assert!(parse_bool("EXA_TLS", value).expect("true-ish value should parse"));
        }
        for value in ["0", "false", "FALSE", "no", "off"] {
            assert!(!parse_bool("EXA_TLS", value).expect("false-ish value should parse"));
        }
    }

    #[test]
    fn append_query_param_handles_question_mark_and_existing_param() {
        assert_eq!(
            append_query_param("exasol://u:p@h:8563", "query_timeout", "10"),
            "exasol://u:p@h:8563?query_timeout=10"
        );
        assert_eq!(
            append_query_param("exasol://u:p@h:8563?tls=0", "query_timeout", "10"),
            "exasol://u:p@h:8563?tls=0&query_timeout=10"
        );
        assert_eq!(
            append_query_param(
                "exasol://u:p@h:8563?tls=0&query_timeout=5",
                "query_timeout",
                "10",
            ),
            "exasol://u:p@h:8563?tls=0&query_timeout=5"
        );
    }

    #[test]
    fn schema_extraction_and_small_helpers_work() {
        assert_eq!(
            schema_from_dsn("this-is-not-a-dsn"),
            None,
            "invalid DSN should not produce schema"
        );
        assert_eq!(
            connection_target_from_dsn("this-is-not-a-dsn"),
            None,
            "invalid DSN should not produce connection target"
        );
        assert_eq!(
            connection_target_from_dsn("exasol://u:p@db.example.com:9999/APP?tls=0"),
            Some(ConnectionTarget {
                host: "db.example.com".to_string(),
                port: 9999,
            })
        );
        assert_eq!(bool_to_numeric(true), 1);
        assert_eq!(bool_to_numeric(false), 0);

        // Env reads must hold ENV_LOCK; other tests set EXA_SCHEMA concurrently.
        with_clean_env(|| {
            assert_eq!(env_or_default("EXA_SCHEMA", "SCHED"), "SCHED");
            assert!(!is_env_set("EXA_SCHEMA"));
            set_env("EXA_SCHEMA", "   ");
            assert!(!is_env_set("EXA_SCHEMA"));
            set_env("EXA_SCHEMA", "APP");
            assert!(is_env_set("EXA_SCHEMA"));
            assert_eq!(env_or_default("EXA_SCHEMA", "SCHED"), "APP");
        });
    }

    #[test]
    fn required_env_rejects_missing_and_empty_values() {
        with_clean_env(|| {
            let missing = required_env("EXA_USER").unwrap_err();
            assert!(matches!(missing, ConfigError::MissingEnv(name) if name == "EXA_USER"));

            set_env("EXA_USER", " ");
            let empty = required_env("EXA_USER").unwrap_err();
            assert!(matches!(
                empty,
                ConfigError::InvalidEnv { name, .. } if name == "EXA_USER"
            ));
        });
    }
}
