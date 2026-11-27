//! Configuration of the agent: the pools the user prepared on the storage nodes.

use std::path::PathBuf;

use serde::Deserialize;

use crate::compression::{Compression, ZSTD_LEVELS};
use crate::pool::Requirements;

fn yes() -> bool {
    true
}

fn one() -> f64 {
    1.0
}

fn default_project_id_base() -> u32 {
    1_000_000
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Pool {
    pub name: String,
    /// Path of the pool on the node; the agent sees it at the same path.
    pub path: PathBuf,
    /// Limit volumes by project quota. Without it a volume is a plain directory.
    #[serde(default = "yes")]
    pub quota: bool,
    /// Refuse a path that is not the root of a mounted filesystem.
    #[serde(default = "yes")]
    pub require_mountpoint: bool,
    /// Volume sizes may add up to the size of the filesystem times this ratio.
    #[serde(default = "one")]
    pub overcommit: f64,
}

impl Pool {
    pub fn requirements(&self) -> Requirements {
        Requirements { mountpoint: self.require_mountpoint, quota: self.quota }
    }
}

fn default_port() -> u16 {
    9754
}

fn one_at_a_time() -> usize {
    1
}

fn a_day() -> u64 {
    86400
}

fn fastest_regular_level() -> i32 {
    1
}

fn seconds<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let text = String::deserialize(deserializer)?;
    crate::parse_duration(&text)
        .ok_or_else(|| serde::de::Error::custom(format!("{text:?} is not a duration like 0, 30m, 24h or 7d")))
}

/// How volumes that follow their pod travel between the nodes.
#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Transfer {
    /// Port an agent gives volumes out on to the agents of other nodes.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Volumes a node receives at a time; the rest wait.
    #[serde(default = "one_at_a_time")]
    pub concurrency: usize,
    /// KiB per second one copy may take of the network; zero is no limit.
    #[serde(default, rename = "bandwidthLimitKiB")]
    pub bandwidth_limit_kib: u64,
    /// How long the node a volume has left keeps its copy in the trash, in seconds; written as
    /// `24h` in the file.
    #[serde(default = "a_day", rename = "keepSource", deserialize_with = "seconds")]
    pub keep_source_seconds: u64,
    /// How the node that holds the data compresses it on its way.
    #[serde(default)]
    pub compression: Compression,
    /// Level of zstd: higher is smaller and slower, below zero is faster and larger.
    #[serde(default = "fastest_regular_level")]
    pub compression_level: i32,
}

impl Transfer {
    /// The level zstd compresses with; `None` when the data travels as it is.
    pub fn zstd_level(&self) -> Option<i32> {
        (self.compression == Compression::Zstd).then_some(self.compression_level)
    }
}

impl Default for Transfer {
    fn default() -> Self {
        Self {
            port: default_port(),
            concurrency: one_at_a_time(),
            bandwidth_limit_kib: 0,
            keep_source_seconds: a_day(),
            compression: Compression::default(),
            compression_level: fastest_regular_level(),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    pub pools: Vec<Pool>,
    #[serde(default)]
    pub transfer: Transfer,
    /// Project ids of volumes are allocated from this number up: other users of project quota on
    /// the same filesystem keep the ids below it.
    #[serde(default = "default_project_id_base")]
    pub project_id_base: u32,
}

/// Why a configuration of the agent is not one.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("pool {0:?}: a name and an absolute path are required")]
    PoolWithoutNameOrPath(String),
    #[error("pool {0}: overcommit must be a positive number")]
    Overcommit(String),
    #[error("pool {0}: the name is used twice")]
    PoolNamedTwice(String),
    #[error("transfer: concurrency must be at least 1")]
    NoConcurrency,
    #[error("transfer: compressionLevel must be from {lowest} to {highest} and not 0")]
    CompressionLevel { lowest: i32, highest: i32 },
}

impl Config {
    pub fn parse(json: &str) -> Result<Self, ConfigError> {
        let config: Config = serde_json::from_str(json)?;
        for (i, pool) in config.pools.iter().enumerate() {
            if pool.name.is_empty() || !pool.path.is_absolute() {
                return Err(ConfigError::PoolWithoutNameOrPath(pool.name.clone()));
            }
            if !(pool.overcommit.is_finite() && pool.overcommit > 0.0) {
                return Err(ConfigError::Overcommit(pool.name.clone()));
            }
            if config.pools[..i].iter().any(|p| p.name == pool.name) {
                return Err(ConfigError::PoolNamedTwice(pool.name.clone()));
            }
        }
        if config.transfer.concurrency == 0 {
            return Err(ConfigError::NoConcurrency);
        }
        let level = config.transfer.compression_level;
        if level == 0 || !ZSTD_LEVELS.contains(&level) {
            return Err(ConfigError::CompressionLevel { lowest: *ZSTD_LEVELS.start(), highest: *ZSTD_LEVELS.end() });
        }
        Ok(config)
    }

    pub fn pool(&self, name: &str) -> Option<&Pool> {
        self.pools.iter().find(|p| p.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_defaults_are_the_safe_ones() {
        let config = Config::parse(r#"{"pools":[{"name":"default","path":"/mnt/pool"}]}"#).unwrap();
        let pool = config.pool("default").unwrap();
        assert!(pool.quota && pool.require_mountpoint);
        assert_eq!(pool.overcommit, 1.0);
        assert_eq!(config.project_id_base, 1_000_000);
    }

    #[test]
    fn transfer_settings_have_defaults_and_take_durations() {
        let config = Config::parse(r#"{"pools":[]}"#).unwrap();
        assert_eq!(
            config.transfer,
            Transfer {
                port: 9754,
                concurrency: 1,
                bandwidth_limit_kib: 0,
                keep_source_seconds: 86400,
                ..Default::default()
            }
        );
        let json =
            r#"{"pools":[],"transfer":{"port":7000,"concurrency":3,"bandwidthLimitKiB":20480,"keepSource":"30m"}}"#;
        assert_eq!(
            Config::parse(json).unwrap().transfer,
            Transfer {
                port: 7000,
                concurrency: 3,
                bandwidth_limit_kib: 20480,
                keep_source_seconds: 1800,
                ..Default::default()
            }
        );
        for json in [
            r#"{"pools":[],"transfer":{"concurrency":0}}"#,
            r#"{"pools":[],"transfer":{"keepSource":"soon"}}"#,
            r#"{"pools":[],"transfer":{"keepSource":3600}}"#,
        ] {
            assert!(Config::parse(json).is_err(), "{json}");
        }
    }

    #[test]
    fn data_is_compressed_on_its_way_unless_told_otherwise() {
        let transfer = Config::parse(r#"{"pools":[]}"#).unwrap().transfer;
        assert_eq!((transfer.compression, transfer.compression_level), (Compression::Zstd, 1));
        assert_eq!(transfer.zstd_level(), Some(1));

        let transfer = Config::parse(r#"{"pools":[],"transfer":{"compressionLevel":-3}}"#).unwrap().transfer;
        assert_eq!(transfer.zstd_level(), Some(-3));
        let transfer = Config::parse(r#"{"pools":[],"transfer":{"compressionLevel":19}}"#).unwrap().transfer;
        assert_eq!(transfer.zstd_level(), Some(19));

        let transfer =
            Config::parse(r#"{"pools":[],"transfer":{"compression":"none","compressionLevel":5}}"#).unwrap().transfer;
        assert_eq!(transfer.compression, Compression::None);
        assert_eq!(transfer.zstd_level(), None);

        for json in [
            r#"{"pools":[],"transfer":{"compression":"gzip"}}"#,
            r#"{"pools":[],"transfer":{"compressionLevel":0}}"#,
            r#"{"pools":[],"transfer":{"compressionLevel":20}}"#,
            r#"{"pools":[],"transfer":{"compressionLevel":-8}}"#,
            r#"{"pools":[],"transfer":{"compressionLevel":"fast"}}"#,
        ] {
            assert!(Config::parse(json).is_err(), "{json}");
        }
    }

    #[test]
    fn broken_pools_are_rejected() {
        for json in [
            r#"{"pools":[{"name":"a","path":"relative"}]}"#,
            r#"{"pools":[{"name":"","path":"/p"}]}"#,
            r#"{"pools":[{"name":"a","path":"/p","overcommit":0}]}"#,
            r#"{"pools":[{"name":"a","path":"/p"},{"name":"a","path":"/q"}]}"#,
            r#"{"pools":[{"name":"a","path":"/p","qouta":false}]}"#,
        ] {
            assert!(Config::parse(json).is_err(), "{json}");
        }
    }

    #[test]
    fn refusal_of_a_configuration_names_what_is_wrong_with_it() {
        let refusal = |json: &str| Config::parse(json).unwrap_err();
        let twice = refusal(r#"{"pools":[{"name":"a","path":"/p"},{"name":"a","path":"/q"}]}"#);
        assert!(matches!(&twice, ConfigError::PoolNamedTwice(pool) if pool == "a"), "{twice:?}");
        assert_eq!(twice.to_string(), "pool a: the name is used twice");
        let relative = refusal(r#"{"pools":[{"name":"a","path":"relative"}]}"#);
        assert!(matches!(relative, ConfigError::PoolWithoutNameOrPath(_)), "{relative:?}");
        let overcommit = refusal(r#"{"pools":[{"name":"a","path":"/p","overcommit":0}]}"#);
        assert!(matches!(overcommit, ConfigError::Overcommit(_)), "{overcommit:?}");
        let idle = refusal(r#"{"pools":[],"transfer":{"concurrency":0}}"#);
        assert!(matches!(idle, ConfigError::NoConcurrency), "{idle:?}");
        // A misspelt field is refused where the text is read.
        let misspelt = refusal(r#"{"pools":[{"name":"a","path":"/p","qouta":false}]}"#);
        assert!(matches!(&misspelt, ConfigError::Json(_)) && misspelt.to_string().contains("qouta"), "{misspelt}");
    }
}
