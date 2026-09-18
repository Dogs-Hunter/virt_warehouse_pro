use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: SocketAddr,
    pub data_dir: PathBuf,
    pub shards: usize,
    pub wal_batch: usize,
    pub wal_flush_interval: Duration,
    pub nats_url: Option<String>,
    pub live_tail: bool,
    pub fail_after_replicate_id: Option<String>,
    pub instance_id: String,
    pub writer_priority_delay: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let bind = value("WAREHOUSE_BIND", "127.0.0.1:8080")
            .parse()
            .context("invalid WAREHOUSE_BIND")?;
        let data_dir = PathBuf::from(value("WAREHOUSE_DATA_DIR", "data"));
        let shards = parse_usize("WAREHOUSE_SHARDS", 16)?;
        let wal_batch = parse_usize("WAREHOUSE_WAL_BATCH", 1024)?;
        let flush_ms = parse_usize("WAREHOUSE_WAL_FLUSH_MS", 2)?;
        anyhow::ensure!(shards.is_power_of_two(), "WAREHOUSE_SHARDS must be a power of two");
        anyhow::ensure!(wal_batch > 0, "WAREHOUSE_WAL_BATCH must be positive");
        Ok(Self {
            bind,
            data_dir,
            shards,
            wal_batch,
            wal_flush_interval: Duration::from_millis(flush_ms as u64),
            nats_url: env::var("WAREHOUSE_NATS_URL")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            live_tail: value("WAREHOUSE_LIVE_TAIL", "false") == "true",
            fail_after_replicate_id: env::var("WAREHOUSE_FAIL_AFTER_REPLICATE_ID")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            instance_id: value("WAREHOUSE_INSTANCE_ID", "warehouse-local"),
            writer_priority_delay: Duration::from_millis(parse_usize("WAREHOUSE_WRITER_PRIORITY_DELAY_MS", 0)? as u64),
        })
    }
}

fn value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn parse_usize(name: &str, default: usize) -> Result<usize> {
    value(name, &default.to_string())
        .parse()
        .with_context(|| format!("invalid {name}"))
}
