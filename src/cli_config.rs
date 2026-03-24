//! Product-specific config for the haqlite CLI.

use serde::Deserialize;
use std::path::PathBuf;

/// haqlite-specific config, loaded from the `[serve]` section of haqlite.toml.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct HaqliteConfig {
    #[serde(default)]
    pub serve: Option<ServeConfig>,
}

/// Configuration for `haqlite serve`.
#[derive(Debug, Clone, Deserialize)]
pub struct ServeConfig {
    /// Path to the SQLite database file.
    pub db_path: PathBuf,

    /// SQL schema to execute on startup (CREATE TABLE IF NOT EXISTS ...).
    #[serde(default)]
    pub schema: Option<String>,

    /// HTTP API port for client connections.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Internal forwarding port for leader write forwarding.
    #[serde(default = "default_forwarding_port")]
    pub forwarding_port: u16,

    /// S3 key prefix for all databases.
    #[serde(default = "default_prefix")]
    pub prefix: String,

    /// Shared secret for inter-node forwarding authentication.
    #[serde(default)]
    pub secret: Option<String>,

    /// WAL sync interval in milliseconds.
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,

    /// Follower WAL pull interval in milliseconds.
    #[serde(default = "default_follower_pull_ms")]
    pub follower_pull_ms: u64,
}

fn default_port() -> u16 {
    8080
}
fn default_forwarding_port() -> u16 {
    18080
}
fn default_prefix() -> String {
    "haqlite/".to_string()
}
fn default_sync_interval_ms() -> u64 {
    1000
}
fn default_follower_pull_ms() -> u64 {
    1000
}
