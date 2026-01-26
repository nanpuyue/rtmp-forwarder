use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use std::fs;
use anyhow::{Result, Context};
use crate::server::UpstreamConfig;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AppConfig {
    pub listen_addr: String,
    pub upstreams: Vec<UpstreamConfig>,
    pub relay_addr: Option<String>,
    pub relay_enabled: bool,
    pub web_addr: String,
    pub log_level: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:1935".to_string(),
            upstreams: Vec::new(),
            relay_addr: None,
            relay_enabled: false,
            web_addr: "0.0.0.0:8080".to_string(),
            log_level: "info".to_string(),
        }
    }
}

pub type SharedConfig = Arc<RwLock<AppConfig>>;

pub fn load_config() -> AppConfig {
    match fs::read_to_string("config.json") {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => AppConfig::default(),
    }
}

pub fn save_config(config: &AppConfig) -> Result<()> {
    let content = serde_json::to_string_pretty(config)?;
    fs::write("config.json", content).context("failed to write config.json")?;
    Ok(())
}
