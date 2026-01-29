use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use std::fs;
use anyhow::{Result, Context};
use crate::server::ForwarderConfig;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AppConfig {
    pub listen_addr: String,
    pub forwarders: Vec<ForwarderConfig>,
    pub relay_addr: String,
    pub relay_enabled: bool,
    pub web_addr: String,
    pub log_level: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:1935".to_string(),
            forwarders: Vec::new(),
            relay_addr: String::new(),
            relay_enabled: false,
            web_addr: "0.0.0.0:8080".to_string(),
            log_level: "info".to_string(),
        }
    }
}

impl AppConfig {
    /// Convert relay configuration to forwarder list
    pub fn get_forwarders(&self) -> Vec<ForwarderConfig> {
        let mut forwarders = self.forwarders.clone();
        // Always insert relay config to keep forwarder indices stable
        forwarders.insert(0, ForwarderConfig {
            addr: self.relay_addr.clone(),
            app: None,
            stream: None,
            enabled: self.relay_enabled,
        });
        forwarders
    }
    
    /// Save configuration to file
    pub fn save(&mut self) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write("config.json", content).context("failed to write config.json")?;
        Ok(())
    }

    pub fn load(path: &str) -> Result<AppConfig> {
        let content = fs::read_to_string(path).context(format!("Failed to read config file: {}", path))?;
        serde_json::from_str(&content).context("Failed to parse config JSON")
    }
}

pub type SharedConfig = Arc<RwLock<AppConfig>>;
