use std::fs;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwarderConfig {
    pub addr: String,
    pub app: Option<String>,
    pub stream: Option<String>,
    pub enabled: bool,
}

impl ForwarderConfig {
    pub fn rtmp_url(&self) -> String {
        let mut url = format!("rtmp://{}", self.addr);
        if let Some(app) = &self.app
            && !app.is_empty()
        {
            url.push('/');
            url.push_str(app);
            if let Some(stream) = &self.stream
                && !stream.is_empty()
            {
                if !app.ends_with('/') {
                    url.push('/');
                }
                url.push_str(stream);
            }
        }
        url
    }
}

pub type SharedConfig = Arc<RwLock<AppConfig>>;

/// Web配置子集，仅包含允许通过Web界面配置的字段
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WebConfig {
    pub forwarders: Vec<ForwarderConfig>,
    pub relay_addr: String,
    pub relay_enabled: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AppConfig {
    pub listen_addr: String,
    pub forwarders: Vec<ForwarderConfig>,
    pub relay_addr: String,
    pub relay_enabled: bool,
    pub web_addr: String,
    pub log_level: String,
    #[serde(skip)]
    pub config_path: String,
}

pub trait GetForwarders {
    fn get_forwarders(&self) -> Vec<ForwarderConfig>;
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
            config_path: "config.json".to_string(),
        }
    }
}

impl From<&AppConfig> for WebConfig {
    fn from(config: &AppConfig) -> Self {
        Self {
            forwarders: config.forwarders.clone(),
            relay_addr: config.relay_addr.clone(),
            relay_enabled: config.relay_enabled,
        }
    }
}

impl AppConfig {
    /// Save configuration to file
    pub fn save(&mut self) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(&self.config_path, content)
            .context(format!("failed to write config file: {}", self.config_path))?;
        Ok(())
    }

    /// Update configuration from WebConfig, preserving config_path
    pub fn update_from_web_config(&mut self, web_config: &WebConfig) {
        self.forwarders = web_config.forwarders.clone();
        self.relay_addr = web_config.relay_addr.clone();
        self.relay_enabled = web_config.relay_enabled;
        // 其他字段保持不变，特别是 config_path
    }

    pub fn load(path: &str) -> Result<AppConfig> {
        let content =
            fs::read_to_string(path).context(format!("Failed to read config file: {}", path))?;
        let mut config: AppConfig =
            serde_json::from_str(&content).context("Failed to parse config JSON")?;
        config.config_path = path.to_string();
        Ok(config)
    }

    pub fn load_or_create(path: &str, cli_config: AppConfig) -> Result<AppConfig> {
        if fs::metadata(path).is_ok() {
            Self::load(path)
        } else {
            let mut config = cli_config;
            config.config_path = path.to_string();
            config.save()?;
            Ok(config)
        }
    }
}

impl GetForwarders for WebConfig {
    fn get_forwarders(&self) -> Vec<ForwarderConfig> {
        let mut forwarders = self.forwarders.clone();
        // Always insert relay config to keep forwarder indices stable
        forwarders.insert(
            0,
            ForwarderConfig {
                addr: self.relay_addr.clone(),
                app: None,
                stream: None,
                enabled: self.relay_enabled,
            },
        );
        forwarders
    }
}

impl GetForwarders for AppConfig {
    fn get_forwarders(&self) -> Vec<ForwarderConfig> {
        WebConfig::from(self).get_forwarders()
    }
}
