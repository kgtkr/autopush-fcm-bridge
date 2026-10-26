use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub host: String,
    pub port: u16,
    pub autopush_ws_url: String,
    pub max_channels_per_uaid: usize,
    pub ws_ping_interval_secs: u64,
    pub token_sweep_interval_secs: u64,
    pub service_account_json: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let database_url =
            env::var("DATABASE_URL").context("DATABASE_URL environment variable is required")?;

        let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = env::var("PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .context("PORT must be a valid u16 integer")?;

        let autopush_ws_url = env::var("AUTOPUSH_WS_URL")
            .unwrap_or_else(|_| "wss://push.services.mozilla.com/".to_string());

        let max_channels_per_uaid = env::var("MAX_CHANNELS_PER_UAID")
            .unwrap_or_else(|_| "50".to_string())
            .parse::<usize>()
            .context("MAX_CHANNELS_PER_UAID must be a valid integer")?;

        let ws_ping_interval_secs = env::var("WS_PING_INTERVAL_SECS")
            .unwrap_or_else(|_| "60".to_string())
            .parse::<u64>()
            .context("WS_PING_INTERVAL_SECS must be a valid integer")?;

        let token_sweep_interval_secs = env::var("TOKEN_SWEEP_INTERVAL_SECS")
            .unwrap_or_else(|_| "21600".to_string())
            .parse::<u64>()
            .context("TOKEN_SWEEP_INTERVAL_SECS must be a valid integer")?;

        let service_account_json = if let Ok(json_str) = env::var("FIREBASE_SERVICE_ACCOUNT_JSON") {
            json_str
        } else if let Ok(path) = env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            std::fs::read_to_string(&path).with_context(|| {
                format!("Failed to read service account credentials from {path}")
            })?
        } else {
            anyhow::bail!("Either GOOGLE_APPLICATION_CREDENTIALS or FIREBASE_SERVICE_ACCOUNT_JSON must be set");
        };

        Ok(Self {
            database_url,
            host,
            port,
            autopush_ws_url,
            max_channels_per_uaid,
            ws_ping_interval_secs,
            token_sweep_interval_secs,
            service_account_json,
        })
    }
}
