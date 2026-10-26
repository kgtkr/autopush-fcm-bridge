use anyhow::{Context, Result};
use chrono::Utc;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum FcmError {
    #[error("Token is unregistered or invalid (target app uninstalled)")]
    Unregistered,
    #[error("Google API error: {0}")]
    Api(String),
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Auth error: {0}")]
    Auth(String),
}

#[derive(Deserialize, Clone)]
pub struct ServiceAccount {
    pub project_id: String,
    pub private_key: String,
    pub client_email: String,
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}

#[derive(Serialize)]
struct JwtClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: i64,
    iat: i64,
}

#[derive(Clone)]
struct CachedToken {
    access_token: String,
    expires_at: i64,
}

pub struct FcmClient {
    service_account: ServiceAccount,
    http_client: reqwest::Client,
    cached_token: Arc<RwLock<Option<CachedToken>>>,
}

#[derive(Serialize)]
struct FcmSendRequest<'a> {
    validate_only: bool,
    message: FcmMessage<'a>,
}

#[derive(Serialize)]
struct FcmMessage<'a> {
    token: &'a str,
    data: HashMap<String, String>,
    android: FcmAndroidConfig,
}

#[derive(Serialize)]
struct FcmAndroidConfig {
    priority: &'static str,
}

#[derive(Deserialize)]
struct GoogleTokenResponse {
    access_token: String,
    expires_in: i64,
}

#[derive(Deserialize)]
struct GoogleErrorResponse {
    error: Option<GoogleErrorDetail>,
}

#[derive(Deserialize)]
struct GoogleErrorDetail {
    code: Option<i32>,
    message: Option<String>,
    status: Option<String>,
    details: Option<Vec<GoogleErrorInfo>>,
}

#[derive(Deserialize)]
struct GoogleErrorInfo {
    #[serde(rename = "@type")]
    #[allow(dead_code)]
    error_type: Option<String>,
    #[serde(rename = "errorCode")]
    error_code: Option<String>,
}

impl FcmClient {
    pub fn new(service_account_json: &str) -> Result<Self> {
        let service_account: ServiceAccount = serde_json::from_str(service_account_json)
            .context("Failed to parse Firebase service account JSON")?;

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;

        Ok(Self {
            service_account,
            http_client,
            cached_token: Arc::new(RwLock::new(None)),
        })
    }

    pub fn project_id(&self) -> &str {
        &self.service_account.project_id
    }

    async fn get_access_token(&self) -> Result<String, FcmError> {
        let now = Utc::now().timestamp();

        {
            let cache = self.cached_token.read().await;
            if let Some(ref token) = *cache {
                // If token is valid for at least 5 more minutes, reuse it
                if token.expires_at > now + 300 {
                    return Ok(token.access_token.clone());
                }
            }
        }

        let mut cache_mut = self.cached_token.write().await;
        // Double check after acquiring write lock
        if let Some(ref token) = *cache_mut {
            if token.expires_at > now + 300 {
                return Ok(token.access_token.clone());
            }
        }

        let claims = JwtClaims {
            iss: &self.service_account.client_email,
            scope: "https://www.googleapis.com/auth/firebase.messaging",
            aud: &self.service_account.token_uri,
            exp: now + 3600,
            iat: now,
        };

        let key = EncodingKey::from_rsa_pem(self.service_account.private_key.as_bytes())
            .map_err(|e| FcmError::Auth(format!("Invalid private key: {e}")))?;

        let jwt = jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &key)
            .map_err(|e| FcmError::Auth(format!("Failed to sign JWT: {e}")))?;

        let res = self
            .http_client
            .post(&self.service_account.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &jwt),
            ])
            .send()
            .await?;

        if !res.status().is_success() {
            let err_body = res.text().await.unwrap_or_default();
            return Err(FcmError::Auth(format!(
                "OAuth token request failed: {err_body}"
            )));
        }

        let token_resp: GoogleTokenResponse = res.json().await?;
        let access_token = token_resp.access_token;
        let expires_at = now + token_resp.expires_in;

        *cache_mut = Some(CachedToken {
            access_token: access_token.clone(),
            expires_at,
        });

        Ok(access_token)
    }

    pub async fn validate_token(&self, fcm_token: &str) -> Result<(), FcmError> {
        let mut data = HashMap::new();
        data.insert("type".to_string(), "ping".to_string());

        self.send_raw(fcm_token, data, true).await?;
        Ok(())
    }

    pub async fn send_notification(
        &self,
        fcm_token: &str,
        channel_id: Uuid,
        data_payload: Option<&str>,
        headers: Option<&HashMap<String, String>>,
    ) -> Result<(), FcmError> {
        let mut data = HashMap::new();
        data.insert("type".to_string(), "notification".to_string());
        data.insert("channel_id".to_string(), channel_id.to_string());

        if let Some(payload) = data_payload {
            data.insert("data".to_string(), payload.to_string());
        }

        if let Some(hdrs) = headers {
            if let Ok(hdr_json) = serde_json::to_string(hdrs) {
                data.insert("headers".to_string(), hdr_json);
            }

            for (k, v) in hdrs {
                let norm = k.replace(['-', '_'], "").to_lowercase();
                if norm == "cryptokey" {
                    data.insert("crypto_key".to_string(), v.clone());
                } else if norm == "encryption" {
                    data.insert("encryption".to_string(), v.clone());
                } else if norm == "contentencoding" || norm == "encoding" {
                    data.insert("encoding".to_string(), v.clone());
                }
            }
        }

        self.send_raw(fcm_token, data, false).await?;
        Ok(())
    }

    pub async fn send_channel_expired(
        &self,
        fcm_token: &str,
        channel_id: Uuid,
    ) -> Result<(), FcmError> {
        let mut data = HashMap::new();
        data.insert("type".to_string(), "channel_expired".to_string());
        data.insert("channel_id".to_string(), channel_id.to_string());

        self.send_raw(fcm_token, data, false).await?;
        Ok(())
    }

    async fn send_raw(
        &self,
        fcm_token: &str,
        data: HashMap<String, String>,
        validate_only: bool,
    ) -> Result<(), FcmError> {
        let access_token = self.get_access_token().await?;
        let url = format!(
            "https://fcm.googleapis.com/v1/projects/{}/messages:send",
            self.service_account.project_id
        );

        let req_body = FcmSendRequest {
            validate_only,
            message: FcmMessage {
                token: fcm_token,
                data,
                android: FcmAndroidConfig { priority: "HIGH" },
            },
        };

        let res = self
            .http_client
            .post(&url)
            .bearer_auth(access_token)
            .json(&req_body)
            .send()
            .await?;

        let status = res.status();
        if status.is_success() {
            return Ok(());
        }

        let err_text = res.text().await.unwrap_or_default();
        if let Ok(err_resp) = serde_json::from_str::<GoogleErrorResponse>(&err_text) {
            if let Some(detail) = err_resp.error {
                if detail.code == Some(404)
                    || detail.status.as_deref() == Some("NOT_FOUND")
                    || detail.status.as_deref() == Some("INVALID_ARGUMENT")
                {
                    return Err(FcmError::Unregistered);
                }

                if let Some(details) = detail.details {
                    for item in details {
                        if item.error_code.as_deref() == Some("UNREGISTERED")
                            || item.error_code.as_deref() == Some("INVALID_ARGUMENT")
                        {
                            return Err(FcmError::Unregistered);
                        }
                    }
                }

                return Err(FcmError::Api(format!(
                    "FCM API error {}: {}",
                    status,
                    detail.message.unwrap_or(err_text)
                )));
            }
        }

        if status.as_u16() == 404 || status.as_u16() == 400 {
            return Err(FcmError::Unregistered);
        }

        Err(FcmError::Api(format!(
            "FCM request failed {status}: {err_text}"
        )))
    }
}
