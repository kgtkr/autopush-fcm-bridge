use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;
use uuid::Uuid;

use crate::pool::{AutopushPool, ChannelError};

const MAX_FCM_TOKEN_LEN: usize = 4096;
const MAX_APP_SERVER_KEY_LEN: usize = 255;
const MAX_SYNC_CHANNEL_IDS: usize = 100;

#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<AutopushPool>,
    pub db: sqlx::PgPool,
}

#[derive(Debug, Deserialize)]
pub struct RegisterChannelRequest {
    pub fcm_token: String,
    pub app_server_key: String,
}

#[derive(Debug, Serialize)]
pub struct RegisterChannelResponse {
    pub channel_id: Uuid,
    pub push_endpoint: String,
    pub app_server_key: String,
}

#[derive(Debug, Deserialize)]
pub struct SyncChannelsRequest {
    pub fcm_token: String,
    pub channel_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct SyncChannelsResponse {
    pub active_channel_ids: Vec<Uuid>,
    pub expired_channel_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub enum AppError {
    BadRequest(String),
    Internal(anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::Internal(err) => {
                error!("Internal server error: {err:#}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                )
            }
        };

        (status, Json(ErrorResponse { error: msg })).into_response()
    }
}

impl From<ChannelError> for AppError {
    fn from(err: ChannelError) -> Self {
        match err {
            ChannelError::InvalidFcmToken => {
                AppError::BadRequest("Invalid or unregistered FCM token".to_string())
            }
            ChannelError::Internal(err) => AppError::Internal(err),
        }
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        AppError::Internal(err)
    }
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/channels", post(register_channel))
        .route("/v1/channels/:channel_id", delete(unregister_channel))
        .route("/v1/channels/sync", post(sync_channels))
        .with_state(state)
}

async fn healthz(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, &'static str)> {
    crate::db::check_health(&state.db)
        .await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Database unavailable"))?;

    Ok(Json(serde_json::json!({ "status": "ok" })))
}

async fn register_channel(
    State(state): State<AppState>,
    Json(payload): Json<RegisterChannelRequest>,
) -> Result<(StatusCode, Json<RegisterChannelResponse>), AppError> {
    let fcm_token = payload.fcm_token.trim();
    if fcm_token.is_empty() {
        return Err(AppError::BadRequest(
            "fcm_token cannot be empty".to_string(),
        ));
    }
    if fcm_token.len() > MAX_FCM_TOKEN_LEN {
        return Err(AppError::BadRequest(format!(
            "fcm_token is too long (max {MAX_FCM_TOKEN_LEN} characters)"
        )));
    }

    let app_server_key = payload.app_server_key.trim();
    if app_server_key.is_empty() {
        return Err(AppError::BadRequest(
            "app_server_key cannot be empty".to_string(),
        ));
    }
    if app_server_key.len() > MAX_APP_SERVER_KEY_LEN {
        return Err(AppError::BadRequest(format!(
            "app_server_key is too long (max {MAX_APP_SERVER_KEY_LEN} characters)"
        )));
    }

    let (channel_id, push_endpoint) = state
        .pool
        .register_channel(fcm_token, app_server_key)
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(RegisterChannelResponse {
            channel_id,
            push_endpoint,
            app_server_key: app_server_key.to_string(),
        }),
    ))
}

async fn unregister_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    state.pool.unregister_channel(channel_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn sync_channels(
    State(state): State<AppState>,
    Json(payload): Json<SyncChannelsRequest>,
) -> Result<Json<SyncChannelsResponse>, AppError> {
    let fcm_token = payload.fcm_token.trim();
    if fcm_token.is_empty() {
        return Err(AppError::BadRequest(
            "fcm_token cannot be empty".to_string(),
        ));
    }
    if fcm_token.len() > MAX_FCM_TOKEN_LEN {
        return Err(AppError::BadRequest(format!(
            "fcm_token is too long (max {MAX_FCM_TOKEN_LEN} characters)"
        )));
    }
    if payload.channel_ids.len() > MAX_SYNC_CHANNEL_IDS {
        return Err(AppError::BadRequest(format!(
            "Too many channel_ids (max {MAX_SYNC_CHANNEL_IDS})"
        )));
    }

    let (active_channel_ids, expired_channel_ids) = state
        .pool
        .sync_channels(fcm_token, &payload.channel_ids)
        .await?;

    Ok(Json(SyncChannelsResponse {
        active_channel_ids,
        expired_channel_ids,
    }))
}
