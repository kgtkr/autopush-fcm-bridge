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

use crate::pool::AutopushPool;

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

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        AppError::Internal(err.into())
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
    if payload.fcm_token.trim().is_empty() {
        return Err(AppError::BadRequest(
            "fcm_token cannot be empty".to_string(),
        ));
    }
    if payload.app_server_key.trim().is_empty() {
        return Err(AppError::BadRequest(
            "app_server_key cannot be empty".to_string(),
        ));
    }

    let (channel_id, push_endpoint) = state
        .pool
        .register_channel(&payload.fcm_token, &payload.app_server_key)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("FCM token validation failed") {
                AppError::BadRequest(msg)
            } else {
                AppError::Internal(e)
            }
        })?;

    Ok((
        StatusCode::CREATED,
        Json(RegisterChannelResponse {
            channel_id,
            push_endpoint,
            app_server_key: payload.app_server_key,
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
    if payload.fcm_token.trim().is_empty() {
        return Err(AppError::BadRequest(
            "fcm_token cannot be empty".to_string(),
        ));
    }

    let (active_channel_ids, expired_channel_ids) = state
        .pool
        .sync_channels(&payload.fcm_token, &payload.channel_ids)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("FCM token validation failed") {
                AppError::BadRequest(msg)
            } else {
                AppError::Internal(e)
            }
        })?;

    Ok(Json(SyncChannelsResponse {
        active_channel_ids,
        expired_channel_ids,
    }))
}
