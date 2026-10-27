use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct AutopushSession {
    pub uaid: String,
    pub created_at: DateTime<Utc>,
    pub last_connected_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct Channel {
    pub channel_id: Uuid,
    pub uaid: String,
    pub fcm_token: String,
    pub app_server_key: String,
    pub push_endpoint: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("channel_id", &self.channel_id)
            .field("uaid", &self.uaid)
            .field("fcm_token", &"<redacted>")
            .field("app_server_key", &"<redacted>")
            .field("push_endpoint", &"<redacted>")
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

pub async fn create_pool(database_url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(database_url)
        .await
        .context("Failed to connect to PostgreSQL")
}

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .context("Failed to run database migrations")
}

pub async fn check_health(pool: &PgPool) -> Result<()> {
    sqlx::query!("SELECT 1 as one")
        .fetch_one(pool)
        .await
        .context("Database health check failed")?;
    Ok(())
}

pub async fn delete_empty_sessions(pool: &PgPool) -> Result<u64> {
    let result = sqlx::query!(
        r#"
        DELETE FROM autopush_sessions
        WHERE NOT EXISTS (
            SELECT 1 FROM channels c WHERE c.uaid = autopush_sessions.uaid
        )
        "#
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn get_all_sessions(pool: &PgPool) -> Result<Vec<AutopushSession>> {
    let sessions = sqlx::query_as!(
        AutopushSession,
        r#"
        SELECT uaid, created_at, last_connected_at
        FROM autopush_sessions
        ORDER BY created_at ASC
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(sessions)
}

pub async fn insert_session(pool: &PgPool, uaid: &str) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO autopush_sessions (uaid)
        VALUES ($1)
        ON CONFLICT (uaid) DO UPDATE SET last_connected_at = NOW()
        "#,
        uaid
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn touch_session(pool: &PgPool, uaid: &str) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE autopush_sessions
        SET last_connected_at = NOW()
        WHERE uaid = $1
        "#,
        uaid
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn delete_session(pool: &PgPool, uaid: &str) -> Result<()> {
    sqlx::query!(
        r#"
        DELETE FROM autopush_sessions
        WHERE uaid = $1
        "#,
        uaid
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn delete_session_if_empty(pool: &PgPool, uaid: &str) -> Result<bool> {
    let result = sqlx::query!(
        r#"
        DELETE FROM autopush_sessions
        WHERE uaid = $1
          AND NOT EXISTS (
              SELECT 1 FROM channels c WHERE c.uaid = autopush_sessions.uaid
          )
        "#,
        uaid
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn find_candidate_uaid(pool: &PgPool, max_channels: usize) -> Result<Option<String>> {
    let uaid = sqlx::query_scalar!(
        r#"
        SELECT s.uaid
        FROM autopush_sessions s
        LEFT JOIN channels c ON s.uaid = c.uaid
        GROUP BY s.uaid
        HAVING COUNT(c.channel_id) < $1
        ORDER BY COUNT(c.channel_id) DESC, s.created_at ASC
        LIMIT 1
        "#,
        max_channels as i64
    )
    .fetch_optional(pool)
    .await?;

    Ok(uaid)
}

pub async fn get_channel(pool: &PgPool, channel_id: Uuid) -> Result<Option<Channel>> {
    let channel = sqlx::query_as!(
        Channel,
        r#"
        SELECT channel_id, uaid, fcm_token, app_server_key, push_endpoint, created_at, updated_at
        FROM channels
        WHERE channel_id = $1
        "#,
        channel_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(channel)
}

pub async fn get_channels_by_uaid(pool: &PgPool, uaid: &str) -> Result<Vec<Channel>> {
    let channels = sqlx::query_as!(
        Channel,
        r#"
        SELECT channel_id, uaid, fcm_token, app_server_key, push_endpoint, created_at, updated_at
        FROM channels
        WHERE uaid = $1
        "#,
        uaid
    )
    .fetch_all(pool)
    .await?;

    Ok(channels)
}

pub async fn insert_channel(
    pool: &PgPool,
    channel_id: Uuid,
    uaid: &str,
    fcm_token: &str,
    app_server_key: &str,
    push_endpoint: &str,
) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO channels (channel_id, uaid, fcm_token, app_server_key, push_endpoint, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        "#,
        channel_id,
        uaid,
        fcm_token,
        app_server_key,
        push_endpoint
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn delete_channel(pool: &PgPool, channel_id: Uuid) -> Result<Option<String>> {
    let uaid = sqlx::query_scalar!(
        r#"
        DELETE FROM channels
        WHERE channel_id = $1
        RETURNING uaid
        "#,
        channel_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(uaid)
}

pub async fn count_channels_by_uaid(pool: &PgPool, uaid: &str) -> Result<i64> {
    let count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!"
        FROM channels
        WHERE uaid = $1
        "#,
        uaid
    )
    .fetch_one(pool)
    .await?;

    Ok(count)
}

pub async fn get_channels_by_fcm_token(pool: &PgPool, fcm_token: &str) -> Result<Vec<Channel>> {
    let channels = sqlx::query_as!(
        Channel,
        r#"
        SELECT channel_id, uaid, fcm_token, app_server_key, push_endpoint, created_at, updated_at
        FROM channels
        WHERE fcm_token = $1
        "#,
        fcm_token
    )
    .fetch_all(pool)
    .await?;

    Ok(channels)
}

pub async fn get_distinct_fcm_tokens(pool: &PgPool) -> Result<Vec<String>> {
    let tokens = sqlx::query_scalar!(
        r#"
        SELECT DISTINCT fcm_token as "fcm_token!"
        FROM channels
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(tokens)
}

pub async fn sync_channels(
    pool: &PgPool,
    fcm_token: &str,
    channel_ids: &[Uuid],
) -> Result<(Vec<Uuid>, Vec<Uuid>)> {
    if channel_ids.is_empty() {
        return Ok((vec![], vec![]));
    }

    let existing_ids = sqlx::query_scalar!(
        r#"
        SELECT channel_id
        FROM channels
        WHERE channel_id = ANY($1)
        "#,
        channel_ids
    )
    .fetch_all(pool)
    .await?;

    let existing_set: std::collections::HashSet<Uuid> = existing_ids.into_iter().collect();

    let mut active = Vec::new();
    let mut expired = Vec::new();

    for id in channel_ids {
        if existing_set.contains(id) {
            active.push(*id);
        } else {
            expired.push(*id);
        }
    }

    if !active.is_empty() {
        sqlx::query!(
            r#"
            UPDATE channels
            SET fcm_token = $1, updated_at = NOW()
            WHERE channel_id = ANY($2)
            "#,
            fcm_token,
            &active
        )
        .execute(pool)
        .await?;
    }

    Ok((active, expired))
}
