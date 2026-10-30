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
    pub autopush_channel_id: Uuid,
    pub uaid: String,
    pub app_server_key: String,
    pub push_endpoint: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("channel_id", &self.channel_id)
            .field("autopush_channel_id", &self.autopush_channel_id)
            .field("uaid", &self.uaid)
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
        SELECT channel_id, autopush_channel_id, uaid, app_server_key, push_endpoint, created_at, updated_at
        FROM channels
        WHERE channel_id = $1
        "#,
        channel_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(channel)
}

pub async fn get_channel_by_autopush_id(
    pool: &PgPool,
    autopush_channel_id: Uuid,
) -> Result<Option<Channel>> {
    let channel = sqlx::query_as!(
        Channel,
        r#"
        SELECT channel_id, autopush_channel_id, uaid, app_server_key, push_endpoint, created_at, updated_at
        FROM channels
        WHERE autopush_channel_id = $1
        "#,
        autopush_channel_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(channel)
}

pub async fn get_channels_by_uaid(pool: &PgPool, uaid: &str) -> Result<Vec<Channel>> {
    let channels = sqlx::query_as!(
        Channel,
        r#"
        SELECT channel_id, autopush_channel_id, uaid, app_server_key, push_endpoint, created_at, updated_at
        FROM channels
        WHERE uaid = $1
        "#,
        uaid
    )
    .fetch_all(pool)
    .await?;

    Ok(channels)
}

pub async fn get_fcm_tokens_by_channel(pool: &PgPool, channel_id: Uuid) -> Result<Vec<String>> {
    let tokens = sqlx::query_scalar!(
        r#"
        SELECT fcm_token
        FROM channel_fcm_tokens
        WHERE channel_id = $1
        "#,
        channel_id
    )
    .fetch_all(pool)
    .await?;

    Ok(tokens)
}

pub async fn upsert_channel_fcm_token(
    pool: &PgPool,
    channel_id: Uuid,
    fcm_token: &str,
) -> Result<()> {
    let mut tx = pool.begin().await?;

    sqlx::query!(
        r#"
        INSERT INTO channel_fcm_tokens (channel_id, fcm_token, created_at, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        ON CONFLICT (channel_id, fcm_token) DO UPDATE
        SET updated_at = NOW()
        "#,
        channel_id,
        fcm_token
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query!(
        r#"
        UPDATE channels
        SET updated_at = NOW()
        WHERE channel_id = $1
        "#,
        channel_id
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

pub async fn upsert_channel(
    pool: &PgPool,
    channel_id: Uuid,
    autopush_channel_id: Uuid,
    uaid: &str,
    fcm_token: &str,
    app_server_key: &str,
    push_endpoint: &str,
) -> Result<()> {
    let mut tx = pool.begin().await?;

    sqlx::query!(
        r#"
        INSERT INTO channels (channel_id, autopush_channel_id, uaid, app_server_key, push_endpoint, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        ON CONFLICT (channel_id) DO UPDATE
        SET autopush_channel_id = EXCLUDED.autopush_channel_id,
            uaid = EXCLUDED.uaid,
            app_server_key = EXCLUDED.app_server_key,
            push_endpoint = EXCLUDED.push_endpoint,
            updated_at = NOW()
        "#,
        channel_id,
        autopush_channel_id,
        uaid,
        app_server_key,
        push_endpoint
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query!(
        r#"
        INSERT INTO channel_fcm_tokens (channel_id, fcm_token, created_at, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        ON CONFLICT (channel_id, fcm_token) DO UPDATE
        SET updated_at = NOW()
        "#,
        channel_id,
        fcm_token
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

pub async fn delete_channel(pool: &PgPool, channel_id: Uuid) -> Result<Option<(String, Uuid)>> {
    let record = sqlx::query!(
        r#"
        DELETE FROM channels
        WHERE channel_id = $1
        RETURNING uaid, autopush_channel_id
        "#,
        channel_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| (r.uaid, r.autopush_channel_id)))
}

pub async fn remove_channel_fcm_token(
    pool: &PgPool,
    channel_id: Uuid,
    fcm_token: &str,
) -> Result<Option<(String, Uuid)>> {
    let mut tx = pool.begin().await?;

    sqlx::query!(
        r#"
        DELETE FROM channel_fcm_tokens
        WHERE channel_id = $1 AND fcm_token = $2
        "#,
        channel_id,
        fcm_token
    )
    .execute(&mut *tx)
    .await?;

    let deleted = sqlx::query!(
        r#"
        DELETE FROM channels c
        WHERE c.channel_id = $1
          AND NOT EXISTS (
              SELECT 1 FROM channel_fcm_tokens cft WHERE cft.channel_id = c.channel_id
          )
        RETURNING c.uaid, c.autopush_channel_id
        "#,
        channel_id
    )
    .fetch_optional(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(deleted.map(|r| (r.uaid, r.autopush_channel_id)))
}

#[allow(dead_code)]
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

pub async fn get_channel_ids_by_fcm_token(pool: &PgPool, fcm_token: &str) -> Result<Vec<Uuid>> {
    let channel_ids = sqlx::query_scalar!(
        r#"
        SELECT channel_id
        FROM channel_fcm_tokens
        WHERE fcm_token = $1
        "#,
        fcm_token
    )
    .fetch_all(pool)
    .await?;

    Ok(channel_ids)
}

pub async fn get_distinct_fcm_tokens(pool: &PgPool) -> Result<Vec<String>> {
    let tokens = sqlx::query_scalar!(
        r#"
        SELECT DISTINCT fcm_token as "fcm_token!"
        FROM channel_fcm_tokens
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
) -> Result<(
    Vec<Uuid>,
    Vec<Uuid>,
    std::collections::HashMap<Uuid, String>,
)> {
    if channel_ids.is_empty() {
        return Ok((vec![], vec![], std::collections::HashMap::new()));
    }

    let existing_rows = sqlx::query!(
        r#"
        SELECT channel_id, push_endpoint
        FROM channels
        WHERE channel_id = ANY($1)
        "#,
        channel_ids
    )
    .fetch_all(pool)
    .await?;

    let existing_map: std::collections::HashMap<Uuid, String> = existing_rows
        .into_iter()
        .map(|r| (r.channel_id, r.push_endpoint))
        .collect();

    let mut active = Vec::new();
    let mut expired = Vec::new();
    let mut channel_endpoints = std::collections::HashMap::new();

    for id in channel_ids {
        if let Some(ep) = existing_map.get(id) {
            active.push(*id);
            channel_endpoints.insert(*id, ep.clone());
        } else {
            expired.push(*id);
        }
    }

    if !active.is_empty() {
        let mut tx = pool.begin().await?;

        sqlx::query!(
            r#"
            INSERT INTO channel_fcm_tokens (channel_id, fcm_token, created_at, updated_at)
            SELECT UNNEST($1::uuid[]), $2, NOW(), NOW()
            ON CONFLICT (channel_id, fcm_token) DO UPDATE
            SET updated_at = NOW()
            "#,
            &active,
            fcm_token
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query!(
            r#"
            UPDATE channels
            SET updated_at = NOW()
            WHERE channel_id = ANY($1)
            "#,
            &active
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
    }

    Ok((active, expired, channel_endpoints))
}
