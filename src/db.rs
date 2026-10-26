use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};
use uuid::Uuid;

#[derive(Debug, Clone, FromRow)]
pub struct AutopushSession {
    pub uaid: String,
    pub created_at: DateTime<Utc>,
    pub last_connected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct Channel {
    pub channel_id: Uuid,
    pub uaid: String,
    pub fcm_token: String,
    pub app_server_key: String,
    pub push_endpoint: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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

pub async fn delete_empty_sessions(pool: &PgPool) -> Result<u64> {
    let result = sqlx::query(
        r#"
        DELETE FROM autopush_sessions
        WHERE uaid NOT IN (SELECT DISTINCT uaid FROM channels)
        "#,
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn get_all_sessions(pool: &PgPool) -> Result<Vec<AutopushSession>> {
    let sessions = sqlx::query_as::<_, AutopushSession>(
        r#"
        SELECT uaid, created_at, last_connected_at
        FROM autopush_sessions
        ORDER BY created_at ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(sessions)
}

pub async fn insert_session(pool: &PgPool, uaid: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO autopush_sessions (uaid)
        VALUES ($1)
        ON CONFLICT (uaid) DO UPDATE SET last_connected_at = NOW()
        "#,
    )
    .bind(uaid)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn touch_session(pool: &PgPool, uaid: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE autopush_sessions
        SET last_connected_at = NOW()
        WHERE uaid = $1
        "#,
    )
    .bind(uaid)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn delete_session(pool: &PgPool, uaid: &str) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM autopush_sessions
        WHERE uaid = $1
        "#,
    )
    .bind(uaid)
    .execute(pool)
    .await?;

    Ok(())
}

#[derive(FromRow)]
struct CandidateUaidRow {
    uaid: String,
}

pub async fn find_candidate_uaid(pool: &PgPool, max_channels: usize) -> Result<Option<String>> {
    let row = sqlx::query_as::<_, CandidateUaidRow>(
        r#"
        SELECT s.uaid
        FROM autopush_sessions s
        LEFT JOIN channels c ON s.uaid = c.uaid
        GROUP BY s.uaid
        HAVING COUNT(c.channel_id) < $1
        ORDER BY COUNT(c.channel_id) DESC, s.created_at ASC
        LIMIT 1
        "#,
    )
    .bind(max_channels as i64)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| r.uaid))
}

pub async fn get_channel(pool: &PgPool, channel_id: Uuid) -> Result<Option<Channel>> {
    let channel = sqlx::query_as::<_, Channel>(
        r#"
        SELECT channel_id, uaid, fcm_token, app_server_key, push_endpoint, created_at, updated_at
        FROM channels
        WHERE channel_id = $1
        "#,
    )
    .bind(channel_id)
    .fetch_optional(pool)
    .await?;

    Ok(channel)
}

pub async fn get_channels_by_uaid(pool: &PgPool, uaid: &str) -> Result<Vec<Channel>> {
    let channels = sqlx::query_as::<_, Channel>(
        r#"
        SELECT channel_id, uaid, fcm_token, app_server_key, push_endpoint, created_at, updated_at
        FROM channels
        WHERE uaid = $1
        "#,
    )
    .bind(uaid)
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
    sqlx::query(
        r#"
        INSERT INTO channels (channel_id, uaid, fcm_token, app_server_key, push_endpoint, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        "#,
    )
    .bind(channel_id)
    .bind(uaid)
    .bind(fcm_token)
    .bind(app_server_key)
    .bind(push_endpoint)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn delete_channel(pool: &PgPool, channel_id: Uuid) -> Result<Option<String>> {
    let channel = get_channel(pool, channel_id).await?;
    if let Some(ch) = channel {
        sqlx::query(
            r#"
            DELETE FROM channels
            WHERE channel_id = $1
            "#,
        )
        .bind(channel_id)
        .execute(pool)
        .await?;

        Ok(Some(ch.uaid))
    } else {
        Ok(None)
    }
}

pub async fn count_channels_by_uaid(pool: &PgPool, uaid: &str) -> Result<i64> {
    #[derive(FromRow)]
    struct CountRow {
        count: i64,
    }

    let row = sqlx::query_as::<_, CountRow>(
        r#"
        SELECT COUNT(*) as count
        FROM channels
        WHERE uaid = $1
        "#,
    )
    .bind(uaid)
    .fetch_one(pool)
    .await?;

    Ok(row.count)
}

pub async fn get_channels_by_fcm_token(pool: &PgPool, fcm_token: &str) -> Result<Vec<Channel>> {
    let channels = sqlx::query_as::<_, Channel>(
        r#"
        SELECT channel_id, uaid, fcm_token, app_server_key, push_endpoint, created_at, updated_at
        FROM channels
        WHERE fcm_token = $1
        "#,
    )
    .bind(fcm_token)
    .fetch_all(pool)
    .await?;

    Ok(channels)
}

pub async fn get_distinct_fcm_tokens(pool: &PgPool) -> Result<Vec<String>> {
    #[derive(FromRow)]
    struct TokenRow {
        fcm_token: String,
    }

    let rows = sqlx::query_as::<_, TokenRow>(
        r#"
        SELECT DISTINCT fcm_token
        FROM channels
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| r.fcm_token).collect())
}

pub async fn sync_channels(
    pool: &PgPool,
    fcm_token: &str,
    channel_ids: &[Uuid],
) -> Result<(Vec<Uuid>, Vec<Uuid>)> {
    if channel_ids.is_empty() {
        return Ok((vec![], vec![]));
    }

    #[derive(FromRow)]
    struct IdRow {
        channel_id: Uuid,
    }

    let existing_rows = sqlx::query_as::<_, IdRow>(
        r#"
        SELECT channel_id
        FROM channels
        WHERE channel_id = ANY($1)
        "#,
    )
    .bind(channel_ids)
    .fetch_all(pool)
    .await?;

    let existing_set: std::collections::HashSet<Uuid> =
        existing_rows.into_iter().map(|r| r.channel_id).collect();

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
        sqlx::query(
            r#"
            UPDATE channels
            SET fcm_token = $1, updated_at = NOW()
            WHERE channel_id = ANY($2)
            "#,
        )
        .bind(fcm_token)
        .bind(&active)
        .execute(pool)
        .await?;
    }

    Ok((active, expired))
}
