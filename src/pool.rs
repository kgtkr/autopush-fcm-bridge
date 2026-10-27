use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, Mutex, RwLock},
    time::{interval, sleep, timeout},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    config::Config,
    db,
    fcm::{FcmClient, FcmError},
};

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type WsSink = futures_util::stream::SplitSink<WsStream, Message>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutopushIncoming {
    message_type: String,
    uaid: Option<String>,
    #[serde(rename = "channelID")]
    channel_id: Option<String>,
    #[allow(dead_code)]
    status: Option<i32>,
    push_endpoint: Option<String>,
    version: Option<String>,
    data: Option<String>,
    headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HelloMsg<'a> {
    message_type: &'static str,
    use_webpush: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    uaid: Option<&'a str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RegisterMsg<'a> {
    message_type: &'static str,
    #[serde(rename = "channelID")]
    channel_id: &'a str,
    key: &'a str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UnregisterMsg<'a> {
    message_type: &'static str,
    #[serde(rename = "channelID")]
    channel_id: &'a str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AckUpdate<'a> {
    #[serde(rename = "channelID")]
    channel_id: &'a str,
    version: &'a str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AckMsg<'a> {
    message_type: &'static str,
    updates: Vec<AckUpdate<'a>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("Invalid or unregistered FCM token")]
    InvalidFcmToken,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub enum WorkerCommand {
    Register {
        channel_id: Uuid,
        app_server_key: String,
        resp_tx: oneshot::Sender<Result<String, String>>,
    },
    Unregister {
        channel_id: Uuid,
    },
    Shutdown,
}

pub struct AutopushPool {
    config: Arc<Config>,
    db_pool: sqlx::PgPool,
    fcm_client: Arc<FcmClient>,
    workers: Arc<RwLock<HashMap<String, mpsc::Sender<WorkerCommand>>>>,
    alloc_mutex: Mutex<()>,
}

impl AutopushPool {
    pub fn new(config: Arc<Config>, db_pool: sqlx::PgPool, fcm_client: Arc<FcmClient>) -> Self {
        Self {
            config,
            db_pool,
            fcm_client,
            workers: Arc::new(RwLock::new(HashMap::new())),
            alloc_mutex: Mutex::new(()),
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let deleted = db::delete_empty_sessions(&self.db_pool).await?;
        if deleted > 0 {
            info!("Cleaned up {deleted} empty autopush session(s) at startup");
        }

        let sessions = db::get_all_sessions(&self.db_pool).await?;
        info!("Restoring {} existing Autopush session(s)", sessions.len());

        for session in sessions {
            let uaid = session.uaid;
            let (tx, rx) = mpsc::channel(32);
            self.workers.write().await.insert(uaid.clone(), tx);

            let pool = Arc::clone(self);
            let uaid_clone = uaid.clone();
            tokio::spawn(async move {
                pool.run_worker(Some(uaid_clone), rx).await;
            });
        }

        // Spawn token sweeper task
        let pool = Arc::clone(self);
        tokio::spawn(async move {
            pool.run_token_sweeper().await;
        });

        Ok(())
    }

    pub async fn shutdown_all(&self) {
        let mut workers = self.workers.write().await;
        info!(
            "Shutting down {} active Autopush WebSocket worker(s)...",
            workers.len()
        );
        for (_, tx) in workers.drain() {
            let _ = tx.send(WorkerCommand::Shutdown).await;
        }
    }

    pub async fn register_channel(
        self: &Arc<Self>,
        fcm_token: &str,
        app_server_key: &str,
    ) -> Result<(Uuid, String), ChannelError> {
        // 1. Verify FCM token (validate_only: true)
        if let Err(e) = self.fcm_client.validate_token(fcm_token).await {
            return match e {
                FcmError::Unregistered => Err(ChannelError::InvalidFcmToken),
                other => Err(ChannelError::Internal(anyhow!(
                    "FCM token validation failed: {other}"
                ))),
            };
        }

        // Serialize session candidate selection, registration, and channel DB insertion
        // to avoid race conditions with concurrent registrations or empty-session cleanup.
        let _alloc_guard = self.alloc_mutex.lock().await;

        // 2. Find candidate UAID or spawn new one
        let mut target_uaid =
            db::find_candidate_uaid(&self.db_pool, self.config.max_channels_per_uaid).await?;

        let mut worker_tx = None;
        if let Some(ref uaid) = target_uaid {
            let workers = self.workers.read().await;
            if let Some(tx) = workers.get(uaid) {
                if !tx.is_closed() {
                    worker_tx = Some(tx.clone());
                }
            }
        }

        let (uaid, tx) = match (target_uaid.take(), worker_tx) {
            (Some(u), Some(tx)) => (u, tx),
            _ => {
                info!("No existing available UAID found. Creating a new Autopush session...");
                let (new_uaid, tx) = self.spawn_new_worker().await?;
                (new_uaid, tx)
            }
        };

        // 3. Register channel on the worker
        let channel_id = Uuid::new_v4();
        let (resp_tx, resp_rx) = oneshot::channel();

        tx.send(WorkerCommand::Register {
            channel_id,
            app_server_key: app_server_key.to_string(),
            resp_tx,
        })
        .await
        .map_err(|_| anyhow!("Failed to send register command to worker for UAID {uaid}"))?;

        let reg_res = timeout(Duration::from_secs(5), resp_rx)
            .await
            .map_err(|_| anyhow!("Timed out waiting for Autopush registration"))
            .and_then(|r| r.map_err(|_| anyhow!("Worker dropped response channel")))
            .and_then(|r| r.map_err(|err_msg| anyhow!("Autopush registration failed: {err_msg}")));

        let push_endpoint = match reg_res {
            Ok(ep) => ep,
            Err(e) => {
                // If this was a newly created session with 0 channels and registration failed, clean it up
                self.shutdown_worker_if_empty(&uaid).await;
                return Err(ChannelError::Internal(e));
            }
        };

        // 4. Save to DB
        if let Err(e) = db::insert_channel(
            &self.db_pool,
            channel_id,
            &uaid,
            fcm_token,
            app_server_key,
            &push_endpoint,
        )
        .await
        {
            let _ = tx.send(WorkerCommand::Unregister { channel_id }).await;
            self.shutdown_worker_if_empty(&uaid).await;
            return Err(ChannelError::Internal(e));
        }

        info!("Registered channel {channel_id} on UAID {uaid}");

        Ok((channel_id, push_endpoint))
    }

    pub async fn unregister_channel(&self, channel_id: Uuid) -> Result<()> {
        let _alloc_guard = self.alloc_mutex.lock().await;

        let uaid_opt = db::delete_channel(&self.db_pool, channel_id).await?;
        if let Some(uaid) = uaid_opt {
            {
                let workers = self.workers.read().await;
                if let Some(tx) = workers.get(&uaid) {
                    let _ = tx.send(WorkerCommand::Unregister { channel_id }).await;
                }
            }

            // Atomically delete session if 0 channels remain, and shut down worker if deleted
            self.shutdown_worker_if_empty(&uaid).await;
        }
        Ok(())
    }

    pub async fn sync_channels(
        &self,
        fcm_token: &str,
        channel_ids: &[Uuid],
    ) -> Result<(Vec<Uuid>, Vec<Uuid>), ChannelError> {
        // Validate FCM token
        if let Err(e) = self.fcm_client.validate_token(fcm_token).await {
            return match e {
                FcmError::Unregistered => Err(ChannelError::InvalidFcmToken),
                other => Err(ChannelError::Internal(anyhow!(
                    "FCM token validation failed: {other}"
                ))),
            };
        }

        db::sync_channels(&self.db_pool, fcm_token, channel_ids)
            .await
            .map_err(ChannelError::Internal)
    }

    async fn shutdown_worker_if_empty(&self, uaid: &str) {
        if let Ok(true) = db::delete_session_if_empty(&self.db_pool, uaid).await {
            info!("UAID {uaid} has 0 channels remaining. Shutting down worker.");
            let mut workers = self.workers.write().await;
            if let Some(tx) = workers.remove(uaid) {
                let _ = tx.send(WorkerCommand::Shutdown).await;
            }
        }
    }

    async fn shutdown_and_delete_worker(&self, uaid: &str) {
        let mut workers = self.workers.write().await;
        if let Some(tx) = workers.remove(uaid) {
            let _ = tx.send(WorkerCommand::Shutdown).await;
        }
        let _ = db::delete_session(&self.db_pool, uaid).await;
    }

    async fn spawn_new_worker(self: &Arc<Self>) -> Result<(String, mpsc::Sender<WorkerCommand>)> {
        let (uaid_tx, uaid_rx) = oneshot::channel();
        let (cmd_tx, cmd_rx) = mpsc::channel(32);

        let pool = Arc::clone(self);
        tokio::spawn(async move {
            pool.run_new_worker(uaid_tx, cmd_rx).await;
        });

        let new_uaid = timeout(Duration::from_secs(8), uaid_rx)
            .await
            .map_err(|_| anyhow!("Timed out waiting for new UAID from Autopush"))?
            .map_err(|_| anyhow!("Failed to receive new UAID"))??;

        self.workers
            .write()
            .await
            .insert(new_uaid.clone(), cmd_tx.clone());
        Ok((new_uaid, cmd_tx))
    }

    async fn run_new_worker(
        self: Arc<Self>,
        uaid_tx: oneshot::Sender<Result<String>>,
        cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) {
        let ws_url = self.config.autopush_ws_url.clone();
        let req = match ws_url.into_client_request() {
            Ok(r) => r,
            Err(e) => {
                let _ = uaid_tx.send(Err(anyhow!("Invalid WebSocket URL: {e}")));
                return;
            }
        };

        let handshake_res = timeout(Duration::from_secs(5), async {
            let (ws_stream, _) = connect_async(req)
                .await
                .map_err(|e| anyhow!("Failed to connect to Autopush WebSocket: {e}"))?;

            let (mut write, mut read) = ws_stream.split();

            // Send hello without uaid to request a new uaid
            let hello = HelloMsg {
                message_type: "hello",
                use_webpush: true,
                uaid: None,
            };

            let hello_json = serde_json::to_string(&hello).unwrap();
            write
                .send(Message::Text(hello_json))
                .await
                .map_err(|e| anyhow!("Failed to send hello to Autopush: {e}"))?;

            // Wait for hello response
            let mut confirmed_uaid = None;
            while let Some(msg_res) = read.next().await {
                match msg_res {
                    Ok(Message::Text(text)) => {
                        if let Ok(incoming) = serde_json::from_str::<AutopushIncoming>(&text) {
                            if incoming.message_type == "hello" {
                                if let Some(uaid) = incoming.uaid {
                                    confirmed_uaid = Some(uaid);
                                    break;
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Close(_)) => {
                        break;
                    }
                    Err(e) => {
                        return Err(anyhow!("Error reading initial hello from Autopush: {e}"));
                    }
                    _ => {}
                }
            }

            let uaid =
                confirmed_uaid.ok_or_else(|| anyhow!("Did not receive UAID in hello response"))?;
            let ws_stream = write.reunite(read).expect("Reunite ws stream");
            Ok::<(String, WsStream), anyhow::Error>((uaid, ws_stream))
        })
        .await;

        let (uaid, ws_stream) = match handshake_res {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => {
                let _ = uaid_tx.send(Err(e));
                return;
            }
            Err(_) => {
                let _ = uaid_tx.send(Err(anyhow!(
                    "Timed out during initial Autopush hello handshake"
                )));
                return;
            }
        };

        if let Err(e) = db::insert_session(&self.db_pool, &uaid).await {
            let _ = uaid_tx.send(Err(anyhow!("Failed to save new session to DB: {e}")));
            return;
        }

        if uaid_tx.send(Ok(uaid.clone())).is_err() {
            let _ = db::delete_session(&self.db_pool, &uaid).await;
            return;
        }

        // Now run normal worker loop
        self.run_worker_loop(uaid, cmd_rx, Some(ws_stream)).await;
    }

    async fn run_worker(
        self: Arc<Self>,
        initial_uaid: Option<String>,
        cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) {
        let uaid = match initial_uaid {
            Some(u) => u,
            None => return,
        };

        self.run_worker_loop(uaid, cmd_rx, None).await;
    }

    async fn run_worker_loop(
        self: Arc<Self>,
        uaid: String,
        mut cmd_rx: mpsc::Receiver<WorkerCommand>,
        initial_ws: Option<WsStream>,
    ) {
        let mut current_ws = initial_ws;
        let mut retry_delay_secs = 2u64;

        let mut pending_registrations: HashMap<String, oneshot::Sender<Result<String, String>>> =
            HashMap::new();

        'reconnect_loop: loop {
            let ws_stream = match current_ws.take() {
                Some(ws) => ws,
                None => {
                    info!("Connecting Autopush WebSocket for UAID {uaid}...");
                    let req = match self.config.autopush_ws_url.clone().into_client_request() {
                        Ok(r) => r,
                        Err(e) => {
                            error!("Invalid WebSocket URL: {e}");
                            sleep(Duration::from_secs(retry_delay_secs)).await;
                            continue;
                        }
                    };

                    let reconnect_res = timeout(Duration::from_secs(12), async {
                        let (mut ws, _) = connect_async(req).await?;
                        let hello = HelloMsg {
                            message_type: "hello",
                            use_webpush: true,
                            uaid: Some(&uaid),
                        };
                        let hello_json = serde_json::to_string(&hello).unwrap();
                        ws.send(Message::Text(hello_json)).await?;

                        let mut hello_received = false;
                        let mut uaid_changed = false;
                        while let Some(msg_res) = ws.next().await {
                            match msg_res {
                                Ok(Message::Text(text)) => {
                                    if let Ok(incoming) =
                                        serde_json::from_str::<AutopushIncoming>(&text)
                                    {
                                        if incoming.message_type == "hello" {
                                            hello_received = true;
                                            if let Some(ref received_uaid) = incoming.uaid {
                                                if received_uaid != &uaid {
                                                    warn!(
                                                        "UAID expired on Autopush! Old: {uaid}, New: {received_uaid}"
                                                    );
                                                    uaid_changed = true;
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }
                                Ok(Message::Ping(data)) => {
                                    let _ = ws.send(Message::Pong(data)).await;
                                }
                                Ok(Message::Close(_)) => {
                                    break;
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                                _ => {}
                            }
                        }

                        Ok::<(WsStream, bool, bool), tokio_tungstenite::tungstenite::Error>((
                            ws,
                            hello_received,
                            uaid_changed,
                        ))
                    })
                    .await;

                    match reconnect_res {
                        Ok(Ok((ws, hello_received, uaid_changed))) => {
                            if uaid_changed {
                                // UAID has expired on Autopush side
                                self.handle_uaid_expired(&uaid).await;
                                break 'reconnect_loop;
                            }
                            if !hello_received {
                                warn!("Connection closed before receiving hello for {uaid}. Retrying in {retry_delay_secs}s...");
                                sleep(Duration::from_secs(retry_delay_secs)).await;
                                retry_delay_secs = (retry_delay_secs * 2).min(60);
                                continue;
                            }

                            let _ = db::touch_session(&self.db_pool, &uaid).await;
                            retry_delay_secs = 2;
                            ws
                        }
                        Ok(Err(e)) => {
                            warn!("WebSocket connection failure for {uaid}: {e}. Retrying in {retry_delay_secs}s...");
                            sleep(Duration::from_secs(retry_delay_secs)).await;
                            retry_delay_secs = (retry_delay_secs * 2).min(60);
                            continue;
                        }
                        Err(_) => {
                            warn!("Timed out connecting/handshaking WebSocket for {uaid}. Retrying in {retry_delay_secs}s...");
                            sleep(Duration::from_secs(retry_delay_secs)).await;
                            retry_delay_secs = (retry_delay_secs * 2).min(60);
                            continue;
                        }
                    }
                }
            };

            let (mut ws_write, mut ws_read) = ws_stream.split();
            let mut ping_timer = interval(Duration::from_secs(self.config.ws_ping_interval_secs));
            ping_timer.tick().await; // skip initial immediate tick

            'msg_loop: loop {
                tokio::select! {
                    _ = ping_timer.tick() => {
                        if let Err(e) = ws_write.send(Message::Ping(vec![])).await {
                            warn!("Failed to send ping for UAID {uaid}: {e}");
                            break 'msg_loop;
                        }
                    }

                    ws_msg = ws_read.next() => {
                        match ws_msg {
                            Some(Ok(Message::Text(text))) => {
                                match serde_json::from_str::<AutopushIncoming>(&text) {
                                    Ok(incoming) => {
                                        match incoming.message_type.as_str() {
                                            "register" => {
                                                if let Some(cid) = incoming.channel_id {
                                                    if let Some(tx) = pending_registrations.remove(&cid) {
                                                        if let Some(endpoint) = incoming.push_endpoint {
                                                            let _ = tx.send(Ok(endpoint));
                                                        } else {
                                                            let _ = tx.send(Err("No pushEndpoint returned".to_string()));
                                                        }
                                                    }
                                                }
                                            }
                                            "notification" => {
                                                self.handle_incoming_notification(&uaid, incoming, &mut ws_write).await;
                                            }
                                            "ack" => {}
                                            other => {
                                                info!("Received Autopush message type '{other}' for UAID {uaid}");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Failed to parse incoming Autopush JSON: {e}");
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(data))) => {
                                if let Err(e) = ws_write.send(Message::Pong(data)).await {
                                    warn!("Failed to respond to Ping for UAID {uaid}: {e}");
                                    break 'msg_loop;
                                }
                            }
                            Some(Ok(Message::Close(frame))) => {
                                info!("WebSocket closed by server for {uaid}: {frame:?}");
                                break 'msg_loop;
                            }
                            Some(Err(e)) => {
                                warn!("WebSocket error for {uaid}: {e}");
                                break 'msg_loop;
                            }
                            None => {
                                info!("WebSocket stream ended for {uaid}");
                                break 'msg_loop;
                            }
                            _ => {}
                        }
                    }

                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(WorkerCommand::Register { channel_id, app_server_key, resp_tx }) => {
                                let cid_str = channel_id.to_string();
                                pending_registrations.insert(cid_str.clone(), resp_tx);

                                let reg = RegisterMsg {
                                    message_type: "register",
                                    channel_id: &cid_str,
                                    key: &app_server_key,
                                };
                                let reg_json = serde_json::to_string(&reg).unwrap();
                                if let Err(e) = ws_write.send(Message::Text(reg_json)).await {
                                    warn!("Failed to send register message for channel {channel_id}: {e}");
                                    if let Some(tx) = pending_registrations.remove(&cid_str) {
                                        let _ = tx.send(Err(e.to_string()));
                                    }
                                    break 'msg_loop;
                                }
                            }
                            Some(WorkerCommand::Unregister { channel_id }) => {
                                let cid_str = channel_id.to_string();
                                pending_registrations.remove(&cid_str);

                                let unreg = UnregisterMsg {
                                    message_type: "unregister",
                                    channel_id: &cid_str,
                                };
                                let unreg_json = serde_json::to_string(&unreg).unwrap();
                                let _ = ws_write.send(Message::Text(unreg_json)).await;
                            }
                            Some(WorkerCommand::Shutdown) | None => {
                                info!("Worker for UAID {uaid} received shutdown signal");
                                let _ = ws_write.send(Message::Close(None)).await;
                                break 'reconnect_loop;
                            }
                        }
                    }
                }
            }

            // Fail any in-flight registrations immediately on disconnect rather than waiting for timeout
            for (_, tx) in pending_registrations.drain() {
                let _ = tx.send(Err("WebSocket disconnected during registration".to_string()));
            }

            // Msg loop broke: wait and reconnect
            sleep(Duration::from_secs(retry_delay_secs)).await;
            retry_delay_secs = (retry_delay_secs * 2).min(60);
        }

        // Clean up pending registrations on exit
        for (_, tx) in pending_registrations {
            let _ = tx.send(Err("Worker exited".to_string()));
        }
        info!("Worker for UAID {uaid} finished");
    }

    async fn handle_incoming_notification(
        &self,
        uaid: &str,
        msg: AutopushIncoming,
        ws_write: &mut WsSink,
    ) {
        let channel_id_str = match msg.channel_id {
            Some(ref id) => id.clone(),
            None => return,
        };

        let channel_uuid = match Uuid::parse_str(&channel_id_str) {
            Ok(uuid) => uuid,
            Err(_) => return,
        };

        let channel = match db::get_channel(&self.db_pool, channel_uuid).await {
            Ok(Some(ch)) => ch,
            Ok(None) => {
                warn!("Received notification for unknown channel {channel_id_str}");
                // Send unregister and ack
                if let Some(ref version) = msg.version {
                    let ack = AckMsg {
                        message_type: "ack",
                        updates: vec![AckUpdate {
                            channel_id: &channel_id_str,
                            version,
                        }],
                    };
                    if let Ok(ack_json) = serde_json::to_string(&ack) {
                        let _ = ws_write.send(Message::Text(ack_json)).await;
                    }
                }
                let unreg = UnregisterMsg {
                    message_type: "unregister",
                    channel_id: &channel_id_str,
                };
                if let Ok(unreg_json) = serde_json::to_string(&unreg) {
                    let _ = ws_write.send(Message::Text(unreg_json)).await;
                }
                return;
            }
            Err(e) => {
                error!("Database error looking up channel {channel_id_str}: {e}");
                return;
            }
        };

        info!("Relaying notification for channel {channel_uuid} to FCM...");

        let send_res = self
            .fcm_client
            .send_notification(
                &channel.fcm_token,
                channel_uuid,
                msg.version.as_deref(),
                msg.data.as_deref(),
                msg.headers.as_ref(),
            )
            .await;

        match send_res {
            Ok(()) => {
                info!("FCM notification sent successfully for channel {channel_uuid}. Sending ACK to Autopush.");
                if let Some(ref version) = msg.version {
                    let ack = AckMsg {
                        message_type: "ack",
                        updates: vec![AckUpdate {
                            channel_id: &channel_id_str,
                            version,
                        }],
                    };
                    if let Ok(ack_json) = serde_json::to_string(&ack) {
                        let _ = ws_write.send(Message::Text(ack_json)).await;
                    }
                }
            }
            Err(FcmError::Unregistered) => {
                warn!("FCM token for channel {channel_uuid} is unregistered. Cleaning up channel.");
                // Send ACK and unregister
                if let Some(ref version) = msg.version {
                    let ack = AckMsg {
                        message_type: "ack",
                        updates: vec![AckUpdate {
                            channel_id: &channel_id_str,
                            version,
                        }],
                    };
                    if let Ok(ack_json) = serde_json::to_string(&ack) {
                        let _ = ws_write.send(Message::Text(ack_json)).await;
                    }
                }
                let unreg = UnregisterMsg {
                    message_type: "unregister",
                    channel_id: &channel_id_str,
                };
                if let Ok(unreg_json) = serde_json::to_string(&unreg) {
                    let _ = ws_write.send(Message::Text(unreg_json)).await;
                }

                // Delete channel and clean up session if empty while holding alloc_mutex
                let _alloc_guard = self.alloc_mutex.lock().await;
                let _ = db::delete_channel(&self.db_pool, channel_uuid).await;
                self.shutdown_worker_if_empty(uaid).await;
            }
            Err(e) => {
                error!("Failed to relay notification to FCM for channel {channel_uuid}: {e}");
            }
        }
    }

    async fn handle_uaid_expired(&self, uaid: &str) {
        info!("Handling expired UAID {uaid}: notifying channels and cleaning up DB");
        if let Ok(channels) = db::get_channels_by_uaid(&self.db_pool, uaid).await {
            for ch in channels {
                info!(
                    "Sending channel_expired message to FCM token for channel {}",
                    ch.channel_id
                );
                let _ = self
                    .fcm_client
                    .send_channel_expired(&ch.fcm_token, ch.channel_id)
                    .await;
            }
        }

        let _alloc_guard = self.alloc_mutex.lock().await;
        self.shutdown_and_delete_worker(uaid).await;
    }

    async fn run_token_sweeper(self: Arc<Self>) {
        info!(
            "Starting FCM token sweeper task (interval: {}s)",
            self.config.token_sweep_interval_secs
        );

        let mut sweep_timer = interval(Duration::from_secs(self.config.token_sweep_interval_secs));
        sweep_timer.tick().await; // skip initial immediate tick

        loop {
            sweep_timer.tick().await;
            info!("Running periodic FCM token validation sweep...");

            let tokens = match db::get_distinct_fcm_tokens(&self.db_pool).await {
                Ok(t) => t,
                Err(e) => {
                    error!("Sweeper failed to fetch FCM tokens from DB: {e}");
                    continue;
                }
            };

            for token in tokens {
                match self.fcm_client.validate_token(&token).await {
                    Ok(()) => {
                        // Token is still valid
                    }
                    Err(FcmError::Unregistered) => {
                        warn!("Sweeper found unregistered FCM token. Cleaning up associated channels...");
                        if let Ok(channels) =
                            db::get_channels_by_fcm_token(&self.db_pool, &token).await
                        {
                            for ch in channels {
                                let _ = self.unregister_channel(ch.channel_id).await;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Sweeper validation check error for token: {e}");
                    }
                }
            }
            info!("Periodic FCM token validation sweep completed");
        }
    }
}
