use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::info;

use autopush_fcm_bridge::{
    config::Config,
    db::{create_pool, run_migrations},
    fcm::FcmClient,
    pool::AutopushPool,
    routes::{create_router, AppState},
};

#[derive(Parser, Debug)]
#[command(name = "autopush-fcm-bridge")]
#[command(about = "Mozilla Autopush to FCM bridge server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run database migrations and exit
    Migrate,
    /// Start the bridge server (default)
    Serve,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,autopush_fcm_bridge=debug".into()),
        )
        .init();

    let cli = Cli::parse();
    let config = Config::from_env().context("Failed to load configuration")?;

    match cli.command.unwrap_or(Commands::Serve) {
        Commands::Migrate => {
            info!("Connecting to PostgreSQL for migration...");
            let pool = create_pool(&config.database_url).await?;
            info!("Running database migrations...");
            run_migrations(&pool).await?;
            info!("Migrations executed successfully.");
        }
        Commands::Serve => {
            info!("Starting autopush-fcm-bridge...");
            let pool = create_pool(&config.database_url).await?;

            info!("Running pending database migrations on startup...");
            run_migrations(&pool).await?;

            let fcm_client = Arc::new(
                FcmClient::new(&config.service_account_json)
                    .context("Failed to initialize FCM client")?,
            );
            info!(
                "Initialized FCM client for project: {}",
                fcm_client.project_id()
            );

            let config_arc = Arc::new(config.clone());
            let autopush_pool = Arc::new(AutopushPool::new(
                Arc::clone(&config_arc),
                pool.clone(),
                Arc::clone(&fcm_client),
            ));

            autopush_pool
                .start()
                .await
                .context("Failed to start Autopush connection pool")?;

            let app_state = AppState {
                pool: Arc::clone(&autopush_pool),
                db: pool.clone(),
            };

            let app = create_router(app_state).layer(TraceLayer::new_for_http());

            let addr = format!("{}:{}", config.host, config.port);
            let listener = tokio::net::TcpListener::bind(&addr)
                .await
                .with_context(|| format!("Failed to bind to {addr}"))?;

            info!("Server listening on http://{addr}");

            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .context("Server error")?;

            autopush_pool.shutdown_all().await;

            info!("Server shut down gracefully.");
        }
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Received termination signal. Shutting down...");
}
