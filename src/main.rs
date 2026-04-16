use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use conductor::{
    app::build_router,
    config::ConductorConfig,
    integrations::build_http_client,
    service::{ConductorService, spawn_background_loops},
    storage::postgres::PostgresRepository,
};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(name = "conductor")]
#[command(about = "AI conductor for the NeuralMimicry Continuum stack")]
struct Cli {
    #[arg(
        long,
        env = "CONDUCTOR_CONFIG",
        default_value = "config/conductor.yaml"
    )]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();

    let cli = Cli::parse();
    let config = ConductorConfig::load(&cli.config)?;
    tokio::fs::create_dir_all(&config.storage.root_dir).await?;

    let repository = Arc::new(PostgresRepository::connect(&config.database).await?)
        as Arc<dyn conductor::repository::ConductorRepository>;
    let http = build_http_client(config.discovery.service_timeout_seconds.max(1))?;
    let service = ConductorService::new(config.clone(), repository, http);
    spawn_background_loops(service.clone());

    let router = build_router(service);
    let listener = tokio::net::TcpListener::bind(config.server.bind_addr.as_str()).await?;
    tracing::info!(bind_addr = %config.server.bind_addr, "Conductor listening");
    axum::serve(listener, router).await?;
    Ok(())
}
