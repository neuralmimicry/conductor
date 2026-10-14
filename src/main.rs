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
#[command(version)]
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
    let app_version = env!("CARGO_PKG_VERSION");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();

    let cli = Cli::parse();
    // Install handlers before startup: PID 1 otherwise ignores SIGTERM's
    // default action, forcing container runtimes to use SIGKILL on upgrades.
    let shutdown = shutdown_signal()?;
    tracing::info!(
        version = app_version,
        config = %cli.config.display(),
        "Conductor starting"
    );
    let config = ConductorConfig::load(&cli.config)?;
    tokio::fs::create_dir_all(&config.storage.root_dir).await?;

    let repository = Arc::new(PostgresRepository::connect(&config.database).await?)
        as Arc<dyn conductor::repository::ConductorRepository>;
    let http = build_http_client(config.discovery.service_timeout_seconds.max(1))?;
    let service = ConductorService::new(config.clone(), repository, http);
    spawn_background_loops(service.clone());

    let router = build_router(service.clone());
    let listener = tokio::net::TcpListener::bind(config.server.bind_addr.as_str()).await?;
    tracing::info!(
        version = app_version,
        bind_addr = %config.server.bind_addr,
        "Conductor listening"
    );
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.await;
            service.begin_shutdown();
        })
        .await?;
    Ok(())
}

#[cfg(unix)]
fn shutdown_signal() -> std::io::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = terminate.recv() => {},
            _ = interrupt.recv() => {},
        }
        tracing::info!("shutdown requested; draining HTTP requests before exit");
    })
}

#[cfg(all(test, unix))]
mod tests {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};

    #[tokio::test]
    async fn signal_child() {
        if std::env::var_os("CONDUCTOR_TEST_SIGNAL_CHILD").is_none() {
            return;
        }
        let shutdown = super::shutdown_signal().expect("register shutdown signals");
        println!("SHUTDOWN_HANDLERS_READY");
        shutdown.await;
    }

    #[tokio::test]
    async fn termination_and_interrupt_exit_normally() {
        for signal in [libc::SIGTERM, libc::SIGINT] {
            let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "tests::signal_child", "--nocapture"])
                .env("CONDUCTOR_TEST_SIGNAL_CHILD", "1")
                .stdout(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    let line = lines
                        .next_line()
                        .await
                        .unwrap()
                        .expect("child stayed alive");
                    if line == "SHUTDOWN_HANDLERS_READY" {
                        break;
                    }
                }
            })
            .await
            .expect("child registered its handlers");
            // Signal only the dedicated child, never the test runner or estate.
            assert_eq!(unsafe { libc::kill(child.id().unwrap() as i32, signal) }, 0);
            let status = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait())
                .await
                .expect("signal handler completed")
                .unwrap();
            assert_eq!(status.code(), Some(0), "signal {signal} must exit normally");
        }
    }
}

#[cfg(not(unix))]
fn shutdown_signal() -> std::io::Result<impl Future<Output = ()>> {
    Ok(async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "cannot receive shutdown signal");
        }
    })
}
