use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = murmur_daemon::Cli::parse();
    murmur_daemon::run_daemon(cli).await?;
    Ok(())
}
