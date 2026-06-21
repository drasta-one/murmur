use clap::{Parser, Subcommand};
use tokio_stream::StreamExt;
use murmur_api::{MurmurConfig, DorRuntime, MurmurCommand, MurmurEvent, ErrorCode};
use murmur_core::types::ManifestId;

#[derive(Parser)]
#[command(name = "murmur-cli")]
#[command(about = "CLI to interact with the local murmur daemon")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, default_value = "127.0.0.1:9090")]
    addr: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Get the status of the local node
    Status,
    /// Seed a file into the network
    Seed { file_path: String },
    /// List available manifests (seeded or known files)
    List,
    /// Get (reassemble) a file from the network
    Get { manifest_id: String, out_path: String },
    /// Fetch a URL across multiple WAN connections (Bonded Download)
    BondedFetch { url: String, out_path: String },
    /// Check the progress of a file transfer
    Progress { manifest_id: String },
    /// Stop the local daemon
    Stop,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    
    let cli = Cli::parse();
    
    let config = MurmurConfig {
        daemon_addr: cli.addr.clone(),
    };
    
    let mut runtime = DorRuntime::new(config)?;
    runtime.start().await?;
    
    let cmd_tx = runtime.commands();
    let mut events = runtime.events();

    match cli.command {
        Commands::Status => {
            cmd_tx.send(MurmurCommand::Status).await?;
            while let Some(event) = events.next().await {
                match event {
                    MurmurEvent::StatusReport { node_id, active_peers, is_coordinator } => {
                        println!("Status: Node {} (Coordinator: {}), {} active peers", node_id.0, is_coordinator, active_peers);
                        break;
                    }
                    MurmurEvent::Error { message, .. } => {
                        println!("Error: {}", message);
                        break;
                    }
                    _ => {}
                }
            }
        }
        Commands::Seed { file_path } => {
            cmd_tx.send(MurmurCommand::Seed { file_path }).await?;
            while let Some(event) = events.next().await {
                match event {
                    MurmurEvent::CommandSuccess { message } => {
                        println!("Success: {}", message);
                        break;
                    }
                    MurmurEvent::Error { message, .. } => {
                        println!("Error: {}", message);
                        break;
                    }
                    _ => {}
                }
            }
        }
        Commands::List => {
            cmd_tx.send(MurmurCommand::ListManifests).await?;
            while let Some(event) = events.next().await {
                match event {
                    MurmurEvent::ManifestList { manifests } => {
                        println!("Manifests:");
                        for (id, name) in manifests {
                            println!("  {} - {}", id, name);
                        }
                        break;
                    }
                    MurmurEvent::Error { message, .. } => {
                        println!("Error: {}", message);
                        break;
                    }
                    _ => {}
                }
            }
        }
        Commands::Get { manifest_id, out_path } => {
            let manifest_id = match uuid::Uuid::parse_str(&manifest_id) {
                Ok(id) => murmur_core::types::ManifestId(id),
                Err(_) => {
                    println!("Error: Invalid manifest ID format");
                    return Ok(());
                }
            };
            
            cmd_tx.send(MurmurCommand::StartDownload { url: out_path, manifest_id }).await?;
            
            let mut downloading = false;
            
            while let Some(event) = events.next().await {
                match event {
                    MurmurEvent::CommandSuccess { message } => {
                        println!("{}", message);
                        downloading = true;
                    }
                    MurmurEvent::TransferProgress { percentage, is_complete, .. } => {
                        if is_complete {
                            println!("Download 100% complete.");
                            break;
                        } else {
                            println!("Progress: {:.2}%", percentage);
                        }
                    }
                    MurmurEvent::Error { message, .. } => {
                        println!("Error: {}", message);
                        break;
                    }
                    _ => {}
                }
            }
        }
        Commands::BondedFetch { url, out_path } => {
            cmd_tx.send(MurmurCommand::BondedFetch { url, output_path: out_path }).await?;
            
            let mut downloading = false;
            
            while let Some(event) = events.next().await {
                match event {
                    MurmurEvent::CommandSuccess { message } => {
                        println!("{}", message);
                        downloading = true;
                    }
                    MurmurEvent::BondedFetchProgress { percentage, is_complete, combined_bps, .. } => {
                        if is_complete {
                            println!("Bonded Download 100% complete.");
                            break;
                        } else {
                            let mbps = combined_bps as f64 / 125_000.0;
                            println!("Progress: {:.2}% ({:.2} Mbps combined)", percentage, mbps);
                        }
                    }
                    MurmurEvent::Error { message, .. } => {
                        println!("Error: {}", message);
                        break;
                    }
                    _ => {}
                }
            }
        }
        Commands::Progress { manifest_id } => {
            // Note: The CLI could just connect, get the current progress, and exit.
            // Wait, StartDownload starts the loop for Progress in the server.
            // If the user manually asks for Progress, maybe they want a single snapshot?
            // Currently, `dor-daemon/src/rpc.rs` does not have a `Progress` command, 
            // since `StartDownload` auto-spawns a progress reporter. 
            // But we can just use `StartDownload` logic or print a message.
            println!("Use `murmur-cli get <id>` to track progress directly. Background progress tracking is handled automatically.");
        }
        Commands::Stop => {
            cmd_tx.send(MurmurCommand::Stop).await?;
            while let Some(event) = events.next().await {
                match event {
                    MurmurEvent::CommandSuccess { message } => {
                        println!("Success: {}", message);
                        break;
                    }
                    MurmurEvent::Error { message, .. } => {
                        println!("Error: {}", message);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    
    runtime.shutdown().await;
    Ok(())
}
