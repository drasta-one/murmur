pub mod bonded_download;
mod event_loop;
mod grpc;
mod p2p;
pub mod proxy_orchestrator;
pub mod socks5;
mod state;
pub mod transfer;
pub mod url_manifest;
pub mod wan_fetch;

use clap::Parser;
use murmur_core::types::NodeId;
use state::NodeState;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "murmur-daemon")]
#[command(about = "The DOR cooperative network node daemon")]
pub struct Cli {
    /// Port for the local RPC server
    #[arg(short, long, default_value = "9090")]
    pub rpc_port: u16,

    /// Directory to store downloaded chunks
    #[arg(short, long, default_value = "./.murmur-data")]
    pub storage_dir: std::path::PathBuf,

    /// Run as a malicious liar node (for testing)
    #[arg(long)]
    pub malicious: bool,

    /// Run as a Slow Loris node (for testing)
    #[arg(long)]
    pub slow_loris: bool,

    /// Disable mDNS discovery
    #[arg(long)]
    pub no_mdns: bool,

    /// Optional peer to connect to on startup (e.g. 127.0.0.1:12345)
    #[arg(short, long)]
    pub peer: Vec<String>,

    /// Optional P2P port to bind to (defaults to 0 for random)
    #[arg(long, default_value = "0")]
    pub p2p_port: u16,

    /// Simulated WAN bandwidth in Mbps (for bonding logic)
    #[arg(long, default_value = "50")]
    pub wan_bandwidth: u32,

    /// SOCKS5 Proxy port
    #[arg(long, default_value = "1080")]
    pub socks5_port: u16,
}

pub async fn run_daemon(cli: Cli) -> anyhow::Result<()> {
    // Generate a random Node ID for this daemon
    let node_id = NodeId(rand::random::<u64>());
    info!("Starting murmur-daemon with Node ID {}", node_id.0);

    // cli.wan_bandwidth is in Mbps, convert to bps
    let wan_bps = cli.wan_bandwidth as u64 * 125_000;
    let state = Arc::new(
        NodeState::new(
            node_id,
            cli.storage_dir,
            cli.malicious,
            cli.slow_loris,
            wan_bps,
        )
        .await?,
    );

    // Start the P2P networking listener
    let p2p_listener = TcpListener::bind(format!("0.0.0.0:{}", cli.p2p_port)).await?;
    let actual_p2p_port = p2p_listener.local_addr()?.port();

    info!("P2P server listening on 0.0.0.0:{}", actual_p2p_port);

    // Update local node config and add to overlay
    {
        let mut overlay = state.overlay.write().await;
        let mut node = murmur_core::node::Node::new(
            node_id,
            murmur_core::node::NodeConfig::default(),
            murmur_core::types::SimTime::ZERO,
        );
        node.config.wan_bandwidth = cli.wan_bandwidth as u64;
        node.activate();
        let _ = overlay.add_node(node);
    }

    // Start Experimental QUIC endpoint
    let quic_addr: std::net::SocketAddr = format!(
        "0.0.0.0:{}",
        if cli.p2p_port == 0 {
            0
        } else {
            cli.p2p_port + 1000
        }
    )
    .parse()?;
    let quic_endpoint = murmur_net::quic::make_quic_endpoint(quic_addr)?;
    info!("QUIC endpoint bound to {}", quic_endpoint.local_addr()?);
    let _quic_endpoint_clone = quic_endpoint.clone();

    let state_quic_accept = state.clone();
    tokio::spawn(async move {
        while let Some(incoming) = quic_endpoint.accept().await {
            let _state_conn = state_quic_accept.clone();
            tokio::spawn(async move {
                if let Ok(connection) = incoming.await {
                    info!(
                        "Accepted QUIC connection from {}",
                        connection.remote_address()
                    );
                    if let Ok((send, recv)) = connection.accept_bi().await {
                        // TODO: Implement QUIC handshake & multiplexing similar to TCP
                        let _conn = Arc::new(murmur_net::PeerConnection::new_quic(0, send, recv));
                        // For now we just log it
                        info!(
                            "QUIC streams opened, experimental multiplexing not fully integrated into overlay yet"
                        );
                    }
                }
            });
        }
    });

    // Start mDNS Discovery (if not disabled)
    let (peer_rx, _discovery) = if !cli.no_mdns {
        let discovery = murmur_net::Discovery::new(node_id.0)?;
        discovery.start_broadcasting(actual_p2p_port)?;
        let peer_rx = discovery.start_browsing()?;
        (Some(peer_rx), Some(discovery))
    } else {
        (None, None)
    };
    if !cli.peer.is_empty() {
        for peer_addr in &cli.peer {
            info!("Connecting to bootstrap peer: {}", peer_addr);
            let state_boot = state.clone();
            let peer_addr_clone = peer_addr.clone();
            tokio::spawn(async move {
                loop {
                    if let Ok(stream) = tokio::net::TcpStream::connect(&peer_addr_clone).await {
                        info!("Connected to bootstrap peer {}", peer_addr_clone);
                        let conn = Arc::new(murmur_net::PeerConnection::new_tcp(0, stream));
                        let msg = murmur_core::net::NetMessage::Handshake {
                            node_id: state_boot.node_id,
                        };
                        let _ = conn.send_message(&msg).await;

                        let mut rx = conn.start_recv_loop().await;
                        let state_conn_rx = state_boot.clone();

                        let mut peer_node_id = NodeId(0);
                        while let Some(msg) = rx.recv().await {
                            if let murmur_core::net::NetMessage::Handshake { node_id } = &msg {
                                // Phase 5: Anti-Malicious Peer Banning
                                if state_conn_rx.banned_peers.read().await.contains(node_id) {
                                    tracing::warn!(
                                        "Dropping dialer connection to BANNED peer {}",
                                        node_id.0
                                    );
                                    break; // Break the rx loop, closing connection
                                }

                                peer_node_id = *node_id;
                                state_conn_rx
                                    .connections
                                    .write()
                                    .await
                                    .insert(*node_id, conn.clone());
                                let sim_time = murmur_core::types::SimTime::ZERO;
                                let mut node = murmur_core::node::Node::new(
                                    *node_id,
                                    murmur_core::node::NodeConfig::default(),
                                    sim_time,
                                );
                                node.activate();
                                let _ = state_conn_rx.overlay.write().await.add_node(node);

                                // Phase 4: Send Bitfields for manifests we have
                                {
                                    let manifests = state_conn_rx.manifests.read().await;
                                    for manifest in manifests.values() {
                                        let available = state_conn_rx
                                            .storage
                                            .get_available_chunks(manifest)
                                            .await;
                                        if !available.is_empty()
                                            && let Some(c) = state_conn_rx
                                                .connections
                                                .read()
                                                .await
                                                .get(&peer_node_id)
                                        {
                                            let _ = c
                                                .send_message(
                                                    &murmur_core::net::NetMessage::Bitfield {
                                                        manifest_id: manifest.id,
                                                        chunks: available,
                                                    },
                                                )
                                                .await;
                                        }
                                    }
                                }
                            }
                            p2p::handle_net_message(&state_conn_rx, peer_node_id, msg).await;

                            // Phase 5: Check if the connection was explicitly removed (e.g. banned)
                            if peer_node_id.0 != 0
                                && !state_conn_rx
                                    .connections
                                    .read()
                                    .await
                                    .contains_key(&peer_node_id)
                            {
                                tracing::info!(
                                    "Connection to {} was removed from state, breaking rx loop",
                                    peer_node_id.0
                                );
                                break;
                            }
                        }

                        if peer_node_id.0 != 0 {
                            state_conn_rx
                                .connections
                                .write()
                                .await
                                .remove(&peer_node_id);
                            state_conn_rx
                                .overlay
                                .write()
                                .await
                                .remove_node(peer_node_id);
                            let reassigned = state_conn_rx
                                .tracker
                                .write()
                                .await
                                .handle_node_disconnect(peer_node_id);

                            // Immediately re-request reassigned chunks from ANY other active node
                            for (manifest_id, chunk_id) in reassigned {
                                let active = state_conn_rx.overlay.read().await.active_nodes();
                                if let Some(target) = active.into_iter().next()
                                    && let Some(c) =
                                        state_conn_rx.connections.read().await.get(&target)
                                {
                                    let _ = c
                                        .send_message(&murmur_core::net::NetMessage::RequestChunk {
                                            manifest_id,
                                            chunk_id,
                                        })
                                        .await;
                                    state_conn_rx.tracker.write().await.mark_chunk_in_flight(
                                        manifest_id,
                                        chunk_id,
                                        target,
                                    );
                                }
                            }
                        }
                    }

                    // If connection fails or drops, wait 1 second before retrying
                    info!(
                        "Lost or failed to connect to {}. Retrying in 1s...",
                        peer_addr_clone
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            });
        }
    }

    // Spawn task to handle discovered peers
    if let Some(mut rx) = peer_rx {
        let state_peer_tx = state.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match event {
                    murmur_net::PeerEvent::Discovered(peer) => {
                        let peer_node_id = NodeId(peer.node_id);

                        // Phase 5: Anti-Malicious Peer Banning
                        if state_peer_tx
                            .banned_peers
                            .read()
                            .await
                            .contains(&peer_node_id)
                        {
                            tracing::warn!(
                                "Ignoring mDNS discovery for BANNED peer {}",
                                peer.node_id
                            );
                            continue;
                        }

                        info!(
                            "Discovered peer {} at {}:{}",
                            peer.node_id, peer.ip, peer.port
                        );
                        let sim_time = murmur_core::types::SimTime::ZERO;
                        let mut node = murmur_core::node::Node::new(
                            peer_node_id,
                            murmur_core::node::NodeConfig::default(),
                            sim_time,
                        );
                        node.activate();
                        let _ = state_peer_tx.overlay.write().await.add_node(node);

                        // To avoid double connections, only the higher node ID connects
                        if state_peer_tx.node_id.0 > peer.node_id {
                            let addr = format!("{}:{}", peer.ip, peer.port);
                            match tokio::net::TcpStream::connect(&addr).await {
                                Ok(stream) => {
                                    info!("Connected to peer {}", peer.node_id);
                                    let conn = Arc::new(murmur_net::PeerConnection::new_tcp(
                                        peer.node_id,
                                        stream,
                                    ));
                                    state_peer_tx
                                        .connections
                                        .write()
                                        .await
                                        .insert(NodeId(peer.node_id), conn.clone());

                                    // Send handshake
                                    let msg = murmur_core::net::NetMessage::Handshake {
                                        node_id: state_peer_tx.node_id,
                                    };
                                    if let Err(e) = conn.send_message(&msg).await {
                                        error!(
                                            "Failed to send handshake to {}: {}",
                                            peer.node_id, e
                                        );
                                    }

                                    // Phase 4: Send Bitfields for manifests we have
                                    {
                                        let manifests = state_peer_tx.manifests.read().await;
                                        for manifest in manifests.values() {
                                            let available = state_peer_tx
                                                .storage
                                                .get_available_chunks(manifest)
                                                .await;
                                            if !available.is_empty()
                                                && let Some(c) = state_peer_tx
                                                    .connections
                                                    .read()
                                                    .await
                                                    .get(&NodeId(peer.node_id))
                                            {
                                                let _ = c
                                                    .send_message(
                                                        &murmur_core::net::NetMessage::Bitfield {
                                                            manifest_id: manifest.id,
                                                            chunks: available,
                                                        },
                                                    )
                                                    .await;
                                            }
                                        }
                                    }

                                    // Spawn receive loop
                                    let mut rx = conn.start_recv_loop().await;
                                    let state_conn_rx = state_peer_tx.clone();
                                    let p_node_id = NodeId(peer.node_id);
                                    tokio::spawn(async move {
                                        while let Some(msg) = rx.recv().await {
                                            p2p::handle_net_message(&state_conn_rx, p_node_id, msg)
                                                .await;
                                        }
                                        info!("Connection lost with Node {}", p_node_id.0);
                                        state_conn_rx.connections.write().await.remove(&p_node_id);
                                        state_conn_rx.overlay.write().await.remove_node(p_node_id);

                                        // Phase 5.3: Reassign timed-out / disconnected chunks
                                        let reassigned = state_conn_rx
                                            .tracker
                                            .write()
                                            .await
                                            .handle_node_disconnect(p_node_id);
                                        for (manifest_id, chunk_id) in reassigned {
                                            let holders = {
                                                let m = state_conn_rx.manifest_holders.read().await;
                                                m.get(&manifest_id).cloned().unwrap_or_default()
                                            };
                                            let mut new_target = holders
                                                .iter()
                                                .find(|&&id| id != p_node_id)
                                                .copied();
                                            if new_target.is_none() {
                                                // Fallback: pick any active node from the overlay
                                                let active = state_conn_rx
                                                    .overlay
                                                    .read()
                                                    .await
                                                    .active_nodes();
                                                new_target =
                                                    active.into_iter().find(|&id| id != p_node_id);
                                            }

                                            if let Some(target) = new_target
                                                && let Some(conn) = state_conn_rx
                                                    .connections
                                                    .read()
                                                    .await
                                                    .get(&target)
                                            {
                                                let _ = conn.send_message(&murmur_core::net::NetMessage::RequestChunk { manifest_id, chunk_id }).await;
                                                state_conn_rx
                                                    .tracker
                                                    .write()
                                                    .await
                                                    .mark_chunk_in_flight(
                                                        manifest_id,
                                                        chunk_id,
                                                        target,
                                                    );
                                            }
                                        }
                                    });
                                }
                                Err(e) => error!("Failed to connect to {}: {}", addr, e),
                            }
                        }
                    }
                    murmur_net::PeerEvent::Lost(id) => {
                        info!("Lost peer {}", id);
                        let p_node_id = NodeId(id);
                        state_peer_tx.connections.write().await.remove(&p_node_id);
                        state_peer_tx.overlay.write().await.remove_node(p_node_id);

                        let reassigned = state_peer_tx
                            .tracker
                            .write()
                            .await
                            .handle_node_disconnect(p_node_id);
                        for (manifest_id, chunk_id) in reassigned {
                            let holders = {
                                let m = state_peer_tx.manifest_holders.read().await;
                                m.get(&manifest_id).cloned().unwrap_or_default()
                            };
                            let mut new_target =
                                holders.iter().find(|&&id| id != p_node_id).copied();
                            if new_target.is_none() {
                                let active = state_peer_tx.overlay.read().await.active_nodes();
                                new_target = active.into_iter().find(|&id| id != p_node_id);
                            }

                            if let Some(target) = new_target
                                && let Some(conn) =
                                    state_peer_tx.connections.read().await.get(&target)
                            {
                                let _ = conn
                                    .send_message(&murmur_core::net::NetMessage::RequestChunk {
                                        manifest_id,
                                        chunk_id,
                                    })
                                    .await;
                                state_peer_tx.tracker.write().await.mark_chunk_in_flight(
                                    manifest_id,
                                    chunk_id,
                                    target,
                                );
                            }
                        } // end for
                    } // end PeerEvent::Lost
                } // end match event
            } // end while loop
        });
    }
    // Spawn task to accept incoming P2P connections
    let state_accept = state.clone();
    tokio::spawn(async move {
        loop {
            match p2p_listener.accept().await {
                Ok((mut socket, addr)) => {
                    info!("Accepted P2P connection from {}", addr);
                    let state_conn = state_accept.clone();
                    tokio::spawn(async move {
                        // Wait for Handshake
                        let mut buf = vec![0u8; 1024];
                        use tokio::io::AsyncReadExt;
                        if let Ok(n) = socket.read(&mut buf).await {
                            let mut bytes_mut = bytes::BytesMut::from(&buf[..n]);
                            if let Ok(Some(murmur_core::net::NetMessage::Handshake { node_id })) =
                                murmur_net::framing::PostcardCodec::decode::<
                                    murmur_core::net::NetMessage,
                                >(&mut bytes_mut)
                            {
                                // Phase 5: Anti-Malicious Peer Banning
                                if state_conn.banned_peers.read().await.contains(&node_id) {
                                    tracing::warn!(
                                        "Rejecting incoming connection from BANNED peer {}",
                                        node_id.0
                                    );
                                    // Connection is dropped when `socket` goes out of scope.
                                    return;
                                }

                                info!("Received Handshake from Node {}", node_id.0);
                                let conn = Arc::new(murmur_net::PeerConnection::new_tcp(
                                    node_id.0, socket,
                                ));
                                state_conn
                                    .connections
                                    .write()
                                    .await
                                    .insert(node_id, conn.clone());

                                let sim_time = murmur_core::types::SimTime::ZERO;
                                let mut node = murmur_core::node::Node::new(
                                    node_id,
                                    murmur_core::node::NodeConfig::default(),
                                    sim_time,
                                );
                                node.activate();
                                let _ = state_conn.overlay.write().await.add_node(node);

                                // Send our handshake back
                                let msg = murmur_core::net::NetMessage::Handshake {
                                    node_id: state_conn.node_id,
                                };
                                let _ = conn.send_message(&msg).await;

                                // Phase 4: Send Bitfields for manifests we have
                                {
                                    let manifests = state_conn.manifests.read().await;
                                    for manifest in manifests.values() {
                                        let available =
                                            state_conn.storage.get_available_chunks(manifest).await;
                                        if !available.is_empty()
                                            && let Some(c) =
                                                state_conn.connections.read().await.get(&node_id)
                                        {
                                            let _ = c
                                                .send_message(
                                                    &murmur_core::net::NetMessage::Bitfield {
                                                        manifest_id: manifest.id,
                                                        chunks: available,
                                                    },
                                                )
                                                .await;
                                        }
                                    }
                                }

                                // Spawn receive loop
                                let mut rx = conn.start_recv_loop().await;
                                while let Some(msg) = rx.recv().await {
                                    p2p::handle_net_message(&state_conn, node_id, msg).await;

                                    // Phase 5: Check if the connection was explicitly removed (e.g. banned)
                                    if !state_conn.connections.read().await.contains_key(&node_id) {
                                        tracing::info!(
                                            "Connection to {} was removed from state, breaking rx loop",
                                            node_id.0
                                        );
                                        break;
                                    }
                                }
                                info!("Connection lost with Node {}", node_id.0);
                                state_conn.connections.write().await.remove(&node_id);
                                state_conn.overlay.write().await.remove_node(node_id);
                            }
                        }
                    });
                }
                Err(e) => error!("Failed to accept P2P connection: {}", e),
            }
        }
    });

    // Spawn SOCKS5 Server
    let state_socks = state.clone();
    let socks5_port = cli.socks5_port;
    let socks5 = socks5::Socks5Server::new(socks5_port, state_socks.proxy_orchestrator.clone());
    tokio::spawn(async move {
        if let Err(e) = socks5.run().await {
            error!("SOCKS5 Server error: {}", e);
        }
    });

    // Event loop that drives the coordinator and heartbeats
    let state_loop = state.clone();
    tokio::spawn(event_loop::run_event_loop(state_loop));

    use murmur_proto::control::control_plane_server::ControlPlaneServer;

    let grpc_addr = format!("0.0.0.0:{}", cli.rpc_port).parse()?;
    info!("gRPC Control Plane listening on {}", grpc_addr);

    let control_service = grpc::ControlPlaneService {
        state: state.clone(),
    };

    if let Err(e) = tonic::transport::Server::builder()
        .add_service(ControlPlaneServer::new(control_service))
        .serve(grpc_addr)
        .await
    {
        error!("gRPC server error: {}", e);
    }

    Ok(())
}
