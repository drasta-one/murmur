use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};
use murmur_core::net::NetMessage;
use murmur_core::types::NodeId;
use rand::Rng;

pub struct ProxyOrchestrator {
    node_id: NodeId,
    wan_bandwidth: u64,
    overlay: Arc<RwLock<murmur_overlay::OverlayStateTable>>,
    connections: Arc<RwLock<HashMap<NodeId, Arc<murmur_net::PeerConnection>>>>,
    // stream_id -> (Sender to write to local socks client, remote NodeId)
    pub active_streams: RwLock<HashMap<u32, (mpsc::Sender<Vec<u8>>, NodeId)>>,
    // stream_id -> channel to notify when connect result arrives
    connect_waiters: RwLock<HashMap<u32, oneshot::Sender<bool>>>,
}

impl ProxyOrchestrator {
    pub fn new(
        node_id: NodeId,
        wan_bandwidth: u64,
        overlay: Arc<RwLock<murmur_overlay::OverlayStateTable>>,
        connections: Arc<RwLock<HashMap<NodeId, Arc<murmur_net::PeerConnection>>>>,
    ) -> Self {
        Self {
            node_id,
            wan_bandwidth,
            overlay,
            connections,
            active_streams: RwLock::new(HashMap::new()),
            connect_waiters: RwLock::new(HashMap::new()),
        }
    }

    async fn select_node(&self) -> Option<(NodeId, u64)> {
        let overlay = self.overlay.read().await;
        let mut active_nodes = Vec::new();
        
        let local_config = murmur_core::node::NodeConfig {
            wan_bandwidth: self.wan_bandwidth,
            ..Default::default()
        };
        active_nodes.push((self.node_id, local_config));

        for id in overlay.active_nodes() {
            if let Some(n) = overlay.get_node(id) {
                active_nodes.push((id, n.config.clone()));
            }
        }

        if active_nodes.is_empty() {
            return None;
        }

        let total_bw: u64 = active_nodes.iter().map(|(_, cfg)| cfg.wan_bandwidth.max(1)).sum();
        let mut rng = rand::thread_rng();
        let mut point = rng.gen_range(0..total_bw);

        for (id, cfg) in active_nodes {
            let bw = cfg.wan_bandwidth.max(1);
            if point < bw {
                return Some((id, bw));
            }
            point -= bw;
        }
        
        None
    }

    pub async fn handle_new_connection(self: Arc<Self>, mut client_stream: TcpStream, host: String, port: u16) -> anyhow::Result<()> {
        let (node_id, bw) = self.select_node().await.unwrap_or((self.node_id, 0));
        info!("SOCKS5 routing {}:{} via Node {} (BW: {} Mbps)", host, port, node_id.0, bw / 125_000);

        if node_id == self.node_id {
            // Local direct connection
            let target_addr = format!("{}:{}", host, port);
            match TcpStream::connect(&target_addr).await {
                Ok(mut remote_stream) => {
                    // SOCKS5 success reply
                    client_stream.write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await?;
                    tokio::spawn(async move {
                        if let Err(e) = tokio::io::copy_bidirectional(&mut client_stream, &mut remote_stream).await {
                            debug!("Direct proxy stream ended: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to connect directly to {}: {}", target_addr, e);
                    // SOCKS5 host unreachable
                    client_stream.write_all(&[0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await?;
                }
            }
        } else {
            // Remote connection via P2P
            let stream_id = rand::random::<u32>();
            
            let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
            self.active_streams.write().await.insert(stream_id, (tx, node_id));

            let (connect_tx, connect_rx) = oneshot::channel();
            self.connect_waiters.write().await.insert(stream_id, connect_tx);

            // Send ProxyConnect
            let msg = NetMessage::ProxyConnect { stream_id, host, port };
            if let Some(conn) = self.connections.read().await.get(&node_id) {
                conn.send_message(&msg).await?;
            } else {
                anyhow::bail!("Connection to chosen proxy node lost");
            }

            // Wait for ProxyConnectResult
            let success = match tokio::time::timeout(std::time::Duration::from_secs(10), connect_rx).await {
                Ok(Ok(res)) => res,
                _ => {
                    self.active_streams.write().await.remove(&stream_id);
                    self.connect_waiters.write().await.remove(&stream_id);
                    anyhow::bail!("Timeout waiting for ProxyConnectResult");
                }
            };

            if !success {
                // SOCKS5 host unreachable
                client_stream.write_all(&[0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await?;
                self.active_streams.write().await.remove(&stream_id);
                return Ok(());
            }

            // SOCKS5 success reply
            client_stream.write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await?;

            let connections_clone = self.connections.clone();
            let orch_clone1 = self.clone();
            let orch_clone2 = self.clone();

            let (mut rh, mut wh) = client_stream.into_split();

            // Task 1: Read from P2P (via rx channel) -> Write to SOCKS client
            let stream_id_c = stream_id;
            tokio::spawn(async move {
                while let Some(data) = rx.recv().await {
                    if let Err(_) = wh.write_all(&data).await {
                        break;
                    }
                }
                orch_clone1.active_streams.write().await.remove(&stream_id_c);
            });

            // Task 2: Read from SOCKS client -> Send P2P ProxyData
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                loop {
                    match rh.read(&mut buf).await {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            let msg = NetMessage::ProxyData { stream_id: stream_id_c, data: buf[..n].to_vec() };
                            if let Some(conn) = connections_clone.read().await.get(&node_id) {
                                let _ = conn.send_message(&msg).await;
                            } else {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                // Send close
                if let Some(conn) = connections_clone.read().await.get(&node_id) {
                    let _ = conn.send_message(&NetMessage::ProxyClose { stream_id: stream_id_c }).await;
                }
                orch_clone2.active_streams.write().await.remove(&stream_id_c);
            });
        }

        Ok(())
    }

    pub async fn handle_p2p_message(self: Arc<Self>, node_id: NodeId, msg: NetMessage) {
        match msg {
            NetMessage::ProxyConnect { stream_id, host, port } => {
                let orch = self.clone();
                let connections_c = self.connections.clone();
                tokio::spawn(async move {
                    let target = format!("{}:{}", host, port);
                    match TcpStream::connect(&target).await {
                        Ok(stream) => {
                            if let Some(conn) = connections_c.read().await.get(&node_id) {
                                let _ = conn.send_message(&NetMessage::ProxyConnectResult { stream_id, success: true }).await;
                            }

                            // We need to route incoming ProxyData for this stream_id back to this TCP stream
                            let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
                            orch.active_streams.write().await.insert(stream_id, (tx, node_id));

                            let (mut rh, mut wh) = stream.into_split();

                            // Read from remote target -> Send P2P
                            let connections_c2 = connections_c.clone();
                            let orch_c1 = orch.clone();
                            tokio::spawn(async move {
                                let mut buf = vec![0u8; 8192];
                                loop {
                                    match rh.read(&mut buf).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            if let Some(conn) = connections_c2.read().await.get(&node_id) {
                                                let _ = conn.send_message(&NetMessage::ProxyData { stream_id, data: buf[..n].to_vec() }).await;
                                            } else {
                                                break;
                                            }
                                        }
                                        Err(_) => break,
                                    }
                                }
                                if let Some(conn) = connections_c2.read().await.get(&node_id) {
                                    let _ = conn.send_message(&NetMessage::ProxyClose { stream_id }).await;
                                }
                                orch_c1.active_streams.write().await.remove(&stream_id);
                            });

                            // Read from P2P -> Write to remote target
                            let orch_c2 = orch.clone();
                            tokio::spawn(async move {
                                while let Some(data) = rx.recv().await {
                                    if let Err(_) = wh.write_all(&data).await {
                                        break;
                                    }
                                }
                                orch_c2.active_streams.write().await.remove(&stream_id);
                            });
                        }
                        Err(e) => {
                            debug!("ProxyConnect failed to {}: {}", target, e);
                            if let Some(conn) = connections_c.read().await.get(&node_id) {
                                let _ = conn.send_message(&NetMessage::ProxyConnectResult { stream_id, success: false }).await;
                            }
                        }
                    }
                });
            }
            NetMessage::ProxyConnectResult { stream_id, success } => {
                if let Some(tx) = self.connect_waiters.write().await.remove(&stream_id) {
                    let _ = tx.send(success);
                }
            }
            NetMessage::ProxyData { stream_id, data } => {
                if let Some((tx, _)) = self.active_streams.read().await.get(&stream_id) {
                    let _ = tx.send(data).await;
                }
            }
            NetMessage::ProxyClose { stream_id } => {
                self.active_streams.write().await.remove(&stream_id);
            }
            _ => {}
        }
    }
}
