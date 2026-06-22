use crate::state::NodeState;
use murmur_core::types::NodeId;
use std::sync::Arc;
use tracing::info;

pub async fn handle_net_message(
    state: &Arc<NodeState>,
    peer_id: NodeId,
    msg: murmur_core::net::NetMessage,
) {
    match msg {
        murmur_core::net::NetMessage::Handshake { .. } => {
            // Already handled
        }
        murmur_core::net::NetMessage::HeartbeatPing => {
            if let Some(conn) = state.connections.read().await.get(&peer_id) {
                let _ = conn
                    .send_message(&murmur_core::net::NetMessage::HeartbeatAck)
                    .await;
            }
        }
        murmur_core::net::NetMessage::HeartbeatAck => {
            if let Some(node) = state.overlay.write().await.get_node_mut(peer_id) {
                node.activate();
            }
        }
        murmur_core::net::NetMessage::RequestManifest { manifest_id } => {
            info!("Peer {} requested manifest {}", peer_id.0, manifest_id.0);

            let manifests = state.manifests.read().await;
            if let Some(manifest) = manifests.get(&manifest_id) {
                if let Some(conn) = state.connections.read().await.get(&peer_id) {
                    info!("Sending ManifestData to peer {}", peer_id.0);
                    let _ = conn
                        .send_message(&murmur_core::net::NetMessage::ManifestData {
                            manifest: manifest.clone(),
                        })
                        .await;
                } else {
                    tracing::error!("Could not find connection for peer {}", peer_id.0);
                }
            } else {
                tracing::error!("Manifest {} not found in state!", manifest_id.0);
            }
        }
        murmur_core::net::NetMessage::ManifestData { manifest } => {
            info!(
                "Received manifest {} from peer {}",
                manifest.id.0, peer_id.0
            );

            let is_new = {
                let mut manifests = state.manifests.write().await;
                if let std::collections::hash_map::Entry::Vacant(e) = manifests.entry(manifest.id) {
                    e.insert(manifest.clone());
                    true
                } else {
                    false
                }
            };

            if is_new {
                // Gossip to other peers
                let msg = murmur_core::net::NetMessage::ManifestData {
                    manifest: manifest.clone(),
                };
                for (id, conn) in state.connections.read().await.iter() {
                    if *id != peer_id {
                        info!("Gossiping manifest {} to peer {}", manifest.id.0, id.0);
                        let _ = conn.send_message(&msg).await;
                    }
                }
            }

            state
                .manifest_holders
                .write()
                .await
                .entry(manifest.id)
                .or_default()
                .insert(peer_id);
            state.tracker.write().await.start_transfer(manifest.clone());

            try_request_pending_chunks(state, manifest.id).await;
        }
        murmur_core::net::NetMessage::RequestChunk {
            manifest_id,
            chunk_id,
        } => {
            tracing::info!(
                "Received RequestChunk {} for manifest {}",
                chunk_id.0,
                manifest_id.0
            );
            let manifests = state.manifests.read().await;
            let chunk_meta = manifests
                .get(&manifest_id)
                .and_then(|m| m.get_chunk(chunk_id))
                .cloned();
            drop(manifests);

            if let Some(meta) = chunk_meta {
                if let Some(conn) = state.connections.read().await.get(&peer_id) {
                    tracing::info!("Found connection for peer {}, reading chunk...", peer_id.0);
                    match state
                        .storage
                        .read_chunk(manifest_id, chunk_id, meta.offset, meta.size)
                        .await
                    {
                        Ok(Some(mut data)) => {
                            info!("Sending ChunkData {} to peer {}", chunk_id.0, peer_id.0);
                            if state.slow_loris {
                                tracing::warn!(
                                    "Slow Loris mode: ignoring RequestChunk {} to stall peer {}",
                                    chunk_id.0,
                                    peer_id.0
                                );
                                return; // keep connection open but never respond
                            }
                            if state.malicious {
                                tracing::warn!(
                                    "Malicious mode: tampering with chunk data for chunk {}",
                                    chunk_id.0
                                );
                                for byte in data.iter_mut() {
                                    *byte ^= 0xFF; // Invert all bits to corrupt the data
                                }
                            }
                            let _ = conn
                                .send_message(&murmur_core::net::NetMessage::ChunkData {
                                    manifest_id,
                                    chunk_id,
                                    data,
                                })
                                .await;
                        }
                        Ok(None) => {
                            info!("Chunk {} not found for peer {}", chunk_id.0, peer_id.0);
                            let _ = conn
                                .send_message(&murmur_core::net::NetMessage::ChunkNotFound {
                                    manifest_id,
                                    chunk_id,
                                })
                                .await;
                        }
                        Err(e) => {
                            tracing::error!("Error reading chunk {}: {:?}", chunk_id.0, e);
                            let _ = conn
                                .send_message(&murmur_core::net::NetMessage::ChunkNotFound {
                                    manifest_id,
                                    chunk_id,
                                })
                                .await;
                        }
                    }
                } else {
                    tracing::error!("RequestChunk: No connection found for peer {}", peer_id.0);
                }
            } else {
                tracing::error!(
                    "RequestChunk: Chunk {} not found in manifest {}",
                    chunk_id.0,
                    manifest_id.0
                );
                if let Some(conn) = state.connections.read().await.get(&peer_id) {
                    let _ = conn
                        .send_message(&murmur_core::net::NetMessage::ChunkNotFound {
                            manifest_id,
                            chunk_id,
                        })
                        .await;
                }
            }
        }
        murmur_core::net::NetMessage::ChunkData {
            manifest_id,
            chunk_id,
            data,
        } => {
            let manifests = state.manifests.read().await;
            let chunk_meta = manifests
                .get(&manifest_id)
                .and_then(|m| m.get_chunk(chunk_id))
                .cloned();
            drop(manifests);

            if let Some(meta) = chunk_meta {
                let actual_hash = *blake3::hash(&data).as_bytes();
                if meta.hash == actual_hash || meta.hash == [0u8; 32] {
                    if state
                        .storage
                        .write_chunk(manifest_id, chunk_id, &data, meta.offset)
                        .await
                        .is_ok()
                    {
                        info!(
                            "Received and verified chunk {} from {}",
                            chunk_id.0, peer_id.0
                        );
                        state
                            .tracker
                            .write()
                            .await
                            .mark_chunk_received(manifest_id, chunk_id);

                        // Phase 4: Broadcast Have message to all peers
                        let msg = murmur_core::net::NetMessage::Have {
                            manifest_id,
                            chunk_id,
                        };
                        for (id, conn) in state.connections.read().await.iter() {
                            if *id != peer_id {
                                let _ = conn.send_message(&msg).await;
                            }
                        }

                        // Try to request more
                        try_request_pending_chunks(state, manifest_id).await;

                        let is_complete = {
                            let tracker = state.tracker.read().await;
                            tracker
                                .get_progress(manifest_id)
                                .map(|p| p.is_complete())
                                .unwrap_or(false)
                        };

                        if is_complete
                            && let Some(dest) = state
                                .download_destinations
                                .read()
                                .await
                                .get(&manifest_id)
                                .cloned()
                        {
                            info!("Download complete! Reassembling to {}", dest);
                            if let Some(manifest) =
                                state.manifests.read().await.get(&manifest_id).cloned()
                            {
                                if let Err(e) =
                                    state.storage.reassemble_file(&manifest, &dest).await
                                {
                                    tracing::error!("Failed to reassemble file: {}", e);
                                }
                            } else {
                                tracing::error!(
                                    "Manifest {} not found during reassembly",
                                    manifest_id.0
                                );
                            }
                        }
                    } else {
                        tracing::error!("Failed to write chunk {} to storage", chunk_id.0);
                    }
                } else {
                    tracing::warn!(
                        "Chunk {} verification failed from {} (hash mismatch)",
                        chunk_id.0,
                        peer_id.0
                    );
                    state
                        .tracker
                        .write()
                        .await
                        .unmark_chunk_in_flight(manifest_id, chunk_id);

                    // Phase 5: Anti-Malicious Peer Banning
                    tracing::error!(
                        "BANNING node {} for serving corrupted chunk data!",
                        peer_id.0
                    );
                    state.banned_peers.write().await.insert(peer_id);

                    // Drop connection to penalize the liar node
                    if let Some(_conn) = state.connections.write().await.remove(&peer_id) {
                        info!("Dropped connection to malicious node {}", peer_id.0);
                        // The socket will be closed when `conn` is dropped.
                        // `handle_node_disconnect` will be triggered by the rx loop automatically.
                    }
                }
            } else {
                tracing::warn!("Received unknown chunk {} from {}", chunk_id.0, peer_id.0);
            }
        }
        murmur_core::net::NetMessage::ChunkNotFound {
            manifest_id,
            chunk_id,
        } => {
            tracing::warn!(
                "Peer {} reported chunk {} not found for manifest {}",
                peer_id.0,
                chunk_id.0,
                manifest_id.0
            );
            let mut tracker = state.tracker.write().await;
            tracker.unmark_chunk_in_flight(manifest_id, chunk_id);
            // Peer lied or lost it, remove from their availability
            tracker.remove_peer_chunk_availability(manifest_id, chunk_id, peer_id);
            drop(tracker);

            try_request_pending_chunks(state, manifest_id).await;
        }
        murmur_core::net::NetMessage::Bitfield {
            manifest_id,
            chunks,
        } => {
            info!(
                "Received Bitfield from {} for manifest {}",
                peer_id.0, manifest_id.0
            );
            let mut tracker = state.tracker.write().await;
            for chunk_id in chunks {
                tracker.register_peer_chunk(manifest_id, chunk_id, peer_id);
            }
            drop(tracker);
            try_request_pending_chunks(state, manifest_id).await;
        }
        murmur_core::net::NetMessage::Have {
            manifest_id,
            chunk_id,
        } => {
            state
                .tracker
                .write()
                .await
                .register_peer_chunk(manifest_id, chunk_id, peer_id);
            try_request_pending_chunks(state, manifest_id).await;
        }
        murmur_core::net::NetMessage::AssignFetchRanges {
            url,
            manifest_id,
            coordinator_id,
            assignments,
        } => {
            tracing::info!(
                manifest_id = %manifest_id,
                chunks = assignments.len(),
                "Received bonded fetch assignments from coordinator"
            );

            let state_clone = state.clone();
            tokio::spawn(async move {
                let _ = crate::bonded_download::execute_local_fetch(
                    &url,
                    &assignments,
                    manifest_id,
                    state_clone.clone(),
                    4, // concurrent
                )
                .await;

                // Done with batch, ask for more work!
                let msg = murmur_core::net::NetMessage::RequestMoreWork {
                    manifest_id,
                    node_id: state_clone.node_id,
                };
                if let Some(conn) = state_clone.connections.read().await.get(&coordinator_id) {
                    let _ = conn.send_message(&msg).await;
                }
            });
        }
        murmur_core::net::NetMessage::RequestMoreWork {
            manifest_id,
            node_id,
        } => {
            crate::bonded_download::handle_request_more_work(state.clone(), manifest_id, node_id)
                .await;
        }
        msg @ murmur_core::net::NetMessage::ProxyConnect { .. } => {
            state
                .proxy_orchestrator
                .clone()
                .handle_p2p_message(peer_id, msg)
                .await;
        }
        msg @ murmur_core::net::NetMessage::ProxyConnectResult { .. } => {
            state
                .proxy_orchestrator
                .clone()
                .handle_p2p_message(peer_id, msg)
                .await;
        }
        msg @ murmur_core::net::NetMessage::ProxyData { .. } => {
            state
                .proxy_orchestrator
                .clone()
                .handle_p2p_message(peer_id, msg)
                .await;
        }
        msg @ murmur_core::net::NetMessage::ProxyClose { .. } => {
            state
                .proxy_orchestrator
                .clone()
                .handle_p2p_message(peer_id, msg)
                .await;
        }
    }
}

pub async fn try_request_pending_chunks(
    state: &Arc<NodeState>,
    manifest_id: murmur_core::types::ManifestId,
) {
    let mut tracker = state.tracker.write().await;

    if let Some(progress) = tracker.get_progress(manifest_id) {
        if progress.chunks_pending.is_empty() {
            return;
        }
    } else {
        return;
    }

    // Rarest-first: sort pending chunks by availability
    let pending = tracker.get_rarest_pending_chunks(manifest_id);
    let mut to_request = Vec::new();

    for chunk_id in pending {
        if tracker
            .get_progress(manifest_id)
            .map(|p| p.chunks_in_flight.len())
            .unwrap_or(0)
            >= 5
        {
            break; // Max 5 concurrent
        }

        let mut target_peer = None;
        if let Some(holders) = tracker.get_chunk_holders(manifest_id, chunk_id) {
            for peer in holders {
                if peer == state.node_id {
                    continue;
                }
                target_peer = Some(peer);
                break;
            }
        }

        if let Some(peer) = target_peer {
            to_request.push((chunk_id, peer));
            tracker.mark_chunk_in_flight(manifest_id, chunk_id, peer);
        }
    }

    drop(tracker); // Drop lock before acquiring connections to prevent deadlock

    for (chunk_id, peer) in to_request {
        if let Some(conn) = state.connections.read().await.get(&peer) {
            info!(
                "Requesting chunk {} for manifest {} from peer {}",
                chunk_id.0, manifest_id.0, peer.0
            );
            let _ = conn
                .send_message(&murmur_core::net::NetMessage::RequestChunk {
                    manifest_id,
                    chunk_id,
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_missing_chunk_request_returns_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = Arc::new(
            NodeState::new(
                NodeId(1),
                temp_dir.path().to_path_buf(),
                false,
                false,
                50_000_000,
            )
            .await
            .unwrap(),
        );

        let peer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_listener.local_addr().unwrap();
        let peer_task = tokio::spawn(async move {
            let (socket, _) = peer_listener.accept().await.unwrap();
            let conn = murmur_net::connection::PeerConnection::new_tcp(1, socket);
            let mut rx = conn.start_recv_loop().await;
            rx.recv().await.unwrap()
        });

        let client_conn = tokio::net::TcpStream::connect(peer_addr).await.unwrap();
        let peer_conn = Arc::new(murmur_net::connection::PeerConnection::new_tcp(
            99,
            client_conn,
        ));
        state
            .connections
            .write()
            .await
            .insert(NodeId(99), peer_conn);

        let manifest_id = murmur_core::types::ManifestId::new();

        // Request chunk that doesn't exist
        handle_net_message(
            &state,
            NodeId(99),
            murmur_core::net::NetMessage::RequestChunk {
                manifest_id,
                chunk_id: murmur_core::types::ChunkId(100),
            },
        )
        .await;

        let msg = peer_task.await.unwrap();
        match msg {
            murmur_core::net::NetMessage::ChunkNotFound {
                manifest_id: mid,
                chunk_id,
            } => {
                assert_eq!(chunk_id.0, 100);
                assert_eq!(mid, manifest_id);
            }
            _ => panic!("Expected ChunkNotFound, got {:?}", msg),
        }
    }
    #[tokio::test]
    async fn test_chunk_hash_verification_rejects_tampered_data() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = Arc::new(
            NodeState::new(
                NodeId(1),
                temp_dir.path().to_path_buf(),
                false,
                false,
                50_000_000,
            )
            .await
            .unwrap(),
        );

        let mut manifest = murmur_core::manifest::Manifest {
            id: murmur_core::types::ManifestId::new(),
            name: "test.txt".into(),
            total_size: 100,
            chunks: vec![],
            file_hash: [0; 32],
            chunk_size: 1024,
            source: murmur_core::manifest::ManifestSource::LocalFile {
                path: std::path::PathBuf::from("test.txt"),
            },
            created_at: murmur_core::types::SimTime::ZERO,
        };

        let chunk_data = b"hello world";
        let actual_hash = *blake3::hash(chunk_data).as_bytes();
        let mut tampered_hash = actual_hash;
        tampered_hash[0] ^= 0xff; // Corrupt the hash

        manifest.chunks.push(murmur_core::chunk::ChunkMeta {
            id: murmur_core::types::ChunkId(0),
            offset: 0,
            size: chunk_data.len() as u32,
            hash: tampered_hash,
        });

        let manifest_id = manifest.id;
        state.manifests.write().await.insert(manifest.id, manifest);

        // Handle incoming tampered chunk
        handle_net_message(
            &state,
            NodeId(99),
            murmur_core::net::NetMessage::ChunkData {
                manifest_id,
                chunk_id: murmur_core::types::ChunkId(0),
                data: chunk_data.to_vec(),
            },
        )
        .await;

        // Should NOT be written to storage
        assert!(
            !state
                .storage
                .has_chunk(manifest_id, murmur_core::types::ChunkId(0))
                .await
        );
    }
}
