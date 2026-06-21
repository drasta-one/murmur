use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::state::NodeState;
use murmur_proto::control::control_plane_server::ControlPlane;
use murmur_proto::control::{
    Acknowledgement, BondedFetchRequest, BondedFetchResponse, ChunkSpeedReport,
    ChunkVerifiedReport, ClusterEvent, DaemonEvent, DaemonEventSubscribeRequest,
    EventSubscribeRequest, FetchRangeAssignment, ListManifestsRequest, ListManifestsResponse,
    OstFragmentReport, ProxyStatusRequest, ProxyStatusResponse, SeedRequest, SeedResponse,
    SnapshotRequest, SnapshotResponse, StartDownloadRequest, StartDownloadResponse, StatusRequest,
    StatusResponse, StopRequest, StopResponse, daemon_event::Event as DaemonEventType,
};

pub struct ControlPlaneService {
    pub state: Arc<NodeState>,
}

#[tonic::async_trait]
impl ControlPlane for ControlPlaneService {
    type StreamClusterEventsStream = ReceiverStream<Result<ClusterEvent, Status>>;
    type StreamDaemonEventsStream = ReceiverStream<Result<DaemonEvent, Status>>;

    async fn stream_cluster_events(
        &self,
        _request: Request<EventSubscribeRequest>,
    ) -> Result<Response<Self::StreamClusterEventsStream>, Status> {
        let (_tx, rx) = mpsc::channel(4);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn report_chunk_verified(
        &self,
        request: Request<ChunkVerifiedReport>,
    ) -> Result<Response<Acknowledgement>, Status> {
        let _req = request.into_inner();
        // Implement logic to mark chunk as verified
        Ok(Response::new(Acknowledgement {
            success: true,
            message: "Report received".into(),
        }))
    }

    async fn report_ost_fragment(
        &self,
        request: Request<OstFragmentReport>,
    ) -> Result<Response<Acknowledgement>, Status> {
        let _req = request.into_inner();
        // Implement logic for OST fragment merging
        Ok(Response::new(Acknowledgement {
            success: true,
            message: "Fragment received".into(),
        }))
    }

    async fn start_download(
        &self,
        request: Request<StartDownloadRequest>,
    ) -> Result<Response<StartDownloadResponse>, Status> {
        let req = request.into_inner();
        let manifest_id = match uuid::Uuid::parse_str(&req.manifest_id) {
            Ok(id) => murmur_core::types::ManifestId(id),
            Err(_) => return Err(Status::invalid_argument("Invalid manifest ID")),
        };

        let has_manifest = self.state.manifests.read().await.contains_key(&manifest_id);
        if !has_manifest {
            info!("Requesting manifest {} from peers", manifest_id.0);
            let msg = murmur_core::net::NetMessage::RequestManifest { manifest_id };
            for conn in self.state.connections.read().await.values() {
                let _ = conn.send_message(&msg).await;
            }
        }

        self.state
            .download_destinations
            .write()
            .await
            .insert(manifest_id, req.url.clone());

        // If already complete, reassemble immediately
        let is_complete = {
            let tracker = self.state.tracker.read().await;
            tracker
                .get_progress(manifest_id)
                .map(|p| p.is_complete())
                .unwrap_or(false)
        };

        if is_complete
            && let Some(manifest) = self.state.manifests.read().await.get(&manifest_id).cloned()
        {
            info!("Download already complete! Reassembling to {}", req.url);
            if let Err(e) = self
                .state
                .storage
                .reassemble_file(&manifest, &req.url)
                .await
            {
                tracing::error!("Failed to reassemble file: {}", e);
            }
        }

        Ok(Response::new(StartDownloadResponse {
            success: true,
            message: "Download started (or already complete)".into(),
        }))
    }

    async fn get_snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        let mut nodes = Vec::new();
        let overlay = self.state.overlay.read().await;

        nodes.push(serde_json::json!({
            "id": self.state.node_id.0,
            "ip": "127.0.0.1",
            "port": 0
        }));

        for node_id in overlay.active_nodes() {
            nodes.push(serde_json::json!({
                "id": node_id.0,
                "ip": "127.0.0.1",
                "port": 0
            }));
        }

        let mut links = Vec::new();
        for node_id in overlay.active_nodes() {
            links.push(serde_json::json!({
                "from": self.state.node_id.0,
                "to": node_id.0,
                "active": true
            }));
        }

        let json_str = serde_json::json!({
            "nodes": nodes,
            "chunks": [],
            "links": links
        })
        .to_string();

        Ok(Response::new(SnapshotResponse {
            snapshot_json: json_str.into_bytes(),
        }))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let active_peers = self.state.overlay.read().await.active_nodes().len();
        let is_coordinator =
            self.state.coordinator.lock().await.active_coordinator() == Some(self.state.node_id);

        Ok(Response::new(StatusResponse {
            node_id: self.state.node_id.0,
            active_peers: active_peers as u64,
            is_coordinator,
        }))
    }

    async fn seed(&self, request: Request<SeedRequest>) -> Result<Response<SeedResponse>, Status> {
        use tokio::io::AsyncReadExt;

        let req = request.into_inner();
        info!("Received request to seed file: {}", req.file_path);
        let path = std::path::Path::new(&req.file_path);

        match tokio::fs::File::open(path).await {
            Ok(mut file) => {
                let filename = path.file_name().unwrap().to_string_lossy().to_string();
                let chunk_size = 1024 * 1024; // 1 MB chunks

                let source = murmur_core::manifest::ManifestSource::LocalFile {
                    path: std::path::PathBuf::from(filename.clone()),
                };
                match murmur_core::manifest::Manifest::from_async_reader(
                    filename.clone(),
                    &mut file,
                    chunk_size,
                    source,
                    murmur_core::types::SimTime::ZERO,
                )
                .await
                {
                    Ok(manifest) => {
                        info!(
                            "Created manifest for file: {} chunks",
                            manifest.chunks.len()
                        );

                        let mut file = tokio::fs::File::open(path).await.unwrap();
                        let mut buffer = vec![0u8; chunk_size as usize];

                        self.state
                            .storage
                            .preallocate(manifest.id, manifest.total_size)
                            .await
                            .unwrap();

                        for chunk_meta in &manifest.chunks {
                            let mut read_bytes = 0;
                            while read_bytes < chunk_meta.size as usize {
                                let n = file
                                    .read(&mut buffer[read_bytes..(chunk_meta.size as usize)])
                                    .await
                                    .unwrap();
                                if n == 0 {
                                    break;
                                }
                                read_bytes += n;
                            }
                            let chunk_data = &buffer[..read_bytes];
                            self.state
                                .storage
                                .write_chunk(
                                    manifest.id,
                                    chunk_meta.id,
                                    chunk_data,
                                    chunk_meta.offset,
                                )
                                .await
                                .unwrap();
                        }

                        self.state
                            .manifests
                            .write()
                            .await
                            .insert(manifest.id, manifest.clone());
                        self.state
                            .manifest_holders
                            .write()
                            .await
                            .entry(manifest.id)
                            .or_default()
                            .insert(self.state.node_id);

                        self.state
                            .tracker
                            .write()
                            .await
                            .start_transfer(manifest.clone());
                        let mut chunks = Vec::new();
                        for chunk_meta in &manifest.chunks {
                            self.state
                                .tracker
                                .write()
                                .await
                                .mark_chunk_received(manifest.id, chunk_meta.id);
                            chunks.push(chunk_meta.id);
                        }

                        let manifest_msg = murmur_core::net::NetMessage::ManifestData {
                            manifest: manifest.clone(),
                        };
                        let bitfield_msg = murmur_core::net::NetMessage::Bitfield {
                            manifest_id: manifest.id,
                            chunks,
                        };
                        let conns = self.state.connections.read().await;
                        for (_id, conn) in conns.iter() {
                            let _ = conn.send_message(&manifest_msg).await;
                            let _ = conn.send_message(&bitfield_msg).await;
                        }

                        Ok(Response::new(SeedResponse {
                            success: true,
                            message: "File seeded and manifest broadcasted".into(),
                        }))
                    }
                    Err(e) => Ok(Response::new(SeedResponse {
                        success: false,
                        message: format!("Failed to generate manifest: {}", e),
                    })),
                }
            }
            Err(e) => Ok(Response::new(SeedResponse {
                success: false,
                message: format!("File not found: {}", e),
            })),
        }
    }

    async fn list_manifests(
        &self,
        _request: Request<ListManifestsRequest>,
    ) -> Result<Response<ListManifestsResponse>, Status> {
        let manifests = self.state.manifests.read().await;
        let list = manifests
            .values()
            .map(|m| (m.id.0.to_string(), m.name.clone()))
            .collect();
        Ok(Response::new(ListManifestsResponse { manifests: list }))
    }

    async fn get_proxy_status(
        &self,
        _request: Request<ProxyStatusRequest>,
    ) -> Result<Response<ProxyStatusResponse>, Status> {
        let streams = self.state.proxy_orchestrator.active_streams.read().await;

        let mut streams_per_node = std::collections::HashMap::new();
        for (_, (_, node_id)) in streams.iter() {
            *streams_per_node.entry(node_id.0).or_insert(0) += 1;
        }

        Ok(Response::new(ProxyStatusResponse {
            local_port: 1080, // Currently hardcoded/internal, could be fetched if passed down
            active_streams: streams.len() as u32,
            streams_per_node,
            total_bytes_proxied: 0, // Could be tracked by orchestrator later
        }))
    }

    async fn stop(&self, _request: Request<StopRequest>) -> Result<Response<StopResponse>, Status> {
        info!("Stop requested via gRPC");
        tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            std::process::exit(0);
        });
        Ok(Response::new(StopResponse {
            success: true,
            message: "Daemon stopping".into(),
        }))
    }

    async fn stream_daemon_events(
        &self,
        _request: Request<DaemonEventSubscribeRequest>,
    ) -> Result<Response<Self::StreamDaemonEventsStream>, Status> {
        let (tx, rx) = mpsc::channel(100);

        let tracker = self.state.tracker.clone();
        let bonded_downloads = self.state.bonded_downloads.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                // For simplicity, just poll all active transfers and send progress
                let transfers = tracker.read().await.get_all_progress();
                for (manifest_id, progress) in transfers {
                    let event = DaemonEvent {
                        event: Some(DaemonEventType::Progress(
                            murmur_proto::control::TransferProgressEvent {
                                manifest_id: manifest_id.0.to_string(),
                                percentage: progress.percentage() as f32,
                                is_complete: progress.is_complete(),
                            },
                        )),
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        return; // Client disconnected
                    }
                }

                // Poll bonded downloads
                let bonded = bonded_downloads.read().await;
                for (manifest_id, download) in bonded.iter() {
                    let event = DaemonEvent {
                        event: Some(DaemonEventType::BondedProgress(
                            murmur_proto::control::BondedFetchProgressEvent {
                                manifest_id: manifest_id.0.to_string(),
                                percentage: download.progress_percentage() as f32,
                                is_complete: download.status
                                    == crate::bonded_download::BondedStatus::Completed,
                                node_speeds: download.node_speed_map(),
                                combined_bps: download.combined_bandwidth_bps(),
                            },
                        )),
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        return; // Client disconnected
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ─── Bonding RPCs ────────────────────────────────────────

    async fn bonded_fetch(
        &self,
        request: Request<BondedFetchRequest>,
    ) -> Result<Response<BondedFetchResponse>, Status> {
        let req = request.into_inner();
        info!(url = %req.url, output_path = %req.output_path, "BondedFetch RPC received");

        // Get cluster nodes for bandwidth-weighted assignment
        let overlay = self.state.overlay.read().await;
        let active_ids = overlay.active_nodes();
        let nodes: Vec<_> = active_ids
            .iter()
            .filter_map(|id| overlay.get_node(*id).map(|n| (*id, n.config.clone())))
            .collect();
        drop(overlay);

        if nodes.is_empty() {
            return Ok(Response::new(BondedFetchResponse {
                success: false,
                manifest_id: String::new(),
                total_size: 0,
                chunk_count: 0,
                node_count: 0,
                message: "No active nodes in the cluster".into(),
            }));
        }

        // Determine chunk size
        let chunk_size = if req.chunk_size > 0 {
            req.chunk_size
        } else {
            crate::url_manifest::DEFAULT_BONDED_CHUNK_SIZE
        };

        // Initiate bonded download
        match crate::bonded_download::initiate_bonded_download(
            &req.url,
            &req.output_path,
            self.state.node_id,
            &nodes,
            Some(chunk_size),
        )
        .await
        {
            Ok((download, per_node)) => {
                let manifest_id_str = download.manifest.id.0.to_string();
                let manifest_id = download.manifest.id;
                let total_size = download.manifest.total_size;
                let chunk_count = download.manifest.chunk_count() as u32;
                let node_count = nodes.len() as u32;

                // Store the bonded download state for tracking
                self.state
                    .bonded_downloads
                    .write()
                    .await
                    .insert(download.manifest.id, download.clone());

                self.state
                    .manifests
                    .write()
                    .await
                    .insert(download.manifest.id, download.manifest.clone());

                self.state
                    .tracker
                    .write()
                    .await
                    .start_transfer(download.manifest.clone());

                self.state
                    .download_destinations
                    .write()
                    .await
                    .insert(manifest_id, req.output_path.clone());

                self.state
                    .storage
                    .preallocate(manifest_id, total_size)
                    .await
                    .unwrap();

                // Broadcast ManifestData to peers participating in the download
                let manifest_msg = murmur_core::net::NetMessage::ManifestData {
                    manifest: download.manifest.clone(),
                };
                for node_id in per_node.keys() {
                    if *node_id != self.state.node_id
                        && let Some(conn) = self.state.connections.read().await.get(node_id)
                    {
                        let _ = conn.send_message(&manifest_msg).await;
                    }
                }

                // Dispatch assignments!
                for (node_id, assignments) in per_node {
                    if node_id == self.state.node_id {
                        // Local execution
                        let state_clone = self.state.clone();
                        let url = req.url.clone();
                        tokio::spawn(async move {
                            let _ = crate::bonded_download::execute_local_fetch(
                                &url,
                                &assignments,
                                manifest_id,
                                state_clone.clone(),
                                4,
                            )
                            .await;
                            crate::bonded_download::handle_request_more_work(
                                state_clone.clone(),
                                manifest_id,
                                state_clone.node_id,
                            )
                            .await;
                        });
                    } else {
                        // Send to remote node via P2P
                        let msg = murmur_core::net::NetMessage::AssignFetchRanges {
                            url: req.url.clone(),
                            manifest_id,
                            coordinator_id: self.state.node_id,
                            assignments,
                        };
                        let conns = self.state.connections.read().await;
                        if let Some(conn) = conns.get(&node_id) {
                            let _ = conn.send_message(&msg).await;
                        }
                    }
                }

                Ok(Response::new(BondedFetchResponse {
                    success: true,
                    manifest_id: manifest_id_str,
                    total_size,
                    chunk_count,
                    node_count,
                    message: format!("Bonded download started across {} nodes", node_count),
                }))
            }
            Err(e) => Ok(Response::new(BondedFetchResponse {
                success: false,
                manifest_id: String::new(),
                total_size: 0,
                chunk_count: 0,
                node_count: 0,
                message: format!("Failed to initiate bonded download: {e}"),
            })),
        }
    }

    async fn report_chunk_speed(
        &self,
        request: Request<ChunkSpeedReport>,
    ) -> Result<Response<Acknowledgement>, Status> {
        let req = request.into_inner();
        let node_id = murmur_core::types::NodeId(req.node_id);

        if let Ok(manifest_uuid) = uuid::Uuid::parse_str(&req.manifest_id) {
            let manifest_id = murmur_core::types::ManifestId(manifest_uuid);
            let mut downloads = self.state.bonded_downloads.write().await;
            if let Some(download) = downloads.get_mut(&manifest_id) {
                download.record_speed(
                    node_id,
                    req.bytes_per_sec,
                    req.bytes_per_sec, // Approximate chunk bytes from throughput
                );
            }
        }

        Ok(Response::new(Acknowledgement {
            success: true,
            message: "Speed report recorded".into(),
        }))
    }

    async fn assign_fetch_ranges(
        &self,
        _request: Request<FetchRangeAssignment>,
    ) -> Result<Response<Acknowledgement>, Status> {
        // This is called by the coordinator to assign ranges to this node.
        // For now, stub — the actual fetch execution will be wired in the event loop.
        Ok(Response::new(Acknowledgement {
            success: true,
            message: "Fetch ranges received".into(),
        }))
    }
}
