use serde::{Deserialize, Serialize};
use std::pin::Pin;
use tokio_stream::{Stream, StreamExt};
use murmur_core::types::{ChunkId, ManifestId, NodeId};

pub mod proto {
    pub use murmur_proto::control::*;
}

#[derive(Debug, thiserror::Error)]
pub enum DorApiError {
    #[error("Network error: {0}")]
    Network(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Daemon error: {0}")]
    DaemonError(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MurmurConfig {
    pub daemon_addr: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub ip: String,
    pub port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LeaveReason {
    Graceful,
    Timeout,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BanReason {
    HashMismatch,
    SlowLoris,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ElectionReason {
    CoordinatorDeath,
    CoordinatorSteppedDown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChunkFailReason {
    Timeout,
    NodeDisconnected,
    Corrupted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ErrorCode {
    Internal,
    InvalidRequest,
    NetworkFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChunkOwnershipStatus {
    Unassigned,
    Assigned,
    InProgress,
    Verified,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkOwnershipInfo {
    pub chunk_id: ChunkId,
    pub status: ChunkOwnershipStatus,
    pub assigned_to: Option<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinkInfo {
    pub from: NodeId,
    pub to: NodeId,
    pub active: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MurmurCommand {
    StartDownload { url: String, manifest_id: ManifestId },
    BondedFetch { url: String, output_path: String },
    PauseDownload { manifest_id: ManifestId },
    LeaveCluster,
    RequestSnapshot,
    Seed { file_path: String },
    Status,
    ListManifests,
    GetProxyStatus,
    Stop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MurmurEvent {
    // Cluster lifecycle
    ClusterFormed      { coordinator: NodeId, members: Vec<NodeId>, epoch: u64 },
    CoordinatorChanged { new: NodeId, old: NodeId, reason: ElectionReason, epoch: u64 },
    NodeJoined         { node: NodeInfo },
    NodeLeft           { node_id: NodeId, reason: LeaveReason },
    NodeBanned         { node_id: NodeId, reason: BanReason },

    // Transfer lifecycle
    ManifestReceived   { manifest_id: ManifestId, total_chunks: u32 },
    ChunkAssigned      { chunk_id: ChunkId, assigned_to: NodeId },
    ChunkVerified      { chunk_id: ChunkId, from_node: NodeId, duration_ms: u64 },
    ChunkFailed        { chunk_id: ChunkId, reason: ChunkFailReason },
    ChunkReassigned    { chunk_id: ChunkId, from: NodeId, to: NodeId, reason: String },
    TransferComplete   { manifest_id: ManifestId, path: String, duration_ms: u64 },
    TransferProgress   { manifest_id: ManifestId, percentage: f64, is_complete: bool },

    // Observability
    LinkMeasured       { from: NodeId, to: NodeId, mbps: f32, latency_ms: u32 },
    OstSnapshot        { nodes: Vec<NodeInfo>, chunks: Vec<ChunkOwnershipInfo>, links: Vec<LinkInfo> },
    Error              { code: ErrorCode, message: String },
    
    // Command Responses (for CLI)
    StatusReport { node_id: NodeId, active_peers: usize, is_coordinator: bool },
    ManifestList { manifests: Vec<(String, String)> },
    CommandSuccess { message: String },

    // Bonding events
    BondedFetchProgress {
        manifest_id: ManifestId,
        percentage: f64,
        is_complete: bool,
        combined_bps: u64,
        node_speeds: std::collections::HashMap<u64, u64>,
    },
    ProxyStatusReport {
        local_port: u32,
        active_streams: u32,
        streams_per_node: std::collections::HashMap<u64, u32>,
    },
}

pub type CommandSender = tokio::sync::mpsc::Sender<MurmurCommand>;
pub type EventStream = Pin<Box<dyn Stream<Item = MurmurEvent> + Send>>;

pub struct DorRuntime {
    config: MurmurConfig,
    cmd_tx: tokio::sync::mpsc::Sender<MurmurCommand>,
    cmd_rx: Option<tokio::sync::mpsc::Receiver<MurmurCommand>>,
    event_tx: tokio::sync::broadcast::Sender<MurmurEvent>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl DorRuntime {
    pub fn new(config: MurmurConfig) -> Result<Self, DorApiError> {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(100);
        let (event_tx, _) = tokio::sync::broadcast::channel(100);
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);

        Ok(Self {
            config,
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            event_tx,
            shutdown_tx,
        })
    }

    pub async fn start(&mut self) -> Result<(), DorApiError> {
        use proto::control_plane_client::ControlPlaneClient;
        
        let mut cmd_rx = self.cmd_rx.take().expect("Runtime already started");
        let event_tx = self.event_tx.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        let addr = format!("http://{}", self.config.daemon_addr);
        
        // Ensure we can connect to the daemon
        let client = match ControlPlaneClient::connect(addr.clone()).await {
            Ok(c) => c,
            Err(e) => {
                return Err(DorApiError::Network(e.to_string()));
            }
        };

        // Spawn command processor
        let client_clone = client.clone();
        let event_tx_clone = event_tx.clone();
        tokio::spawn(async move {
            let mut client = client_clone;
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    MurmurCommand::Status => {
                        let req = tonic::Request::new(proto::StatusRequest {});
                        match client.status(req).await {
                            Ok(res) => {
                                let r = res.into_inner();
                                let _ = event_tx_clone.send(MurmurEvent::StatusReport {
                                    node_id: NodeId(r.node_id),
                                    active_peers: r.active_peers as usize,
                                    is_coordinator: r.is_coordinator,
                                });
                            }
                            Err(e) => {
                                let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::NetworkFailure, message: e.to_string() });
                            }
                        }
                    }
                    MurmurCommand::Seed { file_path } => {
                        let req = tonic::Request::new(proto::SeedRequest { file_path });
                        match client.seed(req).await {
                            Ok(res) => {
                                let r = res.into_inner();
                                if r.success {
                                    let _ = event_tx_clone.send(MurmurEvent::CommandSuccess { message: r.message });
                                } else {
                                    let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::Internal, message: r.message });
                                }
                            }
                            Err(e) => {
                                let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::NetworkFailure, message: e.to_string() });
                            }
                        }
                    }
                    MurmurCommand::ListManifests => {
                        let req = tonic::Request::new(proto::ListManifestsRequest {});
                        match client.list_manifests(req).await {
                            Ok(res) => {
                                let r = res.into_inner();
                                let manifests = r.manifests.into_iter().collect();
                                let _ = event_tx_clone.send(MurmurEvent::ManifestList { manifests });
                            }
                            Err(e) => {
                                let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::NetworkFailure, message: e.to_string() });
                            }
                        }
                    }
                    MurmurCommand::GetProxyStatus => {
                        let req = tonic::Request::new(proto::ProxyStatusRequest {});
                        match client.get_proxy_status(req).await {
                            Ok(res) => {
                                let r = res.into_inner();
                                let _ = event_tx_clone.send(MurmurEvent::ProxyStatusReport {
                                    local_port: r.local_port,
                                    active_streams: r.active_streams,
                                    streams_per_node: r.streams_per_node,
                                });
                            }
                            Err(e) => {
                                let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::NetworkFailure, message: e.to_string() });
                            }
                        }
                    }
                    MurmurCommand::StartDownload { url, manifest_id } => {
                        let req = tonic::Request::new(proto::StartDownloadRequest { 
                            url, 
                            manifest_id: manifest_id.0.to_string() 
                        });
                        match client.start_download(req).await {
                            Ok(res) => {
                                let r = res.into_inner();
                                if r.success {
                                    let _ = event_tx_clone.send(MurmurEvent::CommandSuccess { message: r.message });
                                } else {
                                    let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::Internal, message: r.message });
                                }
                            }
                            Err(e) => {
                                let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::NetworkFailure, message: e.to_string() });
                            }
                        }
                    }
                    MurmurCommand::BondedFetch { url, output_path } => {
                        let req = tonic::Request::new(proto::BondedFetchRequest { 
                            url, 
                            output_path,
                            chunk_size: 0 // Use default
                        });
                        match client.bonded_fetch(req).await {
                            Ok(res) => {
                                let r = res.into_inner();
                                if r.success {
                                    tracing::info!("BondedFetch RPC succeeded: {}", r.message);
                                    let _ = event_tx_clone.send(MurmurEvent::CommandSuccess { message: r.message });
                                } else {
                                    tracing::error!("BondedFetch RPC failed: {}", r.message);
                                    let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::Internal, message: r.message });
                                }
                            }
                            Err(e) => {
                                tracing::error!("BondedFetch RPC encountered an error: {}", e);
                                let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::NetworkFailure, message: e.to_string() });
                            }
                        }
                    }
                    MurmurCommand::Stop => {
                        let req = tonic::Request::new(proto::StopRequest {});
                        match client.stop(req).await {
                            Ok(res) => {
                                let _ = event_tx_clone.send(MurmurEvent::CommandSuccess { message: res.into_inner().message });
                            }
                            Err(e) => {
                                let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::NetworkFailure, message: e.to_string() });
                            }
                        }
                    }
                    MurmurCommand::RequestSnapshot => {
                        let req = tonic::Request::new(proto::SnapshotRequest {});
                        match client.get_snapshot(req).await {
                            Ok(res) => {
                                let r = res.into_inner();
                                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&r.snapshot_json) {
                                    let nodes: Vec<NodeInfo> = serde_json::from_value(val["nodes"].clone()).unwrap_or_default();
                                    let chunks: Vec<ChunkOwnershipInfo> = serde_json::from_value(val["chunks"].clone()).unwrap_or_default();
                                    let links: Vec<LinkInfo> = serde_json::from_value(val["links"].clone()).unwrap_or_default();
                                    let _ = event_tx_clone.send(MurmurEvent::OstSnapshot { nodes, chunks, links });
                                } else {
                                    let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::Internal, message: "Failed to parse snapshot".into() });
                                }
                            }
                            Err(e) => {
                                let _ = event_tx_clone.send(MurmurEvent::Error { code: ErrorCode::NetworkFailure, message: e.to_string() });
                            }
                        }
                    }
                    _ => {}
                }
            }
        });

        // Spawn event stream reader
        let mut client_ev = client.clone();
        let event_tx_ev = event_tx.clone();
        tokio::spawn(async move {
            let req = tonic::Request::new(proto::DaemonEventSubscribeRequest {});
            if let Ok(res) = client_ev.stream_daemon_events(req).await {
                let mut stream = res.into_inner();
                loop {
                    tokio::select! {
                        msg = stream.message() => {
                            match msg {
                                Ok(Some(m)) => {
                                    if let Some(event) = m.event {
                                        match event {
                                            proto::daemon_event::Event::Success(s) => {
                                                let _ = event_tx_ev.send(MurmurEvent::CommandSuccess { message: s.message });
                                            }
                                            proto::daemon_event::Event::Error(e) => {
                                                let _ = event_tx_ev.send(MurmurEvent::Error { code: ErrorCode::Internal, message: e.message });
                                            }
                                            proto::daemon_event::Event::Progress(p) => {
                                                if let Ok(id) = uuid::Uuid::parse_str(&p.manifest_id) {
                                                    let _ = event_tx_ev.send(MurmurEvent::TransferProgress { 
                                                        manifest_id: murmur_core::types::ManifestId(id), 
                                                        percentage: p.percentage as f64, 
                                                        is_complete: p.is_complete 
                                                    });
                                                }
                                            }
                                            proto::daemon_event::Event::BondedProgress(bp) => {
                                                if let Ok(id) = uuid::Uuid::parse_str(&bp.manifest_id) {
                                                    let _ = event_tx_ev.send(MurmurEvent::BondedFetchProgress {
                                                        manifest_id: murmur_core::types::ManifestId(id),
                                                        percentage: bp.percentage as f64,
                                                        is_complete: bp.is_complete,
                                                        combined_bps: bp.combined_bps,
                                                        node_speeds: bp.node_speeds.into_iter().collect(),
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(None) => break,
                                Err(_) => break,
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn commands(&self) -> CommandSender {
        self.cmd_tx.clone()
    }

    pub fn events(&self) -> impl Stream<Item = MurmurEvent> + Send + 'static {
        tokio_stream::wrappers::BroadcastStream::new(self.event_tx.subscribe())
            .filter_map(|r| r.ok())
    }
}
