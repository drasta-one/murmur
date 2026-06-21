//! Bonded download orchestrator.
//!
//! Coordinates a multi-WAN download across all nodes in the swarm.
//! The coordinator splits a URL into byte ranges, assigns chunks to
//! nodes proportionally to their WAN bandwidth, and orchestrates
//! reassembly on the requesting node.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use murmur_core::manifest::Manifest;
use murmur_core::node::NodeConfig;
use murmur_core::types::{ChunkId, ManifestId, NodeId};
use murmur_scheduler::strategy::{BandwidthWeightedStrategy, ChunkAssignment, SchedulingStrategy};
use tracing::{info, warn, debug, error};

use crate::url_manifest::{self, UrlResourceInfo, DEFAULT_BONDED_CHUNK_SIZE};
use crate::wan_fetch;

/// Status of a bonded download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BondedStatus {
    /// Probing the URL for size and range support.
    Probing,
    /// Assigning chunks to nodes.
    Scheduling,
    /// Nodes are downloading their assigned ranges.
    Downloading,
    /// LAN transfer phase — chunks moving to the requester.
    Assembling,
    /// Download complete.
    Completed,
    /// Download failed.
    Failed(String),
}

/// Per-node speed observation for dynamic rebalancing.
#[derive(Debug, Clone)]
pub struct NodeSpeedObservation {
    /// Exponential moving average of throughput in bytes/sec.
    pub avg_throughput_bps: u64,
    /// Number of chunks completed by this node.
    pub chunks_completed: u32,
    /// Total bytes downloaded by this node.
    pub bytes_downloaded: u64,
    /// Time of last observation.
    pub last_observed: Instant,
}

impl NodeSpeedObservation {
    fn new() -> Self {
        Self {
            avg_throughput_bps: 0,
            chunks_completed: 0,
            bytes_downloaded: 0,
            last_observed: Instant::now(),
        }
    }

    /// Update with a new throughput measurement using exponential moving average.
    /// Alpha = 0.3 weights recent measurements more heavily.
    fn update(&mut self, throughput_bps: u64, chunk_bytes: u64) {
        const ALPHA: f64 = 0.3;

        if self.avg_throughput_bps == 0 {
            self.avg_throughput_bps = throughput_bps;
        } else {
            self.avg_throughput_bps = (ALPHA * throughput_bps as f64
                + (1.0 - ALPHA) * self.avg_throughput_bps as f64)
                as u64;
        }

        self.chunks_completed += 1;
        self.bytes_downloaded += chunk_bytes;
        self.last_observed = Instant::now();
    }
}

/// A bonded download in progress.
#[derive(Debug, Clone)]
pub struct BondedDownload {
    /// The URL being downloaded.
    pub url: String,
    /// The generated manifest.
    pub manifest: Manifest,
    /// URL resource info from probing.
    pub url_info: UrlResourceInfo,
    /// Where to save the file on the requesting node.
    pub output_path: String,
    /// The node that requested the download.
    pub requester: NodeId,
    /// Current status.
    pub status: BondedStatus,
    /// Remaining chunks to be assigned dynamically via Work Stealing
    pub unassigned_chunks: std::collections::VecDeque<ChunkId>,
    /// Per-node speed observations.
    pub node_speeds: HashMap<NodeId, NodeSpeedObservation>,
    /// When the download started.
    pub started_at: Instant,
}

impl BondedDownload {
    /// Create a new bonded download from a URL probe result.
    pub fn new(
        url: String,
        url_info: UrlResourceInfo,
        manifest: Manifest,
        output_path: String,
        requester: NodeId,
    ) -> Self {
        let chunk_ids = manifest.chunk_ids().into_iter().collect();
        Self {
            url,
            url_info,
            manifest,
            output_path,
            requester,
            status: BondedStatus::Scheduling,
            unassigned_chunks: chunk_ids,
            node_speeds: HashMap::new(),
            started_at: Instant::now(),
        }
    }

    /// Dispense a batch of chunks for a node.
    pub fn dispense_batch(&mut self) -> Vec<(ChunkId, u64, u32)> {
        self.status = BondedStatus::Downloading;
        let mut batch = Vec::new();
        let mut batch_bytes = 0;
        let max_batch_bytes = 8 * 1024 * 1024; // 8 MB
        
        while let Some(chunk_id) = self.unassigned_chunks.front().copied() {
            if let Some(chunk_meta) = self.manifest.get_chunk(chunk_id) {
                if !batch.is_empty() && batch_bytes + chunk_meta.size as u64 > max_batch_bytes {
                    break;
                }
                self.unassigned_chunks.pop_front();
                batch.push((chunk_meta.id, chunk_meta.offset, chunk_meta.size));
                batch_bytes += chunk_meta.size as u64;
            } else {
                self.unassigned_chunks.pop_front(); // skip invalid
            }
        }
        
        batch
    }

    /// Record a speed observation from a node.
    pub fn record_speed(&mut self, node_id: NodeId, throughput_bps: u64, chunk_bytes: u64) {
        self.node_speeds
            .entry(node_id)
            .or_insert_with(NodeSpeedObservation::new)
            .update(throughput_bps, chunk_bytes);
    }

    /// Get the combined effective bandwidth across all nodes.
    pub fn combined_bandwidth_bps(&self) -> u64 {
        self.node_speeds.values().map(|s| s.avg_throughput_bps).sum()
    }

    /// Get overall progress as a percentage.
    pub fn progress_percentage(&self) -> f64 {
        let total = self.manifest.chunk_count();
        if total == 0 {
            return 100.0;
        }
        let completed: u32 = self.node_speeds.values().map(|s| s.chunks_completed).sum();
        (completed as f64 / total as f64) * 100.0
    }

    /// Get per-node speed map (for progress reporting).
    pub fn node_speed_map(&self) -> HashMap<u64, u64> {
        self.node_speeds
            .iter()
            .map(|(id, obs)| (id.0, obs.avg_throughput_bps))
            .collect()
    }

    /// Mark the download as completed.
    pub fn complete(&mut self) {
        self.status = BondedStatus::Completed;
        let elapsed = self.started_at.elapsed();
        let total_bytes = self.manifest.total_size;
        let effective_bps = if elapsed.as_secs() > 0 {
            total_bytes / elapsed.as_secs()
        } else {
            total_bytes * 1000 / elapsed.as_millis().max(1) as u64
        };

        info!(
            manifest_id = %self.manifest.id,
            total_bytes = total_bytes,
            elapsed_secs = elapsed.as_secs_f64(),
            effective_mbps = effective_bps as f64 / 125_000.0,
            "Bonded download completed"
        );
    }

    /// Mark the download as failed.
    pub fn fail(&mut self, reason: String) {
        error!(
            manifest_id = %self.manifest.id,
            reason = %reason,
            "Bonded download failed"
        );
        self.status = BondedStatus::Failed(reason);
    }

    /// Dynamically rebalance uncompleted chunks if nodes are underperforming.
    ///
    /// Returns a map of new assignments to be dispatched. If empty, no rebalancing occurred.
    pub fn rebalance(
        &mut self,
        _nodes: &[(NodeId, NodeConfig)],
        _completed_chunks: &std::collections::HashSet<ChunkId>,
    ) -> HashMap<NodeId, Vec<(ChunkId, u64, u32)>> {
        // Obsolete: Rebalancing is now implicitly handled by sliding window steals
        HashMap::new()
    }
}

/// Orchestrate a bonded download on the local node.
///
/// This is the "worker" side — called on each node that receives
/// chunk assignments from the coordinator. It downloads all assigned
/// ranges and stores them locally.
pub async fn execute_local_fetch(
    url: &str,
    assignments: &[(ChunkId, u64, u32)], // (chunk_id, offset, size)
    manifest_id: ManifestId,
    state: std::sync::Arc<crate::state::NodeState>,
    max_concurrent: usize,
) -> Result<Vec<wan_fetch::FetchResult>> {
    let client = reqwest::Client::builder()
        .user_agent("DOR-Runtime/0.1")
        .build()
        .context("failed to build HTTP client")?;

    info!(
        url = url,
        chunk_count = assignments.len(),
        max_concurrent = max_concurrent,
        "Starting local bonded fetch"
    );

    let u32_assignments: Vec<(u32, u64, u32)> = assignments
        .iter()
        .map(|&(id, off, sz)| (id.0, off, sz))
        .collect();

    let results = wan_fetch::fetch_ranges_concurrent(
        &client,
        url,
        &u32_assignments,
        None, // TODO: Extract etag from ManifestSource
        max_concurrent,
    )
    .await;

    let mut fetch_results = Vec::new();
    let mut failed = 0u32;

    for result in results {
        match result {
            Ok((chunk_id, fetch_result)) => {
                let offset = assignments.iter().find(|(id, _, _)| id.0 == chunk_id).unwrap().1;
                
                // Store the chunk to disk
                state.storage.write_chunk(
                    manifest_id,
                    ChunkId(chunk_id),
                    &fetch_result.data,
                    offset,
                ).await?;

                // Register chunk as received locally
                state.tracker.write().await.mark_chunk_received(manifest_id, ChunkId(chunk_id));

                // Broadcast Have to peers
                let msg = murmur_core::net::NetMessage::Have { manifest_id, chunk_id: ChunkId(chunk_id) };
                let conns = state.connections.read().await;
                for (id, conn) in conns.iter() {
                    let _ = conn.send_message(&msg).await;
                }
                
                // Check if file is complete (in case we were the only node fetching and just finished)
                let is_complete = state.tracker.read().await.get_progress(manifest_id).map(|p| p.is_complete()).unwrap_or(false);
                if is_complete {
                    if let Some(dest) = state.download_destinations.read().await.get(&manifest_id).cloned() {
                        info!("Bonded Download complete! Reassembling to {}", dest);
                        if let Some(manifest) = state.manifests.read().await.get(&manifest_id).cloned() {
                            if let Err(e) = state.storage.reassemble_file(&manifest, &dest).await {
                                tracing::error!("Failed to reassemble file: {}", e);
                            }
                        }
                    }
                }

                debug!(
                    chunk_id = chunk_id,
                    size = fetch_result.data.len(),
                    throughput_mbps = fetch_result.throughput_bps as f64 / 125_000.0,
                    "Chunk fetched and stored"
                );

                fetch_results.push(fetch_result);
            }
            Err(e) => {
                warn!(error = %e, "Failed to fetch chunk range");
                failed += 1;
            }
        }
    }

    if failed > 0 {
        warn!(
            failed = failed,
            succeeded = fetch_results.len(),
            "Some chunks failed to download"
        );
    }

    info!(
        chunks_downloaded = fetch_results.len(),
        total_bytes = fetch_results.iter().map(|r| r.data.len() as u64).sum::<u64>(),
        "Local bonded fetch complete"
    );

    Ok(fetch_results)
}

/// Initiate a full bonded download (coordinator-side entry point).
///
/// 1. Probes the URL
/// 2. Creates a manifest
/// 3. Computes bandwidth-weighted assignments
/// 4. Returns the BondedDownload for tracking
pub async fn initiate_bonded_download(
    url: &str,
    output_path: &str,
    requester_id: NodeId,
    nodes: &[(NodeId, NodeConfig)],
    chunk_size: Option<u32>,
) -> Result<(BondedDownload, HashMap<NodeId, Vec<(ChunkId, u64, u32)>>)> {
    // Step 1: Probe the URL
    let url_info = url_manifest::probe_url(url).await?;

    if !url_info.supports_ranges && nodes.len() > 1 {
        warn!(
            "Server does not support Range requests; falling back to single-node download"
        );
        // Still proceed with single-node assignment
    }

    // Step 2: Generate manifest
    let chunk_sz = chunk_size.unwrap_or(DEFAULT_BONDED_CHUNK_SIZE);
    let name = url_info
        .suggested_filename
        .clone()
        .unwrap_or_else(|| "bonded_download".to_string());

    let manifest = url_manifest::manifest_from_url_info(&url_info, url, &name, chunk_sz);

    info!(
        url = url,
        total_size = url_info.total_size,
        chunk_count = manifest.chunk_count(),
        node_count = nodes.len(),
        "Initiating bonded download"
    );

    // Step 3: Create the download tracker
    let mut download = BondedDownload::new(
        url.to_string(),
        url_info,
        manifest,
        output_path.to_string(),
        requester_id,
    );

    let mut per_node: HashMap<NodeId, Vec<(ChunkId, u64, u32)>> = HashMap::new();
    for (node_id, _) in nodes {
        let batch = download.dispense_batch();
        if !batch.is_empty() {
            per_node.insert(*node_id, batch);
        }
    }

    Ok((download, per_node))
}

/// Handle a RequestMoreWork message from a node (coordinator only).
pub fn handle_request_more_work(
    state: std::sync::Arc<crate::state::NodeState>,
    manifest_id: ManifestId,
    node_id: NodeId,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        let mut bonded_downloads = state.bonded_downloads.write().await;
        let mut batch = Vec::new();
        let mut url = String::new();
        
        if let Some(download) = bonded_downloads.get_mut(&manifest_id) {
            batch = download.dispense_batch();
            url = download.url.clone();
        }
        
        // Drop the lock before awaiting on network/spawn!
        drop(bonded_downloads);
        
        if batch.is_empty() {
            return;
        }
        
        tracing::info!(
            manifest_id = %manifest_id,
            node_id = node_id.0,
            chunks = batch.len(),
            "Dispensed next sliding-window batch via Work Stealing"
        );
        
        if node_id == state.node_id {
            // Local execution
            let state_clone = state.clone();
            tokio::spawn(async move {
                let _ = execute_local_fetch(&url, &batch, manifest_id, state_clone.clone(), 4).await;
                handle_request_more_work(state_clone.clone(), manifest_id, node_id).await;
            });
        } else {
            // Send to remote node via P2P
            let msg = murmur_core::net::NetMessage::AssignFetchRanges {
                url,
                manifest_id,
                coordinator_id: state.node_id,
                assignments: batch,
            };
            let conns = state.connections.read().await;
            if let Some(conn) = conns.get(&node_id) {
                let _ = conn.send_message(&msg).await;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use murmur_core::chunk::ChunkMeta;
    use murmur_core::types::SimTime;

    fn make_test_manifest(total_size: u64, chunk_size: u32) -> Manifest {
        let info = UrlResourceInfo {
            tier: url_manifest::ServerTier::Tier1FullRange,
            total_size,
            supports_ranges: true,
            etag: None,
            last_modified: None,
            content_type: None,
            suggested_filename: Some("test.bin".into()),
        };
        url_manifest::manifest_from_url_info(&info, "https://example.com/file.bin", "test.bin", chunk_size)
    }

    fn make_test_nodes() -> Vec<(NodeId, NodeConfig)> {
        vec![
            (
                NodeId(1),
                NodeConfig {
                    wan_bandwidth: 625_000,  // 5 Mbps = 625 KB/s
                    ..Default::default()
                },
            ),
            (
                NodeId(2),
                NodeConfig {
                    wan_bandwidth: 2_500_000, // 20 Mbps = 2.5 MB/s
                    ..Default::default()
                },
            ),
        ]
    }

    #[test]
    fn bonded_download_dispense_batch() {
        let manifest = make_test_manifest(10_000_000, 1_000_000); // 10 chunks
        let nodes = make_test_nodes();

        let url_info = UrlResourceInfo {
            tier: url_manifest::ServerTier::Tier1FullRange,
            total_size: 10_000_000,
            supports_ranges: true,
            etag: None,
            last_modified: None,
            content_type: None,
            suggested_filename: Some("test.bin".into()),
        };

        let mut download = BondedDownload::new(
            "https://example.com/file.bin".into(),
            url_info,
            manifest,
            "/tmp/file.bin".into(),
            NodeId(1),
        );

        // Dispense batches for each node — sliding window style
        let mut total_chunks = 0;
        for _ in &nodes {
            let batch = download.dispense_batch();
            total_chunks += batch.len();
        }

        // Drain remaining
        loop {
            let batch = download.dispense_batch();
            if batch.is_empty() {
                break;
            }
            total_chunks += batch.len();
        }

        // All 10 chunks should have been dispensed
        assert_eq!(total_chunks, 10);
    }

    #[test]
    fn speed_observation_ema() {
        let mut obs = NodeSpeedObservation::new();

        obs.update(1_000_000, 1_000_000); // 1 MB/s
        assert_eq!(obs.avg_throughput_bps, 1_000_000);
        assert_eq!(obs.chunks_completed, 1);

        obs.update(2_000_000, 1_000_000); // 2 MB/s
        // EMA: 0.3 * 2M + 0.7 * 1M = 1.3M
        assert_eq!(obs.avg_throughput_bps, 1_300_000);
        assert_eq!(obs.chunks_completed, 2);

        obs.update(2_000_000, 1_000_000);
        // EMA: 0.3 * 2M + 0.7 * 1.3M = 1.51M
        assert_eq!(obs.avg_throughput_bps, 1_510_000);
    }

    #[test]
    fn bonded_download_progress() {
        let manifest = make_test_manifest(4_000_000, 1_000_000); // 4 chunks
        let url_info = UrlResourceInfo {
            tier: url_manifest::ServerTier::Tier1FullRange,
            total_size: 4_000_000,
            supports_ranges: true,
            etag: None,
            last_modified: None,
            content_type: None,
            suggested_filename: None,
        };

        let mut download = BondedDownload::new(
            "https://example.com/file.bin".into(),
            url_info,
            manifest,
            "/tmp/file.bin".into(),
            NodeId(1),
        );

        assert_eq!(download.progress_percentage(), 0.0);

        download.record_speed(NodeId(1), 1_000_000, 1_000_000);
        assert_eq!(download.progress_percentage(), 25.0);

        download.record_speed(NodeId(2), 2_000_000, 1_000_000);
        download.record_speed(NodeId(2), 2_000_000, 1_000_000);
        download.record_speed(NodeId(2), 2_000_000, 1_000_000);
        assert_eq!(download.progress_percentage(), 100.0);
    }

    #[test]
    fn combined_bandwidth() {
        let manifest = make_test_manifest(2_000_000, 1_000_000);
        let url_info = UrlResourceInfo {
            tier: url_manifest::ServerTier::Tier1FullRange,
            total_size: 2_000_000,
            supports_ranges: true,
            etag: None,
            last_modified: None,
            content_type: None,
            suggested_filename: None,
        };

        let mut download = BondedDownload::new(
            "https://example.com/file.bin".into(),
            url_info,
            manifest,
            "/tmp/file.bin".into(),
            NodeId(1),
        );

        download.record_speed(NodeId(1), 625_000, 1_000_000);   // 5 Mbps
        download.record_speed(NodeId(2), 2_500_000, 1_000_000);  // 20 Mbps

        // Combined: 625K + 2.5M = 3.125M bytes/sec = 25 Mbps
        assert_eq!(download.combined_bandwidth_bps(), 3_125_000);
    }
}
