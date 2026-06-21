use murmur_core::manifest::Manifest;
use murmur_core::types::{ChunkId, ManifestId, NodeId};
use std::collections::{HashMap, HashSet};
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct TransferProgress {
    pub manifest: Manifest,
    pub chunks_received: HashSet<ChunkId>,
    pub chunks_pending: HashSet<ChunkId>,
    pub chunks_in_flight: HashMap<ChunkId, (NodeId, Instant)>, // which peer is sending, and when requested
    pub started_at: Instant,
}

impl TransferProgress {
    pub fn new(manifest: Manifest) -> Self {
        let mut chunks_pending = HashSet::new();
        for chunk in &manifest.chunks {
            chunks_pending.insert(chunk.id);
        }

        Self {
            manifest,
            chunks_received: HashSet::new(),
            chunks_pending,
            chunks_in_flight: HashMap::new(),
            started_at: Instant::now(),
        }
    }

    pub fn percentage(&self) -> f64 {
        let total = self.manifest.chunks.len();
        if total == 0 {
            return 100.0;
        }
        (self.chunks_received.len() as f64 / total as f64) * 100.0
    }

    pub fn is_complete(&self) -> bool {
        self.chunks_received.len() == self.manifest.chunks.len()
    }
}

pub struct TransferTracker {
    active: HashMap<ManifestId, TransferProgress>,
    global_chunk_availability: HashMap<ManifestId, HashMap<ChunkId, HashSet<NodeId>>>,
}

impl TransferTracker {
    pub fn new() -> Self {
        Self {
            active: HashMap::new(),
            global_chunk_availability: HashMap::new(),
        }
    }
}

impl Default for TransferTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl TransferTracker {
    pub fn get_all_pending_chunks(&self) -> Vec<(ManifestId, ChunkId)> {
        let mut pending = Vec::new();
        for (manifest_id, progress) in &self.active {
            for chunk_id in &progress.chunks_pending {
                pending.push((*manifest_id, *chunk_id));
            }
        }
        pending
    }

    pub fn register_peer_chunk(
        &mut self,
        manifest_id: ManifestId,
        chunk_id: ChunkId,
        node_id: NodeId,
    ) {
        self.global_chunk_availability
            .entry(manifest_id)
            .or_default()
            .entry(chunk_id)
            .or_default()
            .insert(node_id);
    }

    pub fn remove_peer_availability(&mut self, node_id: NodeId) {
        for availability in self.global_chunk_availability.values_mut() {
            for holders in availability.values_mut() {
                holders.remove(&node_id);
            }
        }
    }

    pub fn remove_peer_chunk_availability(
        &mut self,
        manifest_id: ManifestId,
        chunk_id: ChunkId,
        node_id: NodeId,
    ) {
        if let Some(availability) = self.global_chunk_availability.get_mut(&manifest_id)
            && let Some(holders) = availability.get_mut(&chunk_id)
        {
            holders.remove(&node_id);
        }
    }

    pub fn get_rarest_pending_chunks(&self, manifest_id: ManifestId) -> Vec<ChunkId> {
        if let Some(progress) = self.active.get(&manifest_id) {
            let mut pending: Vec<ChunkId> = progress.chunks_pending.iter().copied().collect();
            let availability = self.global_chunk_availability.get(&manifest_id);
            // Sort by availability count (ascending), then by chunk_id to break ties deterministically
            pending.sort_by_key(|chunk_id| {
                let count = availability
                    .and_then(|a| a.get(chunk_id))
                    .map(|s| s.len())
                    .unwrap_or(0);
                (count, chunk_id.0)
            });
            pending
        } else {
            Vec::new()
        }
    }

    pub fn get_chunk_holders(
        &self,
        manifest_id: ManifestId,
        chunk_id: ChunkId,
    ) -> Option<Vec<NodeId>> {
        self.global_chunk_availability
            .get(&manifest_id)
            .and_then(|a| a.get(&chunk_id))
            .map(|holders| holders.iter().copied().collect())
    }

    pub fn start_transfer(&mut self, manifest: Manifest) {
        self.active
            .entry(manifest.id)
            .or_insert_with(|| TransferProgress::new(manifest));
    }

    pub fn mark_chunk_received(&mut self, manifest_id: ManifestId, chunk_id: ChunkId) {
        if let Some(progress) = self.active.get_mut(&manifest_id)
            && (progress.chunks_pending.remove(&chunk_id)
                || progress.chunks_in_flight.remove(&chunk_id).is_some())
        {
            progress.chunks_received.insert(chunk_id);
        }
    }

    pub fn mark_chunk_in_flight(
        &mut self,
        manifest_id: ManifestId,
        chunk_id: ChunkId,
        node_id: NodeId,
    ) {
        if let Some(progress) = self.active.get_mut(&manifest_id)
            && progress.chunks_pending.remove(&chunk_id)
        {
            progress
                .chunks_in_flight
                .insert(chunk_id, (node_id, Instant::now()));
        }
    }

    pub fn unmark_chunk_in_flight(&mut self, manifest_id: ManifestId, chunk_id: ChunkId) {
        if let Some(progress) = self.active.get_mut(&manifest_id)
            && progress.chunks_in_flight.remove(&chunk_id).is_some()
        {
            progress.chunks_pending.insert(chunk_id);
        }
    }

    pub fn get_progress(&self, manifest_id: ManifestId) -> Option<&TransferProgress> {
        self.active.get(&manifest_id)
    }

    pub fn get_all_progress(&self) -> Vec<(ManifestId, TransferProgress)> {
        self.active.iter().map(|(&id, p)| (id, p.clone())).collect()
    }

    pub fn handle_node_disconnect(&mut self, node_id: NodeId) -> Vec<(ManifestId, ChunkId)> {
        self.remove_peer_availability(node_id);

        let mut reassigned = Vec::new();
        for progress in self.active.values_mut() {
            let mut failed_chunks = Vec::new();
            for (&chunk_id, &(holder, _)) in &progress.chunks_in_flight {
                if holder == node_id {
                    failed_chunks.push(chunk_id);
                }
            }
            for chunk_id in failed_chunks {
                progress.chunks_in_flight.remove(&chunk_id);
                progress.chunks_pending.insert(chunk_id);
                reassigned.push((progress.manifest.id, chunk_id));
            }
        }
        reassigned
    }

    pub fn check_timeouts(
        &mut self,
        timeout: std::time::Duration,
    ) -> (Vec<ManifestId>, HashSet<NodeId>) {
        let mut affected_manifests = Vec::new();
        let mut offending_nodes = HashSet::new();
        let now = Instant::now();

        for progress in self.active.values_mut() {
            let mut timed_out_chunks = Vec::new();
            for (&chunk_id, &(node_id, request_time)) in &progress.chunks_in_flight {
                if now.duration_since(request_time) > timeout {
                    timed_out_chunks.push((chunk_id, node_id));
                }
            }

            if !timed_out_chunks.is_empty() {
                affected_manifests.push(progress.manifest.id);
                for (chunk_id, node_id) in timed_out_chunks {
                    progress.chunks_in_flight.remove(&chunk_id);
                    progress.chunks_pending.insert(chunk_id);
                    offending_nodes.insert(node_id);
                }
            }
        }

        // To remove duplicates from affected_manifests without sorting (ManifestId is not Ord)
        let mut unique_manifests = Vec::new();
        for m in affected_manifests {
            if !unique_manifests.contains(&m) {
                unique_manifests.push(m);
            }
        }
        affected_manifests = unique_manifests;

        (affected_manifests, offending_nodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use murmur_core::chunk::ChunkMeta;

    fn dummy_manifest() -> Manifest {
        Manifest {
            id: ManifestId::new(),
            name: "test.txt".into(),
            total_size: 300,
            chunks: vec![
                ChunkMeta {
                    id: ChunkId(1),
                    offset: 0,
                    size: 100,
                    hash: [0; 32],
                },
                ChunkMeta {
                    id: ChunkId(2),
                    offset: 100,
                    size: 100,
                    hash: [0; 32],
                },
                ChunkMeta {
                    id: ChunkId(3),
                    offset: 200,
                    size: 100,
                    hash: [0; 32],
                },
            ],
            file_hash: [0; 32],
            chunk_size: 100,
            source: murmur_core::manifest::ManifestSource::LocalFile {
                path: std::path::PathBuf::from("test.txt"),
            },
            created_at: murmur_core::types::SimTime::ZERO,
        }
    }

    #[test]
    fn test_tracker_updates_on_chunk_receive() {
        let mut tracker = TransferTracker::new();
        let manifest = dummy_manifest();
        tracker.start_transfer(manifest.clone());

        tracker.mark_chunk_received(manifest.id, ChunkId(1));

        let progress = tracker.get_progress(manifest.id).unwrap();
        assert!(progress.chunks_received.contains(&ChunkId(1)));
        assert!(!progress.chunks_pending.contains(&ChunkId(1)));
    }

    #[test]
    fn test_tracker_detects_completion() {
        let mut tracker = TransferTracker::new();
        let manifest = dummy_manifest();
        tracker.start_transfer(manifest.clone());

        tracker.mark_chunk_received(manifest.id, ChunkId(1));
        tracker.mark_chunk_received(manifest.id, ChunkId(2));
        assert!(!tracker.get_progress(manifest.id).unwrap().is_complete());

        tracker.mark_chunk_received(manifest.id, ChunkId(3));
        assert!(tracker.get_progress(manifest.id).unwrap().is_complete());
    }

    #[test]
    fn test_tracker_reports_percentage() {
        let mut tracker = TransferTracker::new();
        let manifest = dummy_manifest();
        tracker.start_transfer(manifest.clone());

        assert_eq!(tracker.get_progress(manifest.id).unwrap().percentage(), 0.0);
        tracker.mark_chunk_received(manifest.id, ChunkId(1));
        assert!((tracker.get_progress(manifest.id).unwrap().percentage() - 33.33).abs() < 0.1);
    }

    #[test]
    fn test_in_flight_tracking() {
        let mut tracker = TransferTracker::new();
        let manifest = dummy_manifest();
        tracker.start_transfer(manifest.clone());

        tracker.mark_chunk_in_flight(manifest.id, ChunkId(1), NodeId(42));

        let progress = tracker.get_progress(manifest.id).unwrap();
        assert!(!progress.chunks_pending.contains(&ChunkId(1)));
        assert_eq!(
            progress.chunks_in_flight.get(&ChunkId(1)).map(|(n, _)| *n),
            Some(NodeId(42))
        );

        tracker.mark_chunk_received(manifest.id, ChunkId(1));

        let progress = tracker.get_progress(manifest.id).unwrap();
        assert!(!progress.chunks_in_flight.contains_key(&ChunkId(1)));
        assert!(progress.chunks_received.contains(&ChunkId(1)));
    }

    #[test]
    fn test_rarest_first_scheduling() {
        let manifest_id = ManifestId::new();
        let mut tracker = TransferTracker::new();

        let mut manifest = Manifest {
            id: manifest_id,
            name: "test.bin".into(),
            total_size: 4000,
            chunks: vec![],
            file_hash: [0; 32],
            chunk_size: 1000,
            source: murmur_core::manifest::ManifestSource::LocalFile {
                path: std::path::PathBuf::from("test.bin"),
            },
            created_at: murmur_core::types::SimTime::ZERO,
        };

        // Add 4 chunks
        for i in 0..4 {
            manifest.chunks.push(murmur_core::chunk::ChunkMeta {
                id: ChunkId(i),
                offset: (i * 1000) as u64,
                size: 1000,
                hash: [0; 32],
            });
        }

        tracker.start_transfer(manifest);

        // Node 1 has Chunk 0, 1, 2, 3
        tracker.register_peer_chunk(manifest_id, ChunkId(0), NodeId(1));
        tracker.register_peer_chunk(manifest_id, ChunkId(1), NodeId(1));
        tracker.register_peer_chunk(manifest_id, ChunkId(2), NodeId(1));
        tracker.register_peer_chunk(manifest_id, ChunkId(3), NodeId(1));

        // Node 2 has Chunk 0, 1
        tracker.register_peer_chunk(manifest_id, ChunkId(0), NodeId(2));
        tracker.register_peer_chunk(manifest_id, ChunkId(1), NodeId(2));

        // Node 3 has Chunk 0
        tracker.register_peer_chunk(manifest_id, ChunkId(0), NodeId(3));

        // Availability:
        // Chunk 0: 3 nodes
        // Chunk 1: 2 nodes
        // Chunk 2: 1 node
        // Chunk 3: 1 node

        let pending = tracker.get_rarest_pending_chunks(manifest_id);

        // Should be sorted by rarity (ascending): Chunk 2, 3 (tie, sorted by ID), then 1, then 0.
        assert_eq!(
            pending,
            vec![ChunkId(2), ChunkId(3), ChunkId(1), ChunkId(0)]
        );
    }
}
