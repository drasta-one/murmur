use std::sync::Arc;
use tokio::time::{interval, Duration};
use crate::state::NodeState;
use crate::p2p;
use tracing::{info, warn};

pub async fn run_event_loop(state: Arc<NodeState>) {
    let mut ticker = interval(Duration::from_millis(100));
    
    // We check timeouts roughly every second. We can just use a counter for ticks.
    let mut tick_count = 0;
    
    loop {
        ticker.tick().await;
        tick_count += 1;
        
        // 1. Broadly ping peers based on local clock (every ~100ms is too fast, let's do every 2s)
        if tick_count % 20 == 0 {
            let active_nodes = state.overlay.read().await.active_nodes();
            let conns = state.connections.read().await;
            
            for peer_id in active_nodes {
                if let Some(conn) = conns.get(&peer_id) {
                    // Heartbeat ping
                    let _ = conn.send_message(&murmur_core::net::NetMessage::HeartbeatPing).await;
                }
            }
        }
        
        // Phase 2.4: Slow Loris Attack Defense
        // Check for chunk timeouts every 1 second
        if tick_count % 10 == 0 {
            let (affected_manifests, offending_nodes) = state.tracker.write().await.check_timeouts(Duration::from_secs(5));
            
            if !offending_nodes.is_empty() {
                let mut banned = state.banned_peers.write().await;
                let mut conns = state.connections.write().await;
                
                for node_id in offending_nodes {
                    warn!("BANNING peer {} due to RequestChunk timeout (Slow Loris protection)", node_id.0);
                    banned.insert(node_id);
                    
                    // Dropping the connection socket will immediately close it, and the rx loop in lib.rs will break
                    if let Some(_conn) = conns.remove(&node_id) {
                        info!("Dropped connection to slow node {}", node_id.0);
                    }
                }
            }
            
            for manifest_id in affected_manifests {
                // Try to request the pending chunks from other healthy peers immediately
                p2p::try_request_pending_chunks(&state, manifest_id).await;
            }
        }
        
        // 2. Step the CoordinatorLifecycle
        let _overlay = state.overlay.write().await;
        let coordinator = state.coordinator.lock().await;
        
        if coordinator.active_coordinator() == Some(state.node_id) {
            // If we are coordinator, run scheduler
            let _scheduler = state.scheduler.lock().await;
            // e.g. scheduler.tick(&overlay, &mut tasks)
        }
    }
}
