use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::info;

pub const DOR_SERVICE_TYPE: &str = "_murmur._tcp.local.";

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: u64,
    pub ip: std::net::IpAddr,
    pub port: u16,
}

pub struct Discovery {
    daemon: ServiceDaemon,
    node_id: u64,
    peers: Arc<RwLock<HashMap<u64, PeerInfo>>>,
}

impl Discovery {
    pub fn new(node_id: u64) -> anyhow::Result<Self> {
        let daemon = ServiceDaemon::new()?;
        Ok(Self {
            daemon,
            node_id,
            peers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn start_broadcasting(&self, port: u16) -> anyhow::Result<()> {
        let instance_name = format!("murmur-node-{}", self.node_id);
        let host_name = format!("{}.local.", instance_name);

        let my_ip = "0.0.0.0"; // Will bind to all interfaces

        let mut properties = HashMap::new();
        properties.insert("node_id".to_string(), self.node_id.to_string());

        let service_info = ServiceInfo::new(
            DOR_SERVICE_TYPE,
            &instance_name,
            &host_name,
            my_ip,
            port,
            Some(properties),
        )?;

        self.daemon.register(service_info)?;
        info!("Started mDNS broadcast for Node {}", self.node_id);
        Ok(())
    }

    pub fn start_browsing(&self) -> anyhow::Result<mpsc::Receiver<PeerEvent>> {
        let receiver = self.daemon.browse(DOR_SERVICE_TYPE)?;
        let (tx, rx) = mpsc::channel(100);
        let peers = self.peers.clone();

        tokio::spawn(async move {
            while let Ok(event) = receiver.recv_async().await {
                match event {
                    ServiceEvent::ServiceResolved(info) => {
                        let props = info.get_properties();
                        if let Some(val) = props.get_property_val_str("node_id") {
                            if let Ok(id) = val.parse::<u64>() {
                                let addrs = info.get_addresses();
                                if let Some(ip) = addrs.iter().next() {
                                    let peer = PeerInfo {
                                        node_id: id,
                                        ip: *ip,
                                        port: info.get_port(),
                                    };
                                    peers.write().await.insert(id, peer.clone());
                                    let _ = tx.send(PeerEvent::Discovered(peer)).await;
                                }
                            }
                        }
                    }
                    ServiceEvent::ServiceRemoved(_type_name, _fullname) => {
                        // We would need to map fullname back to node_id to remove it,
                        // or just scan our map. For simplicity, we skip precise removal logic here,
                        // and rely on TCP drops to actually know when a peer dies.
                    }
                    _ => {}
                }
            }
        });

        Ok(rx)
    }
}

#[derive(Debug, Clone)]
pub enum PeerEvent {
    Discovered(PeerInfo),
    Lost(u64),
}
