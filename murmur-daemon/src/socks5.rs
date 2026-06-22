use crate::proxy_orchestrator::ProxyOrchestrator;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

pub struct Socks5Server {
    port: u16,
    orchestrator: Arc<ProxyOrchestrator>,
}

impl Socks5Server {
    pub fn new(port: u16, orchestrator: Arc<ProxyOrchestrator>) -> Self {
        Self { port, orchestrator }
    }

    pub async fn run(self) -> Result<(), crate::error::DaemonError> {
        let addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("SOCKS5 Proxy listening on {}", addr);

        loop {
            match listener.accept().await {
                Ok((stream, _peer_addr)) => {
                    let orch = self.orchestrator.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, orch).await {
                            debug!("SOCKS5 client error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept SOCKS5 connection: {}", e);
                }
            }
        }
    }
}

async fn handle_client(
    mut stream: TcpStream,
    orchestrator: Arc<ProxyOrchestrator>,
) -> Result<(), crate::error::DaemonError> {
    // 1. Handshake
    let mut header = [0u8; 2];
    stream.read_exact(&mut header).await?;
    if header[0] != 0x05 {
        return Err(crate::error::DaemonError::Socks5(format!(
            "Invalid SOCKS version: {}",
            header[0]
        )));
    }

    let num_methods = header[1] as usize;
    let mut methods = vec![0u8; num_methods];
    stream.read_exact(&mut methods).await?;

    // Respond with NO AUTHENTICATION REQUIRED (0x00)
    stream.write_all(&[0x05, 0x00]).await?;

    // 2. Request
    let mut req_header = [0u8; 4];
    stream.read_exact(&mut req_header).await?;
    if req_header[0] != 0x05 || req_header[1] != 0x01 || req_header[2] != 0x00 {
        // We only support CONNECT (0x01)
        return Err(crate::error::DaemonError::Socks5(
            "Unsupported SOCKS request or command".to_string(),
        ));
    }

    let atyp = req_header[3];
    let (host, port) = match atyp {
        0x01 => {
            // IPv4
            let mut ip = [0u8; 4];
            stream.read_exact(&mut ip).await?;
            let mut port_bytes = [0u8; 2];
            stream.read_exact(&mut port_bytes).await?;
            let port = u16::from_be_bytes(port_bytes);
            (Ipv4Addr::from(ip).to_string(), port)
        }
        0x03 => {
            // Domain Name
            let mut len_buf = [0u8; 1];
            stream.read_exact(&mut len_buf).await?;
            let len = len_buf[0] as usize;
            let mut domain = vec![0u8; len];
            stream.read_exact(&mut domain).await?;
            let mut port_bytes = [0u8; 2];
            stream.read_exact(&mut port_bytes).await?;
            let port = u16::from_be_bytes(port_bytes);
            (String::from_utf8_lossy(&domain).to_string(), port)
        }
        0x04 => {
            // IPv6
            let mut ip = [0u8; 16];
            stream.read_exact(&mut ip).await?;
            let mut port_bytes = [0u8; 2];
            stream.read_exact(&mut port_bytes).await?;
            let port = u16::from_be_bytes(port_bytes);
            (Ipv6Addr::from(ip).to_string(), port)
        }
        _ => {
            return Err(crate::error::DaemonError::Socks5(format!(
                "Unsupported address type: {}",
                atyp
            )));
        }
    };

    debug!("SOCKS5 CONNECT request for {}:{}", host, port);

    // Pass the stream to the orchestrator to route and forward
    orchestrator
        .handle_new_connection(stream, host, port)
        .await?;

    Ok(())
}
