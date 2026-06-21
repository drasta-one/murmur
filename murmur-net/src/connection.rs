use bytes::BytesMut;
use murmur_core::net::NetMessage;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};

use crate::framing::PostcardCodec;

#[derive(Clone)]
pub enum TransportWriteHalf {
    Tcp(Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>),
    Quic(Arc<Mutex<quinn::SendStream>>),
}

pub enum TransportReadHalf {
    Tcp(tokio::net::tcp::OwnedReadHalf),
    Quic(quinn::RecvStream),
}

#[derive(Clone)]
pub struct PeerConnection {
    pub node_id: u64,
    write_half: TransportWriteHalf,
    read_half: Arc<Mutex<Option<TransportReadHalf>>>,
}

impl PeerConnection {
    pub fn new_tcp(node_id: u64, stream: TcpStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            node_id,
            write_half: TransportWriteHalf::Tcp(Arc::new(Mutex::new(write_half))),
            read_half: Arc::new(Mutex::new(Some(TransportReadHalf::Tcp(read_half)))),
        }
    }

    pub fn new_quic(node_id: u64, send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self {
            node_id,
            write_half: TransportWriteHalf::Quic(Arc::new(Mutex::new(send))),
            read_half: Arc::new(Mutex::new(Some(TransportReadHalf::Quic(recv)))),
        }
    }

    pub async fn send_message(&self, message: &NetMessage) -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        PostcardCodec::encode(message, &mut buf)?;

        match &self.write_half {
            TransportWriteHalf::Tcp(tcp) => {
                let mut stream = tcp.lock().await;
                stream.write_all(&buf).await?;
            }
            TransportWriteHalf::Quic(quic) => {
                let mut stream = quic.lock().await;
                stream.write_all(&buf).await?;
            }
        }
        Ok(())
    }

    pub async fn start_recv_loop(&self) -> mpsc::Receiver<NetMessage> {
        let (tx, rx) = mpsc::channel(100);

        let mut read_half = self
            .read_half
            .lock()
            .await
            .take()
            .expect("start_recv_loop called twice");

        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(4096);
            loop {
                let mut temp_buf = [0u8; 1024];
                let n = match &mut read_half {
                    TransportReadHalf::Tcp(tcp) => tcp.read(&mut temp_buf).await,
                    TransportReadHalf::Quic(quic) => quic
                        .read(&mut temp_buf)
                        .await
                        .map(|x| x.unwrap_or(0))
                        .map_err(std::io::Error::other),
                };

                let n = match n {
                    Ok(0) => break, // EOF
                    Ok(n) => n,
                    Err(_) => break,
                };

                buf.extend_from_slice(&temp_buf[..n]);

                // Try decoding frames
                loop {
                    match PostcardCodec::decode::<NetMessage>(&mut buf) {
                        Ok(Some(msg)) => {
                            if tx.send(msg).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            tracing::error!("Failed to decode frame: {}", e);
                        }
                    }
                }
            }
        });

        rx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_peer_connection_send_recv() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let conn = PeerConnection::new_tcp(1, stream);
            let mut rx = conn.start_recv_loop().await;

            if let Some(msg) = rx.recv().await {
                if let NetMessage::HeartbeatPing = msg {
                    // success
                } else {
                    panic!("Expected HeartbeatPing");
                }
            } else {
                panic!("No message received");
            }
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let conn = PeerConnection::new_tcp(2, stream);

        let msg = NetMessage::HeartbeatPing;
        conn.send_message(&msg).await.unwrap();

        server_task.await.unwrap();
    }
}
