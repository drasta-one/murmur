pub mod discovery;
pub mod framing;
pub mod connection;
pub mod quic;

pub use discovery::{Discovery, PeerInfo, PeerEvent};
pub use connection::PeerConnection;
pub use quic::make_quic_endpoint;
