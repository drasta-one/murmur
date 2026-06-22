use crate::error::NetError;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::net::SocketAddr;
use std::sync::Arc;

/// Generates a self-signed TOFU certificate for QUIC.
pub fn generate_self_signed_cert()
-> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), NetError> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let key = cert.key_pair.serialize_der();
    let cert_der = cert.cert.der().clone();
    let private_key = PrivateKeyDer::try_from(key).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Failed to parse private key",
        )
    })?;
    Ok((cert_der.into_owned(), private_key))
}

/// A dummy verifier that accepts any certificate (TOFU/P2P model)
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
        ]
    }
}

pub fn make_quic_endpoint(bind_addr: SocketAddr) -> Result<Endpoint, NetError> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let (cert, key) = generate_self_signed_cert()?;

    // Server Config
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)?;
    server_crypto.alpn_protocols = vec![b"murmur-quic".to_vec()];

    let quic_server_config = QuicServerConfig::try_from(server_crypto)?;
    let server_config = ServerConfig::with_crypto(Arc::new(quic_server_config));

    // Client Config
    let mut client_crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"murmur-quic".to_vec()];

    let quic_client_config = QuicClientConfig::try_from(client_crypto)?;
    let client_config = ClientConfig::new(Arc::new(quic_client_config));

    let mut endpoint = Endpoint::server(server_config, bind_addr)?;
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_quic_connection() -> Result<(), NetError> {
        let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server_endpoint = make_quic_endpoint(server_addr)?;
        let bound_addr = server_endpoint.local_addr()?;

        let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let client_endpoint = make_quic_endpoint(client_addr)?;

        // Spawn server accept task
        let server_task = tokio::spawn(async move {
            let incoming = server_endpoint.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();

            let mut buf = [0u8; 10];
            recv.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"helloworld");

            send.write_all(b"helloworld").await.unwrap();
            send.finish().unwrap();
            sleep(Duration::from_millis(100)).await;
        });

        // Client connect
        let connection = client_endpoint.connect(bound_addr, "localhost")?.await?;
        let (mut send, mut recv) = connection.open_bi().await?;

        send.write_all(b"helloworld").await.unwrap();
        send.finish().unwrap();

        let mut buf = [0u8; 10];
        recv.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"helloworld");

        server_task.await?;
        Ok(())
    }
}
