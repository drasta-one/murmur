//! URL-to-Manifest translator for bonded downloads.
//!
//! Given a URL, probes its size via HTTP HEAD and generates a `Manifest`
//! where each chunk corresponds to an HTTP byte range. This is the bridge
//! between "download this URL" and DOR's chunk-based transfer model.

use anyhow::{Context, Result, bail};
use murmur_core::chunk::ChunkMeta;
use murmur_core::manifest::Manifest;
use murmur_core::types::{ChunkId, ManifestId, SimTime};
use tracing::{info, warn};

/// Capability tier of the remote server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerTier {
    /// Full range support (Accept-Ranges: bytes + Content-Length)
    Tier1FullRange,
    /// Content-Length only (Download race possible)
    Tier2ContentLengthOnly,
    /// Chunked transfer encoding (Streaming response)
    Tier3Chunked,
    /// Redirect or auth required
    Tier4Redirect,
}

/// Metadata about a remote URL resource.
#[derive(Debug, Clone)]
pub struct UrlResourceInfo {
    /// The capability tier of the server.
    pub tier: ServerTier,
    /// Total size of the resource in bytes.
    pub total_size: u64,
    /// Whether the server supports HTTP Range requests.
    pub supports_ranges: bool,
    /// ETag header if provided by server.
    pub etag: Option<String>,
    /// Last-Modified header if provided by server.
    pub last_modified: Option<String>,
    /// The resolved content type (e.g., "application/octet-stream").
    pub content_type: Option<String>,
    /// The suggested filename from Content-Disposition, if any.
    pub suggested_filename: Option<String>,
}

/// Probe a URL via HTTP HEAD to learn its size and range support.
pub async fn probe_url(url: &str) -> Result<UrlResourceInfo> {
    let client = reqwest::Client::builder()
        .user_agent("DOR-Runtime/0.1")
        .build()
        .context("failed to build HTTP client")?;

    let response = client
        .head(url)
        .send()
        .await
        .context("HTTP HEAD request failed")?;

    if response.status().is_redirection() || response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Ok(UrlResourceInfo {
            tier: ServerTier::Tier4Redirect,
            total_size: 0,
            supports_ranges: false,
            etag: None,
            last_modified: None,
            content_type: None,
            suggested_filename: None,
        });
    }

    if !response.status().is_success() {
        bail!(
            "HTTP HEAD returned status {}: {}",
            response.status().as_u16(),
            response.status().canonical_reason().unwrap_or("unknown")
        );
    }

    let headers = response.headers();

    // Extract Content-Length
    let total_size = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    if total_size == 0 {
        bail!("Server did not return Content-Length; cannot create bonded manifest");
    }

    let supports_ranges = headers
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("bytes"));

    let etag = headers
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let last_modified = headers
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let tier = if supports_ranges && total_size > 0 {
        ServerTier::Tier1FullRange
    } else if total_size > 0 {
        ServerTier::Tier2ContentLengthOnly
    } else {
        ServerTier::Tier3Chunked
    };

    // Content-Type
    let content_type = headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Try to extract filename from Content-Disposition or URL path
    let suggested_filename = headers
        .get(reqwest::header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.split("filename=")
                .nth(1)
                .map(|f| f.trim_matches('"').to_string())
        })
        .or_else(|| {
            url.rsplit('/')
                .next()
                .filter(|s| !s.is_empty() && s.contains('.'))
                .map(|s| {
                    // Strip query string if present
                    s.split('?').next().unwrap_or(s).to_string()
                })
        });

    if !supports_ranges {
        warn!(
            url = url,
            total_size = total_size,
            "Server does not advertise Range support; bonding may fall back to single-node"
        );
    }

    info!(
        url = url,
        total_size = total_size,
        supports_ranges = supports_ranges,
        filename = ?suggested_filename,
        "URL probed successfully"
    );

    Ok(UrlResourceInfo {
        tier,
        total_size,
        supports_ranges,
        etag,
        last_modified,
        content_type,
        suggested_filename,
    })
}

/// Default granular chunk size for bonded downloads: 256 KB.
pub const DEFAULT_BONDED_CHUNK_SIZE: u32 = 262_144;

/// Generate a `Manifest` from URL metadata.
///
/// Unlike file-based manifests, the chunk hashes are initially zeroed
/// because we don't know the content yet. Each chunk's hash is filled in
/// after it's downloaded and BLAKE3-verified.
pub fn manifest_from_url_info(
    url_info: &UrlResourceInfo,
    url: &str,
    name: &str,
    chunk_size: u32,
) -> Manifest {
    let total_size = url_info.total_size;
    let mut chunks = Vec::new();
    let mut offset = 0u64;
    let mut chunk_index = 0u32;

    while offset < total_size {
        let remaining = total_size - offset;
        let this_chunk_size = remaining.min(chunk_size as u64) as u32;

        chunks.push(ChunkMeta {
            id: ChunkId(chunk_index),
            offset,
            size: this_chunk_size,
            hash: [0u8; 32], // Placeholder — filled after download
        });

        offset += this_chunk_size as u64;
        chunk_index += 1;
    }

    info!(
        name = name,
        total_size = total_size,
        chunk_count = chunks.len(),
        chunk_size = chunk_size,
        "Generated URL manifest"
    );

    Manifest {
        id: ManifestId::new(),
        name: name.to_string(),
        total_size,
        chunks,
        file_hash: [0u8; 32], // Will be computed after full download
        chunk_size,
        source: murmur_core::manifest::ManifestSource::HttpUrl {
            url: url.to_string(),
            mirrors: vec![],
            etag: url_info.etag.clone(),
            last_modified: url_info.last_modified.clone(),
        },
        created_at: SimTime::ZERO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_from_url_info_even_split() {
        let info = UrlResourceInfo {
            tier: ServerTier::Tier1FullRange,
            total_size: 5_242_880, // 5 MB
            supports_ranges: true,
            etag: None,
            last_modified: None,
            content_type: Some("application/octet-stream".into()),
            suggested_filename: Some("test.bin".into()),
        };

        let manifest = manifest_from_url_info(&info, "http://test", "test.bin", 1_048_576);

        assert_eq!(manifest.chunk_count(), 5);
        assert_eq!(manifest.total_size, 5_242_880);
        assert_eq!(manifest.chunks[0].offset, 0);
        assert_eq!(manifest.chunks[0].size, 1_048_576);
        assert_eq!(manifest.chunks[4].offset, 4_194_304);
        assert_eq!(manifest.chunks[4].size, 1_048_576);
    }

    #[test]
    fn manifest_from_url_info_uneven_split() {
        let info = UrlResourceInfo {
            tier: ServerTier::Tier1FullRange,
            total_size: 2_500_000,
            supports_ranges: true,
            etag: None,
            last_modified: None,
            content_type: None,
            suggested_filename: None,
        };

        let manifest = manifest_from_url_info(&info, "http://test", "partial.bin", 1_000_000);

        assert_eq!(manifest.chunk_count(), 3);
        assert_eq!(manifest.chunks[0].size, 1_000_000);
        assert_eq!(manifest.chunks[1].size, 1_000_000);
        assert_eq!(manifest.chunks[2].size, 500_000);
        assert_eq!(manifest.chunks[2].offset, 2_000_000);
    }

    #[test]
    fn manifest_total_size_matches() {
        let info = UrlResourceInfo {
            tier: ServerTier::Tier1FullRange,
            total_size: 10_000_000,
            supports_ranges: true,
            etag: None,
            last_modified: None,
            content_type: None,
            suggested_filename: None,
        };

        let manifest = manifest_from_url_info(&info, "http://test", "large.bin", 1_048_576);

        let total: u64 = manifest.chunks.iter().map(|c| c.size as u64).sum();
        assert_eq!(total, 10_000_000);
    }

    #[test]
    fn manifest_single_chunk() {
        let info = UrlResourceInfo {
            tier: ServerTier::Tier1FullRange,
            total_size: 500,
            supports_ranges: true,
            etag: None,
            last_modified: None,
            content_type: None,
            suggested_filename: None,
        };

        let manifest = manifest_from_url_info(&info, "http://test", "tiny.bin", 1_048_576);

        assert_eq!(manifest.chunk_count(), 1);
        assert_eq!(manifest.chunks[0].size, 500);
    }
}
