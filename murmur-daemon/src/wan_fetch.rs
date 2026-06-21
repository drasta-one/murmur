//! HTTP Range request worker for bonded downloads.
//!
//! Each node in the swarm runs this worker to download its assigned
//! byte ranges from a URL. Chunks are downloaded with HTTP Range
//! headers and BLAKE3-hashed for integrity verification.

use anyhow::{Context, Result, bail};
use tracing::{debug, info, warn};

/// Download a specific byte range from a URL.
///
/// Sends an HTTP GET request with a `Range: bytes=offset-end` header.
/// Returns the raw bytes for one chunk.
pub async fn fetch_range(
    client: &reqwest::Client,
    url: &str,
    offset: u64,
    size: u32,
    etag: Option<&str>,
) -> Result<Vec<u8>> {
    let end = offset + size as u64 - 1;
    let range_header = format!("bytes={offset}-{end}");

    debug!(
        url = url,
        offset = offset,
        size = size,
        range = %range_header,
        "Fetching byte range"
    );

    let mut req = client
        .get(url)
        .header(reqwest::header::RANGE, &range_header);

    if let Some(e) = etag {
        req = req.header(reqwest::header::IF_RANGE, e);
    }

    let response = req.send().await.context("HTTP Range GET request failed")?;

    let status = response.status();

    // 206 Partial Content is the expected response for Range requests.
    // 200 OK means the server ignored the Range header and returned the full body.
    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        let data = response
            .bytes()
            .await
            .context("failed to read response body")?;

        if data.len() != size as usize {
            warn!(
                expected = size,
                actual = data.len(),
                "Range response size mismatch (may still be valid for last chunk)"
            );
        }

        Ok(data.to_vec())
    } else if status.is_success() {
        // Server returned 200 — it doesn't support Range requests.
        bail!(
            "Server returned {} instead of 206 Partial Content; Range requests not supported",
            status.as_u16()
        );
    } else {
        bail!(
            "HTTP Range GET failed with status {}: {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("unknown")
        );
    }
}

/// Download a full URL without Range requests (fallback for single-node downloads).
pub async fn fetch_full(client: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    info!(url = url, "Fetching full URL (no Range support)");

    let response = client
        .get(url)
        .send()
        .await
        .context("HTTP GET request failed")?;

    if !response.status().is_success() {
        bail!(
            "HTTP GET failed with status {}: {}",
            response.status().as_u16(),
            response.status().canonical_reason().unwrap_or("unknown")
        );
    }

    let data = response
        .bytes()
        .await
        .context("failed to read response body")?;

    Ok(data.to_vec())
}

/// Result of downloading a chunk range, with timing information.
#[derive(Debug, Clone)]
pub struct FetchResult {
    /// The downloaded bytes.
    pub data: Vec<u8>,
    /// BLAKE3 hash of the downloaded data.
    pub hash: [u8; 32],
    /// Time taken to download in milliseconds.
    pub elapsed_ms: u64,
    /// Effective throughput in bytes per second.
    pub throughput_bps: u64,
}

/// Download a byte range and compute its BLAKE3 hash, measuring throughput.
///
/// This is the primary worker function used by each node during a
/// bonded download. It returns timing data for the coordinator to use
/// in dynamic rebalancing.
pub async fn fetch_and_hash(
    client: &reqwest::Client,
    url: &str,
    offset: u64,
    size: u32,
    etag: Option<&str>,
) -> Result<FetchResult> {
    let start = std::time::Instant::now();
    let data = fetch_range(client, url, offset, size, etag).await?;
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis() as u64;

    let hash = blake3::hash(&data);

    let throughput_bps = if elapsed_ms > 0 {
        (data.len() as u64 * 1000) / elapsed_ms
    } else {
        // Sub-millisecond download — assume instantaneous
        data.len() as u64 * 1_000_000
    };

    debug!(
        offset = offset,
        size = data.len(),
        elapsed_ms = elapsed_ms,
        throughput_mbps = throughput_bps as f64 / 125_000.0,
        "Chunk fetched and hashed"
    );

    Ok(FetchResult {
        data,
        hash: *hash.as_bytes(),
        elapsed_ms,
        throughput_bps,
    })
}

/// Concurrently fetch multiple byte ranges, with a bounded concurrency limit.
///
/// Returns results in the same order as the input assignments.
/// Each assignment is (chunk_id, offset, size).
pub async fn fetch_ranges_concurrent(
    client: &reqwest::Client,
    url: &str,
    assignments: &[(u32, u64, u32)],
    etag: Option<&str>,
    max_concurrent: usize,
) -> Vec<Result<(u32, FetchResult)>> {
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    // 1. Group assignments into contiguous blocks (up to 8MB)
    let mut sorted_assignments = assignments.to_vec();
    sorted_assignments.sort_by_key(|a| a.1);

    let mut blocks: Vec<Vec<(u32, u64, u32)>> = Vec::new();
    let mut current_block: Vec<(u32, u64, u32)> = Vec::new();
    let mut current_block_end = 0;

    // 8 MB max aggregate size limits memory usage per request
    const MAX_AGGREGATE_SIZE: u64 = 8 * 1024 * 1024;

    for ass in sorted_assignments {
        let (_id, offset, size) = ass;
        if current_block.is_empty() {
            current_block.push(ass);
            current_block_end = offset + size as u64;
        } else {
            let block_start = current_block[0].1;
            let block_size = current_block_end - block_start;

            if offset == current_block_end && (block_size + size as u64) <= MAX_AGGREGATE_SIZE {
                current_block.push(ass);
                current_block_end = offset + size as u64;
            } else {
                blocks.push(current_block);
                current_block = vec![ass];
                current_block_end = offset + size as u64;
            }
        }
    }
    if !current_block.is_empty() {
        blocks.push(current_block);
    }

    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let mut handles = Vec::with_capacity(blocks.len());

    for block in blocks {
        let client = client.clone();
        let url = url.to_string();
        let etag = etag.map(|s| s.to_string());
        let permit = semaphore.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit.acquire().await.unwrap();

            let block_start = block[0].1;
            let total_size: u32 = block.iter().map(|a| a.2).sum();

            let start_time = std::time::Instant::now();

            debug!(
                chunks = block.len(),
                total_size = total_size,
                "Starting aggregated HTTP Range fetch"
            );

            let fetch_res =
                fetch_range(&client, &url, block_start, total_size, etag.as_deref()).await;
            let elapsed_ms = start_time.elapsed().as_millis() as u64;

            let mut results = Vec::new();

            match fetch_res {
                Ok(data) => {
                    let throughput_bps = if elapsed_ms > 0 {
                        (data.len() as u64 * 1000) / elapsed_ms
                    } else {
                        data.len() as u64 * 1_000_000
                    };

                    let mut cursor = 0;
                    for &(id, _offset, size) in &block {
                        if cursor + size as usize > data.len() {
                            results
                                .push(Err(anyhow::anyhow!("Aggregated data too small for chunks")));
                            break;
                        }
                        let chunk_data = data[cursor..cursor + size as usize].to_vec();
                        let hash = *blake3::hash(&chunk_data).as_bytes();

                        results.push(Ok((
                            id,
                            FetchResult {
                                data: chunk_data,
                                hash,
                                elapsed_ms,     // Give same latency metric to all slices
                                throughput_bps, // Give same throughput metric to all slices
                            },
                        )));

                        cursor += size as usize;
                    }
                }
                Err(e) => {
                    let err_msg = format!("Aggregated fetch failed: {}", e);
                    for &(id, _, _) in &block {
                        results.push(Err(anyhow::anyhow!("{}: {}", err_msg, id)));
                    }
                }
            }
            results
        }));
    }

    let mut final_results = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(results) => final_results.extend(results),
            Err(join_err) => {
                warn!("Fetch task panicked: {}", join_err);
            }
        }
    }

    final_results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_result_throughput_calculation() {
        let result = FetchResult {
            data: vec![0u8; 1_000_000], // 1 MB
            hash: [0u8; 32],
            elapsed_ms: 1000,          // 1 second
            throughput_bps: 1_000_000, // 1 MB/s
        };
        assert_eq!(result.throughput_bps, 1_000_000);
        assert_eq!(result.data.len(), 1_000_000);
    }
}
