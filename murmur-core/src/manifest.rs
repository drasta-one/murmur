//! Manifest — the runtime's integrity contract.
//!
//! The Manifest defines chunk ordering, hashes, and verification state for a transfer.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::chunk::ChunkMeta;
use crate::types::{ChunkId, ManifestId, SimTime};

/// The source of a Manifest's data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManifestSource {
    /// File seeded by a local node
    LocalFile { path: PathBuf },
    /// HTTP/HTTPS URL with known size — bonded download
    HttpUrl {
        url: String,
        mirrors: Vec<String>,
        etag: Option<String>,
        last_modified: Option<String>,
    },
    /// Magnet-link-style descriptor
    DorDescriptor { info_hash: [u8; 32] },
}

/// The integrity contract for a cooperative file transfer.
///
/// A Manifest defines:
/// - The target file's total size and hash
/// - An ordered list of chunks with individual hashes
/// - Chunk size configuration
///
/// Correctness does not depend on trusting nodes — it derives from
/// deterministic verification against this manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Unique manifest identifier.
    pub id: ManifestId,
    /// Human-readable name (e.g., filename).
    pub name: String,
    /// Total file size in bytes.
    pub total_size: u64,
    /// Ordered list of chunks.
    pub chunks: Vec<ChunkMeta>,
    /// BLAKE3 hash of the entire file.
    pub file_hash: [u8; 32],
    /// Chunk size used for splitting (last chunk may be smaller).
    pub chunk_size: u32,
    /// The source of this manifest's data.
    pub source: ManifestSource,
    /// When this manifest was created (simulation time).
    pub created_at: SimTime,
}

impl Manifest {
    /// Create a manifest from raw file data.
    ///
    /// This splits the data into fixed-size chunks, computes BLAKE3 hashes
    /// for each chunk and for the entire file.
    pub fn from_data(
        name: impl Into<String>,
        data: &[u8],
        chunk_size: u32,
        source: ManifestSource,
        created_at: SimTime,
    ) -> Self {
        let file_hash = blake3::hash(data);
        let mut chunks = Vec::new();
        let mut offset = 0u64;

        for (chunk_index, chunk_data) in data.chunks(chunk_size as usize).enumerate() {
            let chunk_hash = blake3::hash(chunk_data);
            chunks.push(ChunkMeta {
                id: ChunkId(chunk_index as u32),
                offset,
                size: chunk_data.len() as u32,
                hash: *chunk_hash.as_bytes(),
            });
            offset += chunk_data.len() as u64;
        }

        Self {
            id: ManifestId::new(),
            name: name.into(),
            total_size: data.len() as u64,
            chunks,
            file_hash: *file_hash.as_bytes(),
            chunk_size,
            source,
            created_at,
        }
    }

    /// Create a manifest incrementally from a reader (useful for large files on disk).
    pub fn from_reader<R: std::io::Read>(
        name: impl Into<String>,
        reader: &mut R,
        chunk_size: u32,
        source: ManifestSource,
        created_at: SimTime,
    ) -> std::io::Result<Self> {
        let mut file_hasher = blake3::Hasher::new();
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        let mut chunk_index = 0u32;
        let mut buffer = vec![0u8; chunk_size as usize];

        loop {
            let mut read_bytes = 0;
            while read_bytes < chunk_size as usize {
                let n = reader.read(&mut buffer[read_bytes..])?;
                if n == 0 {
                    break;
                }
                read_bytes += n;
            }

            if read_bytes == 0 {
                break;
            }

            let chunk_data = &buffer[..read_bytes];
            file_hasher.update(chunk_data);

            let chunk_hash = blake3::hash(chunk_data);
            chunks.push(ChunkMeta {
                id: ChunkId(chunk_index),
                offset,
                size: read_bytes as u32,
                hash: *chunk_hash.as_bytes(),
            });

            offset += read_bytes as u64;
            chunk_index += 1;

            if read_bytes < chunk_size as usize {
                break; // EOF reached
            }
        }

        Ok(Self {
            id: ManifestId::new(),
            name: name.into(),
            total_size: offset,
            chunks,
            file_hash: *file_hasher.finalize().as_bytes(),
            chunk_size,
            source,
            created_at,
        })
    }

    /// Create a manifest incrementally from an async reader.
    pub async fn from_async_reader<R: tokio::io::AsyncRead + Unpin>(
        name: impl Into<String>,
        reader: &mut R,
        chunk_size: u32,
        source: ManifestSource,
        created_at: SimTime,
    ) -> std::io::Result<Self> {
        use tokio::io::AsyncReadExt;

        let mut file_hasher = blake3::Hasher::new();
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        let mut chunk_index = 0u32;
        let mut buffer = vec![0u8; chunk_size as usize];

        loop {
            let mut read_bytes = 0;
            while read_bytes < chunk_size as usize {
                let n = reader.read(&mut buffer[read_bytes..]).await?;
                if n == 0 {
                    break;
                }
                read_bytes += n;
            }

            if read_bytes == 0 {
                break;
            }

            let chunk_data = &buffer[..read_bytes];
            file_hasher.update(chunk_data);

            let chunk_hash = blake3::hash(chunk_data);
            chunks.push(ChunkMeta {
                id: ChunkId(chunk_index),
                offset,
                size: read_bytes as u32,
                hash: *chunk_hash.as_bytes(),
            });

            offset += read_bytes as u64;
            chunk_index += 1;

            if read_bytes < chunk_size as usize {
                break; // EOF reached
            }
        }

        let file_hash = file_hasher.finalize();
        Ok(Self {
            id: ManifestId::new(),
            name: name.into(),
            total_size: offset,
            chunks,
            file_hash: *file_hash.as_bytes(),
            chunk_size,
            source,
            created_at,
        })
    }

    /// Number of chunks in this manifest.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Get chunk metadata by ID.
    pub fn get_chunk(&self, id: ChunkId) -> Option<&ChunkMeta> {
        self.chunks.get(id.0 as usize)
    }

    /// Verify that a chunk's data matches its expected hash.
    pub fn verify_chunk(&self, chunk_id: ChunkId, data: &[u8]) -> bool {
        if let Some(chunk_meta) = self.get_chunk(chunk_id) {
            let actual_hash = blake3::hash(data);
            *actual_hash.as_bytes() == chunk_meta.hash
        } else {
            false
        }
    }

    /// Verify that reassembled file data matches the expected file hash.
    pub fn verify_file(&self, data: &[u8]) -> bool {
        if data.len() as u64 != self.total_size {
            return false;
        }
        let actual_hash = blake3::hash(data);
        *actual_hash.as_bytes() == self.file_hash
    }

    /// Get all chunk IDs in order.
    pub fn chunk_ids(&self) -> Vec<ChunkId> {
        self.chunks.iter().map(|c| c.id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_from_data() {
        let data = vec![0u8; 2_500]; // 2500 bytes
        let source = ManifestSource::LocalFile {
            path: PathBuf::from("test.bin"),
        };
        let manifest = Manifest::from_data("test.bin", &data, 1000, source, SimTime::ZERO);

        assert_eq!(manifest.chunk_count(), 3); // 1000 + 1000 + 500
        assert_eq!(manifest.total_size, 2500);
        assert_eq!(manifest.chunks[0].size, 1000);
        assert_eq!(manifest.chunks[1].size, 1000);
        assert_eq!(manifest.chunks[2].size, 500);
        assert_eq!(manifest.chunks[0].offset, 0);
        assert_eq!(manifest.chunks[1].offset, 1000);
        assert_eq!(manifest.chunks[2].offset, 2000);
    }

    #[test]
    fn chunk_verification_correct() {
        let data = b"hello world, this is DOR!";
        let source = ManifestSource::LocalFile {
            path: PathBuf::from("test.bin"),
        };
        let manifest = Manifest::from_data("test.bin", data, 10, source, SimTime::ZERO);

        // First chunk is "hello worl"
        assert!(manifest.verify_chunk(ChunkId(0), b"hello worl"));
        // Wrong data should fail
        assert!(!manifest.verify_chunk(ChunkId(0), b"wrong data"));
    }

    #[test]
    fn file_verification() {
        let data = b"complete file content";
        let source = ManifestSource::LocalFile {
            path: PathBuf::from("test.bin"),
        };
        let manifest = Manifest::from_data("test.bin", data, 10, source, SimTime::ZERO);

        assert!(manifest.verify_file(data));
        assert!(!manifest.verify_file(b"wrong content here!"));
        assert!(!manifest.verify_file(b"short"));
    }

    #[test]
    fn chunk_ids_in_order() {
        let data = vec![0u8; 3000];
        let source = ManifestSource::LocalFile {
            path: PathBuf::from("test.bin"),
        };
        let manifest = Manifest::from_data("test.bin", &data, 1000, source, SimTime::ZERO);
        let ids = manifest.chunk_ids();
        assert_eq!(ids, vec![ChunkId(0), ChunkId(1), ChunkId(2)]);
    }
}
