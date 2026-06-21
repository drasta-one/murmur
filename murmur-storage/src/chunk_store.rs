use murmur_core::manifest::Manifest;
use murmur_core::types::{ChunkId, ManifestId};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

pub struct ChunkStore {
    storage_dir: PathBuf,
}

impl ChunkStore {
    pub async fn new(base_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        let storage_dir = base_dir.as_ref().join("chunks");
        fs::create_dir_all(&storage_dir).await?;
        Ok(Self { storage_dir })
    }

    fn file_path(&self, manifest_id: ManifestId) -> PathBuf {
        self.storage_dir
            .join(manifest_id.0.to_string())
            .join("file.bin")
    }

    fn marker_path(&self, manifest_id: ManifestId, id: ChunkId) -> PathBuf {
        self.storage_dir
            .join(manifest_id.0.to_string())
            .join(format!("chunk_{}.marker", id.0))
    }

    pub async fn preallocate(
        &self,
        manifest_id: ManifestId,
        total_size: u64,
    ) -> anyhow::Result<()> {
        let path = self.file_path(manifest_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let file = File::create(&path).await?;
        file.set_len(total_size).await?;
        Ok(())
    }

    pub async fn write_chunk(
        &self,
        manifest_id: ManifestId,
        id: ChunkId,
        data: &[u8],
        offset: u64,
    ) -> anyhow::Result<()> {
        let path = self.file_path(manifest_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let data = data.to_vec();
        let path_clone = path.clone();

        tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&path_clone)?;
            file.write_at(&data, offset)?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        // Write marker
        let marker = self.marker_path(manifest_id, id);
        fs::write(&marker, b"").await?;

        Ok(())
    }

    pub async fn read_chunk(
        &self,
        manifest_id: ManifestId,
        id: ChunkId,
        offset: u64,
        size: u32,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        if !self.has_chunk(manifest_id, id).await {
            return Ok(None);
        }

        let path = self.file_path(manifest_id);
        let path_clone = path.clone();

        let data = tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;
            let file = std::fs::File::open(&path_clone)?;
            let mut buf = vec![0u8; size as usize];
            file.read_exact_at(&mut buf, offset)?;
            Ok::<Vec<u8>, anyhow::Error>(buf)
        })
        .await??;

        Ok(Some(data))
    }

    pub async fn has_chunk(&self, manifest_id: ManifestId, id: ChunkId) -> bool {
        self.marker_path(manifest_id, id).exists()
    }

    pub async fn get_available_chunks(&self, manifest: &Manifest) -> Vec<ChunkId> {
        let mut available = Vec::new();
        for chunk_id in manifest.chunk_ids() {
            if self.has_chunk(manifest.id, chunk_id).await {
                available.push(chunk_id);
            }
        }
        available
    }

    pub async fn reassemble_file(
        &self,
        manifest: &Manifest,
        out_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        if manifest.total_size == 0 {
            File::create(out_path).await?;
            info!(
                "Zero-byte file successfully created for manifest {}",
                manifest.id.0
            );
            return Ok(());
        }

        let path = self.file_path(manifest.id);
        if !path.exists() {
            anyhow::bail!("Data file missing for manifest {}", manifest.id.0);
        }
        fs::copy(&path, out_path).await?;
        info!(
            "File successfully reassembled for manifest {}",
            manifest.id.0
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use murmur_core::types::SimTime;

    #[tokio::test]
    async fn test_chunk_store_lifecycle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(temp_dir.path()).await.unwrap();

        let chunk_id = ChunkId(42);
        let data = b"hello chunk";

        let manifest_id = ManifestId::new();

        // Should not exist initially
        assert!(!store.has_chunk(manifest_id, chunk_id).await);
        let read_none = store
            .read_chunk(manifest_id, chunk_id, 0, data.len() as u32)
            .await
            .unwrap();
        assert!(read_none.is_none());

        // Write
        store
            .write_chunk(manifest_id, chunk_id, data, 0)
            .await
            .unwrap();

        // Should exist now
        assert!(store.has_chunk(manifest_id, chunk_id).await);
        let read_data = store
            .read_chunk(manifest_id, chunk_id, 0, data.len() as u32)
            .await
            .unwrap();
        assert_eq!(read_data.unwrap(), data);
    }

    #[tokio::test]
    async fn test_reassemble_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(temp_dir.path()).await.unwrap();

        // Write 3 chunks
        let d1 = b"part 1 ";
        let d2 = b"part 2 ";
        let d3 = b"part 3";
        let manifest_id = ManifestId::new();

        store
            .preallocate(manifest_id, (d1.len() + d2.len() + d3.len()) as u64)
            .await
            .unwrap();

        store
            .write_chunk(manifest_id, ChunkId(0), d1, 0)
            .await
            .unwrap();
        store
            .write_chunk(manifest_id, ChunkId(1), d2, d1.len() as u64)
            .await
            .unwrap();
        store
            .write_chunk(manifest_id, ChunkId(2), d3, (d1.len() + d2.len()) as u64)
            .await
            .unwrap();

        let manifest_bytes = d1
            .iter()
            .chain(d2.iter())
            .chain(d3.iter())
            .copied()
            .collect::<Vec<_>>();
        let mut manifest = Manifest::from_data(
            "test.txt",
            &manifest_bytes,
            7,
            murmur_core::manifest::ManifestSource::LocalFile {
                path: temp_dir.path().join("test.txt"),
            },
            SimTime::ZERO,
        );
        manifest.id = manifest_id;

        let out_path = temp_dir.path().join("out.txt");
        store.reassemble_file(&manifest, &out_path).await.unwrap();

        let assembled = tokio::fs::read(&out_path).await.unwrap();
        assert_eq!(assembled, manifest_bytes);
    }
}
