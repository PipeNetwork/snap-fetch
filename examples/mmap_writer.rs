// Example of how memory-mapped I/O could be implemented for ultra-low memory usage
// This is a concept demonstration - not integrated into the main codebase yet

// This is an example file, not meant to be run directly.
// Add a stub main to satisfy the compiler.
fn main() {
    println!("This is an example implementation of memory-mapped I/O.");
    println!("See the code for how it could be integrated into snap-fetch.");
}

use anyhow::Result;
use memmap2::MmapMut;
use std::fs::OpenOptions;

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// A writer that uses memory-mapped I/O to write chunks directly to disk
/// without keeping them in process memory
pub struct MmapChunkWriter {
    mmap: Arc<Mutex<MmapMut>>,
    file_size: u64,
}

impl MmapChunkWriter {
    /// Create a new memory-mapped writer for a file of the given size
    pub fn new(path: &std::path::Path, file_size: u64) -> Result<Self> {
        // Create and pre-allocate the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        file.set_len(file_size)?;

        // Create memory mapping
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            mmap: Arc::new(Mutex::new(mmap)),
            file_size,
        })
    }

    /// Write a chunk at the specified offset
    pub async fn write_chunk(&self, offset: u64, data: &[u8]) -> Result<()> {
        let mut mmap = self.mmap.lock().await;

        // Ensure we don't write past the end
        let end = offset + data.len() as u64;
        if end > self.file_size {
            anyhow::bail!("Chunk would exceed file size");
        }

        // Write directly to the memory-mapped region
        let start = offset as usize;
        let end = end as usize;
        mmap[start..end].copy_from_slice(data);

        // Optionally flush this region to disk
        // This is more granular than flushing the entire file
        mmap.flush_range(start, data.len())?;

        Ok(())
    }

    /// Flush all pending writes to disk
    pub async fn flush_all(&self) -> Result<()> {
        let mmap = self.mmap.lock().await;
        mmap.flush()?;
        Ok(())
    }
}

/// Example of how the download_file function could use memory-mapped I/O
#[allow(dead_code)]
async fn download_file_with_mmap(
    _url: &str,
    output_path: &std::path::Path,
    file_size: u64,
    _chunk_size: u64,
    chunks_per_file: usize,
) -> Result<()> {
    // Create the memory-mapped writer
    let writer = Arc::new(MmapChunkWriter::new(output_path, file_size)?);

    // Channel for ordered writes (similar to the streaming approach)
    let (tx, mut rx) = mpsc::channel::<(usize, u64, bytes::Bytes)>(chunks_per_file * 2);

    // Writer task that ensures ordered writes
    let writer_clone = writer.clone();
    let writer_handle = tokio::spawn(async move {
        let mut next_chunk = 0;
        let mut pending = std::collections::BTreeMap::new();

        while let Some((chunk_idx, offset, data)) = rx.recv().await {
            pending.insert(chunk_idx, (offset, data));

            // Write any sequential chunks
            while let Some((offset, data)) = pending.remove(&next_chunk) {
                writer_clone.write_chunk(offset, &data).await?;
                next_chunk += 1;
            }
        }

        // Final flush
        writer_clone.flush_all().await?;
        Ok::<_, anyhow::Error>(())
    });

    // ... download chunks and send to writer via tx ...

    drop(tx); // Signal completion
    writer_handle.await??;

    Ok(())
}

/// Benefits of memory-mapped I/O:
///
/// 1. **Near-zero memory overhead**: The OS manages paging, so chunks are written
///    directly to disk-backed memory without keeping them in process memory.
///
/// 2. **Automatic memory management**: The OS will page out written regions as needed,
///    keeping only actively accessed regions in physical memory.
///
/// 3. **Efficient for random access**: Perfect for parallel chunk downloads that may
///    arrive out of order.
///
/// 4. **Crash recovery**: If the process crashes, already-written data is preserved
///    on disk (though may need integrity checking).
///
/// Trade-offs:
///
/// 1. **Platform differences**: Memory mapping behavior varies between OS.
///
/// 2. **File size limits**: Must know file size upfront and pre-allocate.
///
/// 3. **Error handling**: Disk full errors may manifest as segfaults if not careful.
///
/// 4. **Not suitable for pipes/streams**: Only works with seekable files.

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_mmap_writer() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let file_size = 1024 * 1024; // 1MB

        let writer = MmapChunkWriter::new(temp_file.path(), file_size)?;

        // Write some chunks
        let chunk1 = vec![1u8; 1024];
        let chunk2 = vec![2u8; 1024];

        writer.write_chunk(0, &chunk1).await?;
        writer.write_chunk(1024, &chunk2).await?;
        writer.flush_all().await?;

        // Verify the file contents
        let contents = std::fs::read(temp_file.path())?;
        assert_eq!(contents.len(), file_size as usize);
        assert_eq!(&contents[0..1024], &chunk1[..]);
        assert_eq!(&contents[1024..2048], &chunk2[..]);

        Ok(())
    }
}
