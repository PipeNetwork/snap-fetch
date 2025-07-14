/// Platform-specific I/O operations that use io_uring on Linux for better performance
use anyhow::Result;
use bytes::Bytes;
use std::path::Path;

#[cfg(target_os = "linux")]
mod linux_io {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    use tokio::task;

    pub struct PlatformFile {
        fd: i32,
    }

    // SAFETY: We ensure fd is valid and only used for I/O operations
    unsafe impl Send for PlatformFile {}
    unsafe impl Sync for PlatformFile {}

    impl PlatformFile {
        pub async fn create<P: AsRef<Path>>(path: P, size: u64) -> Result<Self> {
            let path = path.as_ref().to_owned();

            // Open file in blocking task to get fd
            let fd = task::spawn_blocking(move || -> Result<i32> {
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o644)
                    .open(&path)?;

                // Pre-allocate the file
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::io::IntoRawFd;
                    // Pre-allocate to the target size
                    file.set_len(size)?;
                    // Try to use fallocate if available
                    let fd = file.as_raw_fd();
                    unsafe {
                        let ret = libc::fallocate(fd, 0, 0, size as libc::off_t);
                        if ret != 0 {
                            // Fallback to regular truncate if fallocate fails
                            file.set_len(size)?;
                        }
                    }
                    // IMPORTANT: Use into_raw_fd() to transfer ownership and prevent the file from being closed
                    Ok(file.into_raw_fd())
                }

                #[cfg(not(target_os = "linux"))]
                {
                    use std::os::unix::io::IntoRawFd;
                    Ok(file.into_raw_fd())
                }
            })
            .await??;

            Ok(PlatformFile { fd })
        }

        pub async fn write_at(&self, buf: Vec<u8>, offset: u64) -> Result<()> {
            let fd = self.fd;

            // Use pwrite for positioned writes
            task::spawn_blocking(move || -> Result<()> {
                let ret = unsafe {
                    libc::pwrite(
                        fd,
                        buf.as_ptr() as *const libc::c_void,
                        buf.len(),
                        offset as libc::off_t,
                    )
                };

                if ret < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }

                if ret as usize != buf.len() {
                    return Err(anyhow::anyhow!(
                        "Short write: expected {} bytes, wrote {}",
                        buf.len(),
                        ret
                    ));
                }

                Ok(())
            })
            .await?
        }

        pub async fn sync_all(&self) -> Result<()> {
            let fd = self.fd;
            task::spawn_blocking(move || -> Result<()> {
                let ret = unsafe { libc::fsync(fd) };
                if ret != 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                Ok(())
            })
            .await?
        }
    }

    impl Drop for PlatformFile {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod other_io {
    use super::*;
    use std::sync::Arc;
    use tokio::fs::File;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    use tokio::sync::Mutex;

    pub struct PlatformFile {
        file: Arc<Mutex<File>>,
        current_pos: Arc<Mutex<u64>>,
    }

    impl PlatformFile {
        pub async fn create<P: AsRef<Path>>(path: P, size: u64) -> Result<Self> {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.as_ref())
                .await?;

            // Pre-allocate the file
            file.set_len(size).await?;

            Ok(PlatformFile {
                file: Arc::new(Mutex::new(file)),
                current_pos: Arc::new(Mutex::new(0)),
            })
        }

        pub async fn write_at(&self, buf: Vec<u8>, offset: u64) -> Result<()> {
            let mut file = self.file.lock().await;
            let mut current_pos = self.current_pos.lock().await;

            // Only seek if necessary
            if *current_pos != offset {
                file.seek(std::io::SeekFrom::Start(offset)).await?;
            }

            file.write_all(&buf).await?;
            *current_pos = offset + buf.len() as u64;

            Ok(())
        }

        pub async fn sync_all(&self) -> Result<()> {
            let mut file = self.file.lock().await;
            file.sync_all().await?;
            Ok(())
        }
    }
}

// Re-export the platform-specific implementation
#[cfg(target_os = "linux")]
pub use linux_io::PlatformFile;

#[cfg(not(target_os = "linux"))]
pub use other_io::PlatformFile;

/// Optimized file writer that batches writes and uses platform-specific optimizations
pub struct OptimizedFileWriter {
    file: PlatformFile,
    write_buffer: Vec<u8>,
    buffer_capacity: usize,
    pending_offset: Option<u64>,
}

impl OptimizedFileWriter {
    pub async fn new<P: AsRef<Path>>(path: P, size: u64, buffer_capacity: usize) -> Result<Self> {
        let file = PlatformFile::create(path, size).await?;
        Ok(OptimizedFileWriter {
            file,
            write_buffer: Vec::with_capacity(buffer_capacity),
            buffer_capacity,
            pending_offset: None,
        })
    }

    pub async fn write_chunk(&mut self, offset: u64, data: &Bytes) -> Result<()> {
        // For Linux with direct I/O, we still benefit from some buffering
        // to reduce the number of system calls

        // If we have a pending write at a different offset, flush first
        if let Some(pending) = self.pending_offset {
            if pending + self.write_buffer.len() as u64 != offset {
                self.flush_buffer().await?;
            }
        }

        // Set pending offset if not set
        if self.pending_offset.is_none() {
            self.pending_offset = Some(offset);
        }

        // Add to buffer
        self.write_buffer.extend_from_slice(data);

        // Flush if buffer is getting full
        if self.write_buffer.len() >= self.buffer_capacity {
            self.flush_buffer().await?;
        }

        Ok(())
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if !self.write_buffer.is_empty() {
            if let Some(offset) = self.pending_offset {
                let buf = std::mem::take(&mut self.write_buffer);
                self.file.write_at(buf, offset).await?;
                self.write_buffer = Vec::with_capacity(self.buffer_capacity);
            }
            self.pending_offset = None;
        }
        Ok(())
    }

    pub async fn finish(mut self) -> Result<()> {
        // Flush any remaining buffered data
        self.flush_buffer().await?;
        self.file.sync_all().await?;
        Ok(())
    }
}

/// Get a string describing the I/O backend being used
pub fn io_backend() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "linux-optimized"
    }

    #[cfg(not(target_os = "linux"))]
    {
        "tokio"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::NamedTempFile;

    #[test]
    fn test_io_backend() {
        #[cfg(target_os = "linux")]
        assert_eq!(io_backend(), "linux-optimized");

        #[cfg(not(target_os = "linux"))]
        assert_eq!(io_backend(), "tokio");
    }

    #[tokio::test]
    async fn test_optimized_file_writer_new() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let file_size = 1024 * 1024; // 1MB
        let buffer_capacity = 4096;

        let writer = OptimizedFileWriter::new(path, file_size, buffer_capacity).await;
        assert!(writer.is_ok());

        let writer = writer.unwrap();
        assert_eq!(writer.buffer_capacity, buffer_capacity);
        assert!(writer.write_buffer.is_empty());
        assert!(writer.pending_offset.is_none());

        // Check that file was created with correct size
        let metadata = tokio::fs::metadata(path).await.unwrap();
        assert_eq!(metadata.len(), file_size);
    }

    #[tokio::test]
    async fn test_optimized_file_writer_single_chunk() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let file_size = 1024;

        let mut writer = OptimizedFileWriter::new(path, file_size, 512)
            .await
            .unwrap();

        let data = Bytes::from(vec![0x42; 256]);
        writer.write_chunk(0, &data).await.unwrap();

        // Should be buffered, not written yet
        assert_eq!(writer.write_buffer.len(), 256);
        assert_eq!(writer.pending_offset, Some(0));

        // Finish should flush the buffer
        writer.finish().await.unwrap();

        // Verify file contents
        let contents = tokio::fs::read(path).await.unwrap();
        assert_eq!(contents.len(), file_size as usize);
        assert_eq!(&contents[0..256], &vec![0x42; 256]);
    }

    #[tokio::test]
    async fn test_optimized_file_writer_multiple_chunks() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let file_size = 1024;

        let mut writer = OptimizedFileWriter::new(path, file_size, 128)
            .await
            .unwrap();

        // Write sequential chunks
        let chunk1 = Bytes::from(vec![0x01; 100]);
        let chunk2 = Bytes::from(vec![0x02; 100]);
        let chunk3 = Bytes::from(vec![0x03; 100]);

        writer.write_chunk(0, &chunk1).await.unwrap();
        writer.write_chunk(100, &chunk2).await.unwrap();
        // This should trigger a flush due to buffer size
        writer.write_chunk(200, &chunk3).await.unwrap();

        writer.finish().await.unwrap();

        // Verify file contents
        let contents = tokio::fs::read(path).await.unwrap();
        assert_eq!(&contents[0..100], &vec![0x01; 100]);
        assert_eq!(&contents[100..200], &vec![0x02; 100]);
        assert_eq!(&contents[200..300], &vec![0x03; 100]);
    }

    #[tokio::test]
    async fn test_optimized_file_writer_non_sequential() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let file_size = 1024;

        let mut writer = OptimizedFileWriter::new(path, file_size, 512)
            .await
            .unwrap();

        // Write non-sequential chunks
        let chunk1 = Bytes::from(vec![0xAA; 100]);
        let chunk2 = Bytes::from(vec![0xBB; 100]);

        writer.write_chunk(0, &chunk1).await.unwrap();
        // Non-sequential write should trigger flush
        writer.write_chunk(500, &chunk2).await.unwrap();

        writer.finish().await.unwrap();

        // Verify file contents
        let contents = tokio::fs::read(path).await.unwrap();
        assert_eq!(&contents[0..100], &vec![0xAA; 100]);
        assert_eq!(&contents[500..600], &vec![0xBB; 100]);
    }

    #[tokio::test]
    async fn test_platform_file_create() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let size = 2048;

        let file = PlatformFile::create(path, size).await;
        assert!(file.is_ok());

        // Verify file was created with correct size
        let metadata = tokio::fs::metadata(path).await.unwrap();
        assert_eq!(metadata.len(), size);
    }

    #[tokio::test]
    async fn test_platform_file_write_at() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let size = 1024;

        let file = PlatformFile::create(path, size).await.unwrap();

        // Write at different offsets
        let data1 = vec![0x11; 100];
        let data2 = vec![0x22; 100];

        file.write_at(data1.clone(), 0).await.unwrap();
        file.write_at(data2.clone(), 500).await.unwrap();
        file.sync_all().await.unwrap();

        // Verify contents
        let contents = tokio::fs::read(path).await.unwrap();
        assert_eq!(&contents[0..100], &data1);
        assert_eq!(&contents[500..600], &data2);
    }
}
