#![allow(dead_code)]

use std::time::{Duration, Instant};

/// Bandwidth detection and optimization utilities
#[allow(dead_code)]
pub struct BandwidthOptimizer {
    samples: Vec<(Duration, u64)>, // (time, bytes)
}

impl BandwidthOptimizer {
    pub fn new() -> Self {
        Self {
            samples: Vec::with_capacity(100),
        }
    }

    /// Record a download sample
    pub fn record_sample(&mut self, duration: Duration, bytes: u64) {
        self.samples.push((duration, bytes));

        // Keep only recent samples
        if self.samples.len() > 100 {
            self.samples.remove(0);
        }
    }

    /// Estimate current bandwidth in bytes per second
    pub fn estimate_bandwidth(&self) -> Option<f64> {
        if self.samples.len() < 3 {
            return None;
        }

        // Use recent samples for estimation
        let recent = &self.samples[self.samples.len().saturating_sub(10)..];
        let total_bytes: u64 = recent.iter().map(|(_, b)| b).sum();
        let total_time: f64 = recent.iter().map(|(d, _)| d.as_secs_f64()).sum();

        if total_time > 0.0 {
            Some(total_bytes as f64 / total_time)
        } else {
            None
        }
    }

    /// Recommend optimal settings based on detected bandwidth
    pub fn recommend_settings(&self) -> OptimalSettings {
        let bandwidth_bps = self.estimate_bandwidth().unwrap_or(100_000_000.0); // Default 100 Mbps
        let bandwidth_mbps = bandwidth_bps * 8.0 / 1_000_000.0;

        if bandwidth_mbps >= 10_000.0 {
            // 10+ Gbps - Ultra high bandwidth
            OptimalSettings {
                chunk_size: 100 * 1024 * 1024, // 100MB chunks
                chunks_per_file: 200,
                write_buffer_size: 64 * 1024 * 1024, // 64MB
                prefetch_chunks: 50,
            }
        } else if bandwidth_mbps >= 1_000.0 {
            // 1-10 Gbps - High bandwidth
            OptimalSettings {
                chunk_size: 50 * 1024 * 1024, // 50MB chunks
                chunks_per_file: 100,
                write_buffer_size: 32 * 1024 * 1024, // 32MB
                prefetch_chunks: 30,
            }
        } else if bandwidth_mbps >= 100.0 {
            // 100 Mbps - 1 Gbps - Medium bandwidth
            OptimalSettings {
                chunk_size: 20 * 1024 * 1024, // 20MB chunks
                chunks_per_file: 50,
                write_buffer_size: 16 * 1024 * 1024, // 16MB
                prefetch_chunks: 20,
            }
        } else {
            // < 100 Mbps - Low bandwidth
            OptimalSettings {
                chunk_size: 10 * 1024 * 1024, // 10MB chunks
                chunks_per_file: 20,
                write_buffer_size: 8 * 1024 * 1024, // 8MB
                prefetch_chunks: 10,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct OptimalSettings {
    pub chunk_size: u64,
    pub chunks_per_file: usize,
    pub write_buffer_size: usize,
    pub prefetch_chunks: usize,
}

/// Set optimal TCP socket options for high-bandwidth transfers
#[cfg(target_os = "linux")]
pub fn optimize_tcp_socket(socket: &std::net::TcpStream) -> std::io::Result<()> {
    use libc::{
        c_void, setsockopt, socklen_t, IPPROTO_TCP, SOL_SOCKET, SO_RCVBUF, SO_SNDBUF, TCP_NODELAY,
    };
    use std::os::unix::io::AsRawFd;

    let fd = socket.as_raw_fd();

    // Set large receive buffer (16MB)
    let rcvbuf: i32 = 16 * 1024 * 1024;
    unsafe {
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_RCVBUF,
            &rcvbuf as *const _ as *const c_void,
            std::mem::size_of::<i32>() as socklen_t,
        );
    }

    // Set large send buffer (4MB)
    let sndbuf: i32 = 4 * 1024 * 1024;
    unsafe {
        setsockopt(
            fd,
            SOL_SOCKET,
            SO_SNDBUF,
            &sndbuf as *const _ as *const c_void,
            std::mem::size_of::<i32>() as socklen_t,
        );
    }

    // Ensure TCP_NODELAY is set
    let nodelay: i32 = 1;
    unsafe {
        setsockopt(
            fd,
            IPPROTO_TCP,
            TCP_NODELAY,
            &nodelay as *const _ as *const c_void,
            std::mem::size_of::<i32>() as socklen_t,
        );
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn optimize_tcp_socket(_socket: &std::net::TcpStream) -> std::io::Result<()> {
    // Not implemented for other platforms
    Ok(())
}

/// Bandwidth test using small chunks to quickly estimate available bandwidth
pub async fn quick_bandwidth_test(
    client: &reqwest::Client,
    url: &str,
) -> Result<f64, anyhow::Error> {
    let start = Instant::now();
    let test_size = 10 * 1024 * 1024; // 10MB test

    // Request a 10MB chunk
    let response = client
        .get(url)
        .header("Range", format!("bytes=0-{}", test_size - 1))
        .send()
        .await?;

    let bytes = response.bytes().await?;
    let elapsed = start.elapsed();

    let bandwidth_bps = bytes.len() as f64 / elapsed.as_secs_f64();
    Ok(bandwidth_bps)
}

/// System tuning recommendations for maximum performance
pub fn print_system_tuning_guide() {
    println!("\n=== System Tuning for Maximum Performance ===\n");

    #[cfg(target_os = "linux")]
    {
        println!("Linux TCP tuning (run as root):");
        println!("  # Increase TCP buffer sizes");
        println!("  sysctl -w net.core.rmem_max=134217728");
        println!("  sysctl -w net.core.wmem_max=134217728");
        println!("  sysctl -w net.ipv4.tcp_rmem='4096 87380 134217728'");
        println!("  sysctl -w net.ipv4.tcp_wmem='4096 65536 134217728'");
        println!();
        println!("  # Enable TCP fast open");
        println!("  sysctl -w net.ipv4.tcp_fastopen=3");
        println!();
        println!("  # Increase connection tracking");
        println!("  sysctl -w net.netfilter.nf_conntrack_max=1048576");
        println!();
        println!("  # File descriptor limits");
        println!("  ulimit -n 1048576");
    }

    #[cfg(target_os = "macos")]
    {
        println!("macOS TCP tuning:");
        println!("  # Increase max sockets");
        println!("  sudo sysctl -w kern.ipc.maxsockbuf=16777216");
        println!("  sudo sysctl -w net.inet.tcp.sendspace=1048576");
        println!("  sudo sysctl -w net.inet.tcp.recvspace=1048576");
        println!();
        println!("  # File descriptor limits");
        println!("  ulimit -n 10240");
    }

    #[cfg(target_os = "windows")]
    {
        println!("Windows TCP tuning (run as Administrator):");
        println!("  # Enable TCP auto-tuning");
        println!("  netsh int tcp set global autotuninglevel=experimental");
        println!();
        println!("  # Disable TCP chimney offload");
        println!("  netsh int tcp set global chimney=disabled");
        println!();
        println!("  # Set receive window auto-tuning");
        println!("  netsh int tcp set global rss=enabled");
    }

    println!("\n=========================================\n");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_bandwidth_optimizer_new() {
        let optimizer = BandwidthOptimizer::new();
        assert_eq!(optimizer.samples.len(), 0);
    }

    #[test]
    fn test_record_sample() {
        let mut optimizer = BandwidthOptimizer::new();

        // Record a few samples
        optimizer.record_sample(Duration::from_secs(1), 1_000_000);
        optimizer.record_sample(Duration::from_secs(2), 2_000_000);

        assert_eq!(optimizer.samples.len(), 2);
        assert_eq!(optimizer.samples[0].1, 1_000_000);
        assert_eq!(optimizer.samples[1].1, 2_000_000);
    }

    #[test]
    fn test_record_sample_limit() {
        let mut optimizer = BandwidthOptimizer::new();

        // Record more than 100 samples
        for i in 0..110 {
            optimizer.record_sample(Duration::from_secs(1), i * 1000);
        }

        // Should only keep the last 100 samples
        assert_eq!(optimizer.samples.len(), 100);
        // First sample should be the 11th one we added (index 10)
        assert_eq!(optimizer.samples[0].1, 10_000);
    }

    #[test]
    fn test_estimate_bandwidth_insufficient_samples() {
        let mut optimizer = BandwidthOptimizer::new();

        // Less than 3 samples should return None
        assert_eq!(optimizer.estimate_bandwidth(), None);

        optimizer.record_sample(Duration::from_secs(1), 1_000_000);
        assert_eq!(optimizer.estimate_bandwidth(), None);

        optimizer.record_sample(Duration::from_secs(1), 1_000_000);
        assert_eq!(optimizer.estimate_bandwidth(), None);
    }

    #[test]
    fn test_estimate_bandwidth_calculation() {
        let mut optimizer = BandwidthOptimizer::new();

        // Record samples: 1MB in 1s = 1MB/s
        optimizer.record_sample(Duration::from_secs(1), 1_000_000);
        optimizer.record_sample(Duration::from_secs(1), 1_000_000);
        optimizer.record_sample(Duration::from_secs(1), 1_000_000);

        let bandwidth = optimizer.estimate_bandwidth().unwrap();
        // Should be approximately 1MB/s (1_000_000 bytes/second)
        assert!((bandwidth - 1_000_000.0).abs() < 0.1);
    }

    #[test]
    fn test_estimate_bandwidth_recent_samples() {
        let mut optimizer = BandwidthOptimizer::new();

        // Add old slow samples
        for _ in 0..90 {
            optimizer.record_sample(Duration::from_secs(10), 100_000);
        }

        // Add recent fast samples (last 10 will be used for estimation)
        for _ in 0..10 {
            optimizer.record_sample(Duration::from_millis(100), 10_000_000);
        }

        let bandwidth = optimizer.estimate_bandwidth().unwrap();
        // Should estimate based on last 10 samples only
        // 10 samples of 10MB in 0.1s each = 100MB total in 1s total = 100MB/s
        assert!(bandwidth > 90_000_000.0); // At least 90MB/s
    }

    #[test]
    fn test_recommend_settings_ultra_high_bandwidth() {
        let mut optimizer = BandwidthOptimizer::new();

        // Simulate 20 Gbps (2.5 GB/s)
        for _ in 0..10 {
            optimizer.record_sample(Duration::from_millis(100), 250_000_000);
        }

        let settings = optimizer.recommend_settings();
        assert_eq!(settings.chunk_size, 100 * 1024 * 1024); // 100MB
        assert_eq!(settings.chunks_per_file, 200);
        assert_eq!(settings.write_buffer_size, 64 * 1024 * 1024); // 64MB
        assert_eq!(settings.prefetch_chunks, 50);
    }

    #[test]
    fn test_recommend_settings_high_bandwidth() {
        let mut optimizer = BandwidthOptimizer::new();

        // Simulate 2 Gbps (250 MB/s)
        for _ in 0..10 {
            optimizer.record_sample(Duration::from_secs(1), 250_000_000);
        }

        let settings = optimizer.recommend_settings();
        assert_eq!(settings.chunk_size, 50 * 1024 * 1024); // 50MB
        assert_eq!(settings.chunks_per_file, 100);
        assert_eq!(settings.write_buffer_size, 32 * 1024 * 1024); // 32MB
        assert_eq!(settings.prefetch_chunks, 30);
    }

    #[test]
    fn test_recommend_settings_medium_bandwidth() {
        let mut optimizer = BandwidthOptimizer::new();

        // Simulate 500 Mbps (62.5 MB/s)
        for _ in 0..10 {
            optimizer.record_sample(Duration::from_secs(1), 62_500_000);
        }

        let settings = optimizer.recommend_settings();
        assert_eq!(settings.chunk_size, 20 * 1024 * 1024); // 20MB
        assert_eq!(settings.chunks_per_file, 50);
        assert_eq!(settings.write_buffer_size, 16 * 1024 * 1024); // 16MB
        assert_eq!(settings.prefetch_chunks, 20);
    }

    #[test]
    fn test_recommend_settings_low_bandwidth() {
        let mut optimizer = BandwidthOptimizer::new();

        // Simulate 50 Mbps (6.25 MB/s)
        for _ in 0..10 {
            optimizer.record_sample(Duration::from_secs(1), 6_250_000);
        }

        let settings = optimizer.recommend_settings();
        assert_eq!(settings.chunk_size, 10 * 1024 * 1024); // 10MB
        assert_eq!(settings.chunks_per_file, 20);
        assert_eq!(settings.write_buffer_size, 8 * 1024 * 1024); // 8MB
        assert_eq!(settings.prefetch_chunks, 10);
    }

    #[test]
    fn test_recommend_settings_no_samples() {
        let optimizer = BandwidthOptimizer::new();

        // Should default to medium bandwidth settings
        let settings = optimizer.recommend_settings();
        assert_eq!(settings.chunk_size, 20 * 1024 * 1024); // 20MB
        assert_eq!(settings.chunks_per_file, 50);
    }

    #[test]
    fn test_optimal_settings_clone() {
        let settings = OptimalSettings {
            chunk_size: 100,
            chunks_per_file: 10,
            write_buffer_size: 1024,
            prefetch_chunks: 5,
        };

        let cloned = settings.clone();
        assert_eq!(settings.chunk_size, cloned.chunk_size);
        assert_eq!(settings.chunks_per_file, cloned.chunks_per_file);
        assert_eq!(settings.write_buffer_size, cloned.write_buffer_size);
        assert_eq!(settings.prefetch_chunks, cloned.prefetch_chunks);
    }
}
