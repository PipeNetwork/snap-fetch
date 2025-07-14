use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use futures::{stream, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{debug, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

mod bandwidth_optimizer;

mod platform_io;

const STATUS_URL: &str = "https://data.pipenetwork.com/publicStatus";
const DEFAULT_CONCURRENCY: usize = 30;
const DEFAULT_RANGE_SIZE: u64 = 20 * 1024 * 1024; // 20MB chunks (increased from 10MB)
const MAX_RETRIES: u32 = 3;

// Bandwidth-aware defaults
#[allow(dead_code)]
const DEFAULT_CHUNKS_PER_FILE_LOW_BW: usize = 20; // < 100 Mbps
const DEFAULT_CHUNKS_PER_FILE_MED_BW: usize = 50; // 100 Mbps - 1 Gbps
#[allow(dead_code)]
const DEFAULT_CHUNKS_PER_FILE_HIGH_BW: usize = 100; // > 1 Gbps

#[derive(Debug, Deserialize)]
struct StatusResponse {
    files: Vec<FileInfo>,
}

#[derive(Debug, Clone, Deserialize)]
struct FileInfo {
    filename: String,
    public_url: String,
    // Include other fields as needed
}

#[derive(Parser, Debug)]
#[clap(name = "snap-fetch", about = "High performance file downloader")]
struct Args {
    /// Output directory for downloaded files
    #[clap(short, long, default_value = "downloads")]
    output_dir: String,

    /// Number of concurrent downloads
    #[clap(short, long, default_value_t = DEFAULT_CONCURRENCY)]
    concurrency: usize,

    /// Size of each byte range request in bytes
    #[clap(short = 's', long, default_value_t = DEFAULT_RANGE_SIZE)]
    chunk_size: u64,

    /// Number of concurrent chunk downloads per file
    #[clap(long, default_value_t = DEFAULT_CHUNKS_PER_FILE_MED_BW)]
    chunks_per_file: usize,

    /// Connection timeout in seconds
    #[clap(long, default_value_t = 300)]
    timeout: u64,

    /// Maximum number of retries for failed chunks
    #[clap(long, default_value_t = MAX_RETRIES)]
    max_retries: u32,

    /// Custom URL for the status JSON (default: https://data.pipenetwork.com/publicStatus)
    #[clap(long)]
    status_url: Option<String>,

    /// Skip files that already exist in the output directory
    #[clap(long)]
    skip_existing: bool,

    /// Substitute base URL for downloads (e.g., http://207.121.20.172:8080)
    #[clap(long)]
    base_url: Option<String>,

    /// Disable Range requests and download files in a single request
    #[clap(long)]
    no_range: bool,

    /// Only download the latest snapshot and newer incremental snapshots
    #[clap(long, default_value_t = true)]
    latest_only: bool,

    /// Save files using the hash from the URL instead of the original filename
    #[clap(long)]
    save_hash_name: bool,

    /// Enable dynamic chunk sizing based on file size
    #[clap(long, default_value_t = true)]
    dynamic_chunks: bool,

    /// Enable aggressive mode for maximum bandwidth utilization (auto-tunes all settings)
    #[clap(long)]
    aggressive: bool,

    /// Show system tuning recommendations for maximum performance
    #[clap(long)]
    show_tuning: bool,

    /// Reduce output to warnings and errors only
    #[clap(short = 'q', long, conflicts_with = "verbose")]
    quiet: bool,

    /// Show verbose output including debug information
    #[clap(short = 'v', long, conflicts_with = "quiet")]
    verbose: bool,

    /// Disable progress bars
    #[clap(long)]
    no_progress: bool,
}

// Function to extract snapshot number from a filename
fn extract_snapshot_number(filename: &str) -> Option<u64> {
    // For full snapshots (format: snapshot-NUMBER-...)
    if let Some(pos) = filename.find("snapshot-") {
        let rest = &filename[pos + "snapshot-".len()..];
        if let Some(end) = rest.find('-') {
            if let Ok(num) = rest[..end].parse::<u64>() {
                return Some(num);
            }
        }
    }
    // For incremental snapshots (format: incremental-snapshot-NUMBER-...)
    else if let Some(pos) = filename.find("incremental-snapshot-") {
        let rest = &filename[pos + "incremental-snapshot-".len()..];
        if let Some(end) = rest.find('-') {
            if let Ok(num) = rest[..end].parse::<u64>() {
                return Some(num);
            }
        }
    }
    None
}

// Function to extract end snapshot number from an incremental snapshot
fn extract_end_snapshot_number(filename: &str) -> Option<u64> {
    if let Some(pos) = filename.find("incremental-snapshot-") {
        let rest = &filename[pos + "incremental-snapshot-".len()..];
        if let Some(first_dash) = rest.find('-') {
            let after_first_num = &rest[first_dash + 1..];
            if let Some(second_dash) = after_first_num.find('-') {
                if let Ok(num) = after_first_num[..second_dash].parse::<u64>() {
                    return Some(num);
                }
            }
        }
    }
    None
}

// Function to determine if a file is a full snapshot
fn is_full_snapshot(filename: &str) -> bool {
    filename.starts_with("snapshot-")
}

// Function to filter out duplicate incremental snapshots by starting block number
fn filter_duplicate_incremental_snapshots(files: Vec<FileInfo>) -> Vec<FileInfo> {
    // First, separate full snapshots and incremental snapshots
    let mut full_snapshots = Vec::new();
    let mut incremental_map = HashMap::new();

    for file in files {
        if !file.filename.starts_with("incremental-snapshot-") {
            // Keep all non-incremental snapshots
            full_snapshots.push(file);
            continue;
        }

        // Extract the block numbers for incremental snapshots
        if let (Some(start), Some(end)) = (
            extract_snapshot_number(&file.filename),
            extract_end_snapshot_number(&file.filename),
        ) {
            // Group by the starting block number only
            incremental_map
                .entry(start)
                .or_insert_with(Vec::new)
                .push((end, file));
        } else {
            // If we can't parse the numbers, just keep the file
            full_snapshots.push(file);
        }
    }

    // For each group of incremental snapshots with the same starting block,
    // keep only the one with the highest ending block number
    for snapshots in incremental_map.values_mut() {
        // Sort by ending block number (highest first) and take the first one
        snapshots.sort_by(|(end1, _), (end2, _)| end2.cmp(end1));

        if let Some((_, latest)) = snapshots.first() {
            debug!(
                "Selected {} as it has the highest ending block for start block {}",
                latest.filename,
                extract_snapshot_number(&latest.filename).unwrap_or(0)
            );
            full_snapshots.push(latest.clone());
        }
    }

    full_snapshots
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    // Initialize logging with appropriate level
    let log_level = if args.quiet {
        Level::WARN
    } else if args.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Show tuning guide if requested
    if args.show_tuning {
        bandwidth_optimizer::print_system_tuning_guide();
        return Ok(());
    }

    // Override settings for aggressive mode
    if args.aggressive {
        if !args.quiet {
            info!("Aggressive mode enabled - optimizing for maximum bandwidth utilization");
        }
        args.chunks_per_file = 100; // Very aggressive parallelism
        args.chunk_size = 50 * 1024 * 1024; // 50MB chunks
        args.concurrency = 50; // More concurrent file downloads
    }

    // Create output directory if it doesn't exist
    fs::create_dir_all(&args.output_dir)?;

    debug!(
        "Starting snap-fetch with concurrency {} and chunk size {}MB (I/O backend: {})",
        args.concurrency,
        args.chunk_size / 1024 / 1024,
        platform_io::io_backend()
    );

    // Create HTTP client with more aggressive settings for high bandwidth
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(args.timeout))
        .pool_idle_timeout(std::time::Duration::from_secs(300)) // Increased from 120
        .pool_max_idle_per_host(200) // Much more aggressive connection pooling
        .http2_initial_stream_window_size(Some(1024 * 1024 * 16)) // 16MB window
        .http2_initial_connection_window_size(Some(1024 * 1024 * 64)) // 64MB window
        .http2_adaptive_window(true)
        .http2_max_frame_size(Some(1024 * 64)) // 64KB frames
        .http2_keep_alive_interval(Some(std::time::Duration::from_secs(10))) // More frequent
        .http2_keep_alive_timeout(std::time::Duration::from_secs(60))
        .http2_keep_alive_while_idle(true)
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .tcp_nodelay(true)
        .build()?;

    // Fetch and parse status JSON
    let status_url = args.status_url.as_deref().unwrap_or(STATUS_URL);
    info!("Fetching metadata from {}", status_url);
    let start = Instant::now();
    let status = client
        .get(status_url)
        .send()
        .await?
        .json::<StatusResponse>()
        .await
        .context("Failed to fetch or parse status JSON")?;

    info!("Found {} files to download", status.files.len());

    // Process files to substitute URLs if needed
    let mut processed_files = Vec::new();
    if let Some(base_url) = &args.base_url {
        debug!("Substituting base URL with: {}", base_url);
        for file in &status.files {
            let mut file_clone = file.clone();
            // Replace the domain part of the URL while keeping the path and query parameters
            if let Some(path_query) = file
                .public_url
                .strip_prefix("https://data.pipenetwork.com/")
            {
                file_clone.public_url =
                    format!("{}/{}", base_url.trim_end_matches('/'), path_query);
                debug!(
                    "URL transformed: {} -> {}",
                    file.public_url, file_clone.public_url
                );
            } else {
                // If the URL doesn't match the expected format, try a more general approach
                // Extract the path and query from the URL
                if let Ok(url) = reqwest::Url::parse(&file.public_url) {
                    let path = url.path();
                    let query = url.query().unwrap_or("");
                    let new_path = if query.is_empty() {
                        path.to_string()
                    } else {
                        format!("{}?{}", path, query)
                    };

                    file_clone.public_url =
                        format!("{}{}", base_url.trim_end_matches('/'), new_path);
                    debug!(
                        "URL transformed (general): {} -> {}",
                        file.public_url, file_clone.public_url
                    );
                } else {
                    warn!("Could not parse URL: {}", file.public_url);
                    file_clone.public_url = file.public_url.clone();
                }
            }
            processed_files.push(file_clone);
        }
    } else {
        processed_files = status.files.clone();
    }

    // Filter out duplicate incremental snapshots
    processed_files = filter_duplicate_incremental_snapshots(processed_files);
    info!(
        "After filtering duplicates: {} files to download",
        processed_files.len()
    );

    // Setup progress bars
    let multi_progress = if args.no_progress {
        MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::hidden())
    } else {
        MultiProgress::new()
    };

    // If specified, only download the latest snapshot and newer incrementals
    let mut files_to_download = Vec::new();

    if args.latest_only {
        // Find the latest full snapshot
        let mut latest_snapshot_number = 0;
        let mut latest_snapshot_index = None;

        for (i, file) in processed_files.iter().enumerate() {
            if is_full_snapshot(&file.filename) {
                if let Some(snapshot_number) = extract_snapshot_number(&file.filename) {
                    if snapshot_number > latest_snapshot_number {
                        latest_snapshot_number = snapshot_number;
                        latest_snapshot_index = Some(i);
                    }
                }
            }
        }

        if let Some(index) = latest_snapshot_index {
            info!(
                "Found latest full snapshot: {} (number: {})",
                processed_files[index].filename, latest_snapshot_number
            );

            // Add the latest full snapshot
            files_to_download.push(processed_files[index].clone());

            // Find incremental snapshots with end numbers higher than the latest snapshot
            for file in &processed_files {
                if !is_full_snapshot(&file.filename) {
                    if let Some(end_snapshot) = extract_end_snapshot_number(&file.filename) {
                        if end_snapshot > latest_snapshot_number {
                            info!(
                                "Adding newer incremental snapshot: {} (ends at: {})",
                                file.filename, end_snapshot
                            );
                            files_to_download.push(file.clone());
                        }
                    }
                }
            }

            info!(
                "Will download {} files: 1 full snapshot + {} incremental snapshots",
                files_to_download.len(),
                files_to_download.len() - 1
            );
        } else {
            warn!("No full snapshots found. Will download all files.");
            files_to_download = processed_files.clone();
        }
    } else {
        files_to_download = processed_files.clone();
    }

    let overall_progress = multi_progress.add(ProgressBar::new(files_to_download.len() as u64));
    let overall_progress = Arc::new(overall_progress);
    overall_progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files ({eta})")?
            .progress_chars("#>-"),
    );

    // Variable to hold file sizes
    let file_sizes: Arc<tokio::sync::Mutex<HashMap<String, u64>>>;

    // Filter out existing files if requested
    if args.skip_existing {
        info!("Checking existing files for size validation...");
        let mut new_files_to_download = Vec::new();

        // First, get sizes for all files (including those that might exist)
        let all_file_sizes = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let size_check_semaphore = Arc::new(Semaphore::new(args.concurrency * 2));

        let size_futures = files_to_download.iter().map(|file| {
            let client = client.clone();
            let url = file.public_url.clone();
            let filename = file.filename.clone();
            let all_file_sizes = all_file_sizes.clone();
            let semaphore = size_check_semaphore.clone();

            async move {
                let _permit = semaphore.acquire().await?;
                match get_file_size(&client, &url).await {
                    Ok(size) => {
                        let mut sizes = all_file_sizes.lock().await;
                        sizes.insert(filename.clone(), size);
                        Ok::<_, anyhow::Error>(())
                    }
                    Err(e) => {
                        warn!("Failed to get size for {}: {}", filename, e);
                        Ok(())
                    }
                }
            }
        });

        // Get all file sizes
        futures::future::join_all(size_futures).await;
        let all_sizes = all_file_sizes.lock().await.clone();

        // Now check each file
        for file in &files_to_download {
            let path = PathBuf::from(&args.output_dir).join(&file.filename);
            let should_skip = if path.exists() {
                match (path.metadata(), all_sizes.get(&file.filename)) {
                    (Ok(metadata), Some(&expected_size)) => {
                        let actual_size = metadata.len();
                        if actual_size == expected_size {
                            debug!(
                                "Skipping existing file: {} (size matches: {} bytes)",
                                file.filename, actual_size
                            );
                            true
                        } else {
                            warn!(
                                "File {} exists but size mismatch: expected {} bytes, found {} bytes. Will re-download.",
                                file.filename, expected_size, actual_size
                            );
                            false
                        }
                    }
                    (Ok(metadata), None) => {
                        // Couldn't get expected size from server, skip if file seems complete
                        let actual_size = metadata.len();
                        if actual_size > 0 {
                            warn!(
                                "Skipping existing file: {} ({} bytes) - couldn't verify size with server",
                                file.filename, actual_size
                            );
                            true
                        } else {
                            debug!("File {} is empty, will re-download", file.filename);
                            false
                        }
                    }
                    (Err(e), _) => {
                        debug!(
                            "Couldn't read metadata for {}: {}. Will re-download.",
                            file.filename, e
                        );
                        false
                    }
                }
            } else {
                false
            };

            if should_skip {
                overall_progress.inc(1);
            } else {
                new_files_to_download.push(file.clone());
            }
        }

        if !args.quiet {
            info!(
                "Will download {} files (skipping {} existing files with matching sizes)",
                new_files_to_download.len(),
                files_to_download.len() - new_files_to_download.len()
            );
        }
        files_to_download = new_files_to_download;

        // Update file_sizes to only include files we're downloading
        let mut remaining_sizes = HashMap::new();
        for file in &files_to_download {
            if let Some(&size) = all_sizes.get(&file.filename) {
                remaining_sizes.insert(file.public_url.clone(), size);
            }
        }
        file_sizes = Arc::new(tokio::sync::Mutex::new(remaining_sizes));
    } else {
        // Parallel HEAD requests to get file sizes ahead of time
        // This avoids sequential HEAD requests during download
        debug!("Fetching file sizes in parallel...");
        let file_sizes_mutex = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        // Use a semaphore to control concurrency of HEAD requests
        let head_semaphore = Arc::new(Semaphore::new(args.concurrency * 2)); // Use more concurrency for HEAD requests

        let head_futures = files_to_download.iter().map(|file| {
            let client = client.clone();
            let url = file.public_url.clone();
            let filename = file.filename.clone();
            let file_sizes = file_sizes_mutex.clone();
            let semaphore = head_semaphore.clone();

            async move {
                let _permit = semaphore.acquire().await?;
                match get_file_size(&client, &url).await {
                    Ok(size) => {
                        let mut sizes = file_sizes.lock().await;
                        sizes.insert(url.clone(), size);
                        Ok::<_, anyhow::Error>(())
                    }
                    Err(e) => {
                        warn!("Failed to get size for {}: {}", filename, e);
                        Ok(())
                    }
                }
            }
        });

        // Execute all HEAD requests in parallel
        futures::future::join_all(head_futures).await;
        debug!(
            "Completed fetching file sizes for {} files",
            file_sizes_mutex.lock().await.len()
        );

        file_sizes = file_sizes_mutex;
    }

    // Setup concurrency control for downloads
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let client = Arc::new(client);
    let file_sizes = Arc::new(file_sizes.lock().await.clone()); // Convert to immutable

    // Create file progress style once, outside the closure
    let file_progress_style = ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} {msg} ({eta})")?
        .progress_chars("#>-");

    // Process all files concurrently with controlled parallelism
    let results = stream::iter(files_to_download.iter().cloned())
        .map(|file| {
            let client = client.clone();
            let semaphore = semaphore.clone();
            let output_dir = args.output_dir.clone();
            let chunk_size = args.chunk_size;
            let chunks_per_file = args.chunks_per_file;
            let max_retries = args.max_retries;
            let file_progress = multi_progress.add(ProgressBar::new(0));
            file_progress.set_style(file_progress_style.clone());
            let overall_progress = overall_progress.clone();
            let no_range = args.no_range;
            let save_hash_name = args.save_hash_name;
            let file_sizes = file_sizes.clone();

            async move {
                let permit = semaphore.acquire().await?;
                let result = download_file(
                    &client,
                    &file,
                    &output_dir,
                    chunk_size,
                    chunks_per_file,
                    max_retries,
                    &file_progress,
                    no_range,
                    save_hash_name,
                    &file_sizes,
                    args.dynamic_chunks,
                )
                .await;
                file_progress.finish_with_message(if result.is_ok() {
                    format!("Downloaded {}", file.filename)
                } else {
                    format!("Failed {}", file.filename)
                });
                overall_progress.inc(1);
                drop(permit);
                result
            }
        })
        .buffer_unordered(args.concurrency * 3) // Increased buffering for higher throughput
        .collect::<Vec<_>>()
        .await;

    overall_progress.finish_with_message(format!("Completed in {:.2?}", start.elapsed()));

    // Count successes and failures
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();

    if !args.quiet {
        info!(
            "Download complete: {} successes, {} failures",
            successes, failures
        );
    }

    // Print out any failures
    if failures > 0 {
        for (i, result) in results.iter().enumerate().filter(|(_, r)| r.is_err()) {
            if let Err(e) = result {
                warn!("Failure #{}: {:#}", i + 1, e);
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn download_file(
    client: &Client,
    file: &FileInfo,
    output_dir: &str,
    chunk_size: u64,
    chunks_per_file: usize,
    max_retries: u32,
    progress: &ProgressBar,
    no_range: bool,
    save_hash_name: bool,
    file_sizes: &HashMap<String, u64>,
    dynamic_chunks: bool,
) -> Result<()> {
    let filename = file.filename.clone();
    let url = file.public_url.clone();

    // Figure out the final filename, possibly from the `hash` parameter
    let save_filename = if save_hash_name {
        if let Ok(parsed_url) = reqwest::Url::parse(&url) {
            parsed_url
                .query_pairs()
                .find(|(key, _)| key == "hash")
                .map(|(_, value)| value.to_string())
                .unwrap_or_else(|| filename.clone())
        } else {
            filename.clone()
        }
    } else {
        filename.clone()
    };

    let output_path = PathBuf::from(output_dir).join(&save_filename);
    debug!("Starting download of {}", filename);

    // Use any HEAD-based size we already got, otherwise fetch now
    let file_size = match file_sizes.get(&url) {
        Some(size) => *size,
        None => {
            debug!("Size for {} not pre-fetched; fetching now...", filename);
            match get_file_size(client, &url).await {
                Ok(sz) => sz,
                Err(e) => {
                    warn!(
                        "Failed to get file size for {}: {}. Will attempt download anyway.",
                        filename, e
                    );
                    0
                }
            }
        }
    };

    if file_size == 0 && !no_range {
        warn!(
            "File size is 0 for {}. This might indicate a server issue.",
            filename
        );
    }

    progress.set_length(file_size);
    progress.set_message(format!(
        "{} ({:.2} MB)",
        filename,
        file_size as f64 / 1024.0 / 1024.0
    ));

    // If user disabled ranges or we don't trust the size, do single-request streaming
    if no_range || file_size == 0 {
        debug!("Using single-request download for {}", filename);
        // Create a regular file for compatibility
        let output_file = File::create(&output_path).context(format!(
            "Failed to create output file {}",
            output_path.display()
        ))?;
        let mut buffered_output = std::io::BufWriter::with_capacity(8 * 1024 * 1024, output_file);
        return download_without_ranges(client, &url, &mut buffered_output, progress, max_retries)
            .await;
    }

    // Create writer using platform-specific optimizations
    let output_path_clone = output_path.clone();

    // Use dynamic chunk logic with more aggressive sizes
    let effective_chunk_size = if dynamic_chunks {
        if file_size < 100 * 1024 * 1024 {
            // If < 100MB, use 10MB chunks (increased from 5MB)
            10 * 1024 * 1024
        } else if file_size < 1024 * 1024 * 1024 {
            // If < 1GB, use 20MB chunks
            20 * 1024 * 1024
        } else {
            // If >= 1GB, use 50MB chunks for maximum throughput
            50 * 1024 * 1024
        }
    } else {
        // If dynamic chunks are turned off, just use the user's chunk_size
        chunk_size
    };

    // Auto-scale chunks_per_file based on file size and chunk size for better bandwidth utilization
    let effective_chunks_per_file = if chunks_per_file == DEFAULT_CHUNKS_PER_FILE_MED_BW {
        // Auto-scaling only if user didn't override the default
        let total_chunks = file_size.div_ceil(effective_chunk_size);
        if total_chunks <= 10 {
            // Small file, use all chunks in parallel
            total_chunks as usize
        } else if file_size > 10 * 1024 * 1024 * 1024 {
            // Very large file (>10GB), use aggressive parallelism
            100.min(total_chunks as usize)
        } else if file_size > 1024 * 1024 * 1024 {
            // Large file (>1GB), use high parallelism
            75.min(total_chunks as usize)
        } else {
            // Medium file, use moderate parallelism
            50.min(total_chunks as usize)
        }
    } else {
        chunks_per_file
    };

    // Calculate all byte ranges
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < file_size {
        let end = std::cmp::min(start + effective_chunk_size - 1, file_size - 1);
        ranges.push((start, end));
        start = end + 1;
    }

    let total_chunks = ranges.len();
    debug!(
        "Downloading {} in {} chunks of ~{} MB each with {} parallel connections",
        filename,
        total_chunks,
        effective_chunk_size as f64 / 1024.0 / 1024.0,
        effective_chunks_per_file
    );

    // Type alias for chunk storage
    type ChunkStorage = Arc<RwLock<Vec<Option<(u64, Bytes)>>>>;

    // Pre-allocated storage for chunks with atomic tracking
    let chunks_storage: ChunkStorage = Arc::new(RwLock::new(vec![None; total_chunks]));
    let next_chunk_to_write = Arc::new(AtomicUsize::new(0));

    // Use unbounded channel for lock-free chunk notifications
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<usize>();

    // Spawn optimized writer task using platform-specific I/O
    let chunks_storage_clone = chunks_storage.clone();
    let next_to_write_clone = next_chunk_to_write.clone();
    let total_chunks_clone = total_chunks;

    let writer_handle = tokio::spawn(async move {
        // Create platform-optimized writer
        let mut writer = platform_io::OptimizedFileWriter::new(
            output_path_clone,
            file_size,
            16 * 1024 * 1024, // 16MB buffer
        )
        .await?;

        let mut consecutive_chunks: Vec<(usize, u64, Bytes)> = Vec::new();

        loop {
            // Check if we've written all chunks
            let next_idx = next_to_write_clone.load(Ordering::Acquire);
            if next_idx >= total_chunks_clone {
                break;
            }

            // Collect consecutive chunks without holding the lock too long
            {
                let chunks = chunks_storage_clone.read().await;
                let mut check_idx = next_idx;

                while check_idx < total_chunks_clone {
                    if let Some((offset, data)) = &chunks[check_idx] {
                        consecutive_chunks.push((check_idx, *offset, data.clone()));
                        check_idx += 1;
                    } else {
                        break;
                    }
                }
            }

            // Process consecutive chunks
            if !consecutive_chunks.is_empty() {
                // Clear processed chunks to free memory immediately
                {
                    let mut chunks = chunks_storage_clone.write().await;
                    for (idx, _, _) in &consecutive_chunks {
                        chunks[*idx] = None;
                    }
                }

                // Write chunks using platform-optimized I/O
                for (idx, offset, data) in consecutive_chunks.drain(..) {
                    writer.write_chunk(offset, &data).await?;

                    // Update next chunk to write
                    next_to_write_clone.store(idx + 1, Ordering::Release);
                }
            } else {
                // Wait for new chunks with shorter timeout for better responsiveness
                match tokio::time::timeout(std::time::Duration::from_millis(5), notify_rx.recv())
                    .await
                {
                    Ok(Some(_)) => continue,
                    Ok(None) => break,  // Channel closed
                    Err(_) => continue, // Timeout, check again
                }
            }
        }

        // Finish writing and sync
        writer.finish().await?;
        Ok::<_, anyhow::Error>(())
    });

    // Download chunks with optimized concurrency
    let chunk_semaphore = Arc::new(Semaphore::new(effective_chunks_per_file));
    let mut download_handles = Vec::with_capacity(total_chunks);

    // Start all chunk downloads immediately for better pipelining
    for (i, (start, end)) in ranges.into_iter().enumerate() {
        let client = client.clone();
        let url = url.clone();
        let semaphore = chunk_semaphore.clone();
        let progress = progress.clone();
        let chunks_storage = chunks_storage.clone();
        let notify_tx = notify_tx.clone();

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await?;
            match download_chunk(client, &url, start, end, i, max_retries).await {
                Ok((offset, data)) => {
                    let data_len = data.len() as u64;

                    // Store chunk with minimal locking
                    {
                        let mut chunks = chunks_storage.write().await;
                        chunks[i] = Some((offset, data));
                    }

                    // Notify writer without blocking
                    let _ = notify_tx.send(i);

                    // Update progress
                    progress.inc(data_len);
                    Ok::<_, anyhow::Error>(())
                }
                Err(e) => Err(e),
            }
        });

        download_handles.push(handle);
    }

    // Wait for all downloads to complete
    for handle in download_handles {
        handle.await??;
    }

    // Close the notification channel
    drop(notify_tx);

    // Wait for writer to finish
    writer_handle.await??;

    progress.finish_with_message(format!(
        "Downloaded {} ({:.2} MB)",
        filename,
        file_size as f64 / 1024.0 / 1024.0
    ));
    debug!("Completed download of {}", filename);

    Ok(())
}

async fn download_without_ranges(
    client: &Client,
    url: &str,
    output_file: &mut std::io::BufWriter<File>,
    progress: &ProgressBar,
    max_retries: u32,
) -> Result<()> {
    let mut attempt = 0;

    loop {
        debug!("Downloading full file from URL: {}", url);

        match client.get(url).send().await {
            Ok(response) => {
                let status = response.status();
                debug!("Full file download status: {}", status);

                if status.is_success() {
                    // Stream the response body to the file
                    let mut stream = response.bytes_stream();
                    let mut total_bytes = 0;

                    while let Some(chunk_result) = stream.next().await {
                        match chunk_result {
                            Ok(chunk) => {
                                output_file.write_all(&chunk)?;
                                let chunk_size = chunk.len() as u64;
                                total_bytes += chunk_size;
                                progress.set_position(total_bytes);
                                progress.set_length(total_bytes);
                                progress.set_message(format!(
                                    "{} ({:.2} MB)",
                                    url.split('/').next_back().unwrap_or("file"),
                                    total_bytes as f64 / 1024.0 / 1024.0
                                ));
                            }
                            Err(e) => {
                                warn!("Error reading stream chunk: {}", e);
                                break;
                            }
                        }
                    }

                    // Ensure all data is flushed to disk
                    output_file.flush()?;

                    if total_bytes > 0 {
                        debug!(
                            "Downloaded {} bytes without using Range requests",
                            total_bytes
                        );
                        progress.finish_with_message(format!(
                            "Downloaded {} ({:.2} MB)",
                            url.split('/').next_back().unwrap_or("file"),
                            total_bytes as f64 / 1024.0 / 1024.0
                        ));
                        return Ok(());
                    } else {
                        debug!("Received empty file when downloading without Range requests");
                        attempt += 1;
                        if attempt >= max_retries {
                            anyhow::bail!("Failed to download file after {} attempts, received empty response", max_retries);
                        }
                    }
                } else {
                    warn!("Failed to download file, status code: {}", status);
                    attempt += 1;
                    if attempt >= max_retries {
                        anyhow::bail!(
                            "Failed to download file after {} attempts, status: {}",
                            max_retries,
                            status
                        );
                    }
                }
            }
            Err(e) => {
                warn!("Error downloading file: {}", e);
                attempt += 1;
                if attempt >= max_retries {
                    return Err(e.into());
                }
            }
        }

        // Exponential backoff with jitter (using hash of URL for deterministic jitter)
        let url_hash = url
            .as_bytes()
            .iter()
            .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64));
        let jitter = (url_hash + attempt as u64 * 13) % 100;
        let backoff = std::time::Duration::from_millis(100 * 2u64.pow(attempt) + jitter);
        debug!(
            "Retrying full file download after {}ms",
            backoff.as_millis()
        );
        tokio::time::sleep(backoff).await;
    }
}

async fn get_file_size(client: &Client, url: &str) -> Result<u64> {
    debug!("Checking size for URL: {}", url);
    let response = client.head(url).send().await.context(format!(
        "Failed to send HEAD request for file size to URL: {}",
        url
    ))?;

    let status = response.status();
    debug!("HEAD request status: {}", status);

    if !status.is_success() {
        anyhow::bail!("Failed to get file size, status code: {}", status);
    }

    // Print all headers for debugging
    debug!("Response headers: {:?}", response.headers());

    if let Some(content_length) = response.headers().get("content-length") {
        let size = content_length.to_str()?.parse::<u64>()?;
        debug!("Content-Length found: {} bytes", size);
        if size == 0 {
            debug!("Warning: Content-Length is zero for URL: {}", url);
        }
        Ok(size)
    } else {
        debug!("Content-Length header not found for URL: {}", url);
        anyhow::bail!("Content-Length header not found")
    }
}

async fn download_chunk(
    client: Client,
    url: &str,
    start: u64,
    end: u64,
    chunk_id: usize,
    retries: u32,
) -> Result<(u64, Bytes)> {
    let mut attempt = 0;
    let range = format!("bytes={}-{}", start, end);

    loop {
        debug!(
            "Downloading chunk {} with range {} for URL: {}",
            chunk_id, range, url
        );

        let result = client.get(url).header("Range", &range).send().await;

        match result {
            Ok(response) => {
                let status = response.status();
                debug!("Chunk {} received HTTP status: {}", chunk_id, status);

                // We want 206 Partial Content for proper chunked downloads
                if status == StatusCode::PARTIAL_CONTENT {
                    let bytes = response.bytes().await?;
                    let expected_len = end - start + 1;

                    if bytes.len() as u64 != expected_len {
                        warn!(
                            "Chunk {} responded with {} bytes, expected {} (range {}-{})",
                            chunk_id,
                            bytes.len(),
                            expected_len,
                            start,
                            end
                        );
                        attempt += 1;
                        if attempt >= retries {
                            anyhow::bail!(
                                "Failed chunk {} after {} attempts. Received incomplete data.",
                                chunk_id,
                                retries
                            );
                        }
                    } else {
                        debug!(
                            "Chunk {} download complete: {} bytes (range {}-{})",
                            chunk_id,
                            bytes.len(),
                            start,
                            end
                        );
                        return Ok((start, bytes));
                    }
                } else if status == StatusCode::OK {
                    // The server might be ignoring Range headers
                    warn!(
                        "Chunk {} returned 200 OK. Server may ignore ranges. Retrying...",
                        chunk_id
                    );
                    attempt += 1;
                    if attempt >= retries {
                        anyhow::bail!(
                            "Server ignored range headers after {} attempts on chunk {}.",
                            retries,
                            chunk_id
                        );
                    }
                } else {
                    // Any other status code is treated as a failure
                    warn!("Failed chunk {} with status code {}", chunk_id, status);
                    attempt += 1;
                    if attempt >= retries {
                        anyhow::bail!(
                            "Failed to download chunk {} after {} attempts, status: {}",
                            chunk_id,
                            retries,
                            status
                        );
                    }
                }
            }
            Err(e) => {
                warn!("Error downloading chunk {}: {}", chunk_id, e);
                attempt += 1;
                if attempt >= retries {
                    return Err(e.into());
                }
            }
        }

        // Exponential backoff with jitter (deterministic based on chunk_id and attempt)
        let jitter = (chunk_id as u64 * 37 + attempt as u64 * 13) % 100;
        let backoff = std::time::Duration::from_millis(100 * 2u64.pow(attempt) + jitter);
        debug!(
            "Retrying chunk {} after {}ms",
            chunk_id,
            backoff.as_millis()
        );
        tokio::time::sleep(backoff).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_snapshot_number_full_snapshot() {
        assert_eq!(
            extract_snapshot_number("snapshot-123456-abc123def456.tar"),
            Some(123456)
        );
        assert_eq!(
            extract_snapshot_number("snapshot-789-xyz.tar.gz"),
            Some(789)
        );
        assert_eq!(extract_snapshot_number("not-a-snapshot.tar"), None);
        assert_eq!(extract_snapshot_number("snapshot-abc-def.tar"), None);
        assert_eq!(extract_snapshot_number("snapshot-"), None);
    }

    #[test]
    fn test_extract_snapshot_number_incremental() {
        assert_eq!(
            extract_snapshot_number("incremental-snapshot-100-200-abc.tar"),
            Some(100)
        );
        assert_eq!(
            extract_snapshot_number("incremental-snapshot-5000-6000-xyz.tar"),
            Some(5000)
        );
        assert_eq!(
            extract_snapshot_number("incremental-snapshot-abc-200.tar"),
            None
        );
    }

    #[test]
    fn test_extract_end_snapshot_number() {
        assert_eq!(
            extract_end_snapshot_number("incremental-snapshot-100-200-abc.tar"),
            Some(200)
        );
        assert_eq!(
            extract_end_snapshot_number("incremental-snapshot-5000-6000-xyz.tar"),
            Some(6000)
        );
        assert_eq!(
            extract_end_snapshot_number("incremental-snapshot-100-abc-def.tar"),
            None
        );
        assert_eq!(extract_end_snapshot_number("snapshot-123-456.tar"), None);
        assert_eq!(extract_end_snapshot_number("not-an-incremental.tar"), None);
    }

    #[test]
    fn test_is_full_snapshot() {
        assert!(is_full_snapshot("snapshot-123456-abc.tar"));
        assert!(is_full_snapshot("snapshot-0-xyz.tar.gz"));
        assert!(!is_full_snapshot("incremental-snapshot-100-200.tar"));
        assert!(!is_full_snapshot("not-a-snapshot.tar"));
        assert!(!is_full_snapshot("snapshotfake.tar"));
    }

    #[test]
    fn test_filter_duplicate_incremental_snapshots() {
        let files = vec![
            FileInfo {
                filename: "snapshot-1000-abc.tar".to_string(),
                public_url: "http://example.com/1".to_string(),
            },
            FileInfo {
                filename: "incremental-snapshot-1000-1100-abc.tar".to_string(),
                public_url: "http://example.com/2".to_string(),
            },
            FileInfo {
                filename: "incremental-snapshot-1000-1200-def.tar".to_string(),
                public_url: "http://example.com/3".to_string(),
            },
            FileInfo {
                filename: "incremental-snapshot-1100-1200-ghi.tar".to_string(),
                public_url: "http://example.com/4".to_string(),
            },
            FileInfo {
                filename: "snapshot-2000-xyz.tar".to_string(),
                public_url: "http://example.com/5".to_string(),
            },
        ];

        let filtered = filter_duplicate_incremental_snapshots(files);

        // Should keep both full snapshots
        assert!(filtered
            .iter()
            .any(|f| f.filename == "snapshot-1000-abc.tar"));
        assert!(filtered
            .iter()
            .any(|f| f.filename == "snapshot-2000-xyz.tar"));

        // Should keep only the incremental with highest end block for start 1000
        assert!(filtered
            .iter()
            .any(|f| f.filename == "incremental-snapshot-1000-1200-def.tar"));
        assert!(!filtered
            .iter()
            .any(|f| f.filename == "incremental-snapshot-1000-1100-abc.tar"));

        // Should keep the incremental with start 1100
        assert!(filtered
            .iter()
            .any(|f| f.filename == "incremental-snapshot-1100-1200-ghi.tar"));

        assert_eq!(filtered.len(), 4);
    }

    #[test]
    fn test_filter_duplicate_incremental_snapshots_invalid_names() {
        let files = vec![
            FileInfo {
                filename: "snapshot-abc-def.tar".to_string(),
                public_url: "http://example.com/1".to_string(),
            },
            FileInfo {
                filename: "incremental-snapshot-invalid-format.tar".to_string(),
                public_url: "http://example.com/2".to_string(),
            },
            FileInfo {
                filename: "regular-file.txt".to_string(),
                public_url: "http://example.com/3".to_string(),
            },
        ];

        let filtered = filter_duplicate_incremental_snapshots(files.clone());

        // Should keep all files when they can't be parsed
        assert_eq!(filtered.len(), files.len());
    }

    #[test]
    fn test_args_defaults() {
        let args = Args::parse_from(&["snap-fetch"]);
        assert_eq!(args.output_dir, "downloads");
        assert_eq!(args.concurrency, DEFAULT_CONCURRENCY);
        assert_eq!(args.chunk_size, DEFAULT_RANGE_SIZE);
        assert_eq!(args.chunks_per_file, DEFAULT_CHUNKS_PER_FILE_MED_BW);
        assert_eq!(args.timeout, 300);
        assert_eq!(args.max_retries, MAX_RETRIES);
        assert!(!args.skip_existing);
        assert!(!args.no_range);
        assert!(args.latest_only);
        assert!(args.dynamic_chunks);
        assert!(!args.aggressive);
        assert!(!args.quiet);
        assert!(!args.verbose);
        assert!(!args.no_progress);
    }

    #[test]
    fn test_args_custom_values() {
        let args = Args::parse_from(&[
            "snap-fetch",
            "--output-dir",
            "custom",
            "--concurrency",
            "10",
            "--chunk-size",
            "5242880",
            "--chunks-per-file",
            "25",
            "--skip-existing",
            "--quiet",
        ]);
        assert_eq!(args.output_dir, "custom");
        assert_eq!(args.concurrency, 10);
        assert_eq!(args.chunk_size, 5242880);
        assert_eq!(args.chunks_per_file, 25);
        assert!(args.skip_existing);
        assert!(args.quiet);
    }
}
