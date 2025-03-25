use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use futures::{stream, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rand::Rng;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::{info, warn};

const STATUS_URL: &str = "https://data.pipenetwork.co/publicStatus";
const DEFAULT_CONCURRENCY: usize = 30;
const DEFAULT_RANGE_SIZE: u64 = 10 * 1024 * 1024; // 10MB chunks
const MAX_RETRIES: u32 = 3;

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
    #[clap(long, default_value_t = 20)]
    chunks_per_file: usize,

    /// Connection timeout in seconds
    #[clap(long, default_value_t = 300)]
    timeout: u64,

    /// Maximum number of retries for failed chunks
    #[clap(long, default_value_t = MAX_RETRIES)]
    max_retries: u32,

    /// Custom URL for the status JSON (default: https://data.pipenetwork.co/publicStatus)
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
            info!(
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
    // Initialize logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Create output directory if it doesn't exist
    fs::create_dir_all(&args.output_dir)?;

    info!(
        "Starting snap-fetch with concurrency {} and chunk size {}MB",
        args.concurrency,
        args.chunk_size / 1024 / 1024
    );

    // Create HTTP client with timeout settings
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(args.timeout))
        .pool_idle_timeout(std::time::Duration::from_secs(120)) // Keep connections alive longer
        .pool_max_idle_per_host(args.concurrency * 4) // Increased from 2x to 4x
        .http2_adaptive_window(true) // Enable HTTP/2 adaptive flow control
        .http2_keep_alive_interval(Some(std::time::Duration::from_secs(30))) // Keep HTTP/2 connections alive
        .http2_keep_alive_timeout(std::time::Duration::from_secs(60))
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .tcp_nodelay(true) // Disable Nagle's algorithm for faster responses
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
        info!("Substituting base URL with: {}", base_url);
        for file in &status.files {
            let mut file_clone = file.clone();
            // Replace the domain part of the URL while keeping the path and query parameters
            if let Some(path_query) = file.public_url.strip_prefix("https://data.pipenetwork.co/") {
                file_clone.public_url =
                    format!("{}/{}", base_url.trim_end_matches('/'), path_query);
                info!(
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
                    info!(
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
    let multi_progress = MultiProgress::new();

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

    // Filter out existing files if requested
    if args.skip_existing {
        let mut new_files_to_download = Vec::new();
        for file in &files_to_download {
            let path = PathBuf::from(&args.output_dir).join(&file.filename);
            if path.exists() {
                info!("Skipping existing file: {}", file.filename);
                overall_progress.inc(1);
            } else {
                new_files_to_download.push(file.clone());
            }
        }
        info!(
            "Downloading {} new files (skipping {} existing files)",
            new_files_to_download.len(),
            files_to_download.len() - new_files_to_download.len()
        );
        files_to_download = new_files_to_download;
    }

    // Parallel HEAD requests to get file sizes ahead of time
    // This avoids sequential HEAD requests during download
    info!("Fetching file sizes in parallel...");
    let file_sizes = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    // Use a semaphore to control concurrency of HEAD requests
    let head_semaphore = Arc::new(Semaphore::new(args.concurrency * 2)); // Use more concurrency for HEAD requests

    let head_futures = files_to_download.iter().map(|file| {
        let client = client.clone();
        let url = file.public_url.clone();
        let filename = file.filename.clone();
        let file_sizes = file_sizes.clone();
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
    info!(
        "Completed fetching file sizes for {} files",
        file_sizes.lock().await.len()
    );

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

    info!(
        "Download complete: {} successes, {} failures",
        successes, failures
    );

    // Print out any failures
    for (i, result) in results.iter().enumerate().filter(|(_, r)| r.is_err()) {
        if let Err(e) = result {
            warn!("Failure #{}: {:#}", i + 1, e);
        }
    }

    Ok(())
}

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
    dynamic_chunks: bool, // <--- Make sure we have this parameter
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
    info!("Starting download of {}", filename);

    // Use any HEAD-based size we already got, otherwise fetch now
    let file_size = match file_sizes.get(&url) {
        Some(size) => *size,
        None => {
            info!("Size for {} not pre-fetched; fetching now...", filename);
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

    // Create an output file
    let output_file = File::create(&output_path).context(format!(
        "Failed to create output file {}",
        output_path.display()
    ))?;

    // Wrap in a buffered writer to reduce I/O overhead
    let mut buffered_output =
        std::io::BufWriter::with_capacity((chunk_size as usize).min(8 * 1024 * 1024), output_file);

    // If user disabled ranges or we don't trust the size, do single-request streaming
    if no_range || file_size == 0 {
        info!("Using single-request download for {}", filename);
        return download_without_ranges(client, &url, &mut buffered_output, progress, max_retries)
            .await;
    }

    // Try to pre-allocate the file if supported
    #[cfg(any(target_family = "unix", target_os = "windows"))]
    let _ = buffered_output.get_ref().set_len(file_size);

    // Use dynamic chunk logic IF `dynamic_chunks` is true:
    let effective_chunk_size = if dynamic_chunks {
        if file_size < 100 * 1024 * 1024 {
            // If < 100MB, use 5MB
            5 * 1024 * 1024
        } else if file_size < 1024 * 1024 * 1024 {
            // If < 1GB, use the user-specified chunk size (the `--chunk-size` argument)
            chunk_size
        } else {
            // If >= 1GB, switch to a bigger chunk (20MB) to reduce overhead
            20 * 1024 * 1024
        }
    } else {
        // If dynamic chunks are turned off, just use the user’s chunk_size
        chunk_size
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
    info!(
        "Downloading {} in {} chunks of ~{} MB each",
        filename,
        total_chunks,
        effective_chunk_size as f64 / 1024.0 / 1024.0
    );

    // A semaphore to limit concurrency for chunks of this file
    let chunk_semaphore = Arc::new(Semaphore::new(chunks_per_file));
    let mut all_results = Vec::new();

    // Spawn tasks for each chunk, up to chunks_per_file concurrency
    let mut stream = futures::stream::iter(ranges.into_iter().enumerate())
        .map(|(i, (start, end))| {
            let client = client.clone();
            let url = url.clone();
            let semaphore = chunk_semaphore.clone();
            let progress = progress.clone(); // in case we need to inc inside

            async move {
                let _permit = semaphore.acquire().await?;
                let result = download_chunk(client, &url, start, end, i, max_retries).await;

                // Update the progress bar with the size we got
                if let Ok((_, ref data)) = result {
                    progress.inc(data.len() as u64);
                }
                result
            }
        })
        .buffer_unordered(chunks_per_file);

    // Collect the chunks as they finish
    while let Some(result) = stream.next().await {
        match result {
            Ok(chunk_data) => {
                all_results.push(chunk_data);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    // Sort the (start, bytes) pairs by start offset
    all_results.sort_by_key(|(start, _)| *start);

    // Write them into the file at the correct offsets
    for (range_start, data) in all_results {
        buffered_output.seek(std::io::SeekFrom::Start(range_start))?;
        buffered_output.write_all(&data)?;
    }

    buffered_output.flush()?;
    progress.finish_with_message(format!(
        "Downloaded {} ({:.2} MB)",
        filename,
        file_size as f64 / 1024.0 / 1024.0
    ));
    info!("Completed download of {}", filename);

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
    let mut rng = rand::thread_rng();

    loop {
        info!("Downloading full file from URL: {}", url);

        match client.get(url).send().await {
            Ok(response) => {
                let status = response.status();
                info!("Full file download status: {}", status);

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
                                    url.split('/').last().unwrap_or("file"),
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
                        info!(
                            "Downloaded {} bytes without using Range requests",
                            total_bytes
                        );
                        progress.finish_with_message(format!(
                            "Downloaded {} ({:.2} MB)",
                            url.split('/').last().unwrap_or("file"),
                            total_bytes as f64 / 1024.0 / 1024.0
                        ));
                        return Ok(());
                    } else {
                        warn!("Received empty file when downloading without Range requests");
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

        // Exponential backoff with jitter
        let backoff =
            std::time::Duration::from_millis(100 * 2u64.pow(attempt) + rng.gen_range(0..100));
        info!(
            "Retrying full file download after {}ms",
            backoff.as_millis()
        );
        tokio::time::sleep(backoff).await;
    }
}

async fn get_file_size(client: &Client, url: &str) -> Result<u64> {
    info!("Checking size for URL: {}", url);
    let response = client.head(url).send().await.context(format!(
        "Failed to send HEAD request for file size to URL: {}",
        url
    ))?;

    let status = response.status();
    info!("HEAD request status: {}", status);

    if !status.is_success() {
        anyhow::bail!("Failed to get file size, status code: {}", status);
    }

    // Print all headers for debugging
    info!("Response headers: {:?}", response.headers());

    if let Some(content_length) = response.headers().get("content-length") {
        let size = content_length.to_str()?.parse::<u64>()?;
        info!("Content-Length found: {} bytes", size);
        if size == 0 {
            warn!("Warning: Content-Length is zero for URL: {}", url);
        }
        Ok(size)
    } else {
        warn!("Content-Length header not found for URL: {}", url);
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
    let mut rng = rand::thread_rng();

    loop {
        info!(
            "Downloading chunk {} with range {} for URL: {}",
            chunk_id, range, url
        );

        let result = client.get(url).header("Range", &range).send().await;

        match result {
            Ok(response) => {
                let status = response.status();
                info!("Chunk {} received HTTP status: {}", chunk_id, status);

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
                        info!(
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

        // Exponential backoff with jitter
        let backoff =
            std::time::Duration::from_millis(100 * 2u64.pow(attempt) + rng.gen_range(0..100));
        info!(
            "Retrying chunk {} after {}ms",
            chunk_id,
            backoff.as_millis()
        );
        tokio::time::sleep(backoff).await;
    }
}
