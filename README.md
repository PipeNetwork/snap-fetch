# snap-fetch

A high-performance file downloader designed to fetch files listed in the JSON response from [data.pipenetwork.com/publicStatus](https://data.pipenetwork.com/publicStatus).

## Solana Validator Usage

Solana validators use these snapshot files to initialize or catch up to the current blockchain state:

### Where to save the files:

Snapshots should be downloaded to your validator's snapshots directory:
```
[ledger-path]/snapshots/
```

Default ledger paths:
- Linux: `~/.local/share/solana/validator/ledger`
- macOS: `~/Library/Application Support/solana/validator/ledger`
- Custom: Many validators specify a custom ledger path with `--ledger`

### How to use with a validator:

1. Download snapshots to your ledger's snapshots directory:
   ```
   snap-fetch -o /path/to/ledger/snapshots --skip-existing
   ```

2. Start your validator with the snapshot-fetch flag:
   ```
   solana-validator --ledger /path/to/ledger --snapshot-fetch ...other-options
   ```

The Solana validator software will automatically:
- Detect the snapshots in the directory
- Load the latest full snapshot
- Apply any newer incremental snapshots
- No manual unzipping or processing is required

## Features

- Optimized for high-bandwidth connections (10-20 Gbps)
- Uses byte range requests to download files in parallel chunks
- Multi-level parallelism:
  - Multiple files downloaded concurrently
  - Each file split into multiple chunks downloaded in parallel
- Progress bars for overall download and individual files
- Configurable concurrency and chunk sizes
- Automatic retries with exponential backoff
- Pre-allocates file space for better performance
- Skip already downloaded files
- URL substitution for downloading from alternative servers
- Support for servers that don't implement Range requests
- Intelligent chunk sizing based on file size
- Connection pooling and HTTP/2 optimizations
- Fast parallel metadata retrieval
- Memory-efficient large file handling
- Buffered I/O for maximum disk performance
- Linux-specific optimizations (pwrite, fallocate) for better performance

## Usage

```
snap-fetch [OPTIONS]

OPTIONS:
    -o, --output-dir <OUTPUT_DIR>       Output directory for downloaded files [default: downloads]
    -c, --concurrency <CONCURRENCY>     Number of concurrent downloads [default: 30]
    -s, --chunk-size <CHUNK_SIZE>       Size of each byte range request in bytes [default: 20971520 (20MB)]
    --chunks-per-file <CHUNKS_PER_FILE> Number of concurrent chunk downloads per file [default: 50]
    --timeout <TIMEOUT>                 Connection timeout in seconds [default: 300]
    --max-retries <MAX_RETRIES>         Maximum number of retries for failed chunks [default: 3]
    --status-url <STATUS_URL>           Custom URL for the status JSON
    --skip-existing                     Skip files that already exist in the output directory
    --base-url <BASE_URL>               Substitute base URL for downloads (e.g., http://207.121.20.172:8080)
    --no-range                          Disable Range requests for servers that don't support them
    --latest-only                       Only download the latest snapshot and newer incremental snapshots [default: true]
    --save-hash-name                    Save files using the hash from the URL instead of the original filename
    --dynamic-chunks                    Enable adaptive chunk sizing based on file size [default: true]
    --aggressive                        Enable aggressive mode for maximum bandwidth utilization
    --show-tuning                       Show system tuning recommendations for maximum performance
    -q, --quiet                         Reduce output to warnings and errors only
    -v, --verbose                       Show verbose output including debug information
    --no-progress                       Disable progress bars
    -h, --help                          Print help
    -V, --version                       Print version
```

## Examples

Download all files with default settings (optimized for 100Mbps-1Gbps):

```
snap-fetch
```

**Maximum performance mode for high-bandwidth connections (1Gbps+):**

```
snap-fetch --aggressive
```

Use custom settings for a 10Gbps connection:

```
snap-fetch --chunks-per-file 200 --chunk-size 104857600 -c 50
```

Save files to a specific directory:

```
snap-fetch -o /path/to/downloads
```

Skip files that already exist (with size validation):

```
snap-fetch --skip-existing
```

**Note:** `--skip-existing` now validates file sizes before skipping. It will re-download files that:
- Are partially downloaded (size mismatch)
- Are empty (0 bytes)
- Have different sizes than the server reports

This ensures you never skip corrupted or incomplete downloads.

Show system tuning recommendations:

```
snap-fetch --show-tuning
```

Use an alternative server to download files:

```
snap-fetch --base-url "http://207.121.20.172:8080"
```

Download all files, not just the latest snapshot:

```
snap-fetch --latest-only=false
```

Run with minimal output (quiet mode):

```
snap-fetch --quiet
```

Run in silent mode for automation:

```
snap-fetch --quiet --no-progress
```

## Building

```
cargo build --release
```

The optimized binary will be in `target/release/snap-fetch`

## Performance Tuning

### Quick Start for High Bandwidth

For connections **1 Gbps or faster**, use aggressive mode:
```
snap-fetch --aggressive
```

### Detailed Tuning Guide

1. **View system-specific tuning recommendations:**
   ```
   snap-fetch --show-tuning
   ```

2. **Automatic optimization based on file size:**
   - The new defaults (50 chunks @ 20MB each) are optimized for 100Mbps-1Gbps
   - Auto-scaling increases parallelism up to 100 chunks for large files
   - Dynamic chunks automatically adjust: 10MB (<100MB files), 20MB (<1GB), 50MB (>1GB)

3. **Manual tuning by connection speed:**
   - **100 Mbps - 1 Gbps**: Use defaults
   - **1-10 Gbps**: `snap-fetch --aggressive` or `--chunks-per-file 100 --chunk-size 52428800`
   - **10+ Gbps**: `snap-fetch --chunks-per-file 200 --chunk-size 104857600`

4. **HTTP/2 optimizations** (automatically configured):
   - 16MB stream windows for better throughput
   - 64MB connection windows
   - 200 persistent connections per host
   - Aggressive keep-alive settings

## Memory and Performance Optimizations

The downloader uses a **streaming write architecture** that provides:

- **99%+ memory reduction**: Uses only ~200-650MB RAM regardless of file size (previously required full file size in RAM)
- **Optimized performance**: Async I/O, lock-free operations, and batch writes minimize overhead
- **Scalable**: Can download any size file on systems with limited RAM

Memory usage formula: `(chunks_per_file × chunk_size) + buffer_overhead`

For memory-constrained systems:
```bash
snap-fetch --chunks-per-file 5 --chunk-size 5242880  # Uses ~25MB active memory
```

## Hardware Requirements

For optimal performance, the following hardware specifications are recommended:

### Minimum Requirements:
- **CPU**: 4+ cores for parallel downloading operations
- **RAM**: 
  - General use: At least 16GB for small to medium files
  - **Validators: Minimum 128GB RAM required** (more is better)
- **Storage**: SSD with at least 200GB free space for snapshots
- **Network**: 1 Gbps connection or better

### Recommended for Validators:
- **CPU**: 8+ cores, modern processor (Intel Xeon, AMD Ryzen/EPYC)
- **RAM**: 
  - **Minimum 128GB RAM** (required for handling large snapshot files)
  - **Recommended: 256GB+ RAM** for optimal performance
- **Storage**: NVMe SSD with 1TB+ free space (higher endurance drives recommended)
- **Network**: 10+ Gbps connection for fast snapshot downloads

### Network Considerations:
- Fast, stable internet connection is essential
- Network bandwidth is typically the bottleneck rather than CPU
- Consider using a datacenter with good connectivity to Solana infrastructure

### Storage Considerations:
- Write speed directly impacts download performance
- RAID configurations can improve throughput
- For validators, consider separate drives for ledger and snapshots

## System Requirements

- Rust 1.60 or later
- Sufficient disk space for downloads
- Internet connection (optimal performance on 10-20 Gbps links)
- For large files (>50GB): Sufficient RAM to buffer all chunks

## Development

### Running Tests

```bash
# Run all tests
cargo test

# Run only unit tests
cargo test --bins

# Run only integration tests  
cargo test --tests

# Run with coverage (requires cargo-tarpaulin)
cargo install cargo-tarpaulin
cargo tarpaulin --verbose --all-features --workspace --timeout 120
```
