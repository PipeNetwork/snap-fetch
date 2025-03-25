# snap-fetch

A high-performance file downloader designed to fetch files listed in the JSON response from [data.pipenetwork.co/publicStatus](https://data.pipenetwork.co/publicStatus).

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

## Usage

```
snap-fetch [OPTIONS]

OPTIONS:
    -o, --output-dir <OUTPUT_DIR>       Output directory for downloaded files [default: downloads]
    -c, --concurrency <CONCURRENCY>     Number of concurrent downloads [default: 10]
    -s, --chunk-size <CHUNK_SIZE>       Size of each byte range request in bytes [default: 10485760 (10MB)]
    --chunks-per-file <CHUNKS_PER_FILE> Number of concurrent chunk downloads per file [default: 20]
    --timeout <TIMEOUT>                 Connection timeout in seconds [default: 300]
    --max-retries <MAX_RETRIES>         Maximum number of retries for failed chunks [default: 3]
    --status-url <STATUS_URL>           Custom URL for the status JSON
    --skip-existing                     Skip files that already exist in the output directory
    --base-url <BASE_URL>               Substitute base URL for downloads (e.g., http://207.121.20.172:8080)
    --no-range                          Disable Range requests for servers that don't support them
    --latest-only                       Only download the latest snapshot and newer incremental snapshots [default: true]
    --save-hash-name                    Save files using the hash from the URL instead of the original filename
    --dynamic-chunks                    Enable adaptive chunk sizing based on file size [default: true]
    -h, --help                          Print help
    -V, --version                       Print version
```

## Examples

Download all files with default settings:

```
snap-fetch
```

Use 20 concurrent downloads with 20MB chunks and 30 concurrent chunks per file:

```
snap-fetch -c 20 -s 20971520 --chunks-per-file 30
```

Save files to a specific directory:

```
snap-fetch -o /path/to/downloads
```

Skip files that already exist:

```
snap-fetch --skip-existing
```

Use an alternative server to download files:

```
snap-fetch --base-url "http://207.121.20.172:8080"
```

Use an alternative server without Range requests:

```
snap-fetch --base-url "http://207.121.20.172:8080" --no-range
```

Download all files, not just the latest snapshot:

```
snap-fetch --latest-only=false
```

## Building

```
cargo build --release
```

The optimized binary will be in `target/release/snap-fetch`

## Performance Tuning

For maximum performance on high-bandwidth connections:

1. Increase the concurrency (`-c`) based on available CPU cores
2. Adjust chunk size (`-s`) based on latency and connection speed:
   - For high-latency connections, use larger chunks (20-50MB)
   - For low-latency connections, use smaller chunks (5-10MB)
3. Increase `--chunks-per-file` to maximize parallel downloads per file
4. For very high bandwidth connections (>10Gbps), try these settings:
   ```
   snap-fetch -c 20 -s 20971520 --chunks-per-file 50
   ```
5. If the server doesn't support Range requests, use `--no-range` flag (this will reduce performance but ensure compatibility)
6. The `--dynamic-chunks` option (enabled by default) automatically optimizes chunk sizes:
   - Small files (<100MB): Uses 5MB chunks for faster startup
   - Medium files (<1GB): Uses the default chunk size (10MB or user-specified)
   - Large files (>1GB): Uses larger 20MB chunks to reduce HTTP overhead

## Memory Considerations

When downloading very large files (>50GB), be aware of memory usage:

- The current implementation stores all chunks in memory before writing to disk
- A 90GB file will require approximately 90GB of RAM during download
- For systems with limited memory, consider using smaller chunks and fewer concurrent chunks per file
- Future versions may implement streaming writes to reduce memory requirements

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
