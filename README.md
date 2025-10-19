# Reddit Dump Filter - Parallel Processing Edition

A high-performance, memory-efficient script for filtering Reddit dump files (`.zst` compressed) and outputting results to Parquet format with support for parallel processing, checkpointing, and configurable resource limits.

## Features

### 🚀 Performance
- **Parallel Processing**: Process multiple files concurrently using multiprocessing
- **Optimized Memory Usage**: Configurable RAM limits with monitoring
- **Direct Streaming**: Reads compressed files and writes directly to Parquet without intermediate steps

### 💾 Persistent Output
- **Separate Output Files**: Each input file generates its own Parquet output file
- **Checkpoint/Resume**: Automatically tracks processed files for resumable processing
- **Distributed Processing**: Perfect for processing large datasets across multiple runs on resource-constrained systems

### ⚙️ Flexible Configuration
- **Centralized Constants**: All hardcoded values extracted to top-level configuration
- **Command-Line Arguments**: Extensive CLI options for runtime configuration
- **Multiple Matching Modes**: Exact, partial, and regex-based filtering

### 🛡️ Robustness
- **Error Handling**: Comprehensive error handling and logging
- **Progress Tracking**: Real-time progress updates with statistics
- **Memory Monitoring**: Warnings when approaching memory limits

## Requirements

```bash
pip install zstandard pandas pyarrow orjson psutil
```

## Usage

### Basic Usage

Process all Reddit dump files in a directory:

```bash
python reddit_zst_filter.py /path/to/reddit/dumps
```

### Filter Specific Subreddits

```bash
python reddit_zst_filter.py /path/to/dumps \
    --output_dir ./results \
    --value "python,programming,machinelearning"
```

### Regex Matching

```bash
python reddit_zst_filter.py /path/to/dumps \
    --regex \
    --value "tech.*|science.*|data.*"
```

### Custom Memory and Process Limits

```bash
python reddit_zst_filter.py /path/to/dumps \
    --max_ram 16 \
    --processes 8 \
    --output_dir ./output
```

### Resume Processing from Checkpoint

```bash
# First run - processes some files
python reddit_zst_filter.py /path/to/dumps --output_dir ./results

# Second run - automatically resumes, skips already processed files
python reddit_zst_filter.py /path/to/dumps --output_dir ./results
```

### Partial String Matching

```bash
python reddit_zst_filter.py /path/to/dumps \
    --partial \
    --value "tech,science"
```

### Filter by Custom Field

```bash
python reddit_zst_filter.py /path/to/dumps \
    --field author \
    --value "specific_user"
```

## Command-Line Arguments

### Input/Output
- `input` (required): Input folder to recursively read .zst files from
- `--output_dir`: Output directory for Parquet files (default: `output`)

### Filtering
- `--field`: Field to filter on (default: `subreddit`)
- `--value`: Value(s) to match, comma separated, case insensitive (default: `pushshift`)
- `--value_list`: File with newline separated values to match
- `--partial`: Partial match instead of exact match
- `--regex`: Treat values as regex patterns

### File Selection
- `--file_filter`: Regex pattern filenames must match (default: `^RC_|^RS_`)

### Performance
- `--processes`: Number of parallel processes (default: 10)
- `--max_ram`: Maximum RAM usage in GB (default: 8.0)

### Checkpoint/Resume
- `--checkpoint`: Path to checkpoint file (default: `processing_checkpoint.json`)
- `--no_checkpoint`: Disable checkpoint/resume functionality

## Architecture

### Key Components

#### Configuration Constants
All hardcoded values are now centralized at the top of the file for easy modification:
- Chunk sizes for file reading
- Logging configuration
- Default processing parameters
- Memory limits and safety margins

#### Classes

**`FileReader`**: Handles reading and decompressing .zst files
- Efficient streaming decompression
- Unicode error handling
- Line-by-line iteration

**`CheckpointManager`**: Manages processing checkpoints
- Tracks processed files
- Enables resumable processing
- Automatic checkpoint saving

**`MemoryMonitor`**: Monitors memory usage
- Real-time RAM tracking
- Configurable memory limits
- Usage statistics and warnings

**`DataNormalizer`**: Handles Parquet compatibility
- Type normalization for problematic fields
- Ensures consistent data types

### Processing Flow

1. **Initialization**
   - Parse command-line arguments
   - Load checkpoint (if exists)
   - Initialize memory monitor
   - Collect input files

2. **File Discovery**
   - Recursively scan input directory
   - Filter files by pattern
   - Exclude already processed files (from checkpoint)

3. **Parallel Processing**
   - Distribute files across worker processes
   - Each process handles one file at a time
   - Each file generates a separate output Parquet file

4. **Per-File Processing**
   - Stream decompress .zst file
   - Parse JSON lines
   - Filter based on criteria
   - Write matches to individual Parquet file

5. **Progress Tracking**
   - Update checkpoint after each file
   - Log statistics and memory usage
   - Monitor RAM limits

6. **Completion**
   - Generate final statistics
   - Report total records matched
   - Display output file information

## Output Structure

Each input file generates a corresponding output file:

```
output/
├── RC_2023-01.parquet
├── RC_2023-02.parquet
├── RC_2023-03.parquet
└── processing_checkpoint.json
```

This structure allows for:
- **Iterative Processing**: Process data across multiple runs
- **Partial Results**: Access results from completed files immediately
- **Resource Management**: Avoid memory overflow from accumulating results
- **Distributed Processing**: Process different files on different machines

## Configuration Constants

Located at the top of the script for easy customization:

```python
# File Reading Configuration
CHUNK_SIZE = 2**28  # 268MB
MAX_WINDOW_SIZE = (2**29) * 2  # 1GB
ZST_MAX_WINDOW_SIZE = 2**31  # 2GB

# Processing Configuration
DEFAULT_PROCESSES = 10
DEFAULT_MAX_RAM_GB = 8.0
PROGRESS_LOG_INTERVAL = 5000000  # lines

# Output Configuration
DEFAULT_OUTPUT_DIR = "output"
PARQUET_COMPRESSION = 'snappy'
```

## Memory Management

The script includes intelligent memory management:

1. **Configurable Limits**: Set maximum RAM via `--max_ram` argument
2. **Safety Margin**: Uses 90% of max RAM to leave headroom
3. **Real-time Monitoring**: Continuous memory usage tracking
4. **Warnings**: Alerts when approaching memory limits
5. **Per-File Processing**: Each file processed independently to avoid accumulation

## Error Handling

- **Graceful Degradation**: Processing continues even if individual files fail
- **Detailed Logging**: All errors logged with context
- **Interrupt Handling**: Clean shutdown on Ctrl+C
- **Checkpoint Preservation**: Progress saved even on interruption

## Performance Optimization

1. **Parallel Processing**: Multiple files processed simultaneously
2. **Streaming Decompression**: No need to decompress entire file at once
3. **Direct Parquet Writing**: No intermediate formats
4. **Optimized JSON Parsing**: Uses `orjson` for fast parsing
5. **Single-value Optimization**: Faster path for exact single-value matching

## Logging

Logs are written to both console and file:
- Console: Real-time progress and statistics
- File: `logs/bot.log` with rotation (16MB max, 5 backups)

## Use Cases

### Distributed Processing on Limited Hardware

Process a large dataset across multiple runs:

```bash
# Run 1: Process with 4GB RAM limit
python reddit_zst_filter.py /data/reddit --max_ram 4 --processes 2

# Run 2: Resume later (automatically skips completed files)
python reddit_zst_filter.py /data/reddit --max_ram 4 --processes 2
```

### Multi-machine Processing

Split processing across multiple machines:

```bash
# Machine 1: Process first half
python reddit_zst_filter.py /data/reddit --output_dir ./results_1

# Machine 2: Process second half (different output dir)
python reddit_zst_filter.py /data/reddit --output_dir ./results_2

# Combine results later by copying Parquet files
```

### Memory-constrained Environments

Process large files without running out of memory:

```bash
python reddit_zst_filter.py /data/reddit \
    --max_ram 2 \
    --processes 1 \
    --output_dir ./results
```

## Migration from Original Script

### Key Changes

1. **Output Format**:
   - OLD: Single combined Parquet file
   - NEW: Separate Parquet file per input file

2. **Arguments**:
   - Removed: `--output`, `--batch_size`
   - Added: `--output_dir`, `--max_ram`, `--checkpoint`, `--no_checkpoint`

3. **Processing**:
   - OLD: Accumulates records in memory, periodic batch writes
   - NEW: Writes each file's results immediately

4. **Resumability**:
   - OLD: Must reprocess all files on restart
   - NEW: Automatically resumes from checkpoint

### Backward Compatibility

The script maintains all original command-line arguments except:
- `--output` replaced with `--output_dir`
- `--batch_size` removed (no longer needed)

## License

Same as original script.
