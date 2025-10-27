# Reddit Dump Extractor

Tools for filtering and extracting data from compressed Reddit dump files (.zst format).

## About

This repository provides **two separate methods** for processing large volumes of Reddit data, optimized for different resource constraints and performance requirements.

### Two Processing Methods

| Method               | File                             | Technology | RAM | Best For |
|----------------------|----------------------------------|-----------|-----|----------|
| **zstd_jq**          | `reddit_zst_filter_zstd_jq.py`   | Shell Pipes | Very Low (2-4GB) | Huge files, limited RAM |
| **zstandard_python** | `reddit_zst_filter_zstandard.py` | Python | High (8GB+) | Speed, error tracking |

---

## Installation

### macOS / Linux

```bash
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

**For reddit_zst_filter_zstd_jq method on macOS:**
```bash
brew install zstd coreutils jq
```

**For reddit_zst_filter_zstd_jq method on Linux:**
```bash
sudo apt install zstd coreutils jq
```

**For reddit_zst_filter_zstd_jq method on Windows (WSL recommended):**
```bash
wsl --install
# Inside WSL:
sudo apt update && sudo apt install zstd coreutils jq python3 python3-pip
pip3 install -r requirements.txt
```

---

## reddit_zst_filter_zstd_jq: Shell Pipes

**Run:** `reddit_zst_filter_zstd_jq.py`

Uses Unix pipes (`zstd` + `jq` + `gsplit`) for streaming processing. Minimizes RAM usage by not storing data in memory.

### Features

- Very low RAM usage (2-4GB sufficient)
- Streaming processing without memory storage
- Automatic chunking
- ⚙Optimized for huge files (>5GB)

### Usage

```bash
python reddit_zst_filter_zstd_jq.py <input_folder> [options]
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--output_dir` | Output directory | `output` |
| `--format` | Format: `parquet` or `csv` | `csv` |
| `--field` | Field to filter | `subreddit` |
| `--value` | Values (comma-separated) | `ukraine` |
| `--regex` | Enable regex patterns | `false` |
| `--file_filter` | Regex for filenames | `^RC_\|^RS_` |
| `--chunk_size` | Lines per chunk | `1000000` |
| `--config` | Path to config.json | `config.json` |

### Examples

**Basic filtering:**
```bash
python reddit_zst_filter_zstd_jq.py /path/to/dumps --value ukraine
```

**Multiple values:**
```bash
python reddit_zst_filter_zstd_jq.py /path/to/dumps \
  --value "ukraine,worldnews,europe"
```

**Regex search in text:**
```bash
python reddit_zst_filter_zstd_jq.py /path/to/dumps \
  --field body \
  --value "kyiv|war" \
  --regex
```

**Smaller chunks for limited memory:**
```bash
python reddit_zst_filter_zstd_jq.py /path/to/dumps \
  --chunk_size 500000
```

**Parquet format:**
```bash
python reddit_zst_filter_zstd_jq.py /path/to/dumps \
  --format parquet \
  --output_dir parquet_output
```

**Only posts from 2021:**
```bash
python reddit_zst_filter_zstd_jq.py /path/to/dumps \
  --file_filter "^RS_2021-"
```

### Memory Optimization via `--chunk_size`

- **250,000** — for 2GB available RAM
- **500,000** — for 3-4GB
- **1,000,000** (default) — for 4-8GB
- **2,000,000** — for 8GB+

---

## reddit_zst_filter_zstandard.py: Python

**Run:** `reddit_zst_filter_zstandard.py`

Uses the `zstandard` Python library for decompression and direct in-memory JSON processing.

### Features

- Fast streaming processing
- Detailed error tracking  
- Intelligent memory management with monitoring
- Flexible filtering (exact match or regex)

### Usage

```bash
python reddit_zst_filter_zstandard.py <input_folder> [options]
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--output_dir` | Output directory | `output` |
| `--format` | Format: `parquet` or `csv` | `csv` |
| `--field` | Field to filter | `subreddit` |
| `--value` | Values (comma-separated) | `ukraine` |
| `--regex` | Enable regex patterns | `false` |
| `--file_filter` | Regex for filenames | `^RC_\|^RS_` |
| `--config` | Path to config.json | `config.json` |

### Examples

**Filter by subreddit:**
```bash
python reddit_zst_filter_zstandard.py /path/to/dumps --value ukraine
```

**Multiple subreddits:**
```bash
python reddit_zst_filter_zstandard.py /path/to/dumps --value "python,javascript,java"
```

**Regex search:**
```bash
python reddit_zst_filter_zstandard.py /path/to/dumps \
  --field body \
  --value "^(kyiv|ukrainian)" \
  --regex
```

**Parquet format:**
```bash
python reddit_zst_filter_zstandard.py /path/to/dumps \
  --format parquet \
  --output_dir parquet_output
```

**Filter by author:**
```bash
python reddit_zst_filter_zstandard.py /path/to/dumps \
  --field author \
  --value "username"
```

**Only comments from 2023:**
```bash
python reddit_zst_filter_zstandard.py /path/to/dumps \
  --file_filter "^RC_2023-"
```

---

## Available Fields

- `author` — Comment/post author
- `body` — Text content
- `subreddit` — Subreddit name
- `created_utc` — Creation timestamp (UTC)
- `score` — Number of votes
- `id` — Unique identifier
- `link_id` — Post link ID
- `parent_id` — Parent comment ID
- `edited` — Edit timestamp
- `archived` — Archive status
- `gilded` — Number of awards

---

## Configuration

Both methods use `config.json` for global settings. Example of it you can find in config.json file.

---

## Output

Each input `.zst` file is processed separately and outputs its own file:

- **Parquet:** `output/RC_2023-01.parquet`
- **CSV:** `output/RC_2023-01.csv` or `output/RC_2023-01.csv.gz` (if compression enabled)

### Logging

Both methods log:
- Processing progress
- Memory usage (RAM, CPU)
- Records matched per file
- Final statistics (total lines, matches, errors, processing rate)

Logs are saved to `logs/reddit_filter.log` with file rotation.

---

## Method Selection Guide

### When to Use Method 1 (Shell)

✅ You have 2-4GB available RAM  
✅ Processing files larger than 5GB  
✅ Experiencing out-of-memory errors with Method 2  
✅ Need predictable, controlled memory usage  
✅ Simple filters (exact match or basic regex)  

### When to Use Method 2 (Python)

✅ You have 8GB+ available RAM  
✅ Files smaller than 5GB  
✅ Need detailed error reporting  
✅ Want maximum processing speed  
✅ Using complex regex patterns  

---

## Troubleshooting

### Out of Memory Errors

**Option 1: Use Method 1**
```bash
python reddit_zst_filter_zstd_jq.py /path/to/dumps
```

**Option 2: Reduce chunk size (Method 1)**
```bash
python reddit_zst_filter_zstd_jq.py /path/to/dumps --chunk_size 250000
```

**Option 3: Process files individually** instead of directory

**Option 4: Close other applications** to free up RAM

### Command Not Found (Method 1)

**macOS:**
```bash
brew install zstd coreutils jq
```

**Linux:**
```bash
sudo apt install zstd coreutils jq
```

**Windows:**
Use WSL (see Installation above)

### reddit_zst_filter_zstd_jq Returns Empty Results

Check that `jq` and `gsplit` are installed:
```bash
which jq gsplit
```

If `gsplit` not found, reinstall `coreutils`:
```bash
# macOS
brew reinstall coreutils

# Linux
sudo apt install --reinstall coreutils
```

---

## Requirements

### Python Dependencies
- Python 3.10+
- pandas
- pyarrow
- zstandard
- orjson
- psutil

### For reddit_zst_filter_zstd_jq
- `zstd` (zstandard command-line tool)
- `jq` (JSON processor)
- `gsplit` (part of coreutils)

See `requirements.txt` for full list.

---

## File Structure

```
.
├── reddit_filter_utils.py          # Shared utilities
├── reddit_zst_filter_shell.py      # Method 1 (Shell Pipes)
├── reddit_zst_filter_python.py     # Method 2 (Python)
├── config.json                     # Configuration
├── requirements.txt                # Python dependencies
└── logs/                           # Processing logs
```

---

## Data

Reddit dump files can be downloaded from [Academic Torrents](https://academictorrents.com/details/30dee5f0406da7a353aff6a8caa2d54fd01f2ca1)

---

## Credits

This project is based on:
- **[Watchful1](https://github.com/Watchful1)** — author of [PushshiftDumps](https://github.com/Watchful1/PushshiftDumps/tree/master/scripts)
- **Pushshift Team** — for archiving and providing access to Reddit dataset

Updated and restructured version prepared as part of **Computational Social Science (CSS) course, 2025, NaUKMA**