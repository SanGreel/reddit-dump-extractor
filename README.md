# Reddit Dump Extractor

Tools for filtering and extracting data from compressed Reddit dump files (.zst format).

## About

This repository provides **two separate methods** for processing large volumes of Reddit data, optimized for different resource constraints and performance requirements.

### Two Processing Methods

| Method               | File                             | Technology | RAM | Best For |
|----------------------|----------------------------------|-----------|-----|----------|
| **zstd_jq**          | `reddit_zst_filter_zstd_jq.py`   | Shell Pipes | Very Low (2-4GB) | Huge files, limited RAM |
| **zstandard_python** | `reddit_zst_filter_zstandard.py` | Python | High (8GB+); low with `--fields` | Speed, error tracking |

> **Note.** The byte-level prefilter and streaming output (`--fields`), described in
> [Faster filtering and flat memory](#faster-filtering-and-flat-memory), are implemented
> in `reddit_zst_filter_zstandard.py` only; `reddit_zst_filter_zstd_jq.py` is unchanged.
> With `--fields`, the Python method writes matches in batches instead of keeping them
> in memory (under 1 GB per run on full monthly comment dumps in our runs), so it also
> covers the limited-RAM case. The shell method is still available if you prefer `jq`.

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
| `--fields` | Keep only these fields: comma list or preset from `config.json` (`submissions`, `comments`). With CSV, output is written in batches | all fields |
| `--batch_size` | Records per write when `--fields` is set | `100000` |
| `--no_prefilter` | Parse JSON of every line (original behaviour) | `false` |
| `--workers` | Process this many `.zst` files in parallel, one process per file | `1` |

### Faster filtering and flat memory

**Byte-level prefilter (on by default for exact matching).** Before parsing JSON,
each raw block is searched with one compiled regex for
`"<field>" : "<value1|value2|...>"` (case-insensitive). Only matching lines are
parsed; the exact check on the parsed object is unchanged, so the matched
records are identical. Lines that match only by accident (e.g. the value inside
`crosspost_parent_list`) are still rejected by the exact check.
The prefilter is switched off automatically in `--regex` mode and for values
that are not plain ASCII names. Note: malformed lines are then counted only
among candidates; use `--no_prefilter` to count them in the whole file.

**Streaming output.** `--fields` keeps only the listed columns and appends to
the output every `--batch_size` records instead of holding all matches in
memory. Useful for comment dumps from large subreddits. Works for CSV and
Parquet; in streaming Parquet every column is stored as a string (missing
values as nulls), because pandas infers types per batch and Parquet needs one
schema per file. The values are the same text as in the CSV output; cast types
after loading if needed.

**Parallel files.** `--workers N` processes N `.zst` files at once (e.g. several
months), one process per file, logs from all workers go to the same console and
log file. Each worker holds its own buffers, so use `--fields` when running
several workers on comment dumps.

```bash
python reddit_zst_filter_zstandard.py /path/to/dumps \
  --file_filter "^RC_" --value "ukraine,europe" --fields comments
```

Progress is logged with percent of the compressed file and ETA.

**Benchmark** (`benchmarks/compare_with_baseline.py`, first 5,000,000 lines of
`RC_2026-07`, 8 subreddits, 9,154 matches; MacBook, Apple M3, 8 CPUs, Python 3.14):

| | wall time | lines/s | peak RSS | output |
|---|---|---|---|---|
| original (`main`) | 44.8 s | 112k | 2.15 GB | reference |
| prefilter, all fields | 20.4 s | 245k | 0.82 GB | byte-identical |
| prefilter + `--fields comments` | 19.8 s | 252k | 0.74 GB | same records |

`--workers 4` on 4 such files: 79.3 s → 28.0 s (2.8×).

On a full month (MacBook, `RS_2026-07`, 45.7M lines): original 18.7 min,
new version 7.2 min, peak RSS 1.14 GB; output byte-identical to the original.
Comments `RC_2026-07` + `RC_2026-08` (739M lines, two runs in parallel,
`--fields`, 82 columns): 28 min, 220–238k lines/s each, RAM under 1 GB per run.

Outputs of the original and the prefiltered run are cell-by-cell identical.
Equivalence on edge cases: `python tests/test_equivalence.py`.

Reproduce the comparison on your own dump (baseline = `main`, output checked
byte-for-byte, peak memory measured per run):

```bash
python benchmarks/compare_with_baseline.py /path/to/RC_2023-01.zst \
  --value "ukraine,europe" --lines 5000000 --workers 4
```

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

Logs are saved to `logs/bot.log` (see `logging.log_file_name` in `config.json`) with file rotation.

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
├── reddit_zst_filter_zstd_jq.py    # Method 1 (Shell Pipes)
├── reddit_zst_filter_zstandard.py  # Method 2 (Python)
├── config.json                     # Configuration
├── requirements.txt                # Python dependencies
├── tests/test_equivalence.py       # Prefilter / streaming / workers equivalence tests
├── benchmarks/compare_with_baseline.py  # Speed & memory vs. a baseline git ref
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