# Reddit Dump Extractor

This repository contains tools for filtering and extracting data from compressed Reddit dump files (.zst format).

## Available Tools

### **reddit_zst_filter.py** - Unified Python Processor (Dual-Mode)
A comprehensive data processing script that supports both Python-native and shell-based decompression methods. Choose the best approach for your system resources and performance needs.

## Installation

### macOS / Linux

```bash
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

**For shell method on macOS, also install:**
```bash
brew install zstd coreutils jq
```

**For shell method on Linux:**
```bash
sudo apt install zstd coreutils jq
```

### Windows

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**For shell method on Windows:**

The shell-based method works best with WSL (Windows Subsystem for Linux):
```bash
# Install WSL first (PowerShell as Admin)
wsl --install

# Then inside WSL:
sudo apt update
sudo apt install zstd coreutils jq python3 python3-pip
pip3 install -r requirements.txt
```

You can also use the pyenv library to easily manage your Python environments.
Tutorial for macOS / Linux - https://github.com/pyenv/pyenv
Tutorial for Windows - https://github.com/pyenv-win/pyenv-win

## Usage

### reddit_zst_filter.py - Unified Script

```bash
python reddit_zst_filter.py <input_folder> [options]
```

#### Processing Methods

The script supports two decompression and filtering methods:

| Method | Best For | Memory Usage | Speed | Requirements |
|--------|----------|--------------|-------|--------------|
| `python` (default) | Full control, complex filtering, stable systems | High | Fast | Python libraries only |
| `shell` | Limited RAM, huge files, simple filters | Very Low | Moderate | zstd, jq, coreutils |

#### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--output_dir` | Output directory for filtered files | `output` |
| `--format` | Output format: `parquet` or `csv` | `csv` |
| `--field` | Field to filter on | `subreddit` |
| `--value` | Comma-separated values to match | `ukraine` |
| `--regex` | Enable regex pattern matching | `False` |
| `--file_filter` | Regex pattern for input filenames | `^RC_\|^RS_` |
| `--method` | Decompression method: `python` or `shell` | `python` |
| `--chunk_size` | Lines per chunk for shell method | `1000000` |
| `--config` | Path to config file | `config.json` |

#### Processing Methods Explained

**Python Method (`--method python`):**
- Uses the zstandard Python library for decompression
- Filters data in-memory with direct field matching or regex
- More detailed error tracking and progress reporting
- Better for systems with adequate RAM and when you need complex filtering

**Shell Method (`--method shell`):**
- Decompresses with multi-threaded `zstd` command-line tool
- Uses `jq` for fast JSON filtering
- Splits processing into chunks with `gsplit`
- Streams data through Unix pipes to minimize memory footprint
- Better for systems with limited RAM or very large files

### Available Fields

Reddit data contains the following fields that can be used for filtering:

- `author` - Author of the comment/post
- `body` - Text content of the comment
- `subreddit` - Name of the subreddit
- `created_utc` - Creation timestamp (UTC)
- `score` - Number of upvotes/downvotes
- `id` - Unique identifier
- `link_id` - ID of the linked post
- `parent_id` - ID of the parent comment
- `edited` - Edit timestamp
- `archived` - Archive status
- `gilded` - Number of awards received

## Examples

### Python Method (Default)

**Filter by subreddit:**
```bash
python reddit_zst_filter.py /path/to/dumps --value python
```

**Multiple subreddits:**
```bash
python reddit_zst_filter.py /path/to/dumps --value "python,javascript,java"
```

**Output to Parquet format:**
```bash
python reddit_zst_filter.py /path/to/dumps --format parquet --output_dir parquet_output
```

**Filter by author:**
```bash
python reddit_zst_filter.py /path/to/dumps --field author --value "username"
```

**Regex matching:**
```bash
python reddit_zst_filter.py /path/to/dumps --value "^(python|java)" --regex
```

**Custom file filter (process only comments from 2023):**
```bash
python reddit_zst_filter.py /path/to/dumps --file_filter "^RC_2023-"
```

### Shell Method (Memory-Efficient)

**Filter by subreddit:**
```bash
python reddit_zst_filter.py /path/to/dumps --method shell --value ukraine
```

**Multiple subreddits:**
```bash
python reddit_zst_filter.py /path/to/dumps --method shell --value "ukraine,worldnews,europe"
```

**Filter comments by keywords in body text (regex):**
```bash
python reddit_zst_filter.py /path/to/dumps --method shell --field body --value "kyiv|war" --regex
```

**Smaller chunks for very limited memory:**
```bash
python reddit_zst_filter.py /path/to/dumps --method shell --chunk_size 500000
```

**Output to Parquet format:**
```bash
python reddit_zst_filter.py /path/to/dumps --method shell --format parquet --output_dir parquet_output
```

**Process only submissions (posts) from 2021:**
```bash
python reddit_zst_filter.py /path/to/dumps --method shell --file_filter "^RS_2021-"
```

## Configuration

The tool uses a [config.json](config.json) file for advanced settings:

- **File Reading**: Chunk sizes and memory window settings for .zst decompression (Python method)
- **Logging**: Log directory, file rotation, and format
- **Processing**: Progress reporting intervals
- **Output**: Compression settings for Parquet (snappy) and CSV (gzip/none)
- **Data Normalization**: Automatic type conversion for problematic boolean fields

## Output

Each input `.zst` file is processed individually and outputs a separate file:

- **Parquet**: `output/RC_2023-01.parquet`
- **CSV**: `output/RC_2023-01.csv` or `output/RC_2023-01.csv.gz` (if compression enabled)

The tool logs:
- Processing progress
- Memory usage (RAM and CPU percentage)
- Total records matched per file
- Final statistics (total lines, matches, errors, processing rate)

### Directory Structure Example

The output files are organized in the output directory with preserved naming from the input files:

![Directory Structure](media/dir.png)

### CSV Output Example

The CSV output contains all filtered Reddit comments/posts with their metadata fields:

![CSV Example](media/csv.png)

### Performance Characteristics

**Python Method:**
- **Memory**: High - maintains decoded chunks and matched records in memory
- **Speed**: Fast - direct in-memory processing
- **Error Tracking**: Detailed with per-line error logging
- **Best for**: Systems with 8GB+ available RAM

**Shell Method:**
- **Memory**: Very Low - streams through Unix pipes
- **Speed**: Moderate - limited by pipe overhead and I/O
- **Error Tracking**: Basic - handled by shell commands
- **Best for**: Systems with 2-4GB available RAM or large files (>5GB)

**Optimizations (Python method):**
- Streaming decompression with zstandard
- Memory monitoring and reporting
- Fast JSON parsing with orjson
- Efficient field filtering (exact match or regex)
- Sequential file processing
- Configurable chunk sizes and buffer settings

**Optimizations (Shell method):**
- Multi-threaded zstd decompression (`-T0` flag)
- Streaming pipeline (no intermediate files until final output)
- Automatic temporary file cleanup
- Adjustable chunk sizes for memory control

## Troubleshooting

### Out of Memory Errors

If processing fails with out-of-memory errors:

1. **Switch to shell method:**
   ```bash
   python reddit_zst_filter.py /path/to/dumps --method shell
   ```

2. **Reduce chunk size (shell method):**
   ```bash
   python reddit_zst_filter.py /path/to/dumps --method shell --chunk_size 500000
   ```

3. **Process files individually** instead of a directory

4. **Close other applications** to free up system RAM

### Command Not Found Errors (Shell Method)

**macOS:**
```bash
# Install missing tools
brew install zstd coreutils jq
```

**Linux:**
```bash
sudo apt install zstd coreutils jq
```

**Windows:**
Use WSL (see Installation section above)

## Requirements

### For reddit_zst_filter.py
- Python 3.13
- pandas
- pyarrow
- zstandard
- orjson
- psutil

### For shell method (--method shell)
- System tools: zstd, coreutils (gsplit), jq
- Same Python dependencies as above

See [requirements.txt](requirements.txt) for complete Python dependencies.

## Data

Files for your research can be downloaded [here](https://academictorrents.com/details/30dee5f0406da7a353aff6a8caa2d54fd01f2ca1)

## Credits

This project is based on code and approaches from:
- **[Watchful1](https://github.com/Watchful1)** - Original author of [PushshiftDumps](https://github.com/Watchful1/PushshiftDumps/tree/master/scripts) scripts that served as the foundation for this tool
- **Pushshift Team** - For collecting, archiving, and making available the comprehensive Reddit dataset

### Updated Version
This enhanced and adapted version was prepared as part of the **Computational Social Science (CSS) course, 2025, NaUKMA**.

## Acknowledgments

Special thanks to:
- [Watchful1](https://github.com/Watchful1) for creating the original filtering and processing logic
- The Pushshift project for their invaluable work in archiving and providing access to Reddit data
- All contributors to the open-source tools and libraries that make this project possible