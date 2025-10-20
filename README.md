# Reddit Dump Extractor

This repository contains **reddit_zst_filter.py** - a Python script for filtering and extracting data from compressed Reddit dump files (.zst format).

The script allows you to:
- **Filter by subreddits** - Extract comments/posts from specific communities (subreddits are topic-based communities/groups on Reddit like r/python, r/ukraine, etc.)
- **Filter by authors** - Extract all content from specific Reddit users
- **Filter by any field** - Filter using any available metadata field (score, timestamp, etc.)
- **Multiple output formats** - Save results as Parquet or CSV files

The tool is optimized for processing large Reddit archive dumps efficiently with streaming decompression and memory monitoring.

## Data

Files for your research can be downloaded [here](https://academictorrents.com/details/30dee5f0406da7a353aff6a8caa2d54fd01f2ca1) 

## Installation

### macOS / Linux

```bash
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

### Windows

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

You can also use the pyenv library to easily manage your Python environments.
Tutorial for maxOS / Linux - https://github.com/pyenv/pyenv
Tutorial for Windows - https://github.com/pyenv-win/pyenv-win

## Usage

```bash
python reddit_zst_filter.py <input_folder> [options]
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--output_dir` | Output directory for filtered files | `output` |
| `--format` | Output format: `parquet` or `csv` | `csv` |
| `--field` | Field to filter on | `subreddit` |
| `--value` | Comma-separated values to match | `ukraine` |
| `--regex` | Enable regex pattern matching | `False` |
| `--file_filter` | Regex pattern for input filenames | `^RC_\|^RS_` |
| `--config` | Path to config file | `config.json` |

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

## Configuration

The tool uses a [config.json](config.json) file for advanced settings:

- **File Reading**: Chunk sizes and memory window settings for .zst decompression
- **Logging**: Log directory, file rotation, and format
- **Processing**: Progress reporting intervals
- **Output**: Compression settings for Parquet (snappy) and CSV (gzip/none)
- **Data Normalization**: Automatic type conversion for problematic boolean fields

## Output

Each input `.zst` file is processed individually and outputs a separate file:

- **Parquet**: `output/RC_2023-01.parquet`
- **CSV**: `output/RC_2023-01.csv` or `output/RC_2023-01.csv.gz` (if compression enabled)

The tool logs:
- Processing progress every 5M lines
- Memory usage (RAM)
- CPU usage
- Total records matched per file
- Final statistics (total lines, matches, errors, processing rate)


### Directory Structure Example

The output files are organized in the output directory with preserved naming from the input files:

![Directory Structure](media/dir.png)

### CSV Output Example

The CSV output contains all filtered Reddit comments/posts with their metadata fields:

![CSV Example](media/csv.png)

## Performance

The tool is optimized for processing large Reddit dump files:

- Streaming decompression (doesn't load entire file into memory)
- Memory monitoring and reporting
- Fast JSON parsing with orjson
- Efficient field filtering (exact match or regex)
- Sequential file processing
- Configurable chunk sizes and buffer settings

## Requirements

- Python 3.13
- pandas
- pyarrow
- zstandard
- orjson
- psutil

See [requirements.txt](requirements.txt) for complete dependencies.

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