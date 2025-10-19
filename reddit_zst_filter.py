#!/usr/bin/env python3

import zstandard
import os
import sys
import time
import argparse
import re
import logging.handlers
import multiprocessing
from typing import List, Set, Dict, Any, Optional, Union, Tuple
from pathlib import Path
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import orjson
import psutil


class Config:
    def __init__(self, config_path: str = "config.json"):
        with open(config_path, 'r') as f:
            self._config = json.load(f)

    def get(self, *keys):
        value = self._config
        for key in keys:
            value = value[key]
        return value


config = Config()


def json_loads(data: Union[str, bytes]) -> Dict[str, Any]:
    if isinstance(data, str):
        data = data.encode('utf-8')
    elif not isinstance(data, bytes):
        data = str(data).encode('utf-8')
    return orjson.loads(data)


def setup_logging() -> logging.Logger:
    log = logging.getLogger("reddit_filter")
    log.setLevel(logging.INFO)
    log_formatter = logging.Formatter(config.get('logging', 'log_format'))

    log_str_handler = logging.StreamHandler()
    log_str_handler.setFormatter(log_formatter)
    log.addHandler(log_str_handler)

    log_dir = config.get('logging', 'log_dir')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, config.get('logging', 'log_file_name')),
        maxBytes=config.get('logging', 'log_max_bytes'),
        backupCount=config.get('logging', 'log_backup_count')
    )
    log_file_handler.setFormatter(log_formatter)
    log.addHandler(log_file_handler)

    return log


log = setup_logging()


class FileReader:
    @staticmethod
    def read_and_decode(
        reader: zstandard.ZstdDecompressionReader,
        chunk_size: int,
        max_window_size: int,
        previous_chunk: Optional[bytes] = None,
        bytes_read: int = 0
    ) -> str:
        chunk = reader.read(chunk_size)
        bytes_read += chunk_size
        if previous_chunk is not None:
            chunk = previous_chunk + chunk
        try:
            return chunk.decode()
        except UnicodeDecodeError:
            if bytes_read > max_window_size:
                raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
            return FileReader.read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

    @staticmethod
    def yield_lines(file_path: str) -> str:
        with open(file_path, 'rb') as file_handle:
            buffer = ''
            reader = zstandard.ZstdDecompressor(
                max_window_size=config.get('file_reading', 'zst_max_window_size_bytes')
            ).stream_reader(file_handle)
            while True:
                chunk = FileReader.read_and_decode(
                    reader,
                    config.get('file_reading', 'chunk_size_bytes'),
                    config.get('file_reading', 'max_window_size_bytes')
                )
                if not chunk:
                    break
                lines = (buffer + chunk).split("\n")
                for line in lines[:-1]:
                    yield line
                buffer = lines[-1]
            reader.close()


class CheckpointManager:
    def __init__(self, checkpoint_path: str):
        self.checkpoint_path = checkpoint_path
        self.processed_files: Set[str] = set()
        self.load_checkpoint()

    def load_checkpoint(self) -> None:
        if os.path.exists(self.checkpoint_path):
            try:
                with open(self.checkpoint_path, 'r') as f:
                    data = json.load(f)
                    self.processed_files = set(data.get('processed_files', []))
                log.info(f"Loaded checkpoint: {len(self.processed_files)} files already processed")
            except Exception as e:
                log.warning(f"Failed to load checkpoint: {e}")
                self.processed_files = set()

    def save_checkpoint(self) -> None:
        try:
            with open(self.checkpoint_path, 'w') as f:
                json.dump({
                    'processed_files': list(self.processed_files),
                    'last_updated': time.time()
                }, f, indent=2)
        except Exception as e:
            log.error(f"Failed to save checkpoint: {e}")

    def mark_processed(self, file_path: str) -> None:
        self.processed_files.add(file_path)
        self.save_checkpoint()

    def is_processed(self, file_path: str) -> bool:
        return file_path in self.processed_files

    def get_pending_files(self, all_files: List[str]) -> List[str]:
        return [f for f in all_files if not self.is_processed(f)]


class MemoryMonitor:
    def __init__(self, max_ram_gb: float):
        self.max_ram_bytes = max_ram_gb * (1024**3) * config.get('processing', 'ram_safety_margin')
        self.process = psutil.Process()

    def get_current_usage_gb(self) -> float:
        return self.process.memory_info().rss / (1024**3)

    def is_within_limit(self) -> bool:
        return self.process.memory_info().rss < self.max_ram_bytes

    def get_usage_stats(self) -> Dict[str, float]:
        mem_info = self.process.memory_info()
        return {
            'rss_gb': mem_info.rss / (1024**3),
            'vms_gb': mem_info.vms / (1024**3),
            'percent': self.process.memory_percent(),
            'max_gb': self.max_ram_bytes / (1024**3)
        }


class DataNormalizer:
    @staticmethod
    def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        for field in config.get('data_normalization', 'problematic_fields'):
            if field in df.columns:
                df[field] = df[field].astype(str)
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
        return df


def process_file(
    file_path: str,
    field: str,
    values: Union[Set[str], List[re.Pattern]],
    partial: bool,
    regex: bool,
    output_path: str,
    output_format: str
) -> Tuple[str, int, int, int]:
    matched_records = []
    lines_processed = 0
    error_lines = 0

    value = None
    if len(values) == 1 and not regex and not partial:
        value = min(values)

    process = psutil.Process()

    try:
        for line in FileReader.yield_lines(file_path):
            try:
                obj = json_loads(line)
                matched = False
                observed = obj[field].lower()

                if regex:
                    for reg in values:
                        if reg.search(observed):
                            matched = True
                            break
                elif partial:
                    for val in values:
                        if val in observed:
                            matched = True
                            break
                else:
                    if value is not None:
                        matched = (observed == value)
                    else:
                        matched = (observed in values)

                if matched:
                    matched_records.append(obj)
            except (KeyError, json.JSONDecodeError, AttributeError):
                error_lines += 1

            lines_processed += 1

            if lines_processed % config.get('processing', 'progress_log_interval') == 0:
                cpu = process.cpu_percent()
                mem = process.memory_info().rss / (1024**3)
                log.info(
                    f"{os.path.basename(file_path)}: {lines_processed:,} lines, "
                    f"{len(matched_records):,} matched, CPU: {cpu}%, RAM: {mem:.2f} GB")

    except Exception as err:
        log.error(f"Error processing {file_path}: {err}")
        return file_path, lines_processed, 0, error_lines

    if matched_records:
        try:
            df = pd.DataFrame(matched_records)
            df = DataNormalizer.normalize_dataframe(df)

            if output_format == 'parquet':
                df.to_parquet(
                    output_path,
                    engine='pyarrow',
                    compression=config.get('output', 'parquet_compression'),
                    index=False
                )
            elif output_format == 'csv':
                compression = config.get('output', 'csv_compression')
                df.to_csv(
                    output_path,
                    compression=compression,
                    index=False
                )

            log.info(
                f"Completed {os.path.basename(file_path)}: {lines_processed:,} lines, "
                f"{len(matched_records):,} matched, {error_lines:,} errors -> {output_path}")
        except Exception as e:
            log.error(f"Failed to write output file {output_path}: {e}")
            return file_path, lines_processed, 0, error_lines
    else:
        log.info(
            f"Completed {os.path.basename(file_path)}: {lines_processed:,} lines, "
            f"0 matched, {error_lines:,} errors (no output file created)")

    return file_path, lines_processed, len(matched_records), error_lines


def process_file_wrapper(args: Tuple) -> Tuple[str, int, int, int]:
    return process_file(*args)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter Reddit dumps and output to Parquet/CSV format with parallel processing",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("input", help="Input folder to recursively read .zst files from")

    parser.add_argument(
        "--output_dir",
        help=f"Output directory (default: {config.get('output', 'default_output_dir')})",
        default=config.get('output', 'default_output_dir')
    )

    parser.add_argument(
        "--format",
        help=f"Output format: parquet or csv (default: {config.get('output', 'default_format')})",
        choices=['parquet', 'csv'],
        default=config.get('output', 'default_format')
    )

    parser.add_argument(
        "--field",
        help=f"Field to filter on (default: {config.get('defaults', 'field')})",
        default=config.get('defaults', 'field')
    )

    parser.add_argument(
        "--value",
        help=f"Value(s) to match, comma separated (default: {config.get('defaults', 'value')})",
        default=config.get('defaults', 'value')
    )

    parser.add_argument("--value_list", help="File with newline separated values to match", default=None)
    parser.add_argument("--partial", help="Partial match instead of exact match", action='store_true', default=False)
    parser.add_argument("--regex", help="Treat values as regex patterns", action='store_true', default=False)

    parser.add_argument(
        "--file_filter",
        help=f"Regex pattern filenames must match (default: {config.get('file_filtering', 'default_file_filter')})",
        default=config.get('file_filtering', 'default_file_filter')
    )

    parser.add_argument(
        "--processes",
        help=f"Number of parallel processes (default: {config.get('processing', 'default_processes')})",
        default=config.get('processing', 'default_processes'),
        type=int
    )

    parser.add_argument(
        "--max_ram",
        help=f"Maximum RAM usage in GB (default: {config.get('processing', 'default_max_ram_gb')})",
        default=config.get('processing', 'default_max_ram_gb'),
        type=float
    )

    parser.add_argument(
        "--checkpoint",
        help=f"Path to checkpoint file (default: {config.get('output', 'checkpoint_file')})",
        default=config.get('output', 'checkpoint_file')
    )

    parser.add_argument("--no_checkpoint", help="Disable checkpoint/resume functionality", action='store_true', default=False)
    parser.add_argument("--config", help="Path to config file (default: config.json)", default="config.json")

    return parser.parse_args()


def collect_input_files(input_dir: str, file_filter: str) -> List[str]:
    input_files = []
    file_pattern = re.compile(file_filter)
    file_extension = config.get('file_filtering', 'file_extension')

    for subdir, dirs, files in os.walk(input_dir):
        files.sort()
        for file_name in files:
            if file_name.endswith(file_extension) and file_pattern.search(file_name):
                input_path = os.path.join(subdir, file_name)
                input_files.append(input_path)

    return input_files


def load_filter_values(args: argparse.Namespace) -> Union[Set[str], List[re.Pattern]]:
    values = set()

    if args.value_list:
        log.info(f"Reading values from: {args.value_list}")
        with open(args.value_list, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    values.add(line)
    else:
        values = set(v.strip() for v in args.value.split(",") if v.strip())

    if args.regex:
        regexes = []
        for reg in values:
            try:
                regexes.append(re.compile(reg))
            except re.error as e:
                log.error(f"Invalid regex pattern '{reg}': {e}")
                sys.exit(1)
        log.info(f"Compiled {len(regexes)} regex pattern(s) for field '{args.field}'")
        return regexes
    else:
        lower_values = {val.lower() for val in values}
        match_type = "partial" if args.partial else "exact"
        log.info(f"Loaded {len(lower_values)} value(s) for {match_type} matching on field '{args.field}'")
        return lower_values


def generate_output_path(input_file: str, output_dir: str, output_format: str) -> str:
    base_name = os.path.basename(input_file)
    file_extension = config.get('file_filtering', 'file_extension')
    name_without_ext = base_name.replace(file_extension, '')

    if output_format == 'csv':
        csv_compression = config.get('output', 'csv_compression')
        if csv_compression == 'gzip':
            output_name = f"{name_without_ext}.csv.gz"
        else:
            output_name = f"{name_without_ext}.csv"
    else:
        output_name = f"{name_without_ext}.parquet"

    return os.path.join(output_dir, output_name)


def main():
    args = parse_arguments()

    global config
    if args.config != "config.json":
        config = Config(args.config)

    log.info("=" * 80)
    log.info("Reddit Dump Filter - Parallel Processing Edition")
    log.info("=" * 80)
    log.info(f"Input directory: {args.input}")
    log.info(f"Output directory: {args.output_dir}")
    log.info(f"Output format: {args.format}")
    log.info(f"Max RAM: {args.max_ram:.1f} GB")
    log.info(f"Parallel processes: {args.processes}")

    os.makedirs(args.output_dir, exist_ok=True)

    checkpoint_manager = None
    if not args.no_checkpoint:
        checkpoint_path = os.path.join(args.output_dir, args.checkpoint)
        checkpoint_manager = CheckpointManager(checkpoint_path)

    memory_monitor = MemoryMonitor(args.max_ram)
    values = load_filter_values(args)

    log.info(f"Scanning for input files matching pattern: {args.file_filter}")
    all_input_files = collect_input_files(args.input, args.file_filter)
    log.info(f"Found {len(all_input_files)} total files")

    if len(all_input_files) == 0:
        log.error("No matching files found!")
        sys.exit(1)

    if checkpoint_manager:
        input_files = checkpoint_manager.get_pending_files(all_input_files)
        processed_count = len(all_input_files) - len(input_files)
        if processed_count > 0:
            log.info(f"Skipping {processed_count} already processed files")
        log.info(f"Processing {len(input_files)} pending files")
    else:
        input_files = all_input_files

    if len(input_files) == 0:
        log.info("All files already processed!")
        return

    process_args = [
        (
            input_file,
            args.field,
            values,
            args.partial,
            args.regex,
            generate_output_path(input_file, args.output_dir, args.format),
            args.format
        )
        for input_file in input_files
    ]

    total_processed = 0
    total_lines = 0
    total_matched = 0
    total_errors = 0
    start_time = time.time()

    log.info("=" * 80)
    log.info("Starting parallel processing...")
    log.info("=" * 80)

    try:
        with multiprocessing.Pool(processes=args.processes) as pool:
            for result in pool.imap_unordered(process_file_wrapper, process_args):
                file_path, lines_processed, matched_count, error_count = result

                total_processed += 1
                total_lines += lines_processed
                total_matched += matched_count
                total_errors += error_count

                if checkpoint_manager:
                    checkpoint_manager.mark_processed(file_path)

                progress_pct = (total_processed / len(input_files)) * 100
                mem_stats = memory_monitor.get_usage_stats()
                log.info(
                    f"Progress: {total_processed}/{len(input_files)} ({progress_pct:.1f}%) | "
                    f"Total matched: {total_matched:,} | RAM: {mem_stats['rss_gb']:.2f} GB"
                )

                if not memory_monitor.is_within_limit():
                    log.warning(
                        f"Approaching memory limit! Current: {mem_stats['rss_gb']:.2f} GB / "
                        f"Max: {mem_stats['max_gb']:.2f} GB"
                    )

    except KeyboardInterrupt:
        log.warning("Processing interrupted by user")
        pool.terminate()
        pool.join()
        sys.exit(1)
    except Exception as e:
        log.error(f"Error during parallel processing: {e}")
        raise

    elapsed = time.time() - start_time
    log.info("=" * 80)
    log.info("Processing Complete!")
    log.info("=" * 80)
    log.info(f"Files processed: {total_processed}")
    log.info(f"Total lines scanned: {total_lines:,}")
    log.info(f"Total records matched: {total_matched:,}")
    log.info(f"Total errors: {total_errors:,}")
    log.info(f"Elapsed time: {elapsed:.1f} seconds")
    log.info(f"Processing rate: {total_lines / elapsed:.0f} lines/second")
    log.info(f"Output directory: {args.output_dir}")

    if args.format == 'csv':
        csv_compression = config.get('output', 'csv_compression')
        ext = '.csv.gz' if csv_compression == 'gzip' else '.csv'
    else:
        ext = '.parquet'
    output_files = [f for f in os.listdir(args.output_dir) if f.endswith(ext)]
    log.info(f"Output files created: {len(output_files)}")

    total_size = 0
    for output_file in output_files:
        file_path = os.path.join(args.output_dir, output_file)
        total_size += os.path.getsize(file_path)
    log.info(f"Total output size: {total_size / (1024**2):.2f} MB")

    mem_stats = memory_monitor.get_usage_stats()
    log.info(f"Peak RAM usage: {mem_stats['rss_gb']:.2f} GB")
    log.info("=" * 80)


if __name__ == '__main__':
    main()
