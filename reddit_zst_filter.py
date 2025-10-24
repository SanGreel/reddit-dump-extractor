#!/usr/bin/env python3

import zstandard
import os
import sys
import time
import argparse
import re
import logging.handlers
import subprocess
import tempfile
import shutil
from typing import List, Set, Dict, Any, Optional, Union, Tuple
import json
import pandas as pd
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


class MemoryMonitor:
    def __init__(self):
        self.process = psutil.Process()

    def get_current_usage_gb(self) -> float:
        return self.process.memory_info().rss / (1024 ** 3)

    def get_usage_stats(self) -> Dict[str, float]:
        mem_info = self.process.memory_info()
        return {
            'rss_gb': mem_info.rss / (1024 ** 3),
            'vms_gb': mem_info.vms / (1024 ** 3),
            'percent': self.process.memory_percent()
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


def build_jq_filter(field: str, values: Union[Set[str], List[re.Pattern]], regex: bool) -> str:
    """Build jq filter expression based on field, values, and regex flag"""
    if regex:
        patterns = '|'.join(str(p.pattern) for p in values)
        return fr'select(.{field} | test(\"{patterns}\"))'
    else:
        if len(values) == 1:
            value = min(values)
            return fr'select(.{field} == \"{value}\")'
        else:
            values_str = ', '.join(fr'\"{v}\"' for v in values)
            return f'select(.{field} | IN({values_str}))'


def process_file_python(
        file_path: str,
        field: str,
        values: Union[Set[str], List[re.Pattern]],
        regex: bool,
        output_path: str,
        output_format: str
) -> Tuple[str, int, int, int]:
    """Process file using Python's zstandard library"""
    matched_records = []
    lines_processed = 0
    error_lines = 0

    value = None
    if len(values) == 1 and not regex:
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
                mem = process.memory_info().rss / (1024 ** 3)
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


def process_file_shell(
        file_path: str,
        field: str,
        values: Union[Set[str], List[re.Pattern]],
        regex: bool,
        output_path: str,
        output_format: str,
        chunk_size: int = 1000000
) -> Tuple[str, int, int, int]:
    """Process file using shell commands (zstd + jq + gsplit)"""

    jq_filter = build_jq_filter(field, values, regex)
    log.info(f"Processing {os.path.basename(file_path)} with shell filter: {jq_filter}")

    temp_dir = tempfile.mkdtemp(prefix='reddit_filter_')
    lines_processed = 0
    error_lines = 0

    try:
        cmd = (
            f'zstd -dc --memory=2048MB -T0 "{file_path}" | '
            f'gsplit -l {chunk_size} --filter=\'jq -c "{jq_filter}" > {temp_dir}/chunk_$FILE.json\' -'
        )

        log.info(f"Executing: {cmd}")

        process = subprocess.Popen(
            cmd,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        stdout, stderr = process.communicate()

        if process.returncode != 0:
            log.error(f"Command failed with return code {process.returncode}")
            log.error(f"stderr: {stderr.decode()}")
            return file_path, 0, 0, 0

        if stderr:
            log.warning(f"stderr: {stderr.decode()}")

        chunk_files = sorted([
            os.path.join(temp_dir, f)
            for f in os.listdir(temp_dir)
            if f.startswith('chunk_') and f.endswith('.json')
        ])

        if not chunk_files:
            log.info(f"No matches found in {os.path.basename(file_path)}")
            return file_path, 0, 0, 0

        log.info(f"Found {len(chunk_files)} chunk files to process")

        all_records = []
        for chunk_file in chunk_files:
            try:
                with open(chunk_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                record = json.loads(line)
                                all_records.append(record)
                                lines_processed += 1
                            except json.JSONDecodeError:
                                error_lines += 1
            except Exception as e:
                log.error(f"Error reading chunk {chunk_file}: {e}")

        if not all_records:
            log.info(f"No valid records found in {os.path.basename(file_path)}")
            return file_path, lines_processed, 0, error_lines

        log.info(f"Read {len(all_records)} matched records")

        df = pd.DataFrame(all_records)
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
            f"{len(all_records):,} matched, {error_lines:,} errors -> {output_path}")

        return file_path, lines_processed, len(all_records), error_lines

    except Exception as e:
        log.error(f"Error processing {file_path}: {e}")
        return file_path, lines_processed, 0, error_lines

    finally:
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            log.warning(f"Failed to remove temp directory {temp_dir}: {e}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter Reddit dumps and output to Parquet/CSV format",
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

    parser.add_argument(
        "--regex",
        help=f"Treat values as regex patterns (default: {config.get('defaults', 'regex')})",
        action='store_true',
        default=config.get('defaults', 'regex')
    )

    parser.add_argument(
        "--file_filter",
        help=f"Regex pattern filenames must match (default: {config.get('file_filtering', 'default_file_filter')})",
        default=config.get('file_filtering', 'default_file_filter')
    )

    parser.add_argument(
        "--method",
        help="Decompression method: 'python' uses zstandard library, 'shell' uses zstd+jq+gsplit (default: python)",
        choices=['python', 'shell'],
        default='python'
    )

    parser.add_argument(
        "--chunk_size",
        type=int,
        help="Lines per chunk for shell method (default: 1000000)",
        default=1000000
    )

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
        log.info(f"Loaded {len(lower_values)} value(s) for exact matching on field '{args.field}'")
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
    log.info("Reddit Dump Filter (Unified)")
    log.info("=" * 80)
    log.info(f"Input directory: {args.input}")
    log.info(f"Output directory: {args.output_dir}")
    log.info(f"Output format: {args.format}")
    log.info(f"Processing method: {args.method}")

    os.makedirs(args.output_dir, exist_ok=True)

    memory_monitor = MemoryMonitor()
    values = load_filter_values(args)

    log.info(f"Scanning for input files matching pattern: {args.file_filter}")
    input_files = collect_input_files(args.input, args.file_filter)
    log.info(f"Found {len(input_files)} total files")

    if len(input_files) == 0:
        log.error("No matching files found!")
        sys.exit(1)

    total_processed = 0
    total_lines = 0
    total_matched = 0
    total_errors = 0
    start_time = time.time()

    log.info("=" * 80)
    log.info("Starting processing...")
    log.info("=" * 80)

    try:
        for input_file in input_files:
            output_path = generate_output_path(
                input_file, args.output_dir, args.format)

            if args.method == 'shell':
                file_path, lines_processed, matched_count, error_count = process_file_shell(
                    input_file,
                    args.field,
                    values,
                    args.regex,
                    output_path,
                    args.format,
                    args.chunk_size
                )
            else:
                file_path, lines_processed, matched_count, error_count = process_file_python(
                    input_file,
                    args.field,
                    values,
                    args.regex,
                    output_path,
                    args.format
                )

            total_processed += 1
            total_lines += lines_processed
            total_matched += matched_count
            total_errors += error_count

            progress_pct = (total_processed / len(input_files)) * 100
            mem_stats = memory_monitor.get_usage_stats()
            log.info(
                f"Progress: {total_processed}/{len(input_files)} ({progress_pct:.1f}%) | "
                f"Total matched: {total_matched:,} | RAM: {mem_stats['rss_gb']:.2f} GB"
            )

    except KeyboardInterrupt:
        log.warning("Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        log.error(f"Error during processing: {e}")
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
    log.info(f"Total output size: {total_size / (1024 ** 2):.2f} MB")


if __name__ == '__main__':
    main()