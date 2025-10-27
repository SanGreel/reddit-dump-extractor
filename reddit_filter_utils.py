#!/usr/bin/env python3
import zstandard
import os
import sys
import argparse
import re
import logging.handlers
from typing import List, Set, Dict, Any, Optional, Union
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


def json_loads(data: Union[str, bytes]) -> Dict[str, Any]:
    if isinstance(data, str):
        data = data.encode('utf-8')
    elif not isinstance(data, bytes):
        data = str(data).encode('utf-8')
    return orjson.loads(data)


def setup_logging(config: Config) -> logging.Logger:
    """Налаштувати логування (не глобально!)"""
    log = logging.getLogger("reddit_filter")

    # Очистити попередні handlers
    log.handlers.clear()

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


class FileReader:
    def __init__(self, config: Config):
        self.config = config

    def read_and_decode(
            self,
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
            return self.read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

    def yield_lines(self, file_path: str):
        with open(file_path, 'rb') as file_handle:
            buffer = ''
            reader = zstandard.ZstdDecompressor(
                max_window_size=self.config.get('file_reading', 'zst_max_window_size_bytes')
            ).stream_reader(file_handle)
            while True:
                chunk = self.read_and_decode(
                    reader,
                    self.config.get('file_reading', 'chunk_size_bytes'),
                    self.config.get('file_reading', 'max_window_size_bytes')
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
    def normalize_dataframe(df: pd.DataFrame, config: Config) -> pd.DataFrame:
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


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter Reddit dumps and output to Parquet/CSV format",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("input", help="Input folder to recursively read .zst files from")

    parser.add_argument(
        "--output_dir",
        help="Output directory (default: output)",
        default="output"
    )

    parser.add_argument(
        "--format",
        help="Output format: parquet or csv (default: csv)",
        choices=['parquet', 'csv'],
        default='csv'
    )

    parser.add_argument(
        "--field",
        help="Field to filter on (default: subreddit)",
        default='subreddit'
    )

    parser.add_argument(
        "--value",
        help="Value(s) to match, comma separated (default: ukraine)",
        default='ukraine'
    )

    parser.add_argument(
        "--regex",
        help="Treat values as regex patterns",
        action='store_true',
        default=False
    )

    parser.add_argument(
        "--file_filter",
        help="Regex pattern filenames must match (default: ^RC_|^RS_)",
        default="^RC_|^RS_"
    )

    parser.add_argument(
        "--chunk_size",
        type=int,
        help="Lines per chunk for shell method (default: 1000000)",
        default=1000000
    )

    parser.add_argument("--config", help="Path to config file (default: config.json)", default="config.json")

    return parser.parse_args()


def collect_input_files(input_dir: str, file_filter: str, config: Config) -> List[str]:
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


def load_filter_values(args: argparse.Namespace, log: logging.Logger) -> Union[Set[str], List[re.Pattern]]:
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


def generate_output_path(input_file: str, output_dir: str, output_format: str, config: Config) -> str:
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