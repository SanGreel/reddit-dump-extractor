#!/usr/bin/env python3

import json
import subprocess
import os
import sys
import argparse
import re
import logging
import pandas as pd
import tempfile
import shutil


class Config:
    def __init__(self, config_path: str = "config.json"):
        with open(config_path, 'r') as f:
            self._config = json.load(f)

    def get(self, *keys):
        value = self._config
        for key in keys:
            value = value[key]
        return value


def setup_logging(config: Config) -> logging.Logger:
    log = logging.getLogger("reddit_shell_filter")
    log.setLevel(logging.INFO)
    log_formatter = logging.Formatter(config.get('logging', 'log_format'))

    log_str_handler = logging.StreamHandler()
    log_str_handler.setFormatter(log_formatter)
    log.addHandler(log_str_handler)

    log_dir = config.get('logging', 'log_dir')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_handler = logging.FileHandler(
        os.path.join(log_dir, config.get('logging', 'log_file_name'))
    )
    log_file_handler.setFormatter(log_formatter)
    log.addHandler(log_file_handler)

    return log


def build_jq_filter(field: str, values: list, regex: bool) -> str:
    """Build jq filter expression based on field, values, and regex flag"""

    if regex:
        patterns = '|'.join(values)
        return fr'select(.{field} | test(\"{patterns}\"))'
    else:
        if len(values) == 1:
            return fr'select(.{field} == \"{values[0]}\")'
        else:
            values_str = ', '.join(fr'\"{v}\"' for v in values)
            return f'select(.{field} | IN({values_str}))'


def process_zst_file(
        input_file: str,
        field: str,
        values: list,
        regex: bool,
        output_format: str,
        output_path: str,
        config: Config,
        log: logging.Logger,
        chunk_size: int = 1000000
) -> tuple:
    """Process a single .zst file using shell commands and output to Parquet/CSV"""

    jq_filter = build_jq_filter(field, values, regex)
    log.info(f"Processing {os.path.basename(input_file)} with filter: {jq_filter}")

    # Create temporary directory for intermediate JSON files
    temp_dir = tempfile.mkdtemp(prefix='reddit_filter_')

    try:
        # Build the shell command
        cmd = (
            f'zstd -dc --memory=2048MB -T0 "{input_file}" | '
            f'gsplit -l {chunk_size} --filter=\'jq -c "{jq_filter}" > {temp_dir}/chunk_$FILE.json\' -'
        )

        log.info(f"Executing: {cmd}")

        # Execute the command
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
            return input_file, 0, 0

        if stderr:
            log.warning(f"stderr: {stderr.decode()}")

        # Collect all chunk files
        chunk_files = sorted([
            os.path.join(temp_dir, f)
            for f in os.listdir(temp_dir)
            if f.startswith('chunk_') and f.endswith('.json')
        ])

        if not chunk_files:
            log.info(f"No matches found in {os.path.basename(input_file)}")
            return input_file, 0, 0

        log.info(f"Found {len(chunk_files)} chunk files to process")

        all_records = []
        lines_processed = 0

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
                                pass
            except Exception as e:
                log.error(f"Error reading chunk {chunk_file}: {e}")

        if not all_records:
            log.info(f"No valid records found in {os.path.basename(input_file)}")
            return input_file, 0, 0

        log.info(f"Read {len(all_records)} matched records")

        # Create DataFrame and normalize
        df = pd.DataFrame(all_records)

        # Normalize problematic fields
        for field_name in config.get('data_normalization', 'problematic_fields'):
            if field_name in df.columns:
                df[field_name] = df[field_name].astype(str)

        # Convert all object columns to string
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)

        # Write output
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

        log.info(f"Written {len(all_records)} records to {output_path}")

        return input_file, lines_processed, len(all_records)

    except Exception as e:
        log.error(f"Error processing {input_file}: {e}")
        return input_file, 0, 0

    finally:
        # Clean up temporary directory
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            log.warning(f"Failed to remove temp directory {temp_dir}: {e}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter Reddit dumps using shell commands and output to Parquet/CSV"
    )

    parser.add_argument("input", help="Input folder to recursively read .zst files from")
    parser.add_argument("--output_dir", help="Output directory", default="output")
    parser.add_argument("--format", choices=['parquet', 'csv'], default="csv")
    parser.add_argument("--field", help="Field to filter on", default="subreddit")
    parser.add_argument("--value", help="Value(s) to match, comma separated", default="ukraine")
    parser.add_argument("--regex", action='store_true', help="Treat values as regex patterns")
    parser.add_argument("--file_filter", help="Regex pattern filenames must match", default="^RC_|^RS_")
    parser.add_argument("--config", help="Path to config file", default="config.json")
    parser.add_argument("--chunk_size", type=int, help="Lines per chunk for gsplit", default=1000000)

    return parser.parse_args()


def collect_input_files(input_dir: str, file_filter: str, file_extension: str) -> list:
    input_files = []
    file_pattern = re.compile(file_filter)

    for subdir, dirs, files in os.walk(input_dir):
        files.sort()
        for file_name in files:
            if file_name.endswith(file_extension) and file_pattern.search(file_name):
                input_path = os.path.join(subdir, file_name)
                input_files.append(input_path)

    return input_files


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


def main():
    args = parse_arguments()

    # Load config
    config = Config(args.config)
    log = setup_logging(config)

    log.info("=" * 80)
    log.info("Reddit Dump Shell Filter")
    log.info("=" * 80)
    log.info(f"Input directory: {args.input}")
    log.info(f"Output directory: {args.output_dir}")
    log.info(f"Output format: {args.format}")
    log.info(f"Field: {args.field}")
    log.info(f"Values: {args.value}")
    log.info(f"Regex mode: {args.regex}")

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Parse filter values
    values = [v.strip() for v in args.value.split(",") if v.strip()]
    if not args.regex:
        values = [v.lower() for v in values]

    log.info(f"Filter values: {values}")

    # Collect input files
    file_extension = config.get('file_filtering', 'file_extension')
    input_files = collect_input_files(args.input, args.file_filter, file_extension)
    log.info(f"Found {len(input_files)} files to process")

    if len(input_files) == 0:
        log.error("No matching files found!")
        sys.exit(1)

    # Process files
    total_lines = 0
    total_matched = 0

    for idx, input_file in enumerate(input_files, 1):
        log.info(f"Processing file {idx}/{len(input_files)}: {os.path.basename(input_file)}")

        output_path = generate_output_path(input_file, args.output_dir, args.format, config)

        file_path, lines_processed, matched_count = process_zst_file(
            input_file,
            args.field,
            values,
            args.regex,
            args.format,
            output_path,
            config,
            log,
            args.chunk_size
        )

        total_lines += lines_processed
        total_matched += matched_count

        log.info(f"Progress: {idx}/{len(input_files)} | Total matched: {total_matched:,}")

    log.info("=" * 80)
    log.info("Processing Complete!")
    log.info("=" * 80)
    log.info(f"Files processed: {len(input_files)}")
    log.info(f"Total records matched: {total_matched:,}")
    log.info(f"Output directory: {args.output_dir}")


if __name__ == '__main__':
    main()