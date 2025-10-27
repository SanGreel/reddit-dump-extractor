#!/usr/bin/env python3
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Union, List, Set, Tuple
import re
import pandas as pd

from reddit_filter_utils import (
    parse_arguments, Config, MemoryMonitor, load_filter_values,
    collect_input_files, generate_output_path, build_jq_filter,
    DataNormalizer, setup_logging
)


def process_file_shell(
        file_path: str,
        field: str,
        values: Union[Set[str], List[re.Pattern]],
        regex: bool,
        output_path: str,
        output_format: str,
        chunk_size: int,
        config: Config,
        log
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
            if stderr:
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
        df = DataNormalizer.normalize_dataframe(df, config)

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
            f"✓ Completed {os.path.basename(file_path)}: {lines_processed:,} lines, "
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


def main():
    args = parse_arguments()
    config = Config(args.config)
    log = setup_logging(config)

    log.info("=" * 80)
    log.info("Method 2 (Shell Pipes) - Reddit Dump Filter")
    log.info("=" * 80)
    log.info(f"Input directory: {args.input}")
    log.info(f"Output directory: {args.output_dir}")
    log.info(f"Output format: {args.format}")
    log.info(f"Field: {args.field} | Value: {args.value} | Regex: {args.regex}")
    log.info(f"Chunk size: {args.chunk_size:,}")
    log.info("=" * 80)

    os.makedirs(args.output_dir, exist_ok=True)

    memory_monitor = MemoryMonitor()
    values = load_filter_values(args, log)

    log.info(f"Scanning for input files matching pattern: {args.file_filter}")
    input_files = collect_input_files(args.input, args.file_filter, config)
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
                input_file, args.output_dir, args.format, config)
            file_path, lines_processed, matched_count, error_count = process_file_shell(
                input_file,
                args.field,
                values,
                args.regex,
                output_path,
                args.format,
                args.chunk_size,
                config,
                log
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
    if total_lines > 0:
        log.info(f"Processing rate: {total_lines / elapsed:.0f} lines/second")
    log.info(f"Output directory: {args.output_dir}")

    if args.format == 'csv':
        csv_compression = config.get('output', 'csv_compression')
        ext = '.csv.gz' if csv_compression == 'gzip' else '.csv'
    else:
        ext = '.parquet'
    output_files = [f for f in os.listdir(args.output_dir) if f.endswith(ext)]
    log.info(f"Output files created: {len(output_files)}")

    total_size = sum(
        os.path.getsize(os.path.join(args.output_dir, f))
        for f in output_files
    ) if output_files else 0
    log.info(f"Total output size: {total_size / (1024 ** 2):.2f} MB")


if __name__ == '__main__':
    main()