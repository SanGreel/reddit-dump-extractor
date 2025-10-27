#!/usr/bin/env python3
import json
import os
import sys
import time
import re
from typing import Union, List, Set, Tuple

import pandas as pd
import psutil

from reddit_filter_utils import (
    parse_arguments, Config, MemoryMonitor, load_filter_values,
    collect_input_files, generate_output_path, FileReader, json_loads,
    DataNormalizer, setup_logging
)


def process_file_python(
        file_path: str,
        field: str,
        values: Union[Set[str], List[re.Pattern]],
        regex: bool,
        output_path: str,
        output_format: str,
        config: Config,
        log,
        file_reader: FileReader
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
        for line in file_reader.yield_lines(file_path):
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
                f"{len(matched_records):,} matched, {error_lines:,} errors -> {output_path}")
        except Exception as e:
            log.error(f"Failed to write output file {output_path}: {e}")
            return file_path, lines_processed, 0, error_lines
    else:
        log.info(
            f"✓ Completed {os.path.basename(file_path)}: {lines_processed:,} lines, "
            f"0 matched, {error_lines:,} errors (no output file created)")

    return file_path, lines_processed, len(matched_records), error_lines


def main():
    args = parse_arguments()
    config = Config(args.config)
    log = setup_logging(config)

    log.info("=" * 80)
    log.info("Method 1 (Zstandard) - Reddit Dump Filter")
    log.info("=" * 80)
    log.info(f"Input directory: {args.input}")
    log.info(f"Output directory: {args.output_dir}")
    log.info(f"Output format: {args.format}")
    log.info(f"Field: {args.field} | Value: {args.value} | Regex: {args.regex}")
    log.info("=" * 80)

    os.makedirs(args.output_dir, exist_ok=True)

    memory_monitor = MemoryMonitor()
    file_reader = FileReader(config)
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
            file_path, lines_processed, matched_count, error_count = process_file_python(
                input_file,
                args.field,
                values,
                args.regex,
                output_path,
                args.format,
                config,
                log,
                file_reader
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