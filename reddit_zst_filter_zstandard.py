#!/usr/bin/env python3
import json
import logging
import logging.handlers
import multiprocessing as mp
import os
import sys
import time
import re
from typing import Union, List, Set, Tuple, Optional

import pandas as pd
import psutil

from reddit_filter_utils import (
    parse_arguments, Config, MemoryMonitor, load_filter_values,
    collect_input_files, generate_output_path, FileReader, json_loads,
    DataNormalizer, setup_logging, build_prefilter, iter_candidate_lines,
    resolve_fields, format_eta
)


class OutputWriter:
    """
    Collects matched records and writes them out.

    Without `fields`: keeps every record in memory and writes once at the end
    (original behaviour, all columns).
    With `fields`: keeps only those columns and, for CSV, appends to the output
    file every `batch_size` records, so memory stays flat on huge files.
    """

    def __init__(self, output_path: str, output_format: str, config: Config,
                 fields: Optional[List[str]], batch_size: int):
        self.output_path = output_path
        self.output_format = output_format
        self.config = config
        self.fields = fields
        # with a fixed column list both formats can be written in batches
        self.stream = fields is not None
        self.batch_size = batch_size
        self.buffer = []
        self.written = 0
        self.count = 0
        self.parquet_writer = None

    def add(self, obj: dict):
        if self.fields is not None:
            obj = {k: obj[k] for k in self.fields if k in obj}
        self.buffer.append(obj)
        self.count += 1
        if self.stream and len(self.buffer) >= self.batch_size:
            self._flush()

    def _frame(self) -> pd.DataFrame:
        df = pd.DataFrame(self.buffer, columns=self.fields) if self.fields else pd.DataFrame(self.buffer)
        return DataNormalizer.normalize_dataframe(df, self.config)

    def _flush(self):
        if not self.buffer:
            return
        df = self._frame()
        if self.output_format == 'parquet':
            self._write_parquet_batch(df)
        else:
            df.to_csv(
                self.output_path,
                mode='w' if self.written == 0 else 'a',
                header=self.written == 0,
                compression=self.config.get('output', 'csv_compression'),
                index=False
            )
        self.written += len(self.buffer)
        self.buffer = []

    def _write_parquet_batch(self, df: pd.DataFrame):
        """
        Append one row group. Parquet needs the same schema for every batch,
        but pandas infers types per batch (a column can be int in one batch and
        all-empty in the next). So in streaming mode every column is stored as
        a string, with missing values as nulls; the values are the same text
        the CSV output would contain. Cast types after loading if needed.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq
        if self.parquet_writer is None:
            schema = pa.schema([(c, pa.string()) for c in self.fields])
            self.parquet_writer = pq.ParquetWriter(
                self.output_path, schema,
                compression=self.config.get('output', 'parquet_compression'))
        arrays = []
        for c in self.fields:
            col = df[c].astype(object)
            col = col.where(col.notna(), None)
            arrays.append(pa.array([None if v is None else str(v) for v in col], type=pa.string()))
        self.parquet_writer.write_table(pa.Table.from_arrays(arrays, schema=self.parquet_writer.schema))

    def close(self):
        """Write whatever is left. Returns True if an output file exists."""
        if self.stream:
            self._flush()
            if self.parquet_writer is not None:
                self.parquet_writer.close()
            return self.written > 0
        if not self.buffer:
            return False
        df = self._frame()
        if self.output_format == 'parquet':
            df.to_parquet(
                self.output_path,
                engine='pyarrow',
                compression=self.config.get('output', 'parquet_compression'),
                index=False
            )
        else:
            df.to_csv(
                self.output_path,
                compression=self.config.get('output', 'csv_compression'),
                index=False
            )
        self.buffer = []
        return True


def process_file_python(
        file_path: str,
        field: str,
        values: Union[Set[str], List[re.Pattern]],
        regex: bool,
        output_path: str,
        output_format: str,
        config: Config,
        log,
        file_reader: FileReader,
        prefilter=None,
        fields: Optional[List[str]] = None,
        batch_size: int = 100000,
) -> Tuple[str, int, int, int]:
    """
    Process one .zst file using Python's zstandard library.

    With a prefilter, only lines whose raw bytes contain  "field":"value"
    are parsed; the exact check below is unchanged, so the matched records
    are the same. Malformed lines are then counted only among candidates.
    """
    if log is None:  # inside a worker process
        log = logging.getLogger("reddit_filter")
    writer = OutputWriter(output_path, output_format, config, fields, batch_size)
    lines_processed = 0
    error_lines = 0
    name = os.path.basename(file_path)

    value = None
    if len(values) == 1 and not regex:
        value = min(values)

    process = psutil.Process()
    process.cpu_percent()  # first call always returns 0.0; prime it
    log_interval = config.get('processing', 'progress_log_interval')
    next_log = log_interval
    start = time.time()

    def check(line: bytes):
        nonlocal error_lines
        try:
            obj = json_loads(line)
            observed = obj[field].lower()
            if regex:
                matched = any(reg.search(observed) for reg in values)
            elif value is not None:
                matched = observed == value
            else:
                matched = observed in values
            if matched:
                writer.add(obj)
        except (KeyError, json.JSONDecodeError, AttributeError):
            error_lines += 1

    try:
        for block, pos, total in file_reader.iter_blocks(file_path):
            if prefilter is not None:
                for line in iter_candidate_lines(block, prefilter):
                    check(line)
            else:
                for line in block.split(b'\n')[:-1]:
                    check(line)
            lines_processed += block.count(b'\n')

            if lines_processed >= next_log:
                next_log = (lines_processed // log_interval + 1) * log_interval
                elapsed = time.time() - start
                frac = pos / total if total else 0
                eta = format_eta(elapsed * (1 - frac) / frac) if frac > 0 else "?"
                mem = process.memory_info().rss / (1024 ** 3)
                log.info(
                    f"{name}: {frac:6.1%} | {lines_processed:,} lines, "
                    f"{writer.count:,} matched | ETA {eta} | "
                    f"CPU: {process.cpu_percent()}%, RAM: {mem:.2f} GB")

    except Exception as err:
        log.error(f"Error processing {file_path}: {err}")
        return file_path, lines_processed, 0, error_lines

    try:
        created = writer.close()
    except Exception as e:
        log.error(f"Failed to write output file {output_path}: {e}")
        return file_path, lines_processed, 0, error_lines

    if created:
        log.info(
            f"✓ Completed {name}: {lines_processed:,} lines, "
            f"{writer.count:,} matched, {error_lines:,} errors -> {output_path}")
    else:
        log.info(
            f"✓ Completed {name}: {lines_processed:,} lines, "
            f"0 matched, {error_lines:,} errors (no output file created)")

    return file_path, lines_processed, writer.count, error_lines


def _init_worker(log_queue):
    """Worker processes send log records to the main process through a queue."""
    log = logging.getLogger("reddit_filter")
    log.handlers.clear()
    log.setLevel(logging.INFO)
    log.addHandler(logging.handlers.QueueHandler(log_queue))


def _run_task(task):
    """Unpack one file's arguments; runs in a worker process."""
    return process_file_python(*task)


def main():
    args = parse_arguments()
    config = Config(args.config)
    log = setup_logging(config)

    fields = resolve_fields(args.fields, config)
    # .get with defaults: older config.json files don't have the new keys
    batch_size = args.batch_size or config._config.get('processing', {}).get('batch_size', 100000)

    log.info("=" * 80)
    log.info("Method 1 (Zstandard) - Reddit Dump Filter")
    log.info("=" * 80)
    log.info(f"Input directory: {args.input}")
    log.info(f"Output directory: {args.output_dir}")
    log.info(f"Output format: {args.format}")
    log.info(f"Field: {args.field} | Value: {args.value} | Regex: {args.regex}")
    if fields:
        log.info(f"Fields ({len(fields)}, streaming, batch {batch_size:,}): {', '.join(fields)}")
    if args.workers > 1:
        log.info(f"Workers: {args.workers} files in parallel")
    log.info("=" * 80)

    os.makedirs(args.output_dir, exist_ok=True)

    memory_monitor = MemoryMonitor()
    file_reader = FileReader(config)
    values = load_filter_values(args, log)
    prefilter = None
    if args.no_prefilter:
        log.info("Prefilter disabled: --no_prefilter")
    else:
        prefilter = build_prefilter(args.field, values, args.regex, log)

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

    tasks = [
        (input_file, args.field, values, args.regex,
         generate_output_path(input_file, args.output_dir, args.format, config),
         args.format, config, None, file_reader, prefilter, fields, batch_size)
        for input_file in input_files
    ]

    output_of = {task[0]: task[4] for task in tasks}  # input file -> its output path
    created_files = []

    def account(result):
        nonlocal total_processed, total_lines, total_matched, total_errors
        file_path, lines_processed, matched_count, error_count = result
        if matched_count > 0 and os.path.exists(output_of[file_path]):
            created_files.append(output_of[file_path])
        total_processed += 1
        total_lines += lines_processed
        total_matched += matched_count
        total_errors += error_count
        progress_pct = (total_processed / len(input_files)) * 100
        mem_stats = memory_monitor.get_usage_stats()
        log.info(
            f"Progress: {total_processed}/{len(input_files)} ({progress_pct:.1f}%) | "
            f"Total matched: {total_matched:,} | RAM (main process): {mem_stats['rss_gb']:.2f} GB"
        )

    workers = max(1, min(args.workers, len(tasks)))
    try:
        if workers == 1:
            for task in tasks:
                account(process_file_python(*task[:7], log, *task[8:]))
        else:
            # "spawn" behaves the same on macOS, Linux and Windows
            ctx = mp.get_context("spawn")
            log_queue = ctx.Queue()
            listener = logging.handlers.QueueListener(log_queue, *log.handlers)
            listener.start()
            try:
                with ctx.Pool(workers, initializer=_init_worker, initargs=(log_queue,)) as pool:
                    for result in pool.imap_unordered(_run_task, tasks):
                        account(result)
            finally:
                listener.stop()

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
    log.info(f"Total errors: {total_errors:,}" + (" (among prefiltered candidates)" if prefilter else ""))
    log.info(f"Elapsed time: {elapsed:.1f} seconds")
    if total_lines > 0:
        log.info(f"Processing rate: {total_lines / elapsed:.0f} lines/second")
    log.info(f"Output directory: {args.output_dir}")

    # Only files written by this run: the output dir may already hold files
    # from earlier runs (other months, submissions vs comments, ...).
    log.info(f"Output files created: {len(created_files)}")
    total_size = sum(os.path.getsize(f) for f in created_files)
    log.info(f"Total output size: {total_size / (1024 ** 2):.2f} MB")


if __name__ == '__main__':
    main()
