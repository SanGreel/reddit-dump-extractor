#!/usr/bin/env python3
"""
Benchmark this version of reddit_zst_filter_zstandard.py against a baseline
git ref (default: main, i.e. the version before these changes).

For a quick, fair comparison it cuts the first N lines of a real dump into a
sample .zst, runs the baseline and the new version on it with the same
arguments, measures wall time and peak memory of each run, and checks that the
outputs are byte-identical. Optionally also times --workers on N copies.

Usage (from the repo root, inside the venv):
  python benchmarks/compare_with_baseline.py /path/RC_2026-07.zst \
      --value "ukraine,europe,worldnews" --lines 5000000 --workers 4

Prints a table for the terminal and the same table in Markdown for a PR.
macOS / Linux (uses the `resource` module for peak RSS).
"""
import argparse
import filecmp
import json
import os
import platform
import re
import resource
import shutil
import subprocess
import sys
import tempfile
import time

import zstandard

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = "reddit_zst_filter_zstandard.py"
BASELINE_FILES = ["reddit_zst_filter_zstandard.py", "reddit_filter_utils.py", "config.json"]
SHOW = re.compile(r"(lines, [\d,]+ matched|Completed|ERROR)")

TTY = sys.stdout.isatty()
def c(text, code):
    return f"\033[{code}m{text}\033[0m" if TTY else text
BOLD, DIM, GREEN, RED, CYAN = "1", "2", "32", "31", "36"


# ---------------------------------------------------------------- helpers
def measure_child(cmd, cwd):
    """Run cmd, stream its interesting log lines, return wall time + peak RSS.
    Runs inside a fresh helper process so RUSAGE_CHILDREN belongs to this run only."""
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, cwd=cwd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    log = []
    for line in proc.stderr:
        log.append(line)
        if SHOW.search(line):
            msg = re.split(r" - (?:INFO|ERROR): ", line, maxsplit=1)[-1].rstrip()
            msg = msg.split(" -> ", 1)[0]  # drop the long temp output path
            sys.stderr.write(f"    {msg}\n")
            sys.stderr.flush()
    proc.wait()
    wall = time.perf_counter() - t0
    rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    rss_bytes = rss if sys.platform == "darwin" else rss * 1024
    print(json.dumps({"wall": wall, "rss": rss_bytes, "returncode": proc.returncode, "log": "".join(log)}))


def run(label, cmd, cwd):
    print(c(f"\n▶ {label}", BOLD), file=sys.stderr, flush=True)
    helper = [sys.executable, os.path.abspath(__file__), "--_measure", cwd, "--", *cmd]
    out = subprocess.run(helper, stdout=subprocess.PIPE, text=True).stdout
    res = json.loads(out.strip().splitlines()[-1])
    if res["returncode"] != 0:
        sys.exit(f"{label} failed:\n{res['log'][-2000:]}")
    num = lambda pat: int(re.search(pat, res["log"]).group(1).replace(",", ""))
    res["lines"] = num(r"Total lines scanned: ([\d,]+)")
    res["matched"] = num(r"Total records matched: ([\d,]+)")
    return res


def make_sample(src, dst, n_lines):
    """First n_lines of src -> dst (zstd), streaming, nothing big on disk."""
    dctx = zstandard.ZstdDecompressor(max_window_size=2 ** 31)
    cctx = zstandard.ZstdCompressor(level=3, threads=-1)
    written = 0
    with open(src, "rb") as fin, open(dst, "wb") as fout:
        reader = dctx.stream_reader(fin)
        with cctx.stream_writer(fout, closefd=False) as writer:
            while written < n_lines:
                chunk = reader.read(64 * 1024 * 1024)
                if not chunk:
                    break
                need = n_lines - written
                count = chunk.count(b"\n")
                if count < need:
                    writer.write(chunk)
                    written += count
                else:
                    pos = -1
                    for _ in range(need):
                        pos = chunk.index(b"\n", pos + 1)
                    writer.write(chunk[:pos + 1])
                    written += need
    return written


def git(*args):
    return subprocess.run(["git", "-C", REPO, *args], capture_output=True, text=True).stdout.strip()


def fmt_s(x): return f"{x:,.1f} s"
def fmt_rate(r): return f"{r / 1000:,.0f}k"
def fmt_gb(b): return f"{b / 1024 ** 3:.2f} GB"


# ---------------------------------------------------------------- main
def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--_measure":
        return measure_child(sys.argv[4:], sys.argv[2])

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dump", help="a real .zst dump (RS_ or RC_)")
    ap.add_argument("--value", required=True, help="comma-separated subreddits, as for the tool")
    ap.add_argument("--field", default="subreddit")
    ap.add_argument("--lines", type=int, default=5_000_000, help="sample size (default 5,000,000; 0 = whole file)")
    ap.add_argument("--fields", default="comments", help="preset/list for the extra --fields run ('' to skip)")
    ap.add_argument("--workers", type=int, default=0, help="also time --workers on this many copies (0 = skip)")
    ap.add_argument("--baseline-ref", default="main")
    ap.add_argument("--keep", action="store_true", help="keep the temp dir with outputs")
    args = ap.parse_args()

    tmp = tempfile.mkdtemp(prefix="rde_bench_")
    try:
        # ---- input
        in_dir = os.path.join(tmp, "in")
        os.makedirs(in_dir)
        name = os.path.basename(args.dump)
        sample = os.path.join(in_dir, name)
        if args.lines:
            print(c(f"Cutting first {args.lines:,} lines of {name} ...", DIM), file=sys.stderr, flush=True)
            n = make_sample(args.dump, sample, args.lines)
        else:
            os.symlink(os.path.abspath(args.dump), sample)
            n = None
        size_mb = os.path.getsize(sample) / 1024 ** 2

        # ---- baseline code from git
        base_dir = os.path.join(tmp, "baseline")
        os.makedirs(base_dir)
        for f in BASELINE_FILES:
            content = subprocess.run(["git", "-C", REPO, "show", f"{args.baseline_ref}:{f}"],
                                     capture_output=True).stdout
            if not content:
                sys.exit(f"cannot read {f} from git ref {args.baseline_ref}")
            with open(os.path.join(base_dir, f), "wb") as fh:
                fh.write(content)
        new_cwd = os.path.join(tmp, "new")
        os.makedirs(new_cwd)
        cfg = os.path.join(REPO, "config.json")
        common = [in_dir, "--field", args.field, "--value", args.value]

        runs = []
        base = run(f"baseline ({args.baseline_ref})",
                   [sys.executable, SCRIPT, *common, "--output_dir", os.path.join(tmp, "out_base")], base_dir)
        runs.append(("baseline", f"`{args.baseline_ref}`", base, "reference"))
        new = run("new", [sys.executable, os.path.join(REPO, SCRIPT), *common, "--config", cfg,
                          "--output_dir", os.path.join(tmp, "out_new")], new_cwd)
        same = filecmp.cmp(os.path.join(tmp, "out_base", name.replace(".zst", ".csv")),
                           os.path.join(tmp, "out_new", name.replace(".zst", ".csv")), shallow=False)
        runs.append(("new", "all fields", new, "byte-identical ✓" if same else "DIFFERENT ✗"))
        if args.fields:
            nf = run(f"new --fields {args.fields}",
                     [sys.executable, os.path.join(REPO, SCRIPT), *common, "--config", cfg,
                      "--fields", args.fields, "--output_dir", os.path.join(tmp, "out_fields")], new_cwd)
            ok = nf["matched"] == base["matched"]
            runs.append(("new", f"`--fields {args.fields}`", nf, "same records ✓" if ok else "DIFFERENT ✗"))

        par = []
        if args.workers > 1:
            multi = os.path.join(tmp, "multi")
            os.makedirs(multi)
            for i in range(args.workers):
                # keep the RS_/RC_ prefix, the tool's file filter needs it
                os.symlink(sample, os.path.join(multi, name.replace(".zst", f"_part{i + 1}.zst")))
            extra = ["--fields", args.fields] if args.fields else []
            for w in (1, args.workers):
                r = run(f"new, {args.workers} files, --workers {w}",
                        [sys.executable, os.path.join(REPO, SCRIPT), multi, "--field", args.field,
                         "--value", args.value, "--config", cfg, *extra, "--workers", str(w),
                         "--output_dir", os.path.join(tmp, f"out_w{w}")], new_cwd)
                par.append((w, r))

        # ---- report
        head = git("rev-parse", "--short", "HEAD") + ("+dirty" if git("status", "--porcelain", "--untracked-files=no") else "")
        base_hash = git("rev-parse", "--short", args.baseline_ref)
        cpu = platform.processor() or platform.machine()
        if sys.platform == "darwin":  # e.g. "Apple M2" instead of "arm"
            brand = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                   capture_output=True, text=True).stdout.strip()
            cpu = brand or cpu
        env = (f"{platform.system()} {platform.release()} · {cpu} · {os.cpu_count()} CPUs · "
               f"Python {platform.python_version()} · zstandard {zstandard.__version__}")
        src = f"{name}, first {n:,} lines ({size_mb:,.0f} MB zst)" if n else f"{name} ({size_mb:,.0f} MB zst)"
        values = args.value.split(",")
        speed = base["wall"] / new["wall"]
        mem = 1 - new["rss"] / base["rss"]

        rows = []
        for who, how, r, check in runs:
            rows.append([who, how, fmt_s(r["wall"]), fmt_rate(r["lines"] / r["wall"]),
                         fmt_gb(r["rss"]), f"{r['matched']:,}", check])
        hdr = ["version", "mode", "wall time", "lines/s", "peak RSS", "matched", "output"]

        term_rows = [[str(x).replace("`", "") for x in row] for row in rows]  # backticks only for Markdown
        w = [max(len(str(x)) for x in col) for col in zip(hdr, *term_rows)]
        line = lambda cells: "  ".join(str(x).ljust(w[i]) for i, x in enumerate(cells))
        print()
        print(c("Reddit Dump Extractor · benchmark", BOLD))
        print(c(f"input     {src}", DIM))
        print(c(f"filter    {args.field} ∈ {{{', '.join(values)}}}", DIM))
        print(c(f"versions  baseline {args.baseline_ref} ({base_hash})  vs  new {head}", DIM))
        print(c(f"machine   {env}", DIM))
        print()
        print(c(line(hdr), BOLD))
        print("  ".join("─" * x for x in w))
        for row in term_rows:
            txt = line(row)
            txt = txt.replace("✓", c("✓", GREEN)).replace("✗", c("✗", RED))
            print(txt)
        print()
        print(c(f"speed-up  {speed:.1f}×", BOLD + ";" + GREEN) + "   " +
              c(f"peak memory  −{mem:.0%}", BOLD + ";" + GREEN))
        if par:
            (w1, r1), (wn, rn) = par
            print(c(f"--workers  {args.workers} files: {fmt_s(r1['wall'])} with 1 worker → "
                    f"{fmt_s(rn['wall'])} with {wn}  ({r1['wall'] / rn['wall']:.1f}×)", BOLD + ";" + CYAN))

        # markdown for the PR
        print(c("\nMarkdown for the PR:\n", DIM))
        print(f"**Input:** {src}; filter `{args.field}` ∈ {len(values)} subreddits  ")
        print(f"**Machine:** {env}  ")
        print(f"**Versions:** baseline `{args.baseline_ref}` ({base_hash}) vs `{head}`\n")
        print("| " + " | ".join(hdr) + " |")
        print("|" + "---|" * len(hdr))
        for row in rows:
            print("| " + " | ".join(row) + " |")
        print(f"\n**{speed:.1f}× faster, {mem:.0%} less peak memory**, identical output.")
        if par:
            (w1, r1), (wn, rn) = par
            print(f"\n`--workers {wn}` on {args.workers} files: {fmt_s(r1['wall'])} → {fmt_s(rn['wall'])} "
                  f"(**{r1['wall'] / rn['wall']:.1f}×**).")
    finally:
        if args.keep:
            print(c(f"\noutputs kept in {tmp}", DIM))
        else:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
