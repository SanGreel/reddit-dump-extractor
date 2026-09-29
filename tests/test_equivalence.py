"""
Equivalence tests for the byte-level prefilter and streaming output.

Builds a small synthetic .zst dump full of edge cases and checks that
process_file_python finds exactly the same records as a naive reference
(json.loads every line + exact field check), with the prefilter on and off,
with a tiny read chunk so lines and multi-byte characters straddle chunks.

Run from the repo root:  python tests/test_equivalence.py
"""
import json
import logging
import os
import subprocess
import sys
import tempfile

import pandas as pd
import zstandard

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from reddit_filter_utils import Config, FileReader, build_prefilter  # noqa: E402
from reddit_zst_filter_zstandard import process_file_python  # noqa: E402

FIELD = "subreddit"
VALUES = {"ukraine", "europe", "reddit_ukr"}

LINES = [
    # plain matches
    '{"id":"a1","subreddit":"ukraine","body":"hello"}',
    '{"id":"a2","subreddit":"europe","body":"hi"}',
    # case differs from the filter value
    '{"id":"a3","subreddit":"Ukraine","body":"case"}',
    '{"id":"a4","subreddit":"REDDIT_UKR","body":"upper"}',
    # whitespace around the colon
    '{"id":"a5", "subreddit" : "europe", "body":"spaces"}',
    # Cyrillic and emoji (multi-byte) in a matching line
    '{"id":"a6","subreddit":"reddit_ukr","body":"Поверніть Федорова 🇺🇦 — картонки"}',
    # value appears only in a nested object (crosspost) -> NOT a match
    '{"id":"n1","subreddit":"cats","crosspost_parent_list":[{"subreddit":"ukraine"}]}',
    # value appears only inside text -> NOT a match
    '{"id":"n2","subreddit":"news","title":"see \\"subreddit\\":\\"ukraine\\""}',
    # longer name with the value as prefix -> NOT a match
    '{"id":"n3","subreddit":"ukraine2","body":"x"}',
    '{"id":"n4","subreddit":"europeans","body":"x"}',
    # other subreddits with Cyrillic
    '{"id":"n5","subreddit":"pics","body":"' + "Київ " * 40 + '"}',
    # subreddit null / missing -> error, not a match
    '{"id":"e1","subreddit":null,"body":"x"}',
    '{"id":"e2","body":"no subreddit key"}',
    # malformed JSON that contains the pattern -> error, not a match
    '{"id":"e3","subreddit":"ukraine","body":"broken',
    # matching line at both the second-to-last and very last position
    '{"id":"a7","subreddit":"ukraine","body":"' + "довгий " * 30 + '"}',
    '{"id":"a8","subreddit":"europe","body":"last line, no trailing newline"}',
]


def reference(lines):
    ids, errors = set(), 0
    for line in lines:
        try:
            obj = json.loads(line)
            if obj[FIELD].lower() in VALUES:
                ids.add(obj["id"])
        except (KeyError, json.JSONDecodeError, AttributeError):
            errors += 1
    return ids, errors


def make_config(tmp):
    with open("config.json") as f:
        cfg = json.load(f)
    cfg["file_reading"]["chunk_size_bytes"] = 37  # odd, tiny: forces splits mid-line and mid-character
    cfg["logging"]["log_dir"] = os.path.join(tmp, "logs")
    path = os.path.join(tmp, "config.json")
    with open(path, "w") as f:
        json.dump(cfg, f)
    return Config(path)


def run(tmp, config, log, prefilter, fields=None, batch_size=100000, tag=""):
    dump = os.path.join(tmp, "RC_test.zst")
    out = os.path.join(tmp, f"out{tag}.csv")
    _, lines, matched, errors = process_file_python(
        dump, FIELD, VALUES, False, out, "csv", config, log, FileReader(config),
        prefilter=prefilter, fields=fields, batch_size=batch_size)
    df = pd.read_csv(out, dtype=str, keep_default_na=False)
    return set(df["id"]), lines, matched, errors, df


def main():
    log = logging.getLogger("test")
    log.addHandler(logging.NullHandler())
    failures = 0

    def check(name, cond, detail=""):
        nonlocal failures
        tag = "PASS" if cond else "FAIL"
        if sys.stdout.isatty():
            tag = f"\033[{'32' if cond else '31'}m{tag}\033[0m"
        print(f"{tag} {name}" + (f"  {detail}" if detail and not cond else ""))
        failures += 0 if cond else 1

    with tempfile.TemporaryDirectory() as tmp:
        raw = "\n".join(LINES).encode()  # note: no trailing newline
        with open(os.path.join(tmp, "RC_test.zst"), "wb") as f:
            f.write(zstandard.ZstdCompressor().compress(raw))
        config = make_config(tmp)
        ref_ids, ref_errors = reference(LINES)

        pf = build_prefilter(FIELD, VALUES, False, log)
        check("prefilter built for plain names", pf is not None)
        check("prefilter off in regex mode", build_prefilter(FIELD, VALUES, True, log) is None)
        check("prefilter off for non-ASCII values", build_prefilter(FIELD, {"київ"}, False, log) is None)

        ids_full, lines_full, m_full, err_full, _ = run(tmp, config, log, None, tag="_full")
        check("no prefilter == reference ids", ids_full == ref_ids, f"{sorted(ids_full ^ ref_ids)}")
        check("no prefilter == reference errors", err_full == ref_errors, f"{err_full} vs {ref_errors}")
        check("all lines counted (incl. last w/o newline)", lines_full == len(LINES), f"{lines_full}")

        ids_pf, lines_pf, m_pf, err_pf, _ = run(tmp, config, log, pf, tag="_pf")
        check("prefilter == reference ids", ids_pf == ref_ids, f"{sorted(ids_pf ^ ref_ids)}")
        check("prefilter: same line count", lines_pf == lines_full)
        check("prefilter: errors only among candidates", err_pf <= err_full, f"{err_pf}")

        fields = ["id", "subreddit", "body"]
        ids_st, _, m_st, _, df_st = run(tmp, config, log, pf, fields=fields, batch_size=2, tag="_stream")
        check("streaming (batch 2) == reference ids", ids_st == ref_ids, f"{sorted(ids_st ^ ref_ids)}")
        check("streaming: only requested columns", list(df_st.columns) == fields, f"{list(df_st.columns)}")
        check("streaming: no duplicated header rows", "id" not in set(df_st["id"]))
        cyr = df_st.loc[df_st["id"] == "a6", "body"].iloc[0]
        check("streaming: multi-byte text intact", cyr.startswith("Поверніть Федорова 🇺🇦"), cyr)

        # --- streaming Parquet (needs pyarrow)
        try:
            import pyarrow  # noqa: F401
            have_pyarrow = True
        except ImportError:
            have_pyarrow = False
            print("SKIP streaming parquet (pyarrow not installed)")
        if have_pyarrow:
            out_pq = os.path.join(tmp, "out_stream.parquet")
            process_file_python(os.path.join(tmp, "RC_test.zst"), FIELD, VALUES, False, out_pq,
                                "parquet", config, log, FileReader(config),
                                prefilter=pf, fields=fields, batch_size=2)
            pq_df = pd.read_parquet(out_pq)
            check("streaming parquet (batch 2) == reference ids", set(pq_df["id"]) == ref_ids,
                  f"{sorted(set(pq_df['id']) ^ ref_ids)}")
            check("streaming parquet: same values as streaming CSV",
                  pq_df.set_index("id").sort_index().fillna("").equals(
                      df_st.set_index("id").sort_index()))

        # --- --workers: several files in parallel == one by one
        dumps = os.path.join(tmp, "dumps")
        os.makedirs(dumps)
        half = len(LINES) // 2
        for name, part in [("RC_a.zst", LINES[:half]), ("RC_b.zst", LINES[half:])]:
            with open(os.path.join(dumps, name), "wb") as f:
                f.write(zstandard.ZstdCompressor().compress("\n".join(part).encode()))
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        outputs = {}
        for w in (1, 2):
            out_dir = os.path.join(tmp, f"workers{w}")
            r = subprocess.run(
                [sys.executable, os.path.join(repo, "reddit_zst_filter_zstandard.py"), dumps,
                 "--value", ",".join(VALUES), "--fields", ",".join(fields),
                 "--workers", str(w), "--output_dir", out_dir,
                 "--config", os.path.join(tmp, "config.json")],
                cwd=tmp, capture_output=True, text=True)
            ok = r.returncode == 0
            check(f"--workers {w} exits cleanly", ok, r.stderr[-500:])
            outputs[w] = {n: pd.read_csv(os.path.join(out_dir, n), dtype=str, keep_default_na=False)
                          for n in sorted(os.listdir(out_dir))} if ok else {}
            if w == 2 and ok:
                check("--workers 2: per-file progress logged from workers",
                      "RC_a.zst" in r.stderr and "RC_b.zst" in r.stderr)
        # summary counts only this run's files, not what was already in the output dir
        out_dir = os.path.join(tmp, "workers1")
        with open(os.path.join(out_dir, "RS_old_run.csv"), "w") as f:
            f.write("id\nold\n")
        r = subprocess.run(
            [sys.executable, os.path.join(repo, "reddit_zst_filter_zstandard.py"), dumps,
             "--value", ",".join(VALUES), "--fields", ",".join(fields),
             "--output_dir", out_dir, "--config", os.path.join(tmp, "config.json")],
            cwd=tmp, capture_output=True, text=True)
        check("summary: 'Output files created' counts only this run",
              "Output files created: 2" in r.stderr,
              [ln for ln in r.stderr.splitlines() if "Output files created" in ln])
        same = outputs.get(1) and outputs.get(2) and outputs[1].keys() == outputs[2].keys() and all(
            outputs[1][n].equals(outputs[2][n]) for n in outputs[1])
        check("--workers 2 output == --workers 1 output", bool(same))
        all_ids = set().union(*[set(df["id"]) for df in outputs.get(2, {}).values()]) if outputs.get(2) else set()
        check("--workers 2: union of files == reference ids", all_ids == ref_ids, f"{sorted(all_ids ^ ref_ids)}")

    summary = "OK" if failures == 0 else f"{failures} FAILED"
    if sys.stdout.isatty():
        summary = f"\033[1;{'32' if failures == 0 else '31'}m{summary}\033[0m"
    print(f"\n{summary} ({len(LINES)} synthetic lines, "
          f"{len(ref_ids)} expected matches)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
