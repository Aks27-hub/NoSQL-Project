"""
Reference (expected) results for Q1/Q2/Q3, computed in plain Python with common/parser.py.

NOT a pipeline - it is the test oracle every pipeline is compared against:

    python tools/reference.py                                   # both default files -> work/reference.json
    python tools/reference.py --input data/sample.log -o work/ref_sample.json
    python tools/reference.py --compare work/<run_id>/final_results.json [more.json ...]
                    # compare runs with work/reference.json; exit 1 if anything differs

Query semantics implemented here are exactly docs/PARSING_RULES.md section 7.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from common.parser import (ERROR_RATE_DECIMALS, ERROR_STATUS_MAX, ERROR_STATUS_MIN, Q2_TOP_N,  # noqa: E402
                           decode, parse_record)
from orchestrator import find_default_inputs, iter_records  # noqa: E402

def compute(paths: list[str]) -> dict:
    record_count = malformed = 0
    q1 = defaultdict(lambda: [0, 0])                    # (date, status) -> [requests, bytes]
    q2 = defaultdict(lambda: [0, 0, set()])             # path -> [requests, bytes, hosts]
    q3 = defaultdict(lambda: [0, 0, set()])             # (date, hour) -> [errors, total, error hosts]

    for raw in iter_records(paths):
        record_count += 1
        r = parse_record(decode(raw))
        if r is None:
            malformed += 1
            continue
        a = q1[(r["log_date"], r["status_code"])]
        a[0] += 1
        a[1] += r["bytes_transferred"]
        if r["resource_path"] is not None:
            b = q2[r["resource_path"]]
            b[0] += 1
            b[1] += r["bytes_transferred"]
            b[2].add(r["host"])
        c = q3[(r["log_date"], r["log_hour"])]
        c[1] += 1
        if ERROR_STATUS_MIN <= r["status_code"] <= ERROR_STATUS_MAX:
            c[0] += 1
            c[2].add(r["host"])

    q1_rows = [{"log_date": d, "status_code": s, "request_count": n, "total_bytes": b}
               for (d, s), (n, b) in sorted(q1.items())]
    top = sorted(q2.items(), key=lambda kv: (-kv[1][0], kv[0]))[:Q2_TOP_N]
    q2_rows = [{"resource_path": p, "request_count": n, "total_bytes": b, "distinct_host_count": len(h)}
               for p, (n, b, h) in top]
    q3_rows = [{"log_date": d, "log_hour": h, "error_request_count": e, "total_request_count": t,
                "error_rate": round(e / t, ERROR_RATE_DECIMALS), "distinct_error_hosts": len(hs)}
               for (d, h), (e, t, hs) in sorted(q3.items())]
    return {
        "kind": "reference_results",
        "input_files": [os.path.basename(p) for p in paths],
        "totals": {"record_count": record_count, "parsed_count": record_count - malformed,
                   "malformed_count": malformed},
        "results": {"q1_daily_traffic": q1_rows, "q2_top_resources": q2_rows, "q3_hourly_errors": q3_rows},
    }


# ======================================================================= compare
KEYS = {
    "q1_daily_traffic": ("log_date", "status_code"),
    "q3_hourly_errors": ("log_date", "log_hour"),
}
TOL = 1e-6


def _load(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _norm(row: dict) -> dict:
    out = dict(row)
    out.pop("rank_no", None)
    for k, v in out.items():
        if k in ("status_code", "log_hour", "request_count", "total_bytes", "distinct_host_count",
                 "error_request_count", "total_request_count", "distinct_error_hosts"):
            out[k] = int(v)
        elif k == "error_rate":
            out[k] = float(v)
    return out


def _same(a: dict, b: dict) -> bool:
    if a.keys() != b.keys():
        return False
    return all(abs(a[k] - b[k]) <= TOL if k == "error_rate" else a[k] == b[k] for k in a)


def compare(a: dict, b: dict, max_show: int = 10) -> list[str]:
    diffs: list[str] = []
    if a.get("totals") != b.get("totals"):
        diffs.append(f"totals differ: {a.get('totals')} vs {b.get('totals')}")
    ra, rb = a["results"], b["results"]
    for q, key in KEYS.items():
        da = {tuple(_norm(r)[k] for k in key): _norm(r) for r in ra[q]}
        db = {tuple(_norm(r)[k] for k in key): _norm(r) for r in rb[q]}
        only_a, only_b = sorted(set(da) - set(db)), sorted(set(db) - set(da))
        changed = sorted(k for k in set(da) & set(db) if not _same(da[k], db[k]))
        if only_a or only_b or changed:
            diffs.append(f"{q}: {len(da)} vs {len(db)} rows; missing in 2nd={len(only_a)}, "
                         f"extra in 2nd={len(only_b)}, different={len(changed)}")
            diffs += [f"   missing {k}" for k in only_a[:max_show]]
            diffs += [f"   extra   {k}" for k in only_b[:max_show]]
            diffs += [f"   {k}: {da[k]} vs {db[k]}" for k in changed[:max_show]]
    qa = [_norm(r) for r in ra["q2_top_resources"]]
    qb = [_norm(r) for r in rb["q2_top_resources"]]
    if len(qa) != len(qb) or not all(_same(x, y) for x, y in zip(qa, qb)):
        diffs.append(f"q2_top_resources differs ({len(qa)} vs {len(qb)} rows; order matters):")
        for i in range(max(len(qa), len(qb))):
            x = qa[i] if i < len(qa) else None
            y = qb[i] if i < len(qb) else None
            if x is None or y is None or not _same(x, y):
                diffs.append(f"   rank {i + 1}: {x} vs {y}")
    return diffs



def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", "-i", nargs="+", help="raw log files (default: data/NASA_access_log_{Jul95,Aug95}[.gz])")
    ap.add_argument("--output", "-o", default=os.path.join(REPO, "work", "reference.json"))
    ap.add_argument("--compare", nargs="+", metavar="RESULTS_JSON",
                    help="compare these final_results.json files with --output (no recomputation)")
    a = ap.parse_args(argv)
    if a.compare:
        with open(a.output, encoding="utf-8") as fh:
            base = json.load(fh)
        status = 0
        for other in a.compare:
            with open(other, encoding="utf-8") as fh:
                d = compare(base, json.load(fh))
            print(f"{'IDENTICAL' if not d else 'DIFFERENT'}: {a.output}  vs  {other}")
            for line in d:
                print("  " + line)
            status |= bool(d)
        return status
    paths = a.input or find_default_inputs()
    if not paths:
        ap.error("no input given and the NASA files were not found in data/")
    t0 = time.perf_counter()
    res = compute(paths)
    os.makedirs(os.path.dirname(os.path.abspath(a.output)), exist_ok=True)
    with open(a.output, "w", encoding="utf-8") as fh:
        json.dump(res, fh, indent=2)
    t = res["totals"]
    r = res["results"]
    print(f"records={t['record_count']} parsed={t['parsed_count']} malformed={t['malformed_count']}  "
          f"Q1={len(r['q1_daily_traffic'])} Q2={len(r['q2_top_resources'])} Q3={len(r['q3_hourly_errors'])} rows  "
          f"({time.perf_counter() - t0:.1f}s) -> {a.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
