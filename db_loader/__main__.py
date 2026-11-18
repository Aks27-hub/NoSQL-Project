"""
Command-line entry point, so Java / Pig / Hive / shell wrappers can use the loader.

  python -m db_loader validate FILE.json [...]     # offline: no DB, no driver needed
  python -m db_loader load     FILE.json [...]     # validate + write (kind decides which table)
  python -m db_loader new-run-id --pipeline hive
  python -m db_loader start-run  --run-id ID --pipeline hive --batch-size 50000 --input-files a.log,b.log
  python -m db_loader finish-run --run-id ID --runtime-seconds 123.4 [--status FAILED --error "msg"]

FILE may be '-' to read one payload from stdin.
Exit code: 0 = OK, 1 = contract violation, 2 = other error.
"""
from __future__ import annotations

import argparse
import json
import sys

from .contract import KIND_BATCH, PIPELINES, ContractError, validate_payload


def _read(path: str) -> dict:
    if path == "-":
        return json.load(sys.stdin)
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _describe(p: dict) -> str:
    if p["kind"] == KIND_BATCH:
        return (f"batch_stats {p['pipeline_name']} run={p['run_id']} batch={p['batch_id']} "
                f"records={p['record_count']} malformed={p['malformed_count']}")
    r = p["results"]
    return (f"final_results {p['pipeline_name']} run={p['run_id']} batches=1..{p['last_batch_id']} "
            f"q1={len(r['q1_daily_traffic'])} q2={len(r['q2_top_resources'])} q3={len(r['q3_hourly_errors'])} rows")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m db_loader")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("validate").add_argument("files", nargs="+")
    sub.add_parser("load").add_argument("files", nargs="+")
    n = sub.add_parser("new-run-id")
    n.add_argument("--pipeline", required=True, choices=PIPELINES)
    s = sub.add_parser("start-run")
    s.add_argument("--run-id", required=True)
    s.add_argument("--pipeline", required=True, choices=PIPELINES)
    s.add_argument("--batch-size", required=True, type=int)
    s.add_argument("--input-files", required=True)
    f = sub.add_parser("finish-run")
    f.add_argument("--run-id", required=True)
    f.add_argument("--runtime-seconds", required=True, type=float)
    f.add_argument("--status", default="SUCCESS", choices=("SUCCESS", "FAILED"))
    f.add_argument("--error", default=None)
    a = ap.parse_args(argv)

    try:
        if a.cmd == "validate":
            bad = 0
            for path in a.files:
                try:
                    print(f"OK   {path}  ({_describe(validate_payload(_read(path)))})")
                except ContractError as e:
                    bad += 1
                    print(f"FAIL {path}\n  {e}", file=sys.stderr)
            return 1 if bad else 0

        from . import loader  # lazy: 'validate' never needs psycopg2

        if a.cmd == "load":
            for path in a.files:
                p = _read(path)
                (loader.save_batch_stats if p.get("kind") == KIND_BATCH else loader.save_final_results)(p)
                print(f"LOADED {path}")
        elif a.cmd == "new-run-id":
            print(loader.new_run_id(a.pipeline))
        elif a.cmd == "start-run":
            loader.start_run(a.run_id, a.pipeline, a.batch_size, a.input_files)
            print(a.run_id)
        elif a.cmd == "finish-run":
            loader.finish_run(a.run_id, a.runtime_seconds, a.status, a.error)
            print(f"FINISHED {a.run_id}")
        return 0
    except ContractError as e:
        print(str(e), file=sys.stderr)
        return 1
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
