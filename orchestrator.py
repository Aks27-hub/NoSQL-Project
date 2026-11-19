"""
Multi-pipeline ETL orchestrator - the single entry point of the tool.

    python orchestrator.py                                   # interactive: pick a pipeline from a menu
    python orchestrator.py --pipeline pig --batch-size 100000
    python orchestrator.py --pipeline hive --input data/NASA_access_log_Jul95.gz data/NASA_access_log_Aug95.gz
    python orchestrator.py --pipeline mongodb --dry-run       # no PostgreSQL: validate + write JSON only

What it does, in order (runtime rules from the project statement):

  1. register the run in PostgreSQL (db_loader.start_run)            - not timed
  2. runner.prepare()                                                - not timed
  3. START TIMER, open the raw input files (.gz is decompressed on the fly)
  4. cut the input into batches of --batch-size records, IDs 1,2,3..; for each batch:
       write it to a batch file -> runner.ingest_batch() -> check counts -> save_batch_stats
  5. runner.finalize() -> Q1/Q2/Q3 over all batches -> save_final_results (PostgreSQL commit)
  6. STOP TIMER, db_loader.finish_run(runtime)
  7. runner.cleanup()                                                - not timed

The orchestrator never parses log lines; parsing happens inside the selected engine.
It only splits the raw file into records (lines) - see iter_records() below.
"""
from __future__ import annotations

import argparse
import gzip
import importlib
import importlib.util
import json
import os
import shutil
import sys
import time
import traceback
from typing import Any

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO_ROOT)

from db_loader.contract import (CONTRACT_VERSION, KIND_BATCH, KIND_FINAL, Q1, Q2, Q3,  # noqa: E402
                                ContractError, validate_batch_stats, validate_final_results)
from pipelines.base import BatchCounts, BatchInfo, FinalOutput, PipelineRunner, RunContext  # noqa: E402

# ======================================================================= pipeline registry
# Each member's runner: pipelines/<name>/runner.py, class subclassing pipelines.base.PipelineRunner.
RUNNERS = {
    "mapreduce": "pipelines.mapreduce.runner:MapReduceRunner",
    "pig": "pipelines.pig.runner:PigRunner",
    "hive": "pipelines.hive.runner:HiveRunner",
    "mongodb": "pipelines.mongodb.runner:MongoRunner",
}
PIPELINE_ORDER = tuple(RUNNERS)
ALIASES = {"mr": "mapreduce", "mongo": "mongodb"}


def canonical(name: str) -> str:
    name = name.strip().lower()
    return ALIASES.get(name, name)


def is_available(name: str) -> bool:
    try:
        return importlib.util.find_spec(RUNNERS[canonical(name)].split(":")[0]) is not None
    except ModuleNotFoundError:  # pipelines/<name>/ does not exist yet
        return False


def load_runner(name: str) -> PipelineRunner:
    name = canonical(name)
    module_name, class_name = RUNNERS[name].split(":")
    if not is_available(name):
        raise NotImplementedError(f"pipeline {name!r} is not implemented yet "
                                  f"(expected {module_name}.{class_name})")
    runner = getattr(importlib.import_module(module_name), class_name)()
    if not isinstance(runner, PipelineRunner) or runner.name != name:
        raise TypeError(f"{module_name}.{class_name} must subclass PipelineRunner with name = {name!r}")
    return runner


# ======================================================================= records and batches
# RECORD = the bytes up to (not including) a newline byte; a last line without one still counts.
# Nothing is trimmed, decoded or filtered. Files are read in order, .gz decompressed on the fly.
# BATCH k = records (k-1)*N+1 .. k*N across all files; written back byte-for-byte, one per line.
DEFAULT_INPUT_NAMES = ("NASA_access_log_Jul95", "NASA_access_log_Aug95")


def find_default_inputs(repo_root: str = REPO_ROOT) -> list[str]:
    """data/NASA_access_log_{Jul95,Aug95}, plain or .gz, looked up in <repo>/data then ../data."""
    found = []
    for name in DEFAULT_INPUT_NAMES:
        for d in (os.path.join(repo_root, "data"), os.path.join(os.path.dirname(repo_root), "data")):
            hit = next((os.path.join(d, name + ext) for ext in ("", ".gz")
                        if os.path.isfile(os.path.join(d, name + ext))), None)
            if hit:
                found.append(hit)
                break
    return found


def iter_records(paths):
    """Yield every record (raw bytes, no trailing newline) of every file, in order."""
    for path in paths:
        with (gzip.open(path, "rb") if path.endswith(".gz") else open(path, "rb")) as fh:
            for line in fh:  # binary iteration splits on b"\n" only
                yield line[:-1] if line.endswith(b"\n") else line


def iter_batches(paths, batch_size: int, batch_dir: str):
    """Stream the input into batch files of batch_size records; yield each BatchInfo when complete."""
    os.makedirs(batch_dir, exist_ok=True)
    batch_id = count = record_no = 0
    fh = None
    try:
        for rec in iter_records(paths):
            if fh is None:
                batch_id += 1
                first_no = record_no + 1
                path = os.path.join(batch_dir, f"batch_{batch_id:06d}.log")
                fh = open(path, "wb")
            fh.write(rec + b"\n")
            count += 1
            record_no += 1
            if count == batch_size:
                fh.close()
                fh = None
                yield BatchInfo(batch_id, path, count, first_no)
                count = 0
        if fh is not None:  # final, smaller batch
            fh.close()
            fh = None
            yield BatchInfo(batch_id, path, count, first_no)
    finally:
        if fh is not None:
            fh.close()


class RunAborted(RuntimeError):
    pass


# ======================================================================= sinks
class DbSink:
    """Writes through db_loader (PostgreSQL)."""

    label = "PostgreSQL"

    def __init__(self):
        from db_loader import loader  # needs psycopg2
        self.loader = loader

    def new_run_id(self, pipeline):
        return self.loader.new_run_id(pipeline)

    def start_run(self, run_id, pipeline, batch_size, input_files):
        self.loader.start_run(run_id, pipeline, batch_size, list(input_files))

    def save_batch_stats(self, payload):
        self.loader.save_batch_stats(payload)

    def save_final_results(self, payload):
        self.loader.save_final_results(payload)

    def finish_run(self, run_id, runtime, status="SUCCESS", error=None):
        self.loader.finish_run(run_id, runtime, status, error)


class DryRunSink:
    """No database. Validates every payload against the contract and repeats the
    loader's run-level checks, so a pipeline can be developed without PostgreSQL."""

    label = "dry-run (no database)"

    def __init__(self):
        self.batches: dict[int, dict] = {}

    def new_run_id(self, pipeline):
        import datetime
        import uuid
        return f"{pipeline}-{datetime.datetime.now():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"

    def start_run(self, run_id, pipeline, batch_size, input_files):
        pass

    def save_batch_stats(self, payload):
        p = validate_batch_stats(payload)
        self.batches[p["batch_id"]] = p

    def save_final_results(self, payload):
        p = validate_final_results(payload)
        ids = sorted(self.batches)
        if ids != list(range(1, len(ids) + 1)) or p["last_batch_id"] != len(ids):
            raise ValueError(f"final results rejected: batch ids {ids[:3]}.. vs last_batch_id {p['last_batch_id']}")
        sums = {k: sum(b[k] for b in self.batches.values())
                for k in ("record_count", "parsed_count", "malformed_count")}
        if sums != p["totals"]:
            raise ValueError(f"final results rejected: totals {p['totals']} != sum of batches {sums}")

    def finish_run(self, run_id, runtime, status="SUCCESS", error=None):
        pass


# ======================================================================= helpers
def _log(msg: str) -> None:
    print(f"[orchestrator] {msg}", flush=True)


def _choose_pipeline_interactively() -> str:
    print("\nSelect the execution pipeline:")
    for i, name in enumerate(PIPELINE_ORDER, 1):
        status = "" if is_available(name) else "   (not implemented yet)"
        print(f"  {i}. {name}{status}")
    while True:
        choice = input("Enter 1-4 (or a name): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(PIPELINE_ORDER):
            return PIPELINE_ORDER[int(choice) - 1]
        if canonical(choice) in PIPELINE_ORDER:
            return canonical(choice)
        print("  invalid choice")


def _parse_options(pairs: list[str]) -> dict[str, str]:
    opts = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"--opt expects key=value, got {pair!r}")
        k, v = pair.split("=", 1)
        opts[k.strip()] = v
    return opts


def _batch_payload(ctx: RunContext, batch_id: int, counts: BatchCounts, seconds: float) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "kind": KIND_BATCH,
        "pipeline_name": ctx.pipeline_name,
        "run_id": ctx.run_id,
        "batch_id": batch_id,
        "record_count": counts.record_count,
        "parsed_count": counts.parsed_count,
        "malformed_count": counts.malformed_count,
        "batch_runtime_seconds": round(seconds, 3),
    }


def _final_payload(ctx: RunContext, last_batch_id: int, out: FinalOutput, seconds: float) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "kind": KIND_FINAL,
        "pipeline_name": ctx.pipeline_name,
        "run_id": ctx.run_id,
        "last_batch_id": last_batch_id,
        "final_aggregation_seconds": round(seconds, 3),
        "totals": dict(out.totals),
        "results": out.results,
    }


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2)


# ======================================================================= the run
def run(pipeline_name: str, input_files: list[str], batch_size: int, *, dry_run: bool = False,
        work_root: str = "work", keep_batches: bool = False, keep_staging: bool = False,
        options: dict[str, str] | None = None, runner=None, sink=None) -> dict[str, Any]:
    """Execute one complete run. Returns a summary dict. `runner`/`sink` can be injected (tests)."""
    pipeline_name = canonical(pipeline_name)
    if batch_size <= 0:
        raise ValueError("batch size must be > 0")
    for f in input_files:
        if not os.path.isfile(f):
            raise FileNotFoundError(f"input file not found: {f}")

    runner = runner or load_runner(pipeline_name)
    sink = sink or (DryRunSink() if dry_run else DbSink())

    run_id = sink.new_run_id(pipeline_name)
    work_dir = os.path.abspath(os.path.join(work_root, run_id))
    os.makedirs(work_dir, exist_ok=True)
    ctx = RunContext(run_id=run_id, pipeline_name=pipeline_name, batch_size=batch_size,
                     input_files=tuple(os.path.abspath(f) for f in input_files),
                     work_dir=work_dir, repo_root=REPO_ROOT, options=dict(options or {}))

    print("=" * 66)
    print(f" ETL run  | pipeline: {pipeline_name.upper()}  | run id: {run_id}")
    print(f" inputs   | {', '.join(os.path.basename(f) for f in input_files)}")
    print(f" batches  | {batch_size} records each   | results -> {sink.label}")
    print("=" * 66, flush=True)

    # ---- untimed setup
    sink.start_run(run_id, pipeline_name, batch_size, ctx.input_files)
    started = False
    success = False
    batch_rows: list[dict] = []
    try:
        runner.prepare(ctx)
        started = True

        # ---- TIMER STARTS: from opening the raw input ...
        t0 = time.perf_counter()
        last_batch_id = 0
        for batch in iter_batches(ctx.input_files, batch_size, ctx.path("batches")):
            tb = time.perf_counter()
            counts = runner.ingest_batch(ctx, batch)
            secs = time.perf_counter() - tb
            if not isinstance(counts, BatchCounts):
                raise RunAborted(f"ingest_batch must return BatchCounts, got {type(counts).__name__}")
            if counts.record_count != batch.record_count:
                raise RunAborted(
                    f"batch {batch.batch_id}: engine accounted for {counts.record_count} records "
                    f"(parsed {counts.parsed_count} + malformed {counts.malformed_count}) but the batch "
                    f"has {batch.record_count}. Every line must be either parsed or counted as malformed.")
            payload = _batch_payload(ctx, batch.batch_id, counts, secs)
            sink.save_batch_stats(payload)
            batch_rows.append(payload)
            last_batch_id = batch.batch_id
            _log(f"batch {batch.batch_id:>4}: {batch.record_count:>8} records, "
                 f"{counts.malformed_count:>5} malformed, {secs:8.2f}s")
            if not keep_batches:
                os.remove(batch.path)

        if last_batch_id == 0:
            raise RunAborted("input is empty: no records, no batches")

        _log("all batches ingested; running final aggregation (Q1, Q2, Q3)")
        tf = time.perf_counter()
        out = runner.finalize(ctx)
        final_secs = time.perf_counter() - tf
        if not isinstance(out, FinalOutput):
            raise RunAborted(f"finalize must return FinalOutput, got {type(out).__name__}")
        final_payload = _final_payload(ctx, last_batch_id, out, final_secs)
        sink.save_final_results(final_payload)
        # ... until the final results are committed to the database: TIMER STOPS
        runtime = time.perf_counter() - t0

        sink.finish_run(run_id, runtime, "SUCCESS")
        success = True
    except BaseException as e:
        msg = f"{type(e).__name__}: {e}"
        _log(f"RUN FAILED - {msg}")
        e.reported = True  # type: ignore[attr-defined]
        if not isinstance(e, (RunAborted, ContractError, KeyboardInterrupt)):
            traceback.print_exc()
        try:
            sink.finish_run(run_id, 0.0, "FAILED", msg[:2000])
        except Exception as e2:  # noqa: BLE001
            _log(f"(could not mark run as FAILED: {e2})")
        raise
    finally:
        if started:
            try:
                if not keep_staging:
                    runner.cleanup(ctx, success)
            except Exception as e:  # noqa: BLE001
                _log(f"cleanup failed (ignored): {e}")
        if not keep_batches:
            shutil.rmtree(ctx.path("batches"), ignore_errors=True)

    # artefacts for tools/reference.py --compare and debugging (written after the timer)
    _write_json(ctx.path("final_results.json"), final_payload)
    _write_json(ctx.path("batch_stats.json"), batch_rows)

    total = sum(b["record_count"] for b in batch_rows)
    non_empty = sum(1 for b in batch_rows if b["record_count"] > 0)
    summary = {
        "run_id": run_id, "pipeline": pipeline_name, "runtime_seconds": runtime,
        "final_aggregation_seconds": final_secs, "batch_size": batch_size,
        "num_batches": len(batch_rows), "total_records": total,
        "parsed": sum(b["parsed_count"] for b in batch_rows),
        "malformed": sum(b["malformed_count"] for b in batch_rows),
        "avg_batch_size": total / non_empty if non_empty else 0.0,
        "rows": {k: len(final_payload["results"][k]) for k in (Q1, Q2, Q3)},
        "final_results_json": ctx.path("final_results.json"),
    }
    print("=" * 66)
    print(" RUN COMPLETED")
    print(f"  pipeline              : {pipeline_name}")
    print(f"  run id                : {run_id}")
    print(f"  records processed     : {summary['total_records']}"
          f"  (parsed {summary['parsed']}, malformed {summary['malformed']})")
    print(f"  batches               : {summary['num_batches']} x {batch_size} "
          f"(avg batch size {summary['avg_batch_size']:.2f})")
    print(f"  result rows           : Q1={summary['rows'][Q1]}  Q2={summary['rows'][Q2]}  Q3={summary['rows'][Q3]}")
    print(f"  final aggregation     : {final_secs:.3f} s")
    print(f"  RUNTIME               : {runtime:.3f} s   (input read -> results committed)")
    print(f"  results JSON          : {summary['final_results_json']}")
    if not dry_run:
        print(f"  stored in PostgreSQL  : runs / batches / q1..q3 tables, run_id = {run_id}")
    print("=" * 66, flush=True)
    return summary


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Multi-pipeline ETL for the NASA HTTP logs")
    ap.add_argument("--pipeline", "-p", help="mapreduce | pig | hive | mongodb (menu if omitted)")
    ap.add_argument("--input", "-i", nargs="+", help="raw log files (.gz allowed); default: data/NASA_access_log_{Jul95,Aug95}[.gz]")
    ap.add_argument("--batch-size", "-b", type=int, default=100_000, help="records per batch (default 100000)")
    ap.add_argument("--dry-run", action="store_true", help="no PostgreSQL: validate payloads, write JSON to the work dir")
    ap.add_argument("--work-dir", default=os.path.join(REPO_ROOT, "work"), help="local scratch root (default ./work)")
    ap.add_argument("--keep-batches", action="store_true", help="keep the batch files after the run")
    ap.add_argument("--keep-staging", action="store_true", help="skip runner.cleanup() (inspect engine staging data)")
    ap.add_argument("--opt", action="append", default=[], metavar="KEY=VALUE", help="pipeline-specific option, repeatable")
    a = ap.parse_args(argv)

    if a.pipeline:
        pipeline = canonical(a.pipeline)
        if pipeline not in PIPELINE_ORDER:
            ap.error(f"--pipeline must be one of {', '.join(PIPELINE_ORDER)}")
    elif sys.stdin.isatty():
        pipeline = _choose_pipeline_interactively()
    else:
        ap.error("--pipeline is required when not running interactively")

    inputs = a.input or find_default_inputs(REPO_ROOT)
    if not inputs:
        ap.error("no --input given and data/NASA_access_log_Jul95[.gz] / Aug95 not found")

    try:
        run(pipeline, inputs, a.batch_size, dry_run=a.dry_run, work_root=a.work_dir,
            keep_batches=a.keep_batches, keep_staging=a.keep_staging, options=_parse_options(a.opt))
    except NotImplementedError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as e:  # noqa: BLE001
        if not getattr(e, "reported", False):  # failed before the run started (bad input, no DB driver, ...)
            print(f"error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
