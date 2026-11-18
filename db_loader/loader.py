"""
Unified DB Loader - the ONLY code in the project that writes to PostgreSQL.

Public API (used by the orchestrator, or by pipeline wrappers via the CLI):

    new_run_id(pipeline_name)                                     -> str
    start_run(run_id, pipeline_name, batch_size, input_files)
    save_batch_stats(payload)        # after each batch is ingested by the engine
    save_final_results(payload)      # once, after the engine's global Q1/Q2/Q3 step
    finish_run(run_id, runtime_seconds, status="SUCCESS", error_message=None)

Connection (environment variables):
    NASA_DB_HOST (localhost)  NASA_DB_PORT (5432)  NASA_DB_USER (nasa)
    NASA_DB_PASSWORD (nasa)   NASA_DB_NAME (nasa_etl)
Driver: pip install psycopg2-binary
"""
from __future__ import annotations

import datetime as _dt
import math
import os
import uuid
from contextlib import contextmanager
from typing import Iterable

from .contract import PIPELINES, Q1, Q2, Q3, validate_batch_stats, validate_final_results

_injected_conn = None


def use_connection(conn) -> None:
    """Testing hook: make the loader use an existing DB-API connection."""
    global _injected_conn
    _injected_conn = conn


def _connect():
    import psycopg2  # type: ignore
    return psycopg2.connect(
        host=os.getenv("NASA_DB_HOST", "localhost"),
        port=int(os.getenv("NASA_DB_PORT", "5432")),
        user=os.getenv("NASA_DB_USER", "nasa"),
        password=os.getenv("NASA_DB_PASSWORD", "nasa"),
        dbname=os.getenv("NASA_DB_NAME", "nasa_etl"),
    )


@contextmanager
def _transaction():
    conn = _injected_conn or _connect()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if _injected_conn is None:
            conn.close()


def _now() -> _dt.datetime:
    t = _dt.datetime.now()
    return t.replace(microsecond=(t.microsecond // 1000) * 1000)


def _check_run(cur, run_id: str, pipeline: str | None, allowed: tuple[str, ...]) -> str:
    """Lock the run row and check it is in one of the allowed states. Returns the status."""
    cur.execute("SELECT pipeline_name, status FROM runs WHERE run_id = %s FOR UPDATE", (run_id,))
    row = cur.fetchone()
    if row is None:
        raise ValueError(f"unknown run_id {run_id!r}: call start_run first")
    if pipeline is not None and row[0] != pipeline:
        raise ValueError(f"run {run_id} belongs to pipeline {row[0]!r}, payload says {pipeline!r}")
    if row[1] not in allowed:
        raise ValueError(f"run {run_id} is {row[1]}; this action needs status {' or '.join(allowed)}")
    return row[1]


# ---------------------------------------------------------------- public API
def new_run_id(pipeline_name: str) -> str:
    """e.g. 'hive-20261118T140512-3fa9c1'. Also use it to name the engine's staging area."""
    if pipeline_name not in PIPELINES:
        raise ValueError(f"pipeline_name must be one of {PIPELINES}")
    return f"{pipeline_name}-{_dt.datetime.now():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def start_run(run_id: str, pipeline_name: str, batch_size: int, input_files: Iterable[str] | str) -> None:
    """Register the run. Called BEFORE the runtime timer starts (registration is not part of runtime)."""
    if pipeline_name not in PIPELINES:
        raise ValueError(f"pipeline_name must be one of {PIPELINES}")
    if int(batch_size) <= 0:
        raise ValueError("batch_size must be > 0")
    files = input_files if isinstance(input_files, str) else ",".join(input_files)
    with _transaction() as cur:
        cur.execute("INSERT INTO runs (run_id, pipeline_name, batch_size, input_files, status, started_at) "
                    "VALUES (%s, %s, %s, %s, 'RUNNING', %s)",
                    (run_id, pipeline_name, int(batch_size), files, _now()))


def save_batch_stats(payload: dict) -> None:
    """Record one ingested batch. Only allowed while the run is RUNNING (i.e. before finalization).
    Re-saving the same (run_id, batch_id) before finalization replaces it."""
    p = validate_batch_stats(payload)
    with _transaction() as cur:
        _check_run(cur, p["run_id"], p["pipeline_name"], ("RUNNING",))
        cur.execute("DELETE FROM batches WHERE run_id = %s AND batch_id = %s", (p["run_id"], p["batch_id"]))
        cur.execute(
            "INSERT INTO batches (run_id, batch_id, pipeline_name, record_count, parsed_count, malformed_count, "
            "batch_runtime_seconds, executed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (p["run_id"], p["batch_id"], p["pipeline_name"], p["record_count"], p["parsed_count"],
             p["malformed_count"], p["batch_runtime_seconds"], _now()))


def save_final_results(payload: dict) -> None:
    """
    Store the run's global Q1/Q2/Q3 results and move the run RUNNING -> FINALIZED.
    Allowed once per run. Before writing, checks that the engine's totals match the
    batches recorded for this run, so a final step that skipped or double-counted a
    batch is rejected. Empty result lists are valid (e.g. every record was malformed).
    """
    p = validate_final_results(payload)
    run_id, pipe, last = p["run_id"], p["pipeline_name"], p["last_batch_id"]
    with _transaction() as cur:
        _check_run(cur, run_id, pipe, ("RUNNING",))
        cur.execute("SELECT COUNT(*), COALESCE(MAX(batch_id),0), COALESCE(SUM(record_count),0), "
                    "COALESCE(SUM(parsed_count),0), COALESCE(SUM(malformed_count),0) "
                    "FROM batches WHERE run_id = %s", (run_id,))
        n, max_id, rec, parsed, bad = (int(x) for x in cur.fetchone())
        problems = []
        if n == 0:
            problems.append("no batches recorded for this run")
        if n != max_id:
            problems.append(f"batch IDs are not contiguous 1..{max_id} ({n} batches recorded)")
        if last != max_id:
            problems.append(f"last_batch_id={last} but the last recorded batch is {max_id}")
        t = p["totals"]
        if (t["record_count"], t["parsed_count"], t["malformed_count"]) != (rec, parsed, bad):
            problems.append(f"totals {t} != sum of recorded batches "
                            f"(record={rec}, parsed={parsed}, malformed={bad})")
        if problems:
            raise ValueError("final results rejected: " + "; ".join(problems))

        for table in (Q1, Q2, Q3):
            cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))
        meta = (run_id, last, pipe, _now())
        res = p["results"]
        _insert_many(cur, Q1, ("log_date", "status_code", "request_count", "total_bytes"), meta, res[Q1])
        _insert_many(cur, Q2, ("rank_no", "resource_path", "request_count", "total_bytes",
                               "distinct_host_count"), meta, res[Q2])
        _insert_many(cur, Q3, ("log_date", "log_hour", "error_request_count", "total_request_count",
                               "error_rate", "distinct_error_hosts"), meta, res[Q3])
        cur.execute("UPDATE runs SET status = 'FINALIZED', finalized_at = %s, final_aggregation_seconds = %s "
                    "WHERE run_id = %s",
                    (meta[3], None if p["final_aggregation_seconds"] is None
                     else round(p["final_aggregation_seconds"], 3), run_id))


def finish_run(run_id: str, runtime_seconds: float, status: str = "SUCCESS",
               error_message: str | None = None) -> None:
    """
    Called by the orchestrator AFTER it stops the timer.
      SUCCESS: only from FINALIZED (final results must have been committed).
      FAILED:  from RUNNING or FINALIZED.
    """
    if status not in ("SUCCESS", "FAILED"):
        raise ValueError("status must be SUCCESS or FAILED")
    runtime = float(runtime_seconds)
    if not math.isfinite(runtime) or runtime < 0:
        raise ValueError("runtime_seconds must be a finite number >= 0")
    allowed = ("FINALIZED",) if status == "SUCCESS" else ("RUNNING", "FINALIZED")
    with _transaction() as cur:
        _check_run(cur, run_id, None, allowed)
        cur.execute("UPDATE runs SET status = %s, finished_at = %s, runtime_seconds = %s, "
                    "error_message = %s WHERE run_id = %s",
                    (status, _now(), round(runtime, 3), error_message, run_id))


def _insert_many(cur, table: str, cols: tuple[str, ...], meta: tuple, rows: list[dict]) -> None:
    if not rows:
        return
    all_cols = ("run_id", "batch_id", "pipeline_name", "executed_at") + cols
    sql = f"INSERT INTO {table} ({', '.join(all_cols)}) VALUES ({', '.join(['%s'] * len(all_cols))})"
    cur.executemany(sql, [meta + tuple(r[c] for c in cols) for r in rows])
