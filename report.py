"""
Reporting module: reads the stored results from PostgreSQL and prints them with the
run's execution metadata. Separate from the ETL run, so its time is never in the runtime.

    python report.py                    # latest successful run
    python report.py --run-id <run_id>  # a specific run
    python report.py --pipeline hive    # latest successful run of one pipeline
    python report.py --rows all         # print every result row (default: 20 per query)
    python report.py --batches          # also list every batch
    python report.py --list             # all runs
    python report.py --compare          # latest successful run of each pipeline, side by side

Connection: same NASA_DB_* environment variables as db_loader (default nasa/nasa@localhost:5432/nasa_etl).
"""
from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO_ROOT)

from db_loader.loader import _connect  # noqa: E402  (same connection settings as the loader)

QUERIES = [
    ("q1_daily_traffic", "Query 1: Daily Traffic Summary",
     ["log_date", "status_code", "request_count", "total_bytes"], "log_date, status_code"),
    ("q2_top_resources", "Query 2: Top 20 Requested Resources",
     ["rank_no", "resource_path", "request_count", "total_bytes", "distinct_host_count"], "rank_no"),
    ("q3_hourly_errors", "Query 3: Hourly Error Analysis",
     ["log_date", "log_hour", "error_request_count", "total_request_count", "error_rate",
      "distinct_error_hosts"], "log_date, log_hour"),
]


# ======================================================================= formatting
def _fmt(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, (float, Decimal)):
        return f"{v:,.6f}".rstrip("0").rstrip(".") if abs(v) < 1 else f"{v:,.3f}"
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d %H:%M:%S") if hasattr(v, "hour") else v.strftime("%Y-%m-%d")
    return str(v)


def table(headers: list[str], rows: list[tuple], right: set[int] | None = None) -> str:
    """Plain-text table. Columns in `right` (or numeric ones) are right-aligned."""
    cells = [[_fmt(v) for v in r] for r in rows]
    if right is None:
        right = {i for i in range(len(headers)) if rows and all(
            isinstance(r[i], (int, float, Decimal)) and not isinstance(r[i], bool) for r in rows if r[i] is not None)}
    widths = [max([len(h)] + [len(c[i]) for c in cells]) for i, h in enumerate(headers)]
    line = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    def row(vals):
        return "| " + " | ".join(v.rjust(widths[i]) if i in right else v.ljust(widths[i])
                                 for i, v in enumerate(vals)) + " |"
    return "\n".join([line, row(headers), line] + [row(c) for c in cells] + [line])


def kv(pairs: list[tuple[str, object]]) -> str:
    w = max(len(k) for k, _ in pairs)
    return "\n".join(f"  {k.ljust(w)} : {_fmt(v)}" for k, v in pairs)


def banner(title: str) -> str:
    return f"\n{'=' * 78}\n {title}\n{'=' * 78}"


# ======================================================================= data access
def fetch(cur, sql: str, params=()) -> list[tuple]:
    cur.execute(sql, params)
    return cur.fetchall()


def summary(cur, run_id: str) -> dict | None:
    cur.execute("SELECT * FROM v_run_summary WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    return dict(zip([d[0] for d in cur.description], row)) if row else None


def resolve_run_id(cur, run_id: str | None, pipeline: str | None) -> str:
    if run_id:
        return run_id
    sql = "SELECT run_id FROM runs WHERE status = 'SUCCESS'"
    params: tuple = ()
    if pipeline:
        sql += " AND pipeline_name = %s"
        params = (pipeline,)
    rows = fetch(cur, sql + " ORDER BY finished_at DESC LIMIT 1", params)
    if not rows:
        raise SystemExit("no successful run found" + (f" for pipeline {pipeline!r}" if pipeline else ""))
    return rows[0][0]


# ======================================================================= reports
def report_run(cur, run_id: str, max_rows: int | None, show_batches: bool) -> None:
    s = summary(cur, run_id)
    if s is None:
        raise SystemExit(f"run {run_id!r} not found")
    print(banner(f"RUN REPORT  |  pipeline: {s['pipeline_name'].upper()}  |  run id: {run_id}"))
    print(kv([
        ("Selected pipeline", s["pipeline_name"]),
        ("Run status", s["status"]),
        ("Started at", s["started_at"]),
        ("Results committed at", s["finalized_at"]),
        ("RUNTIME (seconds)", s["runtime_seconds"]),
        ("  of which final query step", s["final_aggregation_seconds"]),
        ("Configured batch size", s["configured_batch_size"]),
        ("Number of batches", s["num_batches"]),
        ("Batch IDs", f"1 .. {s['num_batches']}" if s["num_batches"] else "-"),
        ("Average batch size", s["avg_batch_size"]),
        ("Total records processed", s["total_records"]),
        ("Parsed records", s["total_parsed"]),
        ("Malformed records", s["total_malformed"]),
    ]))
    print("  (runtime = from reading the input until the final results were written to PostgreSQL;\n"
          "   average batch size = total records / number of non-empty batches)")

    batches = fetch(cur, "SELECT batch_id, record_count, parsed_count, malformed_count, batch_runtime_seconds, "
                         "executed_at FROM batches WHERE run_id = %s ORDER BY batch_id", (run_id,))
    print(f"\nBatches ({len(batches)}):")
    if show_batches or len(batches) <= 10:
        shown = batches
    else:
        shown = batches[:3] + batches[-2:]
    print(table(["batch_id", "records", "parsed", "malformed", "seconds", "executed_at"], shown))
    if len(shown) < len(batches):
        print(f"  ... {len(batches) - len(shown)} batches not shown (use --batches)")

    for tbl, title, cols, order in QUERIES:
        meta = fetch(cur, f"SELECT pipeline_name, batch_id, executed_at, COUNT(*) OVER () FROM {tbl} "
                          f"WHERE run_id = %s LIMIT 1", (run_id,))
        rows = fetch(cur, f"SELECT {', '.join(cols)} FROM {tbl} WHERE run_id = %s ORDER BY {order}", (run_id,))
        print(banner(title))
        if meta:
            pipe, batch_id, executed_at, n = meta[0]
            print(kv([("Query name", title.split(": ", 1)[1]), ("Pipeline", pipe), ("Run id", run_id),
                      ("Batch id", f"{batch_id}  (results cover batches 1..{batch_id})"),
                      ("Executed at", executed_at), ("Run runtime (seconds)", s["runtime_seconds"]),
                      ("Batch size / avg batch size", f"{_fmt(s['configured_batch_size'])} / "
                                                     f"{_fmt(s['avg_batch_size'])}"),
                      ("Result rows", n)]))
        else:
            print("  (no rows)")
            continue
        shown = rows if max_rows is None else rows[:max_rows]
        print(table(cols, shown))
        if len(shown) < len(rows):
            print(f"  ... {len(rows) - len(shown)} more rows (use --rows all)")


def report_list(cur) -> None:
    rows = fetch(cur, "SELECT run_id, pipeline_name, status, configured_batch_size, num_batches, total_records, "
                      "total_malformed, runtime_seconds, started_at FROM v_run_summary ORDER BY started_at DESC")
    print(banner(f"ALL RUNS ({len(rows)})"))
    print(table(["run_id", "pipeline", "status", "batch_size", "batches", "records", "malformed",
                 "runtime_s", "started_at"], rows))


def report_compare(cur) -> None:
    runs = fetch(cur, "SELECT DISTINCT ON (pipeline_name) run_id, pipeline_name FROM runs WHERE status = 'SUCCESS' "
                      "ORDER BY pipeline_name, finished_at DESC")
    if not runs:
        raise SystemExit("no successful runs yet")
    print(banner("PIPELINE COMPARISON (latest successful run of each pipeline)"))
    rows = []
    for run_id, _ in runs:
        s = summary(cur, run_id)
        rows.append((s["pipeline_name"], run_id, s["configured_batch_size"], s["num_batches"], s["avg_batch_size"],
                     s["total_records"], s["total_malformed"], s["final_aggregation_seconds"], s["runtime_seconds"]))
    print(table(["pipeline", "run_id", "batch_size", "batches", "avg_batch", "records", "malformed",
                 "final_step_s", "RUNTIME_s"], rows))
    if len({r[2] for r in rows}) > 1:
        print("  NOTE: batch sizes differ - runtimes are only comparable at the same batch size.")

    # are the stored results the same for every pipeline?
    base_id = runs[0][0]
    print("\nAre the query results identical across pipelines? (compared row by row with "
          f"{runs[0][1]})")
    checks = []
    for tbl, title, cols, order in QUERIES:
        base = fetch(cur, f"SELECT {', '.join(cols)} FROM {tbl} WHERE run_id = %s ORDER BY {order}", (base_id,))
        for run_id, pipe in runs[1:]:
            other = fetch(cur, f"SELECT {', '.join(cols)} FROM {tbl} WHERE run_id = %s ORDER BY {order}", (run_id,))
            checks.append((title.split(":")[0], pipe, len(other), "IDENTICAL" if other == base else "DIFFERENT"))
    if checks:
        print(table(["query", "pipeline", "rows", "vs " + runs[0][1]], checks))
    else:
        print("  (only one pipeline has a successful run so far)")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Report stored ETL results from PostgreSQL")
    ap.add_argument("--run-id")
    ap.add_argument("--pipeline", choices=("mapreduce", "pig", "hive", "mongodb"))
    ap.add_argument("--rows", default="20", help="rows shown per query: a number or 'all' (default 20)")
    ap.add_argument("--batches", action="store_true", help="list every batch")
    ap.add_argument("--list", action="store_true", help="list all runs")
    ap.add_argument("--compare", action="store_true", help="compare the latest run of each pipeline")
    a = ap.parse_args(argv)
    max_rows = None if a.rows == "all" else int(a.rows)

    conn = _connect()
    try:
        cur = conn.cursor()
        if a.list:
            report_list(cur)
        elif a.compare:
            report_compare(cur)
        else:
            report_run(cur, resolve_run_id(cur, a.run_id, a.pipeline), max_rows, a.batches)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
