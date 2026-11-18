"""
Output contract v2.0: the two JSON structures every pipeline hands to the DB Loader.

  1. Batch stats   - one per batch, produced by the engine's INGEST step
  2. Final results - one per run,   produced by the engine's FINALIZE step,
                     i.e. Q1/Q2/Q3 computed over ALL ingested batches

No database dependency, so every member can validate offline:

    python -m db_loader validate out/batch_001.json out/final.json
"""
from __future__ import annotations

import datetime as _dt
import math
import re
from collections import defaultdict
from typing import Any

CONTRACT_VERSION = "2.0"
PIPELINES = ("mapreduce", "pig", "hive", "mongodb")

KIND_BATCH = "batch_stats"
KIND_FINAL = "final_results"

Q1, Q2, Q3 = "q1_daily_traffic", "q2_top_resources", "q3_hourly_errors"
QUERY_KEYS = (Q1, Q2, Q3)
Q1_COLUMNS = ("log_date", "status_code", "request_count", "total_bytes")
Q2_COLUMNS = ("resource_path", "request_count", "total_bytes", "distinct_host_count")
Q3_COLUMNS = ("log_date", "log_hour", "error_request_count", "total_request_count",
              "error_rate", "distinct_error_hosts")

Q2_TOP_N = 20
ERROR_STATUS_MIN, ERROR_STATUS_MAX = 400, 599
ERROR_RATE_DECIMALS = 6
ERROR_RATE_TOLERANCE = 1e-6

_RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,64}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ContractError(ValueError):
    """Raised when a payload breaks the contract. `.errors` lists every problem found."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        shown = "\n  - ".join(errors[:25])
        more = f"\n  ... and {len(errors) - 25} more" if len(errors) > 25 else ""
        super().__init__(f"{len(errors)} contract violation(s):\n  - {shown}{more}")


# ---------------------------------------------------------------- helpers
def _as_int(value: Any, where: str, errors: list[str], minimum: int | None = None) -> int | None:
    """Accept JSON ints or digit strings (handy when a wrapper reads Hive/Pig TSV output)."""
    if isinstance(value, bool):
        errors.append(f"{where}: expected integer, got boolean")
        return None
    if isinstance(value, int):
        out = value
    elif isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
        out = int(value.strip())
    elif isinstance(value, float) and value.is_integer():
        out = int(value)
    else:
        errors.append(f"{where}: expected integer, got {value!r}")
        return None
    if minimum is not None and out < minimum:
        errors.append(f"{where}: must be >= {minimum}, got {out}")
        return None
    return out


def _as_float(value: Any, where: str, errors: list[str], minimum: float | None = None) -> float | None:
    """Accept finite numbers only (JSON NaN / Infinity are rejected)."""
    if value is None:
        return None
    if isinstance(value, bool):
        errors.append(f"{where}: expected number, got boolean")
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        errors.append(f"{where}: expected number, got {value!r}")
        return None
    if not math.isfinite(out):
        errors.append(f"{where}: must be a finite number, got {value!r}")
        return None
    if minimum is not None and out < minimum:
        errors.append(f"{where}: must be >= {minimum}, got {out}")
        return None
    return out


def _as_date(value: Any, where: str, errors: list[str]) -> str | None:
    if not isinstance(value, str) or not _DATE_RE.match(value):
        errors.append(f"{where}: expected 'YYYY-MM-DD' string, got {value!r}")
        return None
    try:
        _dt.date.fromisoformat(value)
    except ValueError:
        errors.append(f"{where}: not a real date: {value!r}")
        return None
    return value


def _check_keys(obj: Any, required: tuple[str, ...], where: str, errors: list[str],
                optional: tuple[str, ...] = ()) -> bool:
    if not isinstance(obj, dict):
        errors.append(f"{where}: expected an object, got {type(obj).__name__}")
        return False
    missing = [c for c in required if c not in obj]
    extra = [c for c in obj if c not in required and c not in optional]
    if missing:
        errors.append(f"{where}: missing key(s) {missing}")
    if extra:
        errors.append(f"{where}: unexpected key(s) {extra}")
    return not missing


def _common_header(p: dict, errors: list[str]) -> dict:
    out: dict[str, Any] = {"contract_version": CONTRACT_VERSION, "kind": p.get("kind")}
    if p.get("contract_version") != CONTRACT_VERSION:
        errors.append(f"contract_version: expected '{CONTRACT_VERSION}', got {p.get('contract_version')!r}")
    if p.get("pipeline_name") not in PIPELINES:
        errors.append(f"pipeline_name: must be one of {PIPELINES}, got {p.get('pipeline_name')!r}")
    out["pipeline_name"] = p.get("pipeline_name")
    rid = p.get("run_id")
    if not isinstance(rid, str) or not _RUN_ID_RE.match(rid):
        errors.append("run_id: 1-64 chars of [A-Za-z0-9_.:-]")
    out["run_id"] = rid
    return out


def _counts(obj: dict, where: str, errors: list[str]) -> dict:
    rc = _as_int(obj["record_count"], f"{where}.record_count", errors, 0)
    pc = _as_int(obj["parsed_count"], f"{where}.parsed_count", errors, 0)
    mc = _as_int(obj["malformed_count"], f"{where}.malformed_count", errors, 0)
    if None not in (rc, pc, mc) and pc + mc != rc:
        errors.append(f"{where}: parsed_count ({pc}) + malformed_count ({mc}) != record_count ({rc})")
    return {"record_count": rc, "parsed_count": pc, "malformed_count": mc}


# ---------------------------------------------------------------- dispatcher
def validate_payload(payload: dict) -> dict:
    """Validate either payload kind, chosen by payload['kind']."""
    if not isinstance(payload, dict):
        raise ContractError(["payload must be a JSON object"])
    kind = payload.get("kind")
    if kind == KIND_BATCH:
        return validate_batch_stats(payload)
    if kind == KIND_FINAL:
        return validate_final_results(payload)
    raise ContractError([f"kind: must be '{KIND_BATCH}' or '{KIND_FINAL}', got {kind!r}"])


# ---------------------------------------------------------------- 1. batch stats
def validate_batch_stats(payload: dict) -> dict:
    errors: list[str] = []
    req = ("contract_version", "kind", "pipeline_name", "run_id", "batch_id",
           "record_count", "parsed_count", "malformed_count")
    if not _check_keys(payload, req, "batch_stats", errors, optional=("batch_runtime_seconds",)):
        raise ContractError(errors)
    out = _common_header(payload, errors)
    out["batch_id"] = _as_int(payload["batch_id"], "batch_id", errors, 1)
    out.update(_counts(payload, "batch_stats", errors))
    out["batch_runtime_seconds"] = _as_float(payload.get("batch_runtime_seconds"), "batch_runtime_seconds", errors, 0)
    if errors:
        raise ContractError(errors)
    return out


# ---------------------------------------------------------------- 2. final results
def validate_final_results(payload: dict) -> dict:
    errors: list[str] = []
    req = ("contract_version", "kind", "pipeline_name", "run_id", "last_batch_id", "totals", "results")
    if not _check_keys(payload, req, "final_results", errors, optional=("final_aggregation_seconds",)):
        raise ContractError(errors)
    out = _common_header(payload, errors)
    out["last_batch_id"] = _as_int(payload["last_batch_id"], "last_batch_id", errors, 1)
    out["final_aggregation_seconds"] = _as_float(payload.get("final_aggregation_seconds"),
                                                 "final_aggregation_seconds", errors, 0)

    totals = payload["totals"]
    parsed_total = None
    if _check_keys(totals, ("record_count", "parsed_count", "malformed_count"), "totals", errors):
        out["totals"] = _counts(totals, "totals", errors)
        parsed_total = out["totals"]["parsed_count"]

    results = payload["results"]
    if not _check_keys(results, QUERY_KEYS, "results", errors):
        raise ContractError(errors)
    for k in QUERY_KEYS:
        if not isinstance(results[k], list):
            errors.append(f"results.{k}: must be a list")
    if any(not isinstance(results[k], list) for k in QUERY_KEYS):
        raise ContractError(errors)

    q1 = _validate_q1(results[Q1], errors)
    q2 = _validate_q2(results[Q2], errors)
    q3 = _validate_q3(results[Q3], errors)
    out["results"] = {Q1: q1, Q2: q2, Q3: q3}

    if not errors:  # cross-checks only make sense on field-valid data
        _cross_checks(q1, q2, q3, parsed_total, errors)
    if errors:
        raise ContractError(errors)
    return out


def _validate_q1(rows: list, errors: list[str]) -> list[dict]:
    out, seen = [], set()
    for i, row in enumerate(rows):
        w = f"{Q1}[{i}]"
        if not _check_keys(row, Q1_COLUMNS, w, errors):
            continue
        r = {
            "log_date": _as_date(row["log_date"], f"{w}.log_date", errors),
            "status_code": _as_int(row["status_code"], f"{w}.status_code", errors, 0),
            "request_count": _as_int(row["request_count"], f"{w}.request_count", errors, 1),
            "total_bytes": _as_int(row["total_bytes"], f"{w}.total_bytes", errors, 0),
        }
        key = (r["log_date"], r["status_code"])
        if key in seen:
            errors.append(f"{w}: duplicate (log_date, status_code) {key}")
        seen.add(key)
        out.append(r)
    return out


def _validate_q2(rows: list, errors: list[str]) -> list[dict]:
    if len(rows) > Q2_TOP_N:
        errors.append(f"{Q2}: at most {Q2_TOP_N} rows allowed, got {len(rows)}")
    out, seen = [], set()
    for i, row in enumerate(rows):
        w = f"{Q2}[{i}]"
        if not _check_keys(row, Q2_COLUMNS, w, errors, optional=("rank_no",)):
            continue
        path = row["resource_path"]
        if not isinstance(path, str) or path == "":
            errors.append(f"{w}.resource_path: must be a non-empty string")
        r = {
            "rank_no": i + 1,
            "resource_path": path,
            "request_count": _as_int(row["request_count"], f"{w}.request_count", errors, 1),
            "total_bytes": _as_int(row["total_bytes"], f"{w}.total_bytes", errors, 0),
            "distinct_host_count": _as_int(row["distinct_host_count"], f"{w}.distinct_host_count", errors, 1),
        }
        if "rank_no" in row and _as_int(row["rank_no"], f"{w}.rank_no", errors) != i + 1:
            errors.append(f"{w}.rank_no: must equal list position {i + 1}")
        if None not in (r["request_count"], r["distinct_host_count"]) and r["distinct_host_count"] > r["request_count"]:
            errors.append(f"{w}: distinct_host_count > request_count")
        if path in seen:
            errors.append(f"{w}: duplicate resource_path {path!r}")
        seen.add(path)
        out.append(r)
    for i in range(1, len(out)):  # request_count DESC, then resource_path ASC
        a, b = out[i - 1], out[i]
        if None in (a["request_count"], b["request_count"]) or not all(
                isinstance(x["resource_path"], str) for x in (a, b)):
            continue
        if (-a["request_count"], a["resource_path"]) > (-b["request_count"], b["resource_path"]):
            errors.append(f"{Q2}: rows {i - 1} and {i} not ordered by request_count DESC, resource_path ASC")
    return out


def _validate_q3(rows: list, errors: list[str]) -> list[dict]:
    out, seen = [], set()
    for i, row in enumerate(rows):
        w = f"{Q3}[{i}]"
        if not _check_keys(row, Q3_COLUMNS, w, errors):
            continue
        r = {
            "log_date": _as_date(row["log_date"], f"{w}.log_date", errors),
            "log_hour": _as_int(row["log_hour"], f"{w}.log_hour", errors, 0),
            "error_request_count": _as_int(row["error_request_count"], f"{w}.error_request_count", errors, 0),
            "total_request_count": _as_int(row["total_request_count"], f"{w}.total_request_count", errors, 1),
            "distinct_error_hosts": _as_int(row["distinct_error_hosts"], f"{w}.distinct_error_hosts", errors, 0),
        }
        rate = _as_float(row["error_rate"], f"{w}.error_rate", errors)
        if r["log_hour"] is not None and r["log_hour"] > 23:
            errors.append(f"{w}.log_hour: must be 0-23")
        e, t, d = r["error_request_count"], r["total_request_count"], r["distinct_error_hosts"]
        r["error_rate"] = rate
        if None not in (e, t):
            if e > t:
                errors.append(f"{w}: error_request_count > total_request_count")
            expected = e / t
            if rate is None or abs(rate - expected) > ERROR_RATE_TOLERANCE:
                errors.append(f"{w}.error_rate: {rate} != error/total = {expected:.6f}")
            r["error_rate"] = round(expected, ERROR_RATE_DECIMALS)
        if None not in (e, d):
            if d > e:
                errors.append(f"{w}: distinct_error_hosts > error_request_count")
            if e == 0 and d != 0:
                errors.append(f"{w}: distinct_error_hosts must be 0 when error_request_count is 0")
        key = (r["log_date"], r["log_hour"])
        if key in seen:
            errors.append(f"{w}: duplicate (log_date, log_hour) {key}")
        seen.add(key)
        out.append(r)
    return out


def _cross_checks(q1: list, q2: list, q3: list, parsed_total: int | None, errors: list[str]) -> None:
    """Consistency between the three queries - catches parsing/filtering bugs early."""
    q1_total = sum(r["request_count"] for r in q1)
    q3_total = sum(r["total_request_count"] for r in q3)
    if parsed_total is not None:
        if q1_total != parsed_total:
            errors.append(f"cross-check: sum(Q1.request_count)={q1_total} != totals.parsed_count={parsed_total}")
        if q3_total != parsed_total:
            errors.append(f"cross-check: sum(Q3.total_request_count)={q3_total} != totals.parsed_count={parsed_total}")
        if parsed_total > 0 and not q2:
            errors.append(f"cross-check: parsed_count > 0 but {Q2} is empty")
    if q2 and q2[0]["request_count"] > q1_total:
        errors.append("cross-check: Q2 top resource has more requests than the whole dataset")

    q1_by, q1_err, q3_by, q3_err = (defaultdict(int) for _ in range(4))
    for r in q1:
        q1_by[r["log_date"]] += r["request_count"]
        if ERROR_STATUS_MIN <= r["status_code"] <= ERROR_STATUS_MAX:
            q1_err[r["log_date"]] += r["request_count"]
    for r in q3:
        q3_by[r["log_date"]] += r["total_request_count"]
        q3_err[r["log_date"]] += r["error_request_count"]
    for d in sorted(set(q1_by) | set(q3_by)):
        if q1_by[d] != q3_by[d]:
            errors.append(f"cross-check: {d}: Q1 requests={q1_by[d]} != Q3 total requests={q3_by[d]}")
        if q1_err[d] != q3_err[d]:
            errors.append(f"cross-check: {d}: Q1 4xx/5xx requests={q1_err[d]} != Q3 error requests={q3_err[d]}")
