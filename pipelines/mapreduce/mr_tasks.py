#!/usr/bin/env python3
"""
Map / combine / reduce functions of the MapReduce pipeline (Hadoop Streaming).

Hadoop runs this file inside every map and reduce task:   python3 mr_tasks.py <task> [arg]
runner.py ships it to the tasks together with common/access_line.regex and
common/request.regex (hadoop -files), so the regexes are read from the shared files,
never hand-copied. Rules: docs/PARSING_RULES.md.

    task             used in          input lines                       output lines
    parse            ingest (map)     raw log line                      staged record (9 fields)
    q1_map           Q1 map           staged record                     date, status, 1, bytes
    q2_map           Q2 map           staged record (path not NULL)     path, host, 1, bytes
    q3_map           Q3 map           staged record                     date, hour, host, 1, is_error
    sum K            combiner + Q1 reducer: per key (first K fields), add up the other fields
    q2_top_local     Q2 reducer       path, host, n, bytes  (sorted)    top 20 of this reducer's paths
    identity         Q2-top map       any line                          the same line
    q2_top_global    Q2-top reducer   path, count, bytes, hosts         the global top 20, in rank order
    q3_reduce        Q3 reducer       date, hour, host, n, err (sorted) date, hour, err, total, rate, hosts

Staged record = host, timestamp, log_date, log_hour, http_method, resource_path,
protocol_version, status_code, bytes_transferred, tab-separated. Strings are escaped
(backslash, tab, newline, CR) so a tab inside a path or host can never shift a column,
and NULL is written as \\N (an escaped real backslash is \\\\, so the two cannot clash).

Counters (read back by runner.py): NASA/records, NASA/parsed, NASA/malformed in the ingest
job, NASA/staged_parsed in the Q1 job.
"""
from __future__ import annotations

import heapq
import os
import re
import sys
from itertools import groupby

MONTHS = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
          "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
ERROR_MIN, ERROR_MAX = 400, 599
TOP_N = 20
RATE_DECIMALS = 6
NULL = "\\N"
COUNTER_GROUP = "NASA"

# field positions in a staged record
HOST, TIMESTAMP, LOG_DATE, LOG_HOUR, METHOD, PATH, PROTOCOL, STATUS, BYTES = range(9)

_ESC = {"\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r"}
_ESC_RE = re.compile(r"[\\\t\n\r]")
_UNESC = {"\\": "\\", "t": "\t", "n": "\n", "r": "\r"}
_UNESC_RE = re.compile(r"\\(.)", re.DOTALL)


def esc(s: str | None) -> str:
    return NULL if s is None else _ESC_RE.sub(lambda m: _ESC[m.group(0)], s)


def unesc(s: str) -> str | None:
    return None if s == NULL else _UNESC_RE.sub(lambda m: _UNESC[m.group(1)], s)


# ======================================================================= I/O helpers
def _lines():
    """Input lines as text, without the trailing '\\n'. Hadoop hands each record's raw bytes."""
    for raw in sys.stdin.buffer:
        if raw.endswith(b"\n"):
            raw = raw[:-1]
        yield raw.decode("utf-8", errors="replace")


def _fields():
    for line in _lines():
        yield line.split("\t")


def _keyed(k: int):
    """(key fields, value fields) for lines whose first k fields are the key."""
    for f in _fields():
        yield f[:k], f[k:]


_out = sys.stdout.buffer


def _emit(*fields) -> None:
    _out.write(("\t".join(str(x) for x in fields) + "\n").encode("utf-8"))


def _counter(name: str, amount: int) -> None:
    sys.stderr.write(f"reporter:counter:{COUNTER_GROUP},{name},{amount}\n")
    sys.stderr.flush()


def _regex(name: str) -> re.Pattern:
    """The shared regex file, shipped into the task's working dir by `hadoop -files`."""
    for d in (os.getcwd(), os.path.dirname(os.path.abspath(__file__))):
        p = os.path.join(d, name)
        if os.path.isfile(p):
            with open(p, encoding="ascii") as fh:
                return re.compile(fh.read())
    raise FileNotFoundError(f"{name} was not shipped with the job")


# ======================================================================= ingest: parsing
def parse() -> None:
    """Map-only ingest job: one raw line -> one staged record, or counted as malformed."""
    line_re = _regex("access_line.regex")     # 12 groups, anchored ^...$
    request_re = _regex("request.regex")      # 3 groups
    records = parsed = 0
    for line in _lines():                     # nothing trimmed or rewritten before matching
        records += 1
        m = line_re.fullmatch(line)
        if m is None:                         # malformed: counted, never staged
            continue
        g = m.group
        log_date = f"{g(5)}-{MONTHS[g(4)]}-{g(3)}"          # no timezone conversion
        r = request_re.fullmatch(g(10))
        method, path, proto = ((r.group(1) or None, r.group(2) or None, r.group(3) or None)
                               if r else (None, None, None))  # empty or missing -> NULL
        nbytes = 0 if g(12) == "-" else int(g(12))           # '-' -> 0, Python int = 64-bit safe
        _emit(esc(g(1)), f"{log_date} {g(6)}:{g(7)}:{g(8)}", log_date, int(g(6)),
              esc(method), esc(path), esc(proto), int(g(11)), nbytes)
        parsed += 1
    _counter("records", records)
    _counter("parsed", parsed)
    _counter("malformed", records - parsed)


# ======================================================================= map functions
def q1_map() -> None:
    n = 0
    for f in _fields():
        n += 1
        _emit(f[LOG_DATE], f[STATUS], 1, f[BYTES])
    _counter("staged_parsed", n)


def q2_map() -> None:
    for f in _fields():
        if f[PATH] != NULL:                   # only records with a resource_path
            _emit(f[PATH], f[HOST], 1, f[BYTES])


def q3_map() -> None:
    for f in _fields():
        is_error = 1 if ERROR_MIN <= int(f[STATUS]) <= ERROR_MAX else 0
        _emit(f[LOG_DATE], f[LOG_HOUR], f[HOST], 1, is_error)


def identity() -> None:
    for line in _lines():
        _out.write((line + "\n").encode("utf-8"))


# ======================================================================= combine / reduce
def sum_k(k: int) -> None:
    """Input sorted by key: add up the numeric value fields of equal keys."""
    for key, group in groupby(_keyed(k), key=lambda kv: kv[0]):
        totals = None
        for _, vals in group:
            nums = [int(v) for v in vals]
            totals = nums if totals is None else [a + b for a, b in zip(totals, nums)]
        _emit(*key, *totals)


def _rank_key(row):
    """ORDER BY request_count DESC, resource_path ASC (code point order of the real path)."""
    return -row[1], unesc(row[0])


def q2_top_local() -> None:
    """Keys (path, host) arrive sorted, and all keys of a path reach the same reducer
    (partitioned on the path only). Per path: requests, bytes, distinct hosts. Emits only
    this reducer's top 20, which is enough: the global top 20 is a subset of the union."""
    def per_path():
        for path, by_path in groupby(_keyed(2), key=lambda kv: kv[0][0]):
            count = nbytes = hosts = 0
            for _host, by_host in groupby(by_path, key=lambda kv: kv[0][1]):
                hosts += 1
                for _, (n, b) in by_host:
                    count += int(n)
                    nbytes += int(b)
            yield path, count, nbytes, hosts
    for row in heapq.nsmallest(TOP_N, per_path(), key=_rank_key):
        _emit(*row)


def q2_top_global() -> None:
    rows = ((f[0], int(f[1]), int(f[2]), int(f[3])) for f in _fields())
    for row in heapq.nsmallest(TOP_N, rows, key=_rank_key):   # emitted in rank order
        _emit(*row)


def q3_reduce() -> None:
    """Keys (date, hour, host) arrive sorted; a (date, hour) group is whole in one reducer."""
    for (log_date, log_hour), by_hour in groupby(_keyed(3), key=lambda kv: tuple(kv[0][:2])):
        total = errors = error_hosts = 0
        for _host, by_host in groupby(by_hour, key=lambda kv: kv[0][2]):
            n = e = 0
            for _, (cnt, err) in by_host:
                n += int(cnt)
                e += int(err)
            total += n
            errors += e
            error_hosts += 1 if e > 0 else 0
        rate = round(errors / total, RATE_DECIMALS)
        _emit(log_date, log_hour, errors, total, repr(rate), error_hosts)


TASKS = {"parse": parse, "q1_map": q1_map, "q2_map": q2_map, "q3_map": q3_map,
         "identity": identity, "q2_top_local": q2_top_local, "q2_top_global": q2_top_global,
         "q3_reduce": q3_reduce}

if __name__ == "__main__":
    task = sys.argv[1] if len(sys.argv) > 1 else ""
    if task == "sum":
        sum_k(int(sys.argv[2]))
    elif task in TASKS:
        TASKS[task]()
    else:
        sys.exit(f"usage: mr_tasks.py {{{'|'.join(TASKS)}|sum K}}")
    _out.flush()
