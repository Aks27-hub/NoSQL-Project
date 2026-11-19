"""
Shared parsing spec + reference parser (rules in prose: docs/PARSING_RULES.md).

The two regexes live in plain files next to this one (common/access_line.regex,
common/request.regex) so Java / Pig / Hive / Mongo code can load them instead of
hand-copying them.

    python common/parser.py          # print the regexes escaped for each language
    python common/parser.py --test   # check the rules against common/test_vectors.json

This parser is NOT a pipeline. It is the test oracle (used by tools/reference.py);
every engine must reproduce parse_record() exactly with its own primitives.
"""
from __future__ import annotations

import json
import os
import re
import sys
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name: str) -> str:
    with open(os.path.join(_HERE, name), encoding="ascii") as fh:
        return fh.read()


# ---------------------------------------------------------------- the spec
ACCESS_LINE_REGEX = _load("access_line.regex")
REQUEST_REGEX = _load("request.regex")

# capture group numbers, 1-based: Java Matcher.group(n), Hive regexp_extract(.., n),
# Pig REGEX_EXTRACT_ALL field $(n-1), Mongo $regexFind captures[n-1]
LINE_GROUPS = {"host": 1, "timestamp_raw": 2, "day": 3, "month": 4, "year": 5, "hour": 6,
               "minute": 7, "second": 8, "tz_offset": 9, "request": 10, "status": 11, "bytes": 12}
REQUEST_GROUPS = {"http_method": 1, "resource_path": 2, "protocol_version": 3}

MONTHS = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
          "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
ERROR_STATUS_MIN, ERROR_STATUS_MAX = 400, 599
Q2_TOP_N = 20
ERROR_RATE_DECIMALS = 6

RECORD_FIELDS = ("host", "timestamp", "log_date", "log_hour", "http_method",
                 "resource_path", "protocol_version", "status_code", "bytes_transferred")

# no flags needed: the regexes avoid \d \s \S \w, whose meaning differs between engines
LINE_RE = re.compile(ACCESS_LINE_REGEX)
REQUEST_RE = re.compile(REQUEST_REGEX)


# ---------------------------------------------------------------- reference parser
def decode(raw: bytes) -> str:
    """latin-1 maps every byte to one char, so the line structure never changes.
    (Engines may decode UTF-8 with replacement; this cannot change any query output,
    see docs/PARSING_RULES.md section 6.)"""
    return raw.decode("latin-1")


def parse_request(request: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """(http_method, resource_path, protocol_version); empty or missing group -> None."""
    m = REQUEST_RE.fullmatch(request)
    if m is None:
        return None, None, None
    return m.group(1) or None, m.group(2) or None, m.group(3) or None


def parse_record(line: str) -> Optional[dict]:
    """Parse one record (one line WITHOUT its '\\n'). Returns None if the record is malformed."""
    m = LINE_RE.fullmatch(line)  # whole-string match (Java: matcher.matches())
    if m is None:
        return None
    g = lambda name: m.group(LINE_GROUPS[name])  # noqa: E731
    log_date = f"{g('year')}-{MONTHS[g('month')]}-{g('day')}"
    method, path, proto = parse_request(g("request"))
    return {
        "host": g("host"),
        "timestamp": f"{log_date} {g('hour')}:{g('minute')}:{g('second')}",
        "log_date": log_date,
        "log_hour": int(g("hour")),
        "http_method": method,
        "resource_path": path,
        "protocol_version": proto,
        "status_code": int(g("status")),
        "bytes_transferred": 0 if g("bytes") == "-" else int(g("bytes")),
    }


# ---------------------------------------------------------------- CLI
def _request_rule_without_regex(request: str):
    """docs/PARSING_RULES.md section 4 with plain string operations (cross-check of regex 2)."""
    stripped = request.strip(" ")
    tokens = [t for t in stripped.split(" ") if t]
    if not tokens or not all("A" <= c <= "Z" for c in tokens[0]):
        return None, None, None
    rest, proto = stripped[len(tokens[0]):], None
    if len(tokens) >= 2 and tokens[-1].startswith("HTTP/"):
        proto = tokens[-1]
        rest = rest.rstrip(" ")[: -len(proto)]
    return tokens[0], rest.strip(" ") or None, proto


def _self_test() -> int:
    with open(os.path.join(_HERE, "test_vectors.json"), encoding="ascii") as fh:
        vectors = json.load(fh)["vectors"]
    failed = 0
    for v in vectors:
        got = parse_record(v["line"])
        ok = got == v["expected"]
        if ok and got is not None:
            req = LINE_RE.fullmatch(v["line"]).group(LINE_GROUPS["request"])
            ok = parse_request(req) == _request_rule_without_regex(req)
        if not ok:
            failed += 1
            print(f"FAIL {v['id']}: expected {v['expected']}, got {got}")
    print(f"{len(vectors) - failed}/{len(vectors)} test vectors passed")
    return 1 if failed else 0


def _print_escaped() -> int:
    for title, rx in (("ACCESS LINE", ACCESS_LINE_REGEX), ("REQUEST", REQUEST_REGEX)):
        java = rx.replace("\\", "\\\\").replace('"', '\\"')
        print(f"===== {title} regex")
        print(f"raw (file)        : {rx}")
        print(f'Java "..." literal: "{java}"')
        print(f"Pig/HiveQL '...'  : '{rx.replace(chr(92), chr(92) * 2)}'")
        print(f"JSON / Mongo      : {json.dumps(rx)}\n")
    print("line groups   :", LINE_GROUPS)
    print("request groups:", REQUEST_GROUPS)
    return 0


if __name__ == "__main__":
    sys.exit(_self_test() if "--test" in sys.argv else _print_escaped())
