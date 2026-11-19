"""
MongoDB pipeline. All parsing, cleaning and aggregation runs inside MongoDB
(aggregation pipelines). Python only copies raw lines in and reads results out.

Per run, two collections in database `nasa_etl` (names contain the run id):
    raw_<run_id>      raw lines of the CURRENT batch only: {b, line}       (emptied after each batch)
    records_<run_id>  one doc per record of every batch: parsed fields + ok flag

ingest_batch:  insert the batch's raw lines  ->  aggregation: $regexFind (shared regexes) ->
               derived fields  ->  $merge into records  ->  count ok / malformed (in MongoDB)
finalize:      Q1 / Q2 / Q3 as aggregations over records_<run_id> (all batches)

Options (orchestrator --opt key=value):
    uri=mongodb://localhost:27017   (or env NASA_MONGO_URI)
    db=nasa_etl

Check the parsing on the shared test vectors (needs a running MongoDB):
    python pipelines/mongodb/runner.py --test
"""
from __future__ import annotations

import os
import sys

from pymongo import MongoClient

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:  # allows `python pipelines/mongodb/runner.py --test`
    sys.path.insert(0, _REPO)

from pipelines.base import BatchCounts, BatchInfo, FinalOutput, PipelineRunner, RunContext  # noqa: E402


def _read_regex(name: str) -> str:
    with open(os.path.join(_REPO, "common", name), encoding="ascii") as fh:
        return fh.read()


LINE_REGEX = _read_regex("access_line.regex")
REQUEST_REGEX = _read_regex("request.regex")
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_NUMBERS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
INSERT_CHUNK = 20_000


# ======================================================================= aggregation stages
def _cap(arr: str, i: int) -> dict:
    """captures[i] of a $regexFind result held in field `arr` (0-based)."""
    return {"$arrayElemAt": [f"${arr}.captures", i]}


def _null_if_empty(expr) -> dict:
    return {"$cond": [{"$in": [expr, [None, ""]]}, None, expr]}


def parse_stages() -> list[dict]:
    """Raw {line} docs -> parsed record docs. docs/PARSING_RULES.md sections 2-5."""
    return [
        {"$set": {"m": {"$regexFind": {"input": "$line", "regex": LINE_REGEX}}}},
        {"$set": {"ok": {"$ne": ["$m", None]}}},
        {"$set": {"req": {"$cond": ["$ok", {"$regexFind": {"input": _cap("m", 9), "regex": REQUEST_REGEX}}, None]},
                  "mm": {"$cond": ["$ok", {"$arrayElemAt": [MONTH_NUMBERS,
                                                            {"$indexOfArray": [MONTHS, _cap("m", 3)]}]}, None]}}},
        {"$set": {"log_date": {"$cond": ["$ok", {"$concat": [_cap("m", 4), "-", "$mm", "-", _cap("m", 2)]}, None]}}},
        {"$project": {
            "_id": 1, "b": 1, "ok": 1,
            "host": {"$cond": ["$ok", _cap("m", 0), None]},
            "timestamp": {"$cond": ["$ok", {"$concat": ["$log_date", " ", _cap("m", 5), ":",
                                                       _cap("m", 6), ":", _cap("m", 7)]}, None]},
            "log_date": 1,
            "log_hour": {"$cond": ["$ok", {"$toInt": _cap("m", 5)}, None]},
            "http_method": {"$cond": [{"$eq": ["$req", None]}, None, _null_if_empty(_cap("req", 0))]},
            "resource_path": {"$cond": [{"$eq": ["$req", None]}, None, _null_if_empty(_cap("req", 1))]},
            "protocol_version": {"$cond": [{"$eq": ["$req", None]}, None, _null_if_empty(_cap("req", 2))]},
            "status_code": {"$cond": ["$ok", {"$toInt": _cap("m", 10)}, None]},
            "bytes_transferred": {"$cond": ["$ok", {"$cond": [{"$eq": [_cap("m", 11), "-"]}, {"$toLong": 0},
                                                              {"$toLong": _cap("m", 11)}]}, None]},
        }},
    ]


def q1_pipeline() -> list[dict]:
    return [
        {"$match": {"ok": True}},
        {"$group": {"_id": {"d": "$log_date", "s": "$status_code"},
                    "request_count": {"$sum": 1}, "total_bytes": {"$sum": "$bytes_transferred"}}},
        {"$sort": {"_id.d": 1, "_id.s": 1}},
        {"$project": {"_id": 0, "log_date": "$_id.d", "status_code": "$_id.s",
                      "request_count": 1, "total_bytes": 1}},
    ]


def q2_pipeline() -> list[dict]:
    # two-step group: (path, host) first, so distinct hosts never need a huge in-memory set
    return [
        {"$match": {"ok": True, "resource_path": {"$ne": None}}},
        {"$group": {"_id": {"p": "$resource_path", "h": "$host"},
                    "n": {"$sum": 1}, "bytes": {"$sum": "$bytes_transferred"}}},
        {"$group": {"_id": "$_id.p", "request_count": {"$sum": "$n"},
                    "total_bytes": {"$sum": "$bytes"}, "distinct_host_count": {"$sum": 1}}},
        {"$sort": {"request_count": -1, "_id": 1}},  # simple (binary) string order: tie-break by path
        {"$limit": 20},
        {"$project": {"_id": 0, "resource_path": "$_id", "request_count": 1,
                      "total_bytes": 1, "distinct_host_count": 1}},
    ]


def q3_pipeline() -> list[dict]:
    is_error = {"$and": [{"$gte": ["$status_code", 400]}, {"$lte": ["$status_code", 599]}]}
    return [
        {"$match": {"ok": True}},
        {"$group": {"_id": {"d": "$log_date", "h": "$log_hour", "host": "$host"},
                    "total": {"$sum": 1}, "err": {"$sum": {"$cond": [is_error, 1, 0]}}}},
        {"$group": {"_id": {"d": "$_id.d", "h": "$_id.h"},
                    "total_request_count": {"$sum": "$total"}, "error_request_count": {"$sum": "$err"},
                    "distinct_error_hosts": {"$sum": {"$cond": [{"$gt": ["$err", 0]}, 1, 0]}}}},
        {"$sort": {"_id.d": 1, "_id.h": 1}},
        {"$project": {"_id": 0, "log_date": "$_id.d", "log_hour": "$_id.h",
                      "error_request_count": 1, "total_request_count": 1, "distinct_error_hosts": 1,
                      "error_rate": {"$round": [{"$divide": ["$error_request_count",
                                                             "$total_request_count"]}, 6]}}},
    ]


# ======================================================================= runner
class MongoRunner(PipelineRunner):
    name = "mongodb"

    def prepare(self, ctx: RunContext) -> None:
        uri = ctx.options.get("uri") or os.getenv("NASA_MONGO_URI", "mongodb://localhost:27017")
        self.client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
        self.client.admin.command("ping")
        self.db = self.client[ctx.options.get("db", "nasa_etl")]
        self.raw = self.db[f"raw_{ctx.run_id}"]
        self.records = self.db[f"records_{ctx.run_id}"]
        self.raw.drop()
        self.records.drop()
        self.records.create_index("b")

    def ingest_batch(self, ctx: RunContext, batch: BatchInfo) -> BatchCounts:
        # load: raw lines exactly as in the batch file (UTF-8 with replacement, like Hadoop)
        with open(batch.path, "rb") as fh:
            lines = fh.read().split(b"\n")[:-1]  # every line ends with '\n'
        for i in range(0, len(lines), INSERT_CHUNK):
            self.raw.insert_many([{"b": batch.batch_id, "line": l.decode("utf-8", errors="replace")}
                                  for l in lines[i:i + INSERT_CHUNK]], ordered=False)
        # transform inside MongoDB
        self.raw.aggregate(parse_stages() + [{"$merge": {"into": self.records.name,
                                                          "whenMatched": "fail", "whenNotMatched": "insert"}}],
                           allowDiskUse=True)
        self.raw.delete_many({})
        counts = {d["_id"]: d["n"] for d in self.records.aggregate(
            [{"$match": {"b": batch.batch_id}}, {"$group": {"_id": "$ok", "n": {"$sum": 1}}}])}
        return BatchCounts(parsed_count=counts.get(True, 0), malformed_count=counts.get(False, 0))

    def finalize(self, ctx: RunContext) -> FinalOutput:
        agg = lambda p: list(self.records.aggregate(p, allowDiskUse=True))  # noqa: E731
        counts = {d["_id"]: d["n"] for d in agg([{"$group": {"_id": "$ok", "n": {"$sum": 1}}}])}
        parsed, malformed = counts.get(True, 0), counts.get(False, 0)
        return FinalOutput(
            totals={"record_count": parsed + malformed, "parsed_count": parsed, "malformed_count": malformed},
            results={"q1_daily_traffic": agg(q1_pipeline()),
                     "q2_top_resources": agg(q2_pipeline()),
                     "q3_hourly_errors": agg(q3_pipeline())})

    def cleanup(self, ctx: RunContext, success: bool) -> None:
        if getattr(self, "client", None) is None:
            return
        self.raw.drop()
        self.records.drop()
        self.client.close()


# ======================================================================= vector self-test
def _test_vectors() -> int:
    """Run common/test_vectors.json through the same aggregation and compare."""
    import json
    with open(os.path.join(_REPO, "common", "test_vectors.json"), encoding="ascii") as fh:
        vectors = json.load(fh)["vectors"]
    client = MongoClient(os.getenv("NASA_MONGO_URI", "mongodb://localhost:27017"), serverSelectionTimeoutMS=10_000)
    coll = client["nasa_etl"]["vector_test"]
    coll.drop()
    coll.insert_many([{"_id": i, "b": 1, "line": v["line"]} for i, v in enumerate(vectors)])
    got = {d["_id"]: d for d in coll.aggregate(parse_stages())}
    coll.drop()
    fields = ("host", "timestamp", "log_date", "log_hour", "http_method", "resource_path",
              "protocol_version", "status_code", "bytes_transferred")
    failed = 0
    for i, v in enumerate(vectors):
        d = got[i]
        actual = {k: d.get(k) for k in fields} if d["ok"] else None
        if actual != v["expected"]:
            failed += 1
            print(f"FAIL {v['id']}: expected {v['expected']}, got {actual}")
    print(f"MongoDB: {len(vectors) - failed}/{len(vectors)} test vectors passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.path.insert(0, _REPO)
    sys.exit(_test_vectors() if "--test" in sys.argv else print(__doc__) or 0)
