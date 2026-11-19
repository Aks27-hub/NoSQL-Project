"""
The interface every pipeline implements. The orchestrator only talks to this.

Lifecycle of one run (see orchestrator.py):

    runner.prepare(ctx)                  # NOT timed: connectivity checks, clear old staging
    -- timer starts, orchestrator starts reading the raw input --
    for each batch:
        runner.ingest_batch(ctx, batch)  # timed: engine parses + stores this batch
    runner.finalize(ctx)                 # timed: engine runs Q1/Q2/Q3 over ALL ingested batches
    -- results written to PostgreSQL, timer stops --
    runner.cleanup(ctx, success)         # NOT timed: drop staging if you want

Rules for implementers (these keep the four pipelines equivalent):

  * Parse with the shared regexes common/access_line.regex and common/request.regex
    (read the files; `python common/parser.py` prints them escaped) - never hand-copy them. Follow docs/PARSING_RULES.md.
  * ingest_batch must do the parsing INSIDE the engine (Pig script, MR job,
    Hive query, Mongo aggregation). Python is only allowed to start the engine,
    copy the batch file into it (hdfs put / insert raw lines) and read counts back.
  * Every line of the batch file is one record. The engine must account for every
    one of them: parsed_count + malformed_count == batch.record_count. The
    orchestrator checks this and aborts the run if it does not hold.
  * finalize must compute the queries over everything ingested in this run
    (ctx.run_id), not just the last batch.
  * Do not write to PostgreSQL yourself. Return dicts; the orchestrator hands
    them to db_loader.
"""
from __future__ import annotations

import abc
import dataclasses
import os
from typing import Any


@dataclasses.dataclass(frozen=True)
class RunContext:
    run_id: str              # e.g. 'pig-20261119T104500-3fa9c1'; use it to name staging dirs/tables
    pipeline_name: str       # 'mapreduce' | 'pig' | 'hive' | 'mongodb'
    batch_size: int          # configured records per batch
    input_files: tuple[str, ...]
    work_dir: str            # local scratch dir for this run: <work-root>/<run_id>
    repo_root: str           # absolute path of the repository (to find scripts, common/regex, ...)
    options: dict[str, Any] = dataclasses.field(default_factory=dict)  # free-form --opt key=value

    def path(self, *parts: str) -> str:
        """Path inside this run's local work dir."""
        return os.path.join(self.work_dir, *parts)


@dataclasses.dataclass(frozen=True)
class BatchInfo:
    batch_id: int            # 1, 2, 3, ... (contiguous)
    path: str                # local file: exactly record_count lines, raw bytes, '\n'-terminated
    record_count: int        # number of records (lines) in this batch file
    first_record_no: int     # 1-based index of the first record in the whole input (for debugging)


@dataclasses.dataclass(frozen=True)
class BatchCounts:
    parsed_count: int
    malformed_count: int

    @property
    def record_count(self) -> int:
        return self.parsed_count + self.malformed_count


@dataclasses.dataclass(frozen=True)
class FinalOutput:
    """
    totals:  {'record_count', 'parsed_count', 'malformed_count'} as the ENGINE counts them
             over everything it ingested for this run (must equal the sum of the batches).
    results: {'q1_daily_traffic': [...], 'q2_top_resources': [...], 'q3_hourly_errors': [...]}
             rows exactly as in templates/final_results.json (column names in contract.py).
             Q2 must already be the top 20, ordered by request_count DESC, resource_path ASC.
    """
    totals: dict[str, int]
    results: dict[str, list[dict[str, Any]]]


class PipelineRunner(abc.ABC):
    """Subclass this in pipelines/<name>/runner.py (already listed in RUNNERS in orchestrator.py)."""

    name: str = ""  # must equal the contract pipeline name

    def prepare(self, ctx: RunContext) -> None:
        """Untimed. Check the engine is reachable; create/clear THIS run's staging area.
        Must not read the input files."""

    @abc.abstractmethod
    def ingest_batch(self, ctx: RunContext, batch: BatchInfo) -> BatchCounts:
        """Timed. Load batch.path into the engine, parse it there, keep the parsed records
        (and nothing else) in the run's staging area, return the counts."""

    @abc.abstractmethod
    def finalize(self, ctx: RunContext) -> FinalOutput:
        """Timed. Run Q1, Q2, Q3 over all records ingested for ctx.run_id."""

    def cleanup(self, ctx: RunContext, success: bool) -> None:
        """Untimed. Optional: drop staging data. Called even if the run failed."""
