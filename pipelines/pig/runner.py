"""
Apache Pig pipeline (implements pipelines/base.py: PipelineRunner).

    ingest.pig    once per batch: parse the raw batch file inside Pig, keep the parsed
                  records in this run's staging area, count parsed / malformed
    finalize.pig  once per run:   Q1, Q2, Q3 over every staged batch

Python only starts Pig, links/copies the raw batch file into Pig's staging area and
reads Pig's output back. All parsing, cleaning and aggregation happens in Pig Latin.

Options (orchestrator --opt key=value):
    pig_bin=/path/to/pig        default: $PIG_HOME/bin/pig, else `pig` on PATH
    pig_mode=local|mapreduce    default: local (single JVM, local files)
    pig_staging=/some/dir       staging root. Default: under the run's work dir, or a
                                temp dir if that path contains spaces (Pig's -param_file
                                and Hadoop paths do not handle spaces reliably)
    hdfs_root=/user/me/nasa     mapreduce mode only: HDFS staging root (default /tmp/nasa_pig)
    keep_pig_output=1           keep the staging area after the run (debugging)
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from pipelines.base import BatchCounts, BatchInfo, FinalOutput, PipelineRunner, RunContext

HERE = Path(__file__).resolve().parent
TRUE = ("1", "true", "yes", "on")


class PigRunner(PipelineRunner):
    name = "pig"

    # ------------------------------------------------------------------ lifecycle
    def prepare(self, ctx: RunContext) -> None:
        """Untimed: find Pig, create a fresh staging area. Does not read the input."""
        self._bin = self._pig_bin(ctx)
        self._mode = ctx.options.get("pig_mode", "local")
        if self._mode not in ("local", "mapreduce"):
            raise ValueError(f"pig_mode must be 'local' or 'mapreduce', got {self._mode!r}")

        # local scratch: rendered scripts, param files, logs (and staging in local mode)
        root = ctx.options.get("pig_staging")
        if not root:
            root = ctx.path("pig")
            if re.search(r"\s", root):
                root = tempfile.mkdtemp(prefix=f"pig_{ctx.run_id}_")
        self._local = Path(root)
        shutil.rmtree(self._local, ignore_errors=True)
        (self._local / "input").mkdir(parents=True)
        (self._local / "scripts").mkdir()

        if self._mode == "local":
            self._staging = str(self._local / "staging")
            self._out_root = str(self._local / "out")
        else:
            hdfs = ctx.options.get("hdfs_root", "/tmp/nasa_pig").rstrip("/")
            self._staging = f"{hdfs}/{ctx.run_id}/staging"
            self._out_root = f"{hdfs}/{ctx.run_id}/out"
            self._hadoop("fs", "-rm", "-r", "-f", f"{hdfs}/{ctx.run_id}")
            self._hadoop("fs", "-mkdir", "-p", f"{hdfs}/{ctx.run_id}/input")
        if re.search(r"\s", self._staging):
            raise ValueError(f"Pig staging path must not contain spaces: {self._staging!r} "
                             "(set --opt pig_staging=<dir without spaces>)")

        self._line_regex = self._pig_literal(ctx, "access_line.regex")
        self._request_regex = self._pig_literal(ctx, "request.regex")
        self._batches = 0

    def ingest_batch(self, ctx: RunContext, batch: BatchInfo) -> BatchCounts:
        """Timed: hand the untouched batch file to Pig, parse it there, return Pig's counts."""
        bid = batch.batch_id
        batch_file = self._stage_input(batch)
        out_dir = f"{self._out_root}/batch_{bid}"

        self._run_script("ingest.pig", f"ingest_{bid}", {
            "BATCH_FILE": batch_file,
            "BATCH_ID": str(bid),
            "STAGING_DIR": self._staging,
            "OUTPUT_DIR": out_dir,
        })

        rows = self._read_json_rows(f"{out_dir}/stats")
        if len(rows) != 1:
            raise RuntimeError(f"batch {bid}: Pig stats must be exactly 1 row, got {len(rows)}")
        s = rows[0]
        counts = BatchCounts(parsed_count=int(s["parsed_count"] or 0),
                             malformed_count=int(s["malformed_count"] or 0))
        if int(s["batch_id"]) != bid or int(s["record_count"]) != counts.record_count:
            raise RuntimeError(f"batch {bid}: inconsistent Pig stats {s}")
        if counts.record_count != batch.record_count:
            raise RuntimeError(
                f"batch {bid}: Pig read {counts.record_count} records "
                f"(parsed {counts.parsed_count} + malformed {counts.malformed_count}) "
                f"but the batch file has {batch.record_count} lines")
        self._batches += 1
        self._remove_input(batch_file)
        return counts

    def finalize(self, ctx: RunContext) -> FinalOutput:
        """Timed: Q1/Q2/Q3 over everything ingested for ctx.run_id."""
        out_dir = f"{self._out_root}/final"
        self._run_script("finalize.pig", "finalize", {
            "STAGING_DIR": self._staging,
            "OUTPUT_DIR": out_dir,
        })

        bt = self._read_json_rows(f"{out_dir}/batch_totals")
        if len(bt) != 1:
            raise RuntimeError(f"Pig batch_totals must be exactly 1 row, got {len(bt)}")
        bt = bt[0]
        staged = self._read_json_rows(f"{out_dir}/staged_totals")  # empty if nothing parsed
        staged_parsed = int(staged[0]["staged_parsed_count"]) if staged else 0

        totals = {
            "record_count": int(bt["record_count"] or 0),
            "parsed_count": int(bt["parsed_count"] or 0),
            "malformed_count": int(bt["malformed_count"] or 0),
        }
        if int(bt["batch_count"]) != self._batches:
            raise RuntimeError(f"Pig staging holds {bt['batch_count']} batches, "
                               f"{self._batches} were ingested in this run")
        if staged_parsed != totals["parsed_count"]:
            raise RuntimeError(f"Pig staging holds {staged_parsed} parsed records, "
                               f"the batches reported {totals['parsed_count']}")

        q1 = [{"log_date": r["log_date"], "status_code": int(r["status_code"]),
               "request_count": int(r["request_count"]), "total_bytes": int(r["total_bytes"])}
              for r in self._read_json_rows(f"{out_dir}/q1")]
        q2 = [{"resource_path": r["resource_path"], "request_count": int(r["request_count"]),
               "total_bytes": int(r["total_bytes"]), "distinct_host_count": int(r["distinct_host_count"])}
              for r in self._read_json_rows(f"{out_dir}/q2")]  # already ordered and limited by Pig
        q3 = [{"log_date": r["log_date"], "log_hour": int(r["log_hour"]),
               "error_request_count": int(r["error_request_count"]),
               "total_request_count": int(r["total_request_count"]),
               "error_rate": round(float(r["error_rate"]), 6),  # Pig computes it; 6 decimals per spec
               "distinct_error_hosts": int(r["distinct_error_hosts"])}
              for r in self._read_json_rows(f"{out_dir}/q3")]
        if len(q2) > 20:
            raise RuntimeError(f"Pig Q2 returned {len(q2)} rows, expected at most 20")

        return FinalOutput(totals=totals, results={
            "q1_daily_traffic": q1, "q2_top_resources": q2, "q3_hourly_errors": q3})

    def cleanup(self, ctx: RunContext, success: bool) -> None:
        if ctx.options.get("keep_pig_output", "").lower() in TRUE:
            return
        if getattr(self, "_mode", "local") == "mapreduce":
            hdfs = ctx.options.get("hdfs_root", "/tmp/nasa_pig").rstrip("/")
            self._hadoop("fs", "-rm", "-r", "-f", f"{hdfs}/{ctx.run_id}", check=False)
        if getattr(self, "_local", None) is not None:
            shutil.rmtree(self._local, ignore_errors=True)

    # ------------------------------------------------------------------ input staging
    def _stage_input(self, batch: BatchInfo) -> str:
        """Give Pig the batch file unchanged, at a path without spaces."""
        name = f"batch_{batch.batch_id:06d}.log"
        if self._mode == "mapreduce":  # = hdfs put
            dest = f"{self._staging.rsplit('/', 1)[0]}/input/{name}"
            self._hadoop("fs", "-put", "-f", batch.path, dest)
            return dest
        dest = self._local / "input" / name
        try:
            os.symlink(os.path.abspath(batch.path), dest)
        except OSError:
            shutil.copyfile(batch.path, dest)
        return str(dest)

    def _remove_input(self, path: str) -> None:
        if self._mode == "mapreduce":
            self._hadoop("fs", "-rm", "-f", path, check=False)
        else:
            Path(path).unlink(missing_ok=True)

    # ------------------------------------------------------------------ running Pig
    def _run_script(self, script: str, tag: str, params: dict[str, str]) -> None:
        text = (HERE / script).read_text(encoding="utf-8")
        text = (text.replace("__LINE_REGEX__", self._line_regex)
                    .replace("__REQUEST_REGEX__", self._request_regex))
        scripts = self._local / "scripts"
        rendered = scripts / f"{tag}.pig"
        rendered.write_text(text, encoding="utf-8")
        param_file = scripts / f"{tag}.params"
        param_file.write_text("".join(f"{k}={v}\n" for k, v in params.items()), encoding="utf-8")
        log_file = scripts / f"{tag}.log"

        cmd = [self._bin, "-x", self._mode, "-logfile", str(log_file),
               "-param_file", str(param_file), str(rendered)]
        res = subprocess.run(cmd, capture_output=True, text=True, cwd=scripts)
        if res.returncode != 0:
            log = log_file.read_text(errors="replace")[-3000:] if log_file.exists() else ""
            raise RuntimeError(
                f"Pig {script} failed (exit {res.returncode}). Script: {rendered}\n"
                f"--- stderr (tail) ---\n{res.stderr[-3000:]}\n--- pig log (tail) ---\n{log}")

    def _read_json_rows(self, directory: str) -> list[dict[str, Any]]:
        """Rows written by JsonStorage: one JSON object per line in part-* files, in file order."""
        if self._mode == "mapreduce":
            res = self._hadoop("fs", "-cat", f"{directory}/part-*")
            text = res.stdout
        else:
            d = Path(directory)
            if not d.is_dir():
                raise RuntimeError(f"Pig did not create the output directory {d}")
            parts = sorted(p for p in d.iterdir() if p.is_file() and p.name.startswith("part-"))
            text = "".join(p.read_text(encoding="utf-8") for p in parts)
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    @staticmethod
    def _hadoop(*args: str, check: bool = True) -> subprocess.CompletedProcess:
        res = subprocess.run(["hadoop", *args], capture_output=True, text=True)
        if check and res.returncode != 0:
            raise RuntimeError(f"hadoop {' '.join(args)} failed:\n{res.stderr[-2000:]}")
        return res

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _pig_bin(ctx: RunContext) -> str:
        explicit = ctx.options.get("pig_bin")
        candidates = [explicit] if explicit else []
        if os.environ.get("PIG_HOME"):
            candidates.append(os.path.join(os.environ["PIG_HOME"], "bin", "pig"))
        candidates.append("pig")
        for c in candidates:
            if os.path.isfile(c) or shutil.which(c):
                return c
        raise RuntimeError("Apache Pig not found: set PIG_HOME, put `pig` on PATH, "
                           "or pass --opt pig_bin=/path/to/pig")

    @staticmethod
    def pig_escape(regex: str) -> str:
        """A regex as the body of a Pig '...' string literal (Pig turns \\\\ into \\ and \\' into ')."""
        return regex.replace("\\", "\\\\").replace("'", "\\'")

    @classmethod
    def _pig_literal(cls, ctx: RunContext, name: str) -> str:
        """Read a shared regex file (never hand-copied) and escape it for Pig."""
        raw = (Path(ctx.repo_root) / "common" / name).read_text(encoding="ascii")
        raw = raw.rstrip("\r\n")  # a trailing newline in the FILE is not part of the regex
        if "\n" in raw or "\r" in raw:
            raise ValueError(f"common/{name} must hold the regex on a single line")
        return cls.pig_escape(raw)
