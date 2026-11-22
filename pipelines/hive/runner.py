"""
Apache Hive pipeline (implements pipelines/base.py: PipelineRunner).

    ingest.hql    once per batch: an external RegexSerDe table over the raw batch
                  file parses it inside Hive, the parsed records go into this run's
                  ORC table (one partition per batch), the counts into a stats table
    finalize.hql  once per run: Q1, Q2, Q3 in HiveQL over every staged batch

Python only starts Hive (beeline), hands it the untouched batch file and reads
Hive's output files back. All parsing, cleaning and aggregation happens in HiveQL.

Running Hive (one-time, from the repository root; Hive does not run natively on Windows):
    docker run -d --name nasa-hive -p 10000:10000 -p 10002:10002 \\
        -e SERVICE_NAME=hiveserver2 -e SERVICE_OPTS=-Xmx4G \\
        -v "$PWD/work:/work" apache/hive:4.0.1
    docker start nasa-hive        # later sessions; wait ~30 s for HiveServer2
The repository's work/ folder is mounted at /work so Hive can read the batch files
and write its result files there. Run the orchestrator from the repository root.
-Xmx4G: the image's default 1 GB heap runs out of memory in finalize on the full
dataset (Tez runs inside the HiveServer2 JVM in this single-container setup).

How Hive is reached (orchestrator --opt key=value):
    hive_mode=docker|beeline|cli   default: beeline if it is on PATH (or $HIVE_HOME/bin),
                                   else docker
    hive_container=nasa-hive       docker mode: the container started above
    hive_host_root=<dir>           docker mode: host folder mounted into the container
                                   (default <repo>/work)
    hive_container_root=/work      docker mode: where that folder appears in the container
    hive_url=jdbc:hive2://localhost:10000/default   beeline JDBC URL
    hive_user=<name>               beeline -n (default: none)
    hive_bin=/path/to/beeline      beeline (or hive, for hive_mode=cli)
    hive_db=nasa_etl               Hive database for the staging tables
    hive_scratch=<dir>             local scratch dir (default <work dir>/hive)
    keep_hive_tables=1             keep the staging tables after the run (debugging)

beeline / cli modes assume Hive runs on the SAME machine as the orchestrator, because
Hive reads the batch file and writes its results through local paths.

Check the HiveQL parsing on the shared test vectors (needs a running Hive):
    python pipelines/hive/runner.py --test [key=value ...]
"""
from __future__ import annotations

import base64
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:  # allows `python pipelines/hive/runner.py --test`
    sys.path.insert(0, str(_REPO))

from pipelines.base import BatchCounts, BatchInfo, FinalOutput, PipelineRunner, RunContext  # noqa: E402

HERE = Path(__file__).resolve().parent
TRUE = ("1", "true", "yes", "on")
MODES = ("docker", "beeline", "cli")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# This run's two staging tables, created in prepare() (untimed).
# EXTERNAL + external.table.purge: Hive 4 would make a managed table transactional (ACID),
# which is not needed here; DROP TABLE still deletes the data because of the purge property.
# ORC keeps types and NULLs exactly and has no field delimiter to break.
SETUP_SQL = """
CREATE DATABASE IF NOT EXISTS __DB__;
USE __DB__;
DROP TABLE IF EXISTS __PARSED_TABLE__ PURGE;
DROP TABLE IF EXISTS __STATS_TABLE__ PURGE;
CREATE EXTERNAL TABLE __PARSED_TABLE__ (
    host STRING, `timestamp` STRING, log_date STRING, log_hour INT, http_method STRING,
    resource_path STRING, protocol_version STRING, status_code INT, bytes_transferred BIGINT)
PARTITIONED BY (batch_id INT) STORED AS ORC TBLPROPERTIES ('external.table.purge' = 'true');
CREATE EXTERNAL TABLE __STATS_TABLE__ (
    batch_id INT, record_count BIGINT, parsed_count BIGINT, malformed_count BIGINT)
STORED AS ORC TBLPROPERTIES ('external.table.purge' = 'true');
"""

# cleanup() (untimed). __RAW_DROPS__ = raw tables a failed batch may have left behind.
CLEANUP_SQL = """
USE __DB__;
DROP TABLE IF EXISTS __PARSED_TABLE__ PURGE;
DROP TABLE IF EXISTS __STATS_TABLE__ PURGE;
__RAW_DROPS__
"""

# --test only: dump every parsed record. Strings base64 (UTF-8) because the test vectors
# contain tabs and control characters; NULL stays NULL (written as \N).
EXPORT_PARSED_SQL = """
USE __DB__;
SET hive.query.results.cache.enabled=false;
INSERT OVERWRITE LOCAL DIRECTORY '__OUT__'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
SELECT base64(encode(host, 'UTF-8')), base64(encode(`timestamp`, 'UTF-8')), log_date, log_hour,
       base64(encode(http_method, 'UTF-8')), base64(encode(resource_path, 'UTF-8')),
       base64(encode(protocol_version, 'UTF-8')), status_code, bytes_transferred
FROM __PARSED_TABLE__;
"""


class HiveRunner(PipelineRunner):
    name = "hive"

    # ------------------------------------------------------------------ lifecycle
    def prepare(self, ctx: RunContext) -> None:
        """Untimed: find Hive, create a fresh scratch dir and this run's staging tables."""
        o = ctx.options
        self._mode = o.get("hive_mode") or ("beeline" if self._find_bin(o, "beeline") else "docker")
        if self._mode not in MODES:
            raise ValueError(f"hive_mode must be one of {MODES}, got {self._mode!r}")
        self._url = o.get("hive_url", "jdbc:hive2://localhost:10000/default")
        self._user = o.get("hive_user")

        if self._mode == "docker":
            self._container = o.get("hive_container", "nasa-hive")
            self._host_root = Path(o.get("hive_host_root") or _REPO / "work").resolve()
            self._engine_root = PurePosixPath(o.get("hive_container_root", "/work"))
            self._bin = o.get("hive_bin", "beeline")
            if not shutil.which("docker"):
                raise RuntimeError("hive_mode=docker but `docker` is not on PATH")
        else:
            wanted = "hive" if self._mode == "cli" else "beeline"
            self._bin = self._find_bin(o, wanted)
            if not self._bin:
                raise RuntimeError(f"Apache Hive not found: put `{wanted}` on PATH, set HIVE_HOME, "
                                   f"pass --opt hive_bin=/path/to/{wanted}, or use --opt hive_mode=docker")

        # local scratch: rendered scripts, the batch file Hive reads, Hive's result files
        root = o.get("hive_scratch") or ctx.path("hive")
        if self._mode != "docker" and re.search(r"\s", root):
            root = tempfile.mkdtemp(prefix=f"hive_{ctx.run_id}_")  # Hive URIs and spaces do not mix
        self._local = Path(root).resolve()
        shutil.rmtree(self._local, ignore_errors=True)
        for sub in ("input", "scripts", "out"):
            (self._local / sub).mkdir(parents=True)
        self._engine_path(self._local)  # fail now, not mid-run, if Hive cannot see the scratch dir

        db = o.get("hive_db", "nasa_etl")
        if not _IDENT.match(db):
            raise ValueError(f"hive_db must be a plain identifier, got {db!r}")
        tag = re.sub(r"[^a-z0-9]+", "_", ctx.run_id.lower()).strip("_")
        self._tables = {"DB": db, "PARSED_TABLE": f"parsed_{tag}", "STATS_TABLE": f"stats_{tag}"}
        self._raw_prefix = f"raw_{tag}_b"

        self._line_regex = self._hive_literal("access_line.regex")
        self._request_regex = self._hive_literal("request.regex")
        self._batches = 0
        self._run_script(SETUP_SQL, "setup", {})

    def ingest_batch(self, ctx: RunContext, batch: BatchInfo) -> BatchCounts:
        """Timed: hand the untouched batch file to Hive, parse it there, return Hive's counts."""
        bid = batch.batch_id
        in_dir = self._local / "input" / f"batch_{bid:06d}"
        in_dir.mkdir()
        self._stage_input(batch.path, in_dir / f"batch_{bid:06d}.log")
        out_dir = self._local / "out" / f"batch_{bid}"
        out_dir.mkdir()

        self._run_script("ingest.hql", f"ingest_{bid}", {
            "RAW_TABLE": f"{self._raw_prefix}{bid}",
            "INPUT_DIR": self._engine_uri(in_dir),
            "BATCH_ID": str(bid),
            "STATS_OUT": str(self._engine_path(out_dir / "stats")),
        })

        rows = self._read_rows(out_dir / "stats")
        if len(rows) != 1:
            raise RuntimeError(f"batch {bid}: Hive stats must be exactly 1 row, got {len(rows)}")
        got_id, records, parsed, malformed = (int(x) for x in rows[0])
        counts = BatchCounts(parsed_count=parsed, malformed_count=malformed)
        if got_id != bid or records != counts.record_count:
            raise RuntimeError(f"batch {bid}: inconsistent Hive stats {rows[0]}")
        if counts.record_count != batch.record_count:
            raise RuntimeError(
                f"batch {bid}: Hive read {counts.record_count} records "
                f"(parsed {parsed} + malformed {malformed}) but the batch file has {batch.record_count} lines")
        self._batches += 1
        shutil.rmtree(in_dir, ignore_errors=True)
        return counts

    def finalize(self, ctx: RunContext) -> FinalOutput:
        """Timed: Q1/Q2/Q3 over everything ingested for ctx.run_id."""
        out = self._local / "out" / "final"
        out.mkdir()
        self._run_script("finalize.hql", "finalize", {"OUT": str(self._engine_path(out))})

        bt = self._read_rows(out / "batch_totals")
        if len(bt) != 1:
            raise RuntimeError(f"Hive batch_totals must be exactly 1 row, got {len(bt)}")
        batch_count, rec, parsed, bad = (int(x or 0) for x in bt[0])  # SUM over 0 rows is NULL
        staged = self._read_rows(out / "staged_totals")
        staged_parsed = int(staged[0][0]) if staged else 0

        if batch_count != self._batches:
            raise RuntimeError(f"Hive staging holds {batch_count} batches, "
                               f"{self._batches} were ingested in this run")
        if staged_parsed != parsed:
            raise RuntimeError(f"Hive staging holds {staged_parsed} parsed records, "
                               f"the batches reported {parsed}")

        q1 = [{"log_date": d, "status_code": int(s), "request_count": int(n), "total_bytes": int(b)}
              for d, s, n, b in self._read_rows(out / "q1")]
        q2_rows = sorted(self._read_rows(out / "q2"), key=lambda r: int(r[0]))  # rank_no from Hive
        q2 = [{"resource_path": base64.b64decode(p).decode("utf-8"), "request_count": int(n),
               "total_bytes": int(b), "distinct_host_count": int(h)}
              for _rank, p, n, b, h in q2_rows]
        if [int(r[0]) for r in q2_rows] != list(range(1, len(q2_rows) + 1)):
            raise RuntimeError(f"Hive Q2 ranks are not 1..{len(q2_rows)}")
        if len(q2) > 20:
            raise RuntimeError(f"Hive Q2 returned {len(q2)} rows, expected at most 20")
        q3 = [{"log_date": d, "log_hour": int(h), "error_request_count": int(e),
               "total_request_count": int(t),
               "error_rate": round(float(r), 6),  # Hive computes it; 6 decimals per spec
               "distinct_error_hosts": int(dh)}
              for d, h, e, t, r, dh in self._read_rows(out / "q3")]

        return FinalOutput(
            totals={"record_count": rec, "parsed_count": parsed, "malformed_count": bad},
            results={"q1_daily_traffic": q1, "q2_top_resources": q2, "q3_hourly_errors": q3})

    def cleanup(self, ctx: RunContext, success: bool) -> None:
        if getattr(self, "_local", None) is None:
            return
        if ctx.options.get("keep_hive_tables", "").lower() not in TRUE:
            drops = "\n".join(f"DROP TABLE IF EXISTS {self._raw_prefix}{i};"
                              for i in range(1, self._batches + 2))  # +1: a batch that failed mid-way
            try:
                self._run_script(CLEANUP_SQL, "cleanup", {"RAW_DROPS": drops})
            except RuntimeError as e:
                print(f"[hive] cleanup could not drop the staging tables: {e}", file=sys.stderr)
        shutil.rmtree(self._local, ignore_errors=True)

    # ------------------------------------------------------------------ paths
    def _engine_path(self, host_path: Path) -> PurePosixPath:
        """The same file or folder as Hive sees it."""
        host_path = Path(host_path).resolve()
        if self._mode != "docker":
            return PurePosixPath(host_path.as_posix())
        try:
            rel = host_path.relative_to(self._host_root)
        except ValueError:
            raise RuntimeError(
                f"{host_path} is not inside {self._host_root}, the folder mounted into the Hive "
                f"container. Run from the repository root (work dir = ./work), or pass "
                f"--opt hive_host_root=... / hive_container_root=... to match your docker volume") from None
        return self._engine_root.joinpath(*rel.parts)

    def _engine_uri(self, host_path: Path) -> str:
        """file:// URI for a LOCATION clause: explicit, so it works even if fs.defaultFS is HDFS."""
        p = self._engine_path(host_path).as_posix()
        return "file://" + (p if p.startswith("/") else "/" + p)

    @staticmethod
    def _stage_input(src: str, dest: Path) -> None:
        """Give Hive the batch file unchanged: a hard link (no copy), else a byte copy."""
        try:
            os.link(src, dest)
        except OSError:
            shutil.copyfile(src, dest)

    # ------------------------------------------------------------------ running Hive
    def _render(self, script: str, params: dict[str, str]) -> str:
        """script = a .hql file name in this folder, or SQL text (the small inline statements)."""
        text = (HERE / script).read_text(encoding="utf-8") if script.endswith(".hql") else script
        script = script if script.endswith(".hql") else "inline SQL"
        # drop comment lines: some beeline versions mis-split statements on quotes or ';' in comments
        text = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--"))
        values = {**self._tables, **params,
                  "LINE_REGEX": self._line_regex, "REQUEST_REGEX": self._request_regex}
        for key, value in values.items():
            text = text.replace(f"__{key}__", value)
        left = sorted(set(re.findall(r"__[A-Z_]+__", text)))
        if left:
            raise RuntimeError(f"{script}: unfilled placeholders {left}")
        return text + "\n"

    def _run_script(self, script: str, tag: str, params: dict[str, str]) -> None:
        rendered = self._local / "scripts" / f"{tag}.hql"
        rendered.write_text(self._render(script, params), encoding="utf-8", newline="\n")
        engine_script = str(self._engine_path(rendered))

        if self._mode == "cli":
            cmd = [self._bin, "-S", "-f", engine_script]
        else:
            cmd = [self._bin, "-u", self._url, "--silent=true", "--showHeader=false", "-f", engine_script]
            if self._user:
                cmd[3:3] = ["-n", self._user]
            if self._mode == "docker":
                cmd = ["docker", "exec", self._container] + cmd

        res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        (self._local / "scripts" / f"{tag}.log").write_text(
            f"$ {' '.join(cmd)}\n--- stdout ---\n{res.stdout}\n--- stderr ---\n{res.stderr}", encoding="utf-8")
        if res.returncode != 0:
            raise RuntimeError(
                f"Hive {tag} step failed (exit {res.returncode}). Script: {rendered}\n"
                f"--- stderr (tail) ---\n{res.stderr[-3000:]}\n--- stdout (tail) ---\n{res.stdout[-1500:]}")

    @staticmethod
    def _read_rows(directory: Path) -> list[list[str | None]]:
        """Rows Hive wrote with INSERT OVERWRITE LOCAL DIRECTORY: tab-separated, NULL = \\N."""
        d = Path(directory)
        if not d.is_dir():
            raise RuntimeError(f"Hive did not create the output directory {d}")
        rows = []
        for f in sorted(p for p in d.iterdir() if p.is_file() and not p.name.startswith((".", "_"))):
            for line in f.read_text(encoding="utf-8").split("\n"):
                if line:
                    rows.append([None if c == "\\N" else c for c in line.split("\t")])
        return rows

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _find_bin(options: dict, name: str) -> str | None:
        explicit = options.get("hive_bin")
        candidates = [explicit] if explicit else []
        if os.environ.get("HIVE_HOME"):
            candidates.append(os.path.join(os.environ["HIVE_HOME"], "bin", name))
        candidates.append(name)
        for c in candidates:
            if os.path.isfile(c) or shutil.which(c):
                return c
        return None

    @staticmethod
    def hive_escape(regex: str) -> str:
        """A regex as the body of a HiveQL '...' literal (Hive turns \\\\ into \\ and \\' into ')."""
        return regex.replace("\\", "\\\\").replace("'", "\\'")

    @classmethod
    def _hive_literal(cls, name: str) -> str:
        """Read a shared regex file (never hand-copied) and escape it for HiveQL."""
        raw = (_REPO / "common" / name).read_text(encoding="ascii").rstrip("\r\n")
        if "\n" in raw or "\r" in raw or ";" in raw:
            raise ValueError(f"common/{name} must hold the regex on a single line without ';'")
        return cls.hive_escape(raw)


# ======================================================================= vector self-test
def _test_vectors(options: dict[str, str]) -> int:
    """Run common/test_vectors.json through the real ingest.hql + finalize.hql and compare
    every parsed record, the counts and Q1/Q2/Q3 with the Python reference."""
    import json
    import uuid

    from tools.reference import compare, compute

    vectors = json.loads((_REPO / "common" / "test_vectors.json").read_text(encoding="ascii"))["vectors"]
    run_id = f"hive-vectortest-{uuid.uuid4().hex[:6]}"
    work = _REPO / "work" / run_id
    (work / "batches").mkdir(parents=True, exist_ok=True)
    batch_file = work / "batches" / "batch_000001.log"
    batch_file.write_bytes(b"".join(v["line"].encode("utf-8") + b"\n" for v in vectors))
    ctx = RunContext(run_id=run_id, pipeline_name="hive", batch_size=len(vectors),
                     input_files=(str(batch_file),), work_dir=str(work), repo_root=str(_REPO),
                     options=options)

    runner = HiveRunner()
    failed = 0
    try:
        runner.prepare(ctx)
        counts = runner.ingest_batch(ctx, BatchInfo(1, str(batch_file), len(vectors), 1))
        want_bad = sum(v["expected"] is None for v in vectors)
        print(f"counts: parsed={counts.parsed_count} malformed={counts.malformed_count} "
              f"(expected parsed={len(vectors) - want_bad} malformed={want_bad})")
        failed += (counts.malformed_count != want_bad)

        # every parsed record, compared as a multiset (Hive does not keep line order)
        out = runner._local / "out" / "parsed_dump"
        runner._run_script(EXPORT_PARSED_SQL, "export_parsed", {"OUT": str(runner._engine_path(out))})
        b64 = lambda s: None if s is None else base64.b64decode(s).decode("utf-8")  # noqa: E731
        got = sorted(json.dumps({
            "host": b64(h), "timestamp": b64(ts), "log_date": d, "log_hour": int(hr),
            "http_method": b64(m), "resource_path": b64(p), "protocol_version": b64(pv),
            "status_code": int(s), "bytes_transferred": int(by)}, sort_keys=True)
            for h, ts, d, hr, m, p, pv, s, by in runner._read_rows(out))
        want = sorted(json.dumps(v["expected"], sort_keys=True) for v in vectors if v["expected"] is not None)
        if got != want:
            failed += 1
            for rec in sorted(set(want) - set(got)):
                print(f"FAIL expected but missing: {rec}")
            for rec in sorted(set(got) - set(want)):
                print(f"FAIL produced by Hive    : {rec}")
        else:
            print(f"parsed records: {len(got)}/{len(want)} identical to the expected output")

        # Q1/Q2/Q3 against tools/reference.py on the same file
        final = runner.finalize(ctx)
        diffs = compare(compute([str(batch_file)]), {"totals": final.totals, "results": final.results})
        if diffs:
            failed += 1
            print("FAIL Q1/Q2/Q3 differ from tools/reference.py:")
            for line in diffs:
                print("  " + line)
        else:
            print("Q1/Q2/Q3 + totals: identical to tools/reference.py")
    finally:
        runner.cleanup(ctx, success=not failed)
        shutil.rmtree(work, ignore_errors=True)
    print(f"Hive self-test: {'PASSED' if not failed else 'FAILED'}")
    return 1 if failed else 0


if __name__ == "__main__":
    if "--test" in sys.argv:
        opts = dict(a.split("=", 1) for a in sys.argv[1:] if "=" in a)
        sys.exit(_test_vectors(opts))
    print(__doc__)
