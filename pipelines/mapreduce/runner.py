"""
Hadoop MapReduce pipeline (implements pipelines/base.py: PipelineRunner).
Hadoop Streaming jobs; the map and reduce functions are in mr_tasks.py (Python).

    ingest_batch   1 map-only job per batch: parse the raw batch file in the mappers,
                   keep the parsed records in this run's staging area, count
                   records / parsed / malformed with Hadoop counters
    finalize       Q1:  map (date, status)        -> combine/reduce: count, bytes
                   Q2:  map (path, host)          -> reduce: per path count, bytes, distinct
                        hosts, each reducer's top 20  -> 1 reducer: global top 20
                   Q3:  map (date, hour, host)    -> reduce: errors, total, rate, distinct
                        error hosts
                   Q1, Q2 and Q3 run at the same time (independent jobs).

Python here only starts Hadoop, links/copies the raw batch file into Hadoop's input
area and reads the jobs' output and counters back. All parsing and aggregation happens
inside the MapReduce jobs.

Requirements: Java 8-17, Apache Hadoop 3.x (local mode needs no cluster/HDFS/YARN), python3.
    export JAVA_HOME=...  HADOOP_HOME=~/tools/hadoop-3.4.1   (or `hadoop` on PATH)

Options (orchestrator --opt key=value):
    mr_mode=local|cluster      default local: Hadoop's LocalJobRunner on local files.
                               cluster: the jobs go to the configured YARN/HDFS
    hadoop_bin=/path/to/hadoop default: $HADOOP_HOME/bin/hadoop, else `hadoop` on PATH
    streaming_jar=/path.jar    default: $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar
    python_bin=python3         interpreter the tasks run (default: this Python)
    reducers=N                 reduce tasks for Q1 / Q2 / Q3 (default 1; Q2's final top 20: always 1)
    local_tasks=N              local mode: map/reduce tasks run in parallel (default min(cpus, 4))
    parallel_queries=0         run Q1, Q2, Q3 one after another instead of at the same time
    mr_scratch=/some/dir       local scratch root. Default: under the run's work dir, or a temp
                               dir if that path contains spaces (hadoop -files cannot take them)
    hdfs_root=/tmp/nasa_mr     cluster mode: HDFS staging root
    keep_mr_output=1           keep staging and job output after the run (debugging)

Check the parsing on the shared test vectors (runs a real ingest job):
    python pipelines/mapreduce/runner.py --test [key=value ...]
"""
from __future__ import annotations

import concurrent.futures
import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:  # allows `python pipelines/mapreduce/runner.py --test`
    sys.path.insert(0, str(_REPO))

from pipelines.base import BatchCounts, BatchInfo, FinalOutput, PipelineRunner, RunContext  # noqa: E402
from pipelines.mapreduce.mr_tasks import COUNTER_GROUP, unesc  # noqa: E402

HERE = Path(__file__).resolve().parent
TRUE = ("1", "true", "yes", "on")
SHIPPED = {"mr_tasks.py": HERE / "mr_tasks.py",
           "access_line.regex": _REPO / "common" / "access_line.regex",
           "request.regex": _REPO / "common" / "request.regex"}
KEY_PARTITIONER = "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"


class MapReduceRunner(PipelineRunner):
    name = "mapreduce"

    # ------------------------------------------------------------------ lifecycle
    def prepare(self, ctx: RunContext) -> None:
        """Untimed: find Hadoop, create a fresh staging area. Does not read the input."""
        o = ctx.options
        self._mode = o.get("mr_mode", "local")
        if self._mode not in ("local", "cluster"):
            raise ValueError(f"mr_mode must be 'local' or 'cluster', got {self._mode!r}")
        self._hadoop = self._find_hadoop(o)
        self._jar = self._find_streaming_jar(o)
        self._python = o.get("python_bin") or (
            "python3" if re.search(r"\s", sys.executable) else sys.executable)
        self._reducers = int(o.get("reducers", "1"))
        self._local_tasks = int(o.get("local_tasks", min(os.cpu_count() or 1, 4)))
        self._parallel = o.get("parallel_queries", "1").lower() in TRUE
        if self._reducers < 1 or self._local_tasks < 1:
            raise ValueError("reducers and local_tasks must be >= 1")

        root = o.get("mr_scratch")
        if not root:
            root = ctx.path("mapreduce")
            if re.search(r"\s", root):
                root = tempfile.mkdtemp(prefix=f"mr_{ctx.run_id}_")
        self._local = Path(root).resolve()
        if re.search(r"[\s,]", str(self._local)):
            raise ValueError(f"MapReduce scratch path must not contain spaces or commas: {self._local} "
                             "(set --opt mr_scratch=<dir>)")
        shutil.rmtree(self._local, ignore_errors=True)
        for d in ("input", "code", "jobs"):
            (self._local / d).mkdir(parents=True)
        # job logs go to the run's work dir, which outlives cleanup() (useful after a failure)
        self._logs = Path(ctx.path("mapreduce_logs"))
        self._logs.mkdir(parents=True, exist_ok=True)

        # ship the task code and the SHARED regex files (read from common/, never copied by hand)
        for dest, src in SHIPPED.items():
            shutil.copyfile(src, self._local / "code" / dest)
        self._files = ",".join(str(self._local / "code" / f) for f in SHIPPED)

        if self._mode == "local":
            self._root = str(self._local / "data")
        else:
            self._root = f"{o.get('hdfs_root', '/tmp/nasa_mr').rstrip('/')}/{ctx.run_id}"
            self._run_hadoop(["fs", "-rm", "-r", "-f", self._root], check=False)
            self._run_hadoop(["fs", "-mkdir", "-p", f"{self._root}/input"])
        self._staging = f"{self._root}/staging"
        self._out = f"{self._root}/out"
        self._run_id = ctx.run_id
        self._batches = 0
        self._parsed = 0
        self._malformed = 0

    def ingest_batch(self, ctx: RunContext, batch: BatchInfo) -> BatchCounts:
        """Timed: one map-only job parses the untouched batch file; counts come from its counters."""
        bid = batch.batch_id
        src = self._stage_input(batch)
        c = self._job(f"ingest_{bid}", inputs=[src], output=f"{self._staging}/batch_{bid:06d}",
                      mapper="parse", reducers=0)
        records = c.get((COUNTER_GROUP, "records"), 0)
        counts = BatchCounts(parsed_count=c.get((COUNTER_GROUP, "parsed"), 0),
                             malformed_count=c.get((COUNTER_GROUP, "malformed"), 0))
        map_input = c.get(("Map-Reduce Framework", "Map input records"))
        if records != counts.record_count or (map_input is not None and map_input != records):
            raise RuntimeError(f"batch {bid}: inconsistent Hadoop counters {c}")
        if counts.record_count != batch.record_count:
            raise RuntimeError(
                f"batch {bid}: MapReduce read {counts.record_count} records "
                f"(parsed {counts.parsed_count} + malformed {counts.malformed_count}) "
                f"but the batch file has {batch.record_count} lines")
        self._batches += 1
        self._parsed += counts.parsed_count
        self._malformed += counts.malformed_count
        self._remove_input(src)
        return counts

    def finalize(self, ctx: RunContext) -> FinalOutput:
        """Timed: Q1/Q2/Q3 as MapReduce jobs over every batch staged in this run."""
        staged = [f"{self._staging}/batch_*"]
        r = self._reducers
        jobs = {
            "q1": dict(inputs=staged, output=f"{self._out}/q1", mapper="q1_map",
                       combiner="sum 2", reducer="sum 2", reducers=r, key_fields=2),
            "q2_paths": dict(inputs=staged, output=f"{self._out}/q2_paths", mapper="q2_map",
                             combiner="sum 2", reducer="q2_top_local", reducers=r,
                             key_fields=2, partition_fields=1),
            "q3": dict(inputs=staged, output=f"{self._out}/q3", mapper="q3_map",
                       combiner="sum 3", reducer="q3_reduce", reducers=r,
                       key_fields=3, partition_fields=2),
        }
        if self._parallel:
            with concurrent.futures.ThreadPoolExecutor(len(jobs)) as pool:
                futures = {k: pool.submit(self._job, k, **kw) for k, kw in jobs.items()}
                counters = {k: f.result() for k, f in futures.items()}
        else:
            counters = {k: self._job(k, **kw) for k, kw in jobs.items()}
        self._job("q2", inputs=[f"{self._out}/q2_paths"], output=f"{self._out}/q2",
                  mapper="identity", reducer="q2_top_global", reducers=1)

        q1 = [{"log_date": f[0], "status_code": int(f[1]), "request_count": int(f[2]),
               "total_bytes": int(f[3])} for f in self._read_rows(f"{self._out}/q1")]
        q2 = [{"resource_path": unesc(f[0]), "request_count": int(f[1]), "total_bytes": int(f[2]),
               "distinct_host_count": int(f[3])} for f in self._read_rows(f"{self._out}/q2")]
        q3 = [{"log_date": f[0], "log_hour": int(f[1]), "error_request_count": int(f[2]),
               "total_request_count": int(f[3]), "error_rate": round(float(f[4]), 6),
               "distinct_error_hosts": int(f[5])} for f in self._read_rows(f"{self._out}/q3")]

        # totals as the engine counts them: staged records counted by the Q1 job's mappers,
        # malformed records counted by the ingest jobs' mappers
        staged_parsed = counters["q1"].get((COUNTER_GROUP, "staged_parsed"), 0)
        if staged_parsed != self._parsed:
            raise RuntimeError(f"Hadoop staging holds {staged_parsed} parsed records, "
                               f"the batches reported {self._parsed}")
        if sum(row["request_count"] for row in q1) != staged_parsed:
            raise RuntimeError("Q1 request counts do not add up to the staged parsed records")
        if len(q2) > 20:
            raise RuntimeError(f"Q2 returned {len(q2)} rows, expected at most 20")
        return FinalOutput(
            totals={"record_count": staged_parsed + self._malformed,
                    "parsed_count": staged_parsed, "malformed_count": self._malformed},
            results={"q1_daily_traffic": q1, "q2_top_resources": q2, "q3_hourly_errors": q3})

    def cleanup(self, ctx: RunContext, success: bool) -> None:
        if ctx.options.get("keep_mr_output", "").lower() in TRUE:
            return
        if getattr(self, "_mode", "local") == "cluster" and getattr(self, "_root", None):
            self._run_hadoop(["fs", "-rm", "-r", "-f", self._root], check=False)
        if getattr(self, "_local", None) is not None:
            shutil.rmtree(self._local, ignore_errors=True)

    # ------------------------------------------------------------------ input staging
    def _stage_input(self, batch: BatchInfo) -> str:
        """Give Hadoop the batch file unchanged, at a path without spaces."""
        name = f"batch_{batch.batch_id:06d}.log"
        if self._mode == "cluster":  # = hdfs put
            dest = f"{self._root}/input/{name}"
            self._run_hadoop(["fs", "-put", "-f", batch.path, dest])
            return dest
        dest = self._local / "input" / name
        try:
            os.symlink(os.path.abspath(batch.path), dest)
        except OSError:
            shutil.copyfile(batch.path, dest)
        return str(dest)

    def _remove_input(self, path: str) -> None:
        if self._mode == "cluster":
            self._run_hadoop(["fs", "-rm", "-f", path], check=False)
        else:
            Path(path).unlink(missing_ok=True)

    # ------------------------------------------------------------------ running Hadoop
    def _job(self, tag: str, *, inputs: list[str], output: str, mapper: str, reducers: int,
             combiner: str | None = None, reducer: str | None = None,
             key_fields: int | None = None, partition_fields: int | None = None) -> dict:
        """Run one Hadoop Streaming job; return its counters {(group, name): value}."""
        py = f"{self._python} mr_tasks.py"
        conf = {
            "mapreduce.job.name": f"nasa-{self._run_id}-{tag}",
            # only '\n' ends a record (docs/PARSING_RULES.md section 1); Hadoop's default
            # reader would also split on a bare '\r'
            "textinputformat.record.delimiter": "\n",
            "mapreduce.job.reduces": str(reducers),
        }
        if self._mode == "local":
            conf.update({"mapreduce.framework.name": "local", "fs.defaultFS": "file:///",
                         "mapreduce.local.map.tasks.maximum": str(self._local_tasks),
                         "mapreduce.local.reduce.tasks.maximum": str(self._local_tasks)})
        if key_fields:
            # the map output key is the first K fields. The combiner's output is split with the
            # REDUCE-side setting (default: 1 field), so it must be K as well, or the key would
            # shrink to the first field after combining and equal keys would no longer meet.
            conf["stream.num.map.output.key.fields"] = str(key_fields)
            conf["stream.num.reduce.output.key.fields"] = str(key_fields)
        if partition_fields:
            conf["mapreduce.partition.keypartitioner.options"] = f"-k1,{partition_fields}"

        cmd = [self._hadoop, "jar", self._jar]
        for k, v in conf.items():
            cmd += ["-D", f"{k}={v}"]
        cmd += ["-files", self._files]
        for i in inputs:
            cmd += ["-input", i]
        cmd += ["-output", output, "-mapper", f"{py} {mapper}"]
        if combiner:
            cmd += ["-combiner", f"{py} {combiner}"]
        if reducer:
            cmd += ["-reducer", f"{py} {reducer}"]
        if partition_fields:
            cmd += ["-partitioner", KEY_PARTITIONER]
        cmd += ["-numReduceTasks", str(reducers)]

        # Each job gets its own working dir: in local mode Hadoop links the -files into the
        # client's current dir and deletes them when the job ends, which would pull
        # mr_tasks.py away from Q1/Q2/Q3 jobs still running from a shared dir.
        job_dir = self._local / "jobs" / tag
        job_dir.mkdir(parents=True, exist_ok=True)
        res = self._run_hadoop(cmd[1:], check=False, cwd=job_dir)
        log = self._logs / f"{tag}.log"
        log.write_text(res.stdout + res.stderr, encoding="utf-8", errors="replace")
        if res.returncode != 0:
            # the Python tasks' own error lines (tracebacks), without Hadoop's log and Java stack lines
            task_err = [ln for ln in res.stderr.splitlines()
                        if ln.strip() and not re.match(r"\d{4}-\d\d-\d\d |\s+at |java\.|Caused by:|\t", ln)
                        and not ln.startswith(("reporter:", "Streaming Command Failed"))]
            raise RuntimeError(f"MapReduce job {tag} failed (exit {res.returncode}). Full log: {log}\n"
                               f"--- task errors ---\n" + "\n".join(task_err[-30:]) +
                               f"\n--- stderr (tail) ---\n{res.stderr[-2500:]}")
        counters = self.parse_counters(res.stderr)
        if not counters:
            raise RuntimeError(f"MapReduce job {tag}: no counters in Hadoop's output "
                               f"(is the Hadoop log level below INFO?). Log: {log}")
        return counters

    def _run_hadoop(self, args: list[str], check: bool = True,
                    cwd: Path | None = None) -> subprocess.CompletedProcess:
        env = dict(os.environ, HADOOP_ROOT_LOGGER="INFO,console")  # counters are logged at INFO
        res = subprocess.run([self._hadoop, *args], capture_output=True, text=True,
                             errors="replace", env=env, cwd=cwd or self._local)
        if check and res.returncode != 0:
            raise RuntimeError(f"hadoop {' '.join(args[:3])} ... failed:\n{res.stderr[-3000:]}")
        return res

    @staticmethod
    def parse_counters(log: str) -> dict:
        """Counters block that `hadoop jar` prints when a job completes:
              INFO mapreduce.Job: Counters: 22
              <TAB>Group name
              <TAB><TAB>Counter name=value"""
        counters: dict = {}
        group, inside = None, False
        for line in log.splitlines():
            if re.search(r"\bCounters: \d+\s*$", line):
                counters, group, inside = {}, None, True  # keep the last block
                continue
            if not inside:
                continue
            if line.startswith("\t\t") and "=" in line:
                k, _, v = line.strip().rpartition("=")
                try:
                    counters[(group, k)] = int(v)
                except ValueError:
                    pass
            elif line.startswith("\t"):
                group = line.strip()
            else:
                inside = False
        return counters

    def _read_rows(self, directory: str) -> list[list[str]]:
        """Reducer output: tab-separated lines in part-* files, in file order."""
        if self._mode == "cluster":
            text = self._run_hadoop(["fs", "-cat", f"{directory}/part-*"]).stdout
        else:
            parts = sorted(glob.glob(os.path.join(directory, "part-*")))
            if not parts and not os.path.isdir(directory):
                raise RuntimeError(f"MapReduce did not create the output directory {directory}")
            text = "".join(Path(p).read_text(encoding="utf-8") for p in parts)
        return [line.split("\t") for line in text.split("\n") if line]

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _find_hadoop(o: dict) -> str:
        candidates = [o["hadoop_bin"]] if o.get("hadoop_bin") else []
        if os.environ.get("HADOOP_HOME"):
            candidates.append(os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop"))
        candidates.append("hadoop")
        for c in candidates:
            if os.path.isfile(c) or shutil.which(c):
                return os.path.abspath(c) if os.path.isfile(c) else shutil.which(c)
        raise RuntimeError("Hadoop not found: set HADOOP_HOME, put `hadoop` on PATH, "
                           "or pass --opt hadoop_bin=/path/to/hadoop")

    @staticmethod
    def _find_streaming_jar(o: dict) -> str:
        if o.get("streaming_jar"):
            return o["streaming_jar"]
        homes = [os.environ.get("HADOOP_HOME")]
        hadoop = shutil.which("hadoop")
        if hadoop:
            homes.append(str(Path(hadoop).resolve().parents[1]))
        for home in filter(None, homes):
            jars = sorted(glob.glob(os.path.join(home, "share", "hadoop", "tools", "lib",
                                                 "hadoop-streaming-*.jar")))
            jars = [j for j in jars if not j.endswith(("-sources.jar", "-tests.jar"))]
            if jars:
                return jars[-1]
        raise RuntimeError("hadoop-streaming jar not found: set HADOOP_HOME "
                           "or pass --opt streaming_jar=/path/to/hadoop-streaming-x.y.z.jar")


# ======================================================================= vector self-test
def _test_vectors(argv: list[str]) -> int:
    """Run common/test_vectors.json through a real ingest job and compare every field."""
    import json
    opts = dict(a.split("=", 1) for a in argv if "=" in a)
    with open(_REPO / "common" / "test_vectors.json", encoding="ascii") as fh:
        vectors = json.load(fh)["vectors"]
    tmp = Path(tempfile.mkdtemp(prefix="mr_test_"))
    try:
        batch_file = tmp / "vectors.log"
        batch_file.write_bytes("".join(v["line"] + "\n" for v in vectors).encode("utf-8"))
        ctx = RunContext("mapreduce-test", "mapreduce", len(vectors), (str(batch_file),),
                         str(tmp / "work"), str(_REPO), {**opts, "mr_scratch": str(tmp / "mr")})
        runner = MapReduceRunner()
        runner.prepare(ctx)
        counts = runner.ingest_batch(ctx, BatchInfo(1, str(batch_file), len(vectors), 1))
        rows = runner._read_rows(f"{runner._staging}/batch_000001")  # map-only: input order
        fields = ("host", "timestamp", "log_date", "log_hour", "http_method", "resource_path",
                  "protocol_version", "status_code", "bytes_transferred")
        ints = {"log_hour", "status_code", "bytes_transferred"}
        got = iter(rows)
        failed = 0
        for v in vectors:
            if v["expected"] is None:
                continue
            f = next(got, None)
            actual = None if f is None else {
                k: (int(x) if k in ints else unesc(x)) for k, x in zip(fields, f)}
            if actual != v["expected"]:
                failed += 1
                print(f"FAIL {v['id']}: expected {v['expected']}, got {actual}")
        expected_bad = sum(v["expected"] is None for v in vectors)
        if counts.malformed_count != expected_bad or next(got, None) is not None:
            failed += 1
            print(f"FAIL counts: malformed {counts.malformed_count} (expected {expected_bad})")
        print(f"MapReduce: {len(vectors) - failed}/{len(vectors)} test vectors passed "
              f"(parsed {counts.parsed_count}, malformed {counts.malformed_count})")
        runner.cleanup(ctx, True)
        return 1 if failed else 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(_test_vectors(sys.argv[2:]) if "--test" in sys.argv else print(__doc__) or 0)
