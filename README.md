# Multi-Pipeline ETL for NASA HTTP Logs (DAS 839)

The same ETL and the same three queries, run on one of **MapReduce, Pig, Hive or MongoDB**, with the results loaded into PostgreSQL.

## Files

| path | what |
|---|---|
| `orchestrator.py` | **the tool**: pick a pipeline, batch the input, time the run, load the results |
| `pipelines/base.py` | the interface every pipeline implements (`prepare / ingest_batch / finalize / cleanup`) |
| `docs/PARSING_RULES.md` | **the rules all pipelines must follow**. Read this first |
| `common/*.regex` | the two shared regexes (line, request) |
| `common/parser.py` | the same rules in Python + regex escaping helper |
| `common/test_vectors.json` | 51 sample lines with the correct parsed output |
| `tools/reference.py` | correct Q1/Q2/Q3 in plain Python, and `--compare` to check a run against it |
| `db_loader/`, `schema/`, `templates/` | the only code that writes to PostgreSQL; the DB schema; example payloads |

## Setup

```bash
pip install -r requirements.txt
# dataset (not committed): put both files in ./data, .gz is fine
#   https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
#   https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
psql -U nasa -d nasa_etl -f schema/postgres_schema.sql
# DB connection: NASA_DB_HOST / NASA_DB_PORT / NASA_DB_USER / NASA_DB_PASSWORD / NASA_DB_NAME
```

## Run

```bash
python orchestrator.py                       # menu to choose the pipeline
python orchestrator.py -p hive -b 100000     # same batch size for every pipeline in an experiment
python orchestrator.py -p pig --dry-run      # without PostgreSQL (validates + writes JSON)
```

## Adding your pipeline

1. Create `pipelines/<name>/runner.py` with a class that subclasses `PipelineRunner` (see the docstring in `pipelines/base.py`). It is already listed in `RUNNERS` in `orchestrator.py`.
2. Parse **inside your engine** with the shared regexes (`python common/parser.py` prints them escaped for your language).
3. Check against the reference:
   ```bash
   python tools/reference.py
   python orchestrator.py -p <name> --dry-run
   python tools/reference.py --compare work/<run_id>/final_results.json
   ```
