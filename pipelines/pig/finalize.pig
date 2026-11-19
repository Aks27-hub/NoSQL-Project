-- =====================================================================
-- Pig pipeline, FINALIZE step: Q1, Q2, Q3 over EVERY batch ingested in
-- this run (docs/PARSING_RULES.md section 7). Called once per run.
--
-- Parameters:
--   STAGING_DIR  this run's staging area, written by ingest.pig
--   OUTPUT_DIR   where the results are written (JSON lines) for runner.py
-- =====================================================================

records = LOAD '$STAGING_DIR/parsed/batch_*' USING BinStorage() AS (
    host:chararray,
    timestamp:chararray,
    log_date:chararray,
    log_hour:int,
    http_method:chararray,
    resource_path:chararray,
    protocol_version:chararray,
    status_code:int,
    bytes_transferred:long
);

stats = LOAD '$STAGING_DIR/stats/batch_*' USING BinStorage() AS (
    batch_id:long,
    record_count:long,
    parsed_count:long,
    malformed_count:long
);

-- COUNT_STAR everywhere: COUNT would skip tuples whose first field is NULL.

-- ---------------------------------------------------------------- totals
-- Counted by Pig from its own staging, not passed in from Python.
batch_totals = FOREACH (GROUP stats ALL) GENERATE
    COUNT_STAR(stats)            AS batch_count,
    SUM(stats.record_count)      AS record_count,
    SUM(stats.parsed_count)      AS parsed_count,
    SUM(stats.malformed_count)   AS malformed_count;

-- the parsed records actually present in staging (runner.py checks that this
-- equals the per-batch parsed counts, so a lost batch cannot go unnoticed)
staged_totals = FOREACH (GROUP records ALL) GENERATE
    COUNT_STAR(records) AS staged_parsed_count;

-- ---------------------------------------------------------------- Q1
-- Daily Traffic Summary: GROUP BY log_date, status_code. No filters.
q1 = FOREACH (GROUP records BY (log_date, status_code)) GENERATE
    FLATTEN(group)                   AS (log_date, status_code),
    COUNT_STAR(records)              AS request_count,
    SUM(records.bytes_transferred)   AS total_bytes;

-- ---------------------------------------------------------------- Q2
-- Top 20 resources. Only records with a resource_path; any method/status.
-- The path is used exactly as parsed (no TRIM, no character replacement).
with_path = FILTER records BY resource_path IS NOT NULL;
q2_all = FOREACH (GROUP with_path BY resource_path) {
    hosts = DISTINCT with_path.host;
    GENERATE
        group                              AS resource_path,
        COUNT_STAR(with_path)              AS request_count,
        SUM(with_path.bytes_transferred)   AS total_bytes,
        COUNT_STAR(hosts)                  AS distinct_host_count;
};
-- tie-break on resource_path: Pig compares chararrays with String.compareTo
-- (code point order, no locale), as section 6 requires
q2_sorted = ORDER q2_all BY request_count DESC, resource_path ASC;
q2        = LIMIT q2_sorted 20;

-- ---------------------------------------------------------------- Q3
-- Hourly Error Analysis over ALL parsed records; error = 400 <= status <= 599.
-- Only (date, hour) pairs that have requests appear (no filling of gaps).
q3 = FOREACH (GROUP records BY (log_date, log_hour)) {
    errs      = FILTER records BY status_code >= 400 AND status_code <= 599;
    err_hosts = DISTINCT errs.host;
    GENERATE
        FLATTEN(group)                                      AS (log_date, log_hour),
        COUNT_STAR(errs)                                    AS error_request_count,
        COUNT_STAR(records)                                 AS total_request_count,
        (double)COUNT_STAR(errs) / (double)COUNT_STAR(records) AS error_rate,
        COUNT_STAR(err_hosts)                               AS distinct_error_hosts;
};

-- ---------------------------------------------------------------- store
-- JSON lines: strings are escaped properly, so a path with tabs or quotes
-- comes back to Python unchanged. Row order of Q1/Q3 does not matter;
-- Q2 keeps the ORDER above (one part file after LIMIT).
STORE batch_totals  INTO '$OUTPUT_DIR/batch_totals'  USING JsonStorage();
STORE staged_totals INTO '$OUTPUT_DIR/staged_totals' USING JsonStorage();
STORE q1            INTO '$OUTPUT_DIR/q1'            USING JsonStorage();
STORE q2            INTO '$OUTPUT_DIR/q2'            USING JsonStorage();
STORE q3            INTO '$OUTPUT_DIR/q3'            USING JsonStorage();
