-- =====================================================================
-- Pig pipeline, INGEST step: parse ONE raw batch file inside Pig.
-- Rules: docs/PARSING_RULES.md. Called once per batch by runner.py.
--
-- Parameters (runner.py passes them in a -param_file):
--   BATCH_FILE   the raw batch file, exactly as the orchestrator wrote it
--   BATCH_ID     1, 2, 3, ...
--   STAGING_DIR  this run's staging area (parsed records + per-batch stats)
--   OUTPUT_DIR   where the per-batch stats are written for runner.py to read
-- Placeholders (runner.py fills them from common/*.regex, Pig-escaped,
-- so nobody hand-copies the regexes):
--   __LINE_REGEX__   regex 1, the whole log line   (12 groups)
--   __REQUEST_REGEX__ regex 2, the request field   (3 groups)
-- =====================================================================

-- One line = one record. Nothing is trimmed, filtered or rewritten first.
raw = LOAD '$BATCH_FILE' USING TextLoader() AS (line:chararray);

-- REGEX_EXTRACT_ALL uses Matcher.matches() (whole-line match) and returns
-- NULL when the line does not match: that is exactly "malformed" (section 5).
-- A non-participating group comes back as NULL.
matched = FOREACH raw GENERATE REGEX_EXTRACT_ALL(line, '__LINE_REGEX__') AS f;

-- ---------------------------------------------------------------- counts
-- COUNT_STAR, not COUNT: COUNT skips tuples whose FIRST field is NULL, and
-- for a malformed line the first field (f) IS NULL. COUNT would silently
-- drop every malformed record and record_count would come out too small.
flagged   = FOREACH matched GENERATE (f IS NULL ? 0L : 1L) AS is_parsed;
stats_grp = GROUP flagged ALL;
stats     = FOREACH stats_grp GENERATE
                (long)'$BATCH_ID'                         AS batch_id,
                COUNT_STAR(flagged)                       AS record_count,
                SUM(flagged.is_parsed)                    AS parsed_count,
                COUNT_STAR(flagged) - SUM(flagged.is_parsed) AS malformed_count;

-- ---------------------------------------------------------------- parse
ok = FILTER matched BY f IS NOT NULL;

-- group n of regex 1 is f.$(n-1)
fields = FOREACH ok GENERATE
    (chararray)f.$0  AS host,
    (chararray)f.$2  AS day,
    (chararray)f.$3  AS mon,
    (chararray)f.$4  AS year,
    (chararray)f.$5  AS hour,
    (chararray)f.$6  AS minute,
    (chararray)f.$7  AS second,
    (chararray)f.$9  AS request,
    (chararray)f.$10 AS status,
    (chararray)f.$11 AS bytes;

-- Month table by string lookup (no ToDate: it would apply a timezone).
-- The regex only lets the 12 real abbreviations through, so the last
-- branch can only be 'Dec'. (Pig cannot chain ?: without nested
-- parentheses, so this is a CASE.)
dated = FOREACH fields GENERATE
    host, day, year, hour, minute, second, request, status, bytes,
    (CASE mon
        WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
        WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
        WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
        WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' ELSE '12'
     END) AS mm;

with_request = FOREACH dated GENERATE
    host,
    CONCAT(year, '-', mm, '-', day)                                       AS log_date,
    CONCAT(year, '-', mm, '-', day, ' ', hour, ':', minute, ':', second)  AS timestamp,
    hour, status, bytes,
    REGEX_EXTRACT_ALL(request, '__REQUEST_REGEX__')                       AS r;

-- Regex 2: no match (no [A-Z]+ method) -> method, path, protocol all NULL.
-- A missing or empty group is NULL (section 4).
parsed = FOREACH with_request GENERATE
    host,
    timestamp,
    log_date,
    (int)hour AS log_hour,
    ((r IS NOT NULL AND SIZE((chararray)r.$0) > 0L) ? (chararray)r.$0 : (chararray)null) AS http_method,
    ((r IS NOT NULL AND SIZE((chararray)r.$1) > 0L) ? (chararray)r.$1 : (chararray)null) AS resource_path,
    ((r IS NOT NULL AND SIZE((chararray)r.$2) > 0L) ? (chararray)r.$2 : (chararray)null) AS protocol_version,
    (int)status AS status_code,
    (bytes == '-' ? 0L : (long)bytes) AS bytes_transferred;   -- 64-bit

-- ---------------------------------------------------------------- store
-- BinStorage keeps types and NULLs exactly and needs no delimiter, so a
-- path containing a tab, \u0001, quotes or non-ASCII cannot shift columns.
STORE parsed INTO '$STAGING_DIR/parsed/batch_$BATCH_ID' USING BinStorage();
STORE stats  INTO '$STAGING_DIR/stats/batch_$BATCH_ID'  USING BinStorage();
-- the same one-row stats, as JSON, for runner.py
STORE stats  INTO '$OUTPUT_DIR/stats' USING JsonStorage();
