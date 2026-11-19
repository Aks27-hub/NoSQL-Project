# Parsing, cleaning and query rules (shared by all four pipelines)

This document is the specification. All four pipelines (MapReduce, Pig, Hive, MongoDB) must implement exactly these rules. If your engine gives a different result on any test vector, your engine is wrong, not the spec. If you think a rule is wrong, change it for everyone: update this file, the two `.regex` files, `common/test_vectors.json` and `common/parser.py` together.

| File | What it is |
|---|---|
| `common/access_line.regex` | regex 1: the whole log line |
| `common/request.regex` | regex 2: the request field |
| `common/parser.py` | group numbers, month table, constants + reference parser in Python (the test oracle, **not** a pipeline). `python common/parser.py` prints each regex escaped for Java/Pig/Hive/Mongo; `--test` checks the vectors |
| `common/test_vectors.json` | 51 input lines with the exact expected output (real lines from the dataset plus synthetic edge cases) |
| `tools/reference.py` | expected Q1/Q2/Q3 for any input; `--compare` checks a pipeline's output against it |

Every rule below was checked against all 3,461,613 records of the dataset (section 9).

---

## 1. Records and batches

* **Input:** `NASA_access_log_Jul95` then `NASA_access_log_Aug95`, in that order. Either the `.gz` or the decompressed file can be used, because decompression is the only allowed change.
* **Record:** the bytes up to (not including) a `\n`. Only `\n` ends a record. The last line of a file is a record even without a trailing `\n` (Jul95 ends this way).
* Nothing is trimmed, decoded, filtered or rewritten before parsing. An empty line would be a record, and it would be malformed.
* **Batch:** records are numbered continuously across both files. Batch *k* holds records `(k-1)·N+1 … k·N`. Batch IDs start at 1. The last batch may be smaller and still counts. A batch can contain the end of Jul95 and the start of Aug95.
* The orchestrator does this split (`iter_records`/`iter_batches` in `orchestrator.py`) and writes each batch file byte-for-byte. **One line of a batch file is one record.** Your engine must account for every line: `parsed_count + malformed_count == record_count`. The orchestrator stops the run if this doesn't hold.
* Average batch size = total records / number of non-empty batches.

## 2. Regex 1: the log line

```
^([^ ]+) [^ ]+ [^ ]+ \[((0[1-9]|[12][0-9]|3[01])/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/([0-9]{4}):([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]) ([+-][0-9]{4}))\] "(.*)" ([0-9]{3}) ([0-9]+|-)$
```

| group | field | example |
|---|---|---|
| 1 | host | `in24.inetnebr.com` |
| 2 | raw timestamp | `01/Aug/1995:00:00:01 -0400` |
| 3 | day | `01` |
| 4 | month | `Aug` |
| 5 | year | `1995` |
| 6 | hour | `00` |
| 7 | minute | `00` |
| 8 | second | `01` |
| 9 | tz offset | `-0400` (kept, **not applied**) |
| 10 | request | `GET /shuttle/... HTTP/1.0` (may be empty or junk) |
| 11 | status | `200` |
| 12 | bytes | `1839` or `-` |

The line must match **as a whole**: Java `matcher.matches()`, Pig `REGEX_EXTRACT_ALL`, Hive `RegexSerDe`/`RLIKE` with the anchors, Mongo `$regexFind` with the anchors, Python `fullmatch`.

The regex itself does all the validation, so engines need no extra checks:
* Fields are separated by **exactly one space**, and there is nothing before the host or after the bytes.
* ident and authuser can be anything without spaces (always `-` in this dataset).
* day 01–31, month is one of 12 case-sensitive English abbreviations, hour 00–23, minute/second 00–59, offset `±hhmm`.
* The request is everything between the first `"` after `]` and the **last** `" ` before the status. It may itself contain quotes (32 real lines do).
* status = exactly 3 digits. bytes = digits or `-`.

## 3. Derived fields of a parsed record

| field | rule | type |
|---|---|---|
| `host` | group 1, exactly as written (case-sensitive, no lowercasing, no DNS) | string |
| `timestamp` | `YYYY-MM-DD HH:MM:SS` built from groups 5,4,3,6,7,8 | string |
| `log_date` | `YYYY-MM-DD` = group 5 + `-` + month number + `-` + group 3 | string / DATE |
| `log_hour` | integer value of group 6 (0–23) | int |
| `http_method` | from regex 2, NULL if absent | string |
| `resource_path` | from regex 2, NULL if absent | string |
| `protocol_version` | from regex 2, NULL if absent | string |
| `status_code` | integer value of group 11 | int |
| `bytes_transferred` | `-` becomes 0, otherwise the integer value of group 12 | **64-bit** |

* Month table: `Jan 01, Feb 02, Mar 03, Apr 04, May 05, Jun 06, Jul 07, Aug 08, Sep 09, Oct 10, Nov 11, Dec 12`. Build the date by string concatenation and a lookup table.
* **No timezone conversion.** `log_date` and `log_hour` are the server's local time exactly as written. Every line in the dataset is `-0400`.
  * **Hive:** do not use `unix_timestamp()`/`from_unixtime()`/`to_date(...)` on the raw timestamp. They convert to the JVM timezone and depend on the locale for `MMM`, which shifts hours and breaks Q3.
  * **Pig:** do not use `ToDate`.
  * **Mongo:** do not use `$dateFromString` and then `$hour` (UTC).
* Use 64-bit integers for bytes and every sum. The total bytes is 65,524,314,915, which overflows 32-bit.

## 4. Regex 2: the request field (group 10)

```
^ *([A-Z]+)(?: +([^ ].*?))??(?: +(HTTP/[^ ]*))? *$
```

Group 1 = `http_method`, group 2 = `resource_path`, group 3 = `protocol_version`. **A missing or empty group is NULL.**

* In Hive, `regexp_extract` returns `''` for a missing group, so wrap it in `NULLIF(…, '')`.
* In Mongo, `captures` holds `null` for a missing group.

The same rule in words (`common/parser.py --test` implements it a second time without regex and checks both agree; this was also checked on every record of the dataset):

1. Split the request on spaces (only `' '`, never tab or other whitespace). Runs of spaces count as one separator, and leading/trailing spaces are ignored.
2. If there are no tokens, or the first token is not made only of the letters `A–Z`, then method, path and protocol are all **NULL**. The record is still **parsed** (not malformed). It counts in Q1 and Q3, but not in Q2.
3. `http_method` = the first token.
4. If there are at least 2 tokens and the **last** token starts with `HTTP/`, that token is `protocol_version`, kept as written (`HTTP/1.0`, `HTTP/V1.0`, `HTTP/*`). Otherwise `protocol_version` is NULL.
5. `resource_path` = everything between the method and the protocol (or the end), with outer spaces removed and inner spacing kept. It is NULL if empty. The query string is kept, and nothing is URL-decoded or case-changed.

| request | method | path | protocol |
|---|---|---|---|
| `GET /ksc.html HTTP/1.0` | GET | `/ksc.html` | HTTP/1.0 |
| `GET /images/launchmedium.gif` | GET | `/images/launchmedium.gif` | NULL |
| `GET /shuttle/countdown/count.html   HTTP/1.0` | GET | `/shuttle/countdown/count.html` | HTTP/1.0 |
| `GET /ksc.html HTTP/1.0 ` | GET | `/ksc.html` | HTTP/1.0 |
| `GET / ksc.html HTTP/1.0` | GET | `/ ksc.html` | HTTP/1.0 |
| `GET / " HTTP/1.0` | GET | `/ "` | HTTP/1.0 |
| `GET  HTTP/1.0` | GET | NULL | HTTP/1.0 |
| `GET http://www.ksc.nasa.gov` | GET | `http://www.ksc.nasa.gov` | NULL |
| `huttle/countdown/` | NULL | NULL | NULL |
| `\x05\x01` (binary junk) | NULL | NULL | NULL |

How many records fall into each case: all three present 3,455,440; no protocol 6,103; no path 54; nothing (no method) 15.

## 5. Malformed records

**A record is malformed if and only if it does not match regex 1.** Nothing else makes a record malformed. A messy request field only produces NULLs (section 4).

* Malformed records are **counted, never dropped silently**: `malformed_count` is reported per batch and per run, and is stored in `batches`.
* They are excluded from all three queries.
* In this dataset exactly **1** record is malformed: the truncated last line of Jul95, `alyssa.p`.
* The test vectors cover 18 malformed cases (bad month, hour 24, 2-digit status, `-5` bytes, trailing space, tab separator, …).

## 6. Making engines behave the same (read this before writing your parser)

* **No `\d`, `\s`, `\S`, `\w`** in the regexes. Their meaning differs between Java, PCRE and Python (Unicode digits, vertical tab, NBSP). The regexes only use `[0-9]`, `[^ ]`, `[A-Z]`, literal spaces, `.`, `(?:…)` and lazy `*?`/`??`. All of these behave the same in `java.util.regex`, MongoDB's PCRE and Python `re`.
* **Full match.** Both regexes are anchored with `^…$`, so find-style APIs (Hive `regexp_extract`/`RLIKE`, Mongo `$regexFind`) behave like a full match. The dataset has no `\r`, so Java's "`$` before a final line terminator" can't cause a difference.
* **Character encoding.** Hadoop, Pig and Hive decode lines as UTF-8, replacing invalid bytes with U+FFFD. The Python reference uses latin-1. MongoDB: decode the batch file's lines as UTF-8 with `errors="replace"` (same as Hadoop).
  * The dataset has non-ASCII bytes in 18 records, always **inside the request field**, and never in host, timestamp, status or bytes.
  * A full comparison of `java.util.regex` + UTF-8 decoding against the Python reference over all 3,461,613 records found exactly 8 records where `resource_path` differs in those bytes, and **no other difference**.
  * Each of those paths has ≤ 2 requests, while the 20th path in Q2 has 28,852. So **the encoding choice cannot change any query output.**
* **Strings are compared by code point / bytes**, never by locale or collation. This matters for the Q2 tie-break.
  * Java `String.compareTo`, Hive and Pig ordering, and Mongo's default string sort (no `collation`) all do this.
  * In PostgreSQL, order by `rank_no`, not by `resource_path`.
* **Empty means NULL** for the three request fields, whatever your engine returns for a missing group.

## 7. Query definitions (over parsed records only)

**Q1: Daily Traffic Summary.** `GROUP BY log_date, status_code`. `request_count = COUNT(*)`, `total_bytes = SUM(bytes_transferred)`. No filters. The row order doesn't matter.

**Q2: Top Requested Resources.** Use only records with `resource_path IS NOT NULL`. Any method and any status counts (a 404 for a path is still a request for it). `GROUP BY resource_path`, then:
* `request_count = COUNT(*)`
* `total_bytes = SUM(bytes_transferred)`
* `distinct_host_count = COUNT(DISTINCT host)`

Then `ORDER BY request_count DESC, resource_path ASC` (code point order), keep the **first 20**. The tie-break makes the top 20 deterministic. `rank_no` = position 1..20.

**Q3: Hourly Error Analysis.** `GROUP BY log_date, log_hour` over **all** parsed records. An error is a record with `400 <= status_code <= 599`.
* `error_request_count` = the number of error records
* `total_request_count = COUNT(*)`
* `error_rate = error_request_count / total_request_count`, computed in floating point and rounded to 6 decimals (the loader recomputes it and accepts ±1e-6)
* `distinct_error_hosts` = `COUNT(DISTINCT host)` among the error records only, 0 if there are none

Only (date, hour) pairs that have at least one request appear. **Do not fill in hours with no requests** (the dataset has no records at all for 1995-07-29..31 and 1995-08-02).

**Why compute results over all batches at the end, instead of adding up per-batch results?** Batches are only *ingested* one at a time: each engine parses a batch and keeps the parsed records for the run. Q1/Q2/Q3 are then computed once over everything in `finalize()`. Q2's top-20 and both distinct-host counts can't be combined from per-batch partial results: a path can be 21st in every batch but first overall, and the same host shows up in many batches. Computing once at the end is the only exact way, and it makes the results independent of the batch size (checked with batch sizes 1, 7, 1000 and 500,000).

## 8. Engine cheat-sheet

Get each regex escaped for your language with `python common/parser.py`. Better still, read the `.regex` files at runtime so nobody hand-copies it.

**MapReduce (Java).** Compile both `Pattern`s once in `setup()`, and use `matcher.matches()`. A line → `Text.toString()` (UTF-8, as above). Count records with Hadoop counters (`PARSED`, `MALFORMED`) and have the runner read them back.
* Q2 and the distinct counts need a second job, or a composite key such as `(path, host)` → distinct.
* Q2's top 20 needs one final reducer, or an in-memory top-N in a single-reducer job.

**Pig.**
```pig
raw    = LOAD '$BATCH' USING TextLoader() AS (line:chararray);
m      = FOREACH raw GENERATE REGEX_EXTRACT_ALL(line, '<regex 1, Pig-escaped>') AS f;  -- null if no full match
SPLIT m INTO ok IF f IS NOT NULL, bad OTHERWISE;
```
`f.$0` = group 1, …, `f.$11` = group 12. Apply regex 2 with a second `REGEX_EXTRACT_ALL` on the request.
* For the month, use a `CASE` expression or a join with a 12-row relation.
* Cast bytes to `long`.
* Don't use `ToDate`.

**Hive.**
* Use an external table per batch with `ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ("input.regex" = "<regex 1, Java-escaped>")`, with 12 `STRING` columns. Non-matching lines come back as all-NULL rows, so malformed = `host IS NULL`.
* Then `INSERT INTO` a parsed table partitioned by `(run_id, batch_id)`.
* Request fields: `NULLIF(regexp_extract(request, '<regex 2>', n), '')`.
* For the month, use a `CASE`. Bytes: `CAST(IF(bytes='-', '0', bytes) AS BIGINT)`.

**MongoDB.**
* Python reads the batch file and inserts the **raw** lines as `{run_id, batch_id, line_no, line}` (decode as UTF-8 with `errors="replace"`).
* All parsing happens in an aggregation pipeline: `$regexFind: {input: "$line", regex: <regex 1>}`; a `null` result means malformed; take `captures` with `$arrayElemAt`.
* For the month, use `$indexOfArray` on `["Jan",…,"Dec"]` + 1, then pad to 2 digits.
* Apply regex 2 with a second `$regexFind`, and turn `null`/`""` into `null`.
* `$merge` the parsed docs into a per-run collection. Use `$toLong` for bytes, `$addToSet` + `$size` for distinct hosts, and `allowDiskUse: true`.

## 9. Known-good numbers for the full dataset (both months)

Your pipeline must reproduce these. `tools/reference.py --compare` checks every row.

| | value |
|---|---|
| records | 3,461,613 (Jul95 1,891,715 + Aug95 1,569,898) |
| parsed / malformed | 3,461,612 / 1 |
| distinct dates | 58 (no data for Jul 29-31 and Aug 2) |
| Q1 rows | 295 (status codes 200, 302, 304, 400, 403, 404, 500, 501) |
| Q3 rows | 1,369 |
| sum of Q1 total_bytes | 65,524,314,915 |
| Q2 #1 | `/images/NASA-logosmall.gif`: 208,723 requests, 131,725,740 bytes, 81,510 hosts |
| Q2 #20 | `/icons/blank.xbm`: 28,852 requests |

## 10. How to check your pipeline

```bash
python common/parser.py --test                     # the rules pass on all test vectors
python tools/reference.py                          # -> work/reference.json  (~1 min)
python orchestrator.py -p <yours> --dry-run        # no PostgreSQL needed
python tools/reference.py --compare work/<run_id>/final_results.json
```
Also run your engine's parser on `common/test_vectors.json` (put the `line`s in a file, one per line, and compare with `expected`). This catches parsing differences before you spend a full run on them.
