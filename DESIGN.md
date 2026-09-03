# splunk_rest_extractor — design

Goal: point a CLI at a Splunk REST endpoint (8089), give it an SPL search and a
time range, and get **every** event that search matches for that range written to
flat files on disk, with proof that nothing was dropped, for result sets that are
arbitrarily large (TB-scale indexes in production).

Everything in this document that describes Splunk behaviour was verified against
the local instance (Splunk 9.4.8, 16 cores, standalone) on 2026-09-02 unless it is
marked *(unverified)*. The probe scripts live in the session scratchpad; they will
be turned into integration tests.

---

## 1. What Splunk actually does (measured)

These facts drive every design decision below.

| # | Behaviour | Measured on this box | Consequence |
|---|---|---|---|
| 1 | `/results` and `/events` return at most `restapi.maxresultrows` rows per call, regardless of `count` (`count=0` and `count=100000` both returned 50,000). | 50,000 | Paging is mandatory: `count=50000`, step `offset`. |
| 2 | A job stops **scanning** once `search.max_count` events are collected. Job ends `DONE`, `isFailed=false`, `isFinalized=false`, `messages=[]`. Only `eventIsTruncated=true` reveals it. `eventCount` is **not** the true count (reported 530k of 2.08M). | `max_count=500000` | Treat `eventIsTruncated` as a hard failure of the chunk. Never trust `eventCount` from a truncated job. |
| 3 | The job-creation parameter `max_count` overrides the limits.conf cap. A 2.08M-event job completed untruncated. | 1.35 GB dispatch disk for 2.08M events (~650 B/event) | Job mode can handle big chunks, bounded by dispatch disk (`srchDiskQuota`, 10 GB/user default), not by limits.conf. |
| 4 | `earliest_time` is inclusive, `latest_time` is exclusive. Adjacent windows `[a,b)` `[b,c)` partition exactly (verified on static data; sums matched). | | Chunk by half-open epoch intervals. No overlap, no gap, no dedup needed. |
| 5 | Inline `earliest=`/`latest=` in the SPL string **override** the REST `earliest_time`/`latest_time` params. | inline `-2m` beat param `-1h` | The tool must reject SPL containing time modifiers, otherwise chunking silently returns the same window for every chunk. |
| 6 | `index_latest=<epoch>` as a REST job param (also on export) freezes the dataset: repeated counts on the live `_internal` index were identical with the pin and drifted without it. | pinned 575,702 / 575,702, unpinned 578,254 | Every search in a run (plan, extract, validate) uses the same pin = run start time. Planning and validation counts then agree with extraction even on live indexes. |
| 7 | `scanCount` = events read from disk. `eventCount` = events emitted by the search pipeline. They can differ legitimately: search-time props on `PerfmonMk:Process` split 864 indexed events into 139,828 result events. `tstats` and the `data/indexes` metadata count agree with `scanCount`, not with what the search returns. | search 2,083,056 · tstats 1,944,092 · index metadata 2,030,269 | Validation must count **through the same SPL pipeline** (`<spl> \| stats count`). `tstats` and index metadata are only sanity hints. |
| 8 | Because of (7), `(_bkt,_cd)` is **not** a unique event key. 1 h of botsv3: 443,808 rows, 389,373 distinct keys. | | No dedup-by-id anywhere in the pipeline. Uniqueness checks are informational only. |
| 9 | Export (`/jobs/export`) streams NDJSON `{"preview":false,"offset":n,"result":{...}}`; the final line carries `"lastrow":true` (also emitted for zero results). A search that fails with a FATAL lookup error returns **HTTP 200 and an empty body**. Warnings that the job endpoint reports in `messages` do not appear in the export stream. | | Export has no error channel. If used, "no `lastrow` seen" = failure, and an independent count is mandatory. |
| 10 | Job `messages` are typed (`FATAL`/`ERROR`/`WARN`/`INFO`) with text; a failed job has `dispatchState=FAILED`, `isFailed=true`. | | Job mode gives a real error channel. This is the main reason it is the default data path. |
| 11 | Splunk passes through raw bytes that are not valid UTF-8 inside its JSON output (botsv3). | `UnicodeDecodeError` at byte 0x8e | The reader must decode tolerantly and record how many replacements it made. |
| 12 | `tstats` honours the REST `earliest_time`/`latest_time` params. `/services/search/timeparser` resolves relative time (`-1d@d`) to an absolute timestamp. | | Relative inputs are resolved once, up front, and frozen as epochs. |
| 13 | Throughput on loopback: job-mode paging ~75k rows/s (35 MB JSON per 50k page); export ~57k rows/s (2 GB for 2.08M rows in 36 s). A histogram pass over 2.08M events took 8 s (`stats`) / 1 s (`tstats`). | | The bottleneck in production will be the indexers, not the client. Planning passes are cheap on the wire but cost one full scan. |

Concurrency envelope here: `base_max_searches (6) + 1 × 16 cores = 22` concurrent
searches server-wide; the admin role has `srchJobsQuota=50`, `srchDiskQuota=10000 MB`.

---

## 2. Core design decisions

### 2.1 Time range is a first-class input, never part of the SPL

* CLI takes `--earliest` and `--latest`. Accepted forms: ISO-8601, epoch seconds,
  or a Splunk relative expression. Relative expressions are resolved **once** via
  `/services/search/timeparser` at run start and written to the manifest as epoch
  integers. From then on the run only ever deals in epochs.
* The SPL is validated before anything runs. It is **rejected** if it contains
  `earliest=`, `latest=`, `_index_earliest=`, `_index_latest=` anywhere (fact 5).
  A leading `search ` is added if the string does not begin with `search` or `|`.
* Every search the tool issues passes the window as `earliest_time`/`latest_time`
  REST params, as epoch integers, half-open `[start, end)` (fact 4).
* Every search also passes `index_latest=<run_start_epoch>` (fact 6) so the whole
  run sees one consistent snapshot. `--no-pin` disables this for the rare SPL where
  index-time filtering does not apply (e.g. `| inputlookup`).

### 2.2 Chunking: day-aligned, count-targeted, split on overflow

A chunk is a half-open interval that never crosses a UTC day boundary (`--tz` to
change). A chunk is therefore either a whole day or a subdivision of one. This is
what makes "files named by date" fall out naturally and keeps every chunk's search
bounded to one day of buckets.

Planning has two layers:

1. **Histogram pass (default on, `--no-histogram` to skip).** One transforming
   search over the whole range:
   `<spl> | bin _time span=<S> | stats count by _time`
   Default span 1 h; the planner refines any bin that is larger than the chunk
   target with a second histogram at a finer span over just that bin, recursively,
   down to `--min-span` (default 1 s). Output is tiny, cost is one full scan of the
   data (fact 13). The bins are packed in time order into chunks of at most
   `--chunk-target-events` (default 250,000) without crossing a day. Each chunk's
   **expected count** (sum of its bins) is stored in the manifest. Because this is
   an independent search through the same pipeline (fact 7), it doubles as the
   validation oracle.
2. **Reactive splitting.** Regardless of how the plan was built, if a chunk's job
   comes back `eventIsTruncated` (fact 2) the chunk is marked `overflow`, bisected
   in time, and both halves are queued. A chunk that is already at `--min-span` and
   still overflows (more than the cap in one second) is the one case time cannot
   split, and it is re-run in **export mode** (section 2.3), which has no cap.

Chunk target and job `max_count`: each job is created with
`max_count = 2 × chunk_target` so that a plan estimate that is off by up to 2×
still completes instead of forcing a split, and dispatch disk stays bounded
(fact 3). The planner also reads `restapi.maxresultrows` and
`search.max_count` from `/services/configs/conf-limits` when the role permits,
and falls back to 50,000 / 500,000 otherwise.

### 2.3 Data path: job mode by default, export as the escape hatch

**Job mode (default).** `POST search/v2/jobs` with `exec_mode=normal`, the params
above, `adhoc_search_level=fast`, a generous `ttl`; poll until `DONE`/`FAILED`;
then page `GET .../results?output_mode=json&count=<page>&offset=<k·page>&f=<fields>`
until `offset ≥ resultCount`; assert rows fetched == `resultCount`; `DELETE` the job.

Why job mode wins over export as the default, given Splunk's own docs recommend
export for large sets:

* It has an error channel (facts 9, 10). Export returned HTTP 200 and nothing for a
  fatal error, and never surfaces warnings. In a distributed production search a
  peer dropping out mid-search is reported as a job message; export would just
  hand back fewer rows.
* Page-level retry and resume. A dropped connection costs one 50k-row page, not the
  chunk. Results are materialised server-side and ordered, so `offset` is stable.
* The job carries `resultCount`, `eventIsTruncated`, `isFinalized`, `isFailed` and
  `messages`, which are the completeness signals.

Costs and how they are bounded: dispatch disk (~650 B/event measured, capped by
`max_count` and by `--workers × chunk_target`), and one poll loop per chunk. The
tool refuses to start if `workers × max_count × bytes_per_event_estimate` exceeds
the role's `srchDiskQuota` when that value is readable.

**Export mode (`--mode export`, and automatic for unsplittable hot seconds).**
`POST search/v2/jobs/export` with the same params, streamed NDJSON. Rules: the
chunk is only complete when a `lastrow` line was seen; any exception, EOF without
`lastrow`, or a non-200 status fails the chunk; an independent count (histogram or
`| stats count`) is **required** for the chunk, not optional, because there is no
job state to check. Suitable when dispatch disk is the constraint or for the
hot-second overflow case.

### 2.4 Output files

```
<out>/
  manifest.sqlite                # state, see 2.5
  report.json / report.md        # produced by `validate` and at end of `run`
  data/
    2018-08-20/
      1534737600-1534741200.jsonl.gz     # [start,end) epoch, UTC-day directory
      1534741200-1534744800.jsonl.gz
    2018-08-21/
      1534824000-1534910400.jsonl.gz     # a whole quiet day is one file
```

* One file per chunk, written to `<name>.part`, fsync'd, then atomically renamed.
  A file without `.part` is complete by construction. Re-running never rewrites a
  completed chunk unless `--force`.
* Format: NDJSON, gzip. Default fields:
  `_time, _raw, index, sourcetype, source, host, _indextime, _bkt, _cd`.
  `--fields` overrides. `_bkt`/`_cd` are kept for provenance and debugging, not as
  a key (fact 8).
* `--format raw` writes `_raw` only, one event per line. Multi-line events break
  the one-line-per-event property, so this mode warns and records `linecount>1`
  events in the manifest.
* Invalid UTF-8 (fact 11): bytes are decoded with `errors="replace"` by default
  and the replacement count is stored per chunk; `--on-bad-utf8 fail` aborts the
  chunk instead.
* `compact` sub-command concatenates a day's chunk files into
  `data/2018-08-20.jsonl.gz`. gzip members concatenate legally, so this is a
  streaming `cat` plus a manifest update, not a re-compression.
* Row order inside a file is whatever Splunk returned (reverse-chronological for
  event searches). Not sorted; documented.

### 2.5 State and resumability

`manifest.sqlite` (single writer, WAL) holds:

* `run`: run id, SPL, SPL sha256, resolved earliest/latest epochs, pin epoch, all
  effective options, Splunk version/server name, start/finish.
* `chunk`: id, day, start, end, mode, status
  (`pending → running → done | failed | overflow | split | mismatch`),
  expected_count, sid, event_count, result_count, scan_count, written_count,
  pages_done, bytes, sha256, utf8_replacements, messages (JSON), attempts,
  timings.
* `event`: append-only log of state transitions and errors.

`run --resume` (or re-running the same command in the same `<out>`) picks up every
chunk not in `done`. A `running` chunk whose job still exists and is `DONE` resumes
from `pages_done`; otherwise it restarts. The SPL hash and range must match the
manifest or the tool refuses.

### 2.6 Validation

Levels (`--validate`), cumulative:

* `job` (always on, free): `written == resultCount`, `eventIsTruncated == false`,
  `isFailed == false`, `isFinalized == false` (auto-finalised jobs are incomplete),
  no `ERROR`/`FATAL` messages, and in strict mode (default) no `WARN` either.
  Every page accounted for.
* `plan` (default whenever the histogram ran): `written == expected` per chunk.
  A mismatch marks the chunk `mismatch` and re-runs it once; a persistent
  mismatch is reported, never silently accepted. Because the expected number comes
  from an independent search with the same pin, this catches partial peer results
  that carried no message.
* `total`: one `<spl> | stats count` over the full range with the pin, compared to
  the sum of written counts. One extra scan.
* `full` (the `validate` sub-command, runnable any time later): re-opens every
  file (gzip integrity, line count, sha256 vs manifest), re-runs `| stats count`
  per chunk, and optionally `--sample N` re-extracts N random chunks and compares
  a sorted content hash. Produces `report.md`.

What validation deliberately does **not** do: dedup by `_cd` (fact 8), or compare
against `tstats` / index metadata (fact 7). Those numbers are printed as hints
with an explanation when they differ.

### 2.7 Concurrency and being a good citizen

* `--workers` (default 2). Each worker owns one search at a time. Guidance in
  `--help`: stay under a quarter of `base_max_searches + cores × max_searches_per_cpu`
  and well under `srchJobsQuota`; the tool prints both when it can read them.
* HTTP 503 / "quota" messages back off exponentially with jitter; 401 triggers a
  re-login; 5xx and timeouts retry the request; a 404 on a job restarts the chunk.
  Transport errors before the first response of the session (wrong URL, port,
  hostname, or certificate) are not retried: they never fix themselves, and the
  CLI reports them at once with a hint.
* Jobs are deleted as soon as their pages are on disk, and on any failure path,
  so dispatch disk does not accumulate.
* Oldest chunks first by default (`--order`), so a multi-day run near a retention
  edge grabs data before it is frozen off.

### 2.8 SPL guardrails

Chunking is only correct if the SPL is *time-partitionable*: running it on each
sub-window and concatenating must equal running it once. The validator:

* rejects inline time modifiers (fact 5) and side-effect commands
  (`outputlookup`, `collect`, `delete`, `sendemail`);
* warns loudly on commands whose semantics span the whole result set: `head`,
  `tail`, `dedup`, `sort`, `streamstats`, `eventstats`, `transaction`, `stats`
  without `by _time`-style bucketing;
* notes that generating commands (`| tstats`, `| inputlookup`) may ignore
  `index_latest` *(unverified)* and that `| inputlookup` ignores the time range.

### 2.9 Late data and follow-up runs

The pin excludes anything indexed after run start. A **delta run** picks up late
arrivals: same SPL, same `_time` range, `index_earliest=<previous pin>`,
`index_latest=<new pin>`, into a separate `<out>` (or a `delta/` sub-tree).
Splunk's own caveat applies: index-time modifiers still need an event-time
window, which is exactly the original range here.

---

## 3. Production considerations (TB+)

* **Cost of the histogram pass** is one scan. It is worth it: it turns the plan
  into a validation oracle and avoids blind overflow/split cycles. `--no-histogram`
  falls back to fixed `--span` chunks with reactive splitting for cases where the
  extra scan is unacceptable.
* **Search head clusters**: job `sid`s live on one member. Point the tool at a
  single search head, not a load balancer, or use a sticky VIP. Export streams
  are also vulnerable to LB idle timeouts.
* **Auth**: bearer token (`Authorization: Bearer`) preferred; username/password
  session login supported (`/services/auth/login`, re-login on 401). TLS verified
  by default with `--ca-bundle`; `--insecure` for labs.
* **Dispatch disk** is the real ceiling for job mode. The formula in 2.3 is
  checked up front; export mode is the answer when it does not fit.
* **Retention roll-off during a long run**: oldest-first ordering plus the pin.
  Frozen buckets cannot be recovered; the `plan` validation will flag the chunk.
* **Peers dropping out**: strict mode treats any `WARN`/`ERROR` job message as a
  chunk failure. The `plan` count check is the second line of defence when a peer
  fails without a message.

---

## 4. Implementation plan

Python 3.13, `uv` project, dependencies kept small: `httpx` (streaming, timeouts,
connection pooling), stdlib `sqlite3`, `gzip`, `json`, `argparse`. No Splunk SDK:
the thin client is ~200 lines and keeps full control over paging and streaming.

```
splunk_rest_extractor/
  cli.py          plan | run | validate | status | compact
  client.py       auth, jobs (create/poll/results/delete), export stream, timeparser, conf-limits
  spl.py          SPL validation and guardrails
  timerange.py    parse inputs, resolve relative time, day alignment, bisect
  planner.py      histogram pass, refinement, packing into chunks
  executor.py     worker pool, per-chunk state machine (job mode / export mode), retries
  writer.py       NDJSON/raw gzip writer, .part + atomic rename, sha256, utf-8 policy
  state.py        sqlite manifest
  validate.py     job/plan/total/full checks, report generation
tests/
  unit/           spl guardrails, packing, bisect, boundary maths
  integration/    against the local instance (opt-in via env)
```

Milestones:

1. Client + SPL guardrails + time resolution. Integration test reproduces facts
   1, 2, 4, 5, 6 as assertions so a future Splunk version that changes them fails
   loudly.
2. Job-mode executor + writer + manifest. `run` end-to-end with fixed-span chunks.
3. Planner (histogram + refinement + packing), `plan` validation, `status`.
4. Export mode and hot-second fallback.
5. `validate` (total/full/sample), `compact`, `report.md`.

Test datasets on this box:

| Index | Events | Why it is useful |
|---|---|---|
| `botsv3` | 2,083,056 (search-time) / 1,944,092 (scan) | Static. Nearly all events on one day (2018-08-20) so sub-day splitting is exercised. Contains invalid UTF-8 and search-time-multiplied events (facts 7, 8, 11). > `max_count`. |
| `_audit` | 6.6M over 2.7 years | Day-based planning over a long range, > `max_count`. |
| `_internal` | ~575k, live | Pinning (fact 6), resume while data is arriving. |

Chaos tests: kill the process mid-page and `--resume`; run with
`--chunk-target-events 20000` to force hundreds of chunks; set `max_count` low to
force overflow/split; break a lookup mid-SPL to verify job-mode error handling.

---

## 5. Decisions on the open questions (2026-09-02)

1. Output default: NDJSON with metadata. `--format raw` available.
2. Day boundaries: UTC. `--tz` available.
3. `--chunk-target-events` default 250,000; job `max_count` is 2× that.
4. Histogram planning on by default; `--no-histogram` for production runs where
   the extra scan is not acceptable.
5. Auth: bearer token (`SPLUNK_TOKEN`) is the primary path; username/password
   session login is the fallback.

Simplification for v1: a chunk interrupted by a process crash restarts from the
beginning on resume (the `.part` file is discarded). Transient HTTP errors are
still retried per page without restarting the chunk.

---

## 6. Implementation status (2026-09-02)

Everything in sections 2–4 is implemented in `splunk_rest_extractor/` and exercised
against the local instance. See README.md for usage.

| Test | Result |
|---|---|
| `botsv3`, one day, 11 planned chunks, job mode, `--validate total` | 2,083,055 rows in 62 s incl. planning (~48k rows/s, 3 workers); every check OK; independent count identical |
| Same range with `--no-histogram --max-count 100000` (forced overflow) | 36 bisections, 38 leaf chunks, total identical |
| One hour in `--mode export` | 443,808 rows, total identical |
| `_audit`, 2.75 years, 1,005 day chunks | 6,615,382 rows in 2.5 min, total identical |
| SIGTERM at 40 s into a 457-chunk run, then re-run | 99 done / 358 pending at interrupt, no `.part` files left, resume completed, total identical |
| `validate --level full --sample 4` on a fresh run | files intact, all 10 chunks recount identically, 4/4 re-extracted chunks byte-identical after sorting |
| `compact` | 4 chunk files → one day file, gzip integrity OK, line count preserved |
| `tests/integration/test_splunk_facts.py` | 12 tests pin facts 1–12 of section 1 against the live instance; all pass on 9.4.8 |

Things learned while building that were not in the original design:

* `/results` and `/export` format `_time` differently by default
  (`2018-08-20T09:00:59.995-04:00` vs `2018-08-20 09:00:59.995 EDT`). Both accept
  `output_time_format`; the tool pins `%Y-%m-%dT%H:%M:%S.%3N%:z` on both so a
  re-extraction through export is comparable with job-mode files.
* Counting U+FFFD in decoded text over-reports invalid UTF-8: botsv3's `stream:udp`
  events already contain U+FFFD from index time. The tool now counts only what the
  decoder inserted (`len(replace) − len(ignore)`).
* Epochs ≥ 2³¹ are rejected by `latest_time`; the tool only ever sends the
  resolved range, but tests must not use `2**31` as "forever".
* Overflow bisection is expensive in the worst case (every level costs a job that
  scans up to `max_count` before stopping). The histogram plan avoids it almost
  entirely; `--no-histogram` should be paired with a `--span` that is known to be
  small enough.
* SIGINT is ignored for background jobs in non-interactive shells; the CLI handles
  SIGTERM identically (finish the current page, requeue, exit 130).
* Whether the export stream arrives gzip-encoded depends on the server, not the
  client: this instance never sets `Content-Encoding` even when the client
  advertises gzip, while another 9.4.8 instance did. The export reader therefore
  goes through httpx's decoding layer (`iter_bytes`), which handles both.

Independent review pass (2026-09-02, second agent + own read-through) found and fixed:

* `Planner.pack` dropped bin-less time before a hot bin, so the plan did not always
  tile the range. Fixed; a 500-case fuzz test now asserts tiling and count
  conservation for every plan.
* Validation never checked that leaf chunks cover `[earliest, latest)`; a manifest
  with a hole reported OK. A `coverage` check now runs at every level.
* `ChunkWriter` fsynced before `GzipFile.close()` wrote the trailer. The raw file
  handle is now closed after the gzip stream and fsynced before the rename.
* Run creation and chunk splitting were two autocommit statements each; both are
  now single transactions, and splitting is idempotent on the parent.
* A mismatch-retry or permanent failure left the previous attempt's file on disk,
  and `compact` trusted the directory listing. Files are removed on those paths,
  and `compact` is driven by the manifest, refuses incomplete runs, verifies line
  counts, and records the day file path so `validate --level full` keeps working.
* Resume never retried `failed`/`mismatch` chunks; it now requeues them with a
  fresh attempt budget, deletes lingering jobs, and removes `.part` files.
* Oracle searches (`| stats count`, histogram) were not checked for truncation or
  short paging; they now raise on either.
* Legacy time options (`starttime=`, `endtimeu=`, `searchtimespan*`) are rejected
  alongside `earliest`/`latest`.
* Transforming SPL with the default `--fields` lost columns; the default is now
  "all fields" when the SPL contains a whole-set command.
* Planner-hot intervals went to export mode even when a job could hold them; only
  intervals larger than `max_count` do now.
* Resume takes every option except `--workers`/`--max-attempts` from the manifest.
* A lock file prevents two runs on the same `--out`.
* `--format raw` with multi-line events failed `files-intact`; physical line
  counts are now tracked and verified.

Known limitations / next steps:

* A chunk interrupted mid-page restarts from scratch on resume (the job is deleted).
  Keeping the job alive across a restart would need the sid to be re-validated.
* A results page in flight (up to the read timeout) cannot be interrupted; a
  stop request takes effect at the next page boundary.
* Histogram bins that straddle a non-UTC day boundary (half-hour time zones) lose
  their expected count; validation for those chunks falls back to `total`.
* `index_latest` on generating commands (`| tstats`) is not covered by the
  integration tests.
* No throttling based on live server load beyond back-off on 429/503.
* Windows: verified on Windows 11 ARM64 against Splunk 9.4.8 (28 unit, 12 integration
  tests). Needs `tzdata` there (no system zoneinfo) and UTF-8 stdout reconfiguration.
