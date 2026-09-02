# splunk-extract

Pull **every** event an SPL search returns for a time range out of Splunk over the
REST management port (8089) into date-named, gzip-compressed flat files, with
validation that nothing was dropped. Built for result sets far larger than a
single search job can hold. Design, measured Splunk behaviours, and rationale are
in [DESIGN.md](DESIGN.md).

## Quick start (macOS)

```bash
# 1. uv (Python project manager; installs Python 3.13 for you if needed)
brew install uv

# 2. get the code and its one dependency
git clone https://github.com/jagalliers/splunk_rest_extractor.git
cd splunk_rest_extractor
uv sync

# 3. credentials: copy the template and fill in SPLUNK_URL plus a token or user/password
cp .env.example .env
$EDITOR .env
set -a && source .env && set +a       # export the variables for this shell

# 4. sanity check: how would a range be chunked?
uv run splunk-extract plan --insecure --spl 'index=_internal' --earliest -1d@d --latest @d

# 5. extract it
uv run splunk-extract run --insecure --spl 'index=_internal' --earliest -1d@d --latest @d \
    --out runs/internal-yesterday --validate total

# 6. look at the result
uv run splunk-extract status --out runs/internal-yesterday
cat runs/internal-yesterday/report.md
gzcat runs/internal-yesterday/data/*/*.jsonl.gz | head -3
```

`--insecure` skips TLS verification for a self-signed lab certificate. Drop it and
use `--ca-bundle /path/to/ca.pem` against a real search head.

To get a bearer token from Splunk (Settings → Tokens in the UI, or over REST):

```bash
curl -sk -u admin https://127.0.0.1:8089/services/authorization/tokens \
  -d name=admin -d audience=splunk_rest_extractor \
  -d expires_on="$(date -v+30d +%Y-%m-%dT%H:%M:%S%z)" -d output_mode=json | python3 -m json.tool
```

## Install (any platform)

```
uv sync
```

Python 3.11+, one dependency (`httpx`).

## Usage

```
# dry run: show how the range will be chunked and how many events each chunk holds
splunk-extract plan --spl 'index=web sourcetype=access_combined' \
    --earliest 2026-08-01T00:00:00 --latest 2026-09-01T00:00:00

# extract (re-run the identical command to resume an interrupted run)
splunk-extract run --spl 'index=web sourcetype=access_combined' \
    --earliest 2026-08-01T00:00:00 --latest 2026-09-01T00:00:00 \
    --out runs/web-aug --workers 3

# progress / problems
splunk-extract status --out runs/web-aug

# deeper validation any time later (re-reads files, recounts every chunk, re-extracts a sample)
splunk-extract validate --out runs/web-aug --level full --sample 5

# one file per day instead of one per chunk (gzip members are concatenated, not recompressed)
splunk-extract compact --out runs/web-aug --delete-parts
```

Time inputs accept epoch seconds, ISO-8601 (naive = UTC), or Splunk relative
expressions such as `-30d@d`. The range is half-open: `earliest <= _time < latest`.

**Do not put `earliest=`/`latest=` inside the SPL.** Inline modifiers override the
REST parameters and would make every chunk search the same window; the tool
refuses such SPL. The SPL must also be *time-partitionable*: it is run once per
chunk, so commands that look at the whole result set (`head`, `dedup`, `sort`,
`stats` …) produce warnings.

## Output

```
runs/web-aug/
  manifest.sqlite       state: run, chunks, evidence per chunk (counts, sid, sha256, messages)
  run.log
  report.md / .json     validation report
  data/
    2026-08-01/1754006400-1754092800.jsonl.gz     one file per chunk: [start,end) epoch
    2026-08-02/1754092800-1754136000.jsonl.gz
    2026-08-02/1754136000-1754179200.jsonl.gz     busy days are split into several chunks
```

Each line is one event as JSON with, by default,
`_time, _raw, index, sourcetype, source, host, _indextime, _bkt, _cd`
(`--fields` to change, `--fields all` for everything Splunk returns,
`--format raw` for bare `_raw` lines). Files are written to `.part` and renamed
atomically on completion, so any file without `.part` is complete.

## How completeness is ensured

* Every search carries `index_latest=<run start>` so planning, extraction, and
  validation see the same snapshot even on live indexes (`--no-pin` to disable).
* Planning runs one histogram search (`| bin _time | stats count`) and packs bins
  into day-aligned chunks of at most `--chunk-target-events` (default 250k). The
  per-chunk expected count is an independent oracle for validation.
* Each chunk is a search job with an explicit `max_count`; results are paged at
  `restapi.maxresultrows`. A chunk whose job reports `eventIsTruncated` is
  bisected and re-queued; a single second that still overflows is fetched via the
  streaming export endpoint instead.
* Job failures, auto-finalization, and ERROR/WARN messages (e.g. a search peer
  dropping out) fail the chunk; it is retried up to `--max-attempts` times.
* `--validate` levels: `job` (stored job evidence), `plan` (default: written ==
  planned), `total` (one extra `| stats count` over the whole range), `full`
  (re-read files, recount every chunk, `--sample N` re-extracts and diffs).

## Sizing for production

* `--workers`: stay well under the search head's concurrent-search limit and the
  role's `srchJobsQuota`; the tool prints both when they are readable.
* Dispatch disk: a job holds about 0.7 KB per event; `workers × max_count × 0.7 KB`
  must fit in the role's `srchDiskQuota`. Lower `--chunk-target-events` or use
  `--mode export` if it does not.
* `--no-histogram` skips the planning scan (chunks become fixed `--span` windows,
  bisected on overflow); pair it with `--validate total`.
* Point the tool at one search head, not a load balancer: job ids live on the
  member that created them.
* Late-arriving data indexed after the run started is excluded by the pin. Run
  again later with the same range into a new `--out` to pick it up (see DESIGN.md
  §2.9).

## Tests

```
uv run pytest tests/unit                 # no Splunk needed
uv run pytest tests/integration -v       # needs SPLUNK_* in the environment; pins the measured Splunk behaviours
```
