# splunk-extract

Pull **every** event an SPL search returns for a time range out of Splunk over the
REST management port (8089) into date-named, gzip-compressed flat files, with
validation that nothing was dropped. Built for result sets far larger than a
single search job can hold. Design, measured Splunk behaviours, and rationale are
in [DESIGN.md](DESIGN.md).

## Quick start

Install Git and [uv](https://docs.astral.sh/uv/). uv downloads a Python if none is
installed (3.11+ is required).

```
macOS    brew install uv
Windows  winget install --id Git.Git -e --source winget
         winget install --id astral-sh.uv -e --source winget
         (then open a new PowerShell: uv is only on the PATH of shells started after the install)
```

Everything below is the same in bash and PowerShell.

```
git clone https://github.com/jagalliers/splunk_rest_extractor.git
cd splunk_rest_extractor
uv sync

cp .env.example .env
# edit .env: SPLUNK_URL plus a token or username/password. The tool reads it from the current directory.

# sanity check: how would the range be chunked?
uv run splunk-extract plan --insecure --spl 'index=_internal' --earliest -24h --latest now

# extract it, with an independent count over the whole range at the end
uv run splunk-extract run --insecure --spl 'index=_internal' --earliest -24h --latest now --out runs/last-day --validate total

# look at the result
uv run splunk-extract status --out runs/last-day
uv run splunk-extract head --out runs/last-day -n 3
cat runs/last-day/report.md
```

`--insecure` skips TLS verification for a self-signed lab certificate. Drop it and
use `--ca-bundle /path/to/ca.pem` against a real search head. Cloning a private
repository prompts for a GitHub sign-in (on Windows, Git Credential Manager opens a
browser).

## Connection settings

Each setting comes from its flag, else the environment, else `.env` in the current
directory (`--env-file PATH` to read another file). Existing environment variables
win over the file and blank values are ignored.

| flag           | variable                                                      |
|----------------|---------------------------------------------------------------|
| `--url`        | `SPLUNK_URL` (default `https://127.0.0.1:8089`)               |
| `--token`      | `SPLUNK_TOKEN` (preferred)                                    |
| `--username`   | `SPLUNK_USERNAME` (or `SPLUNK_ADMIN_USER`)                    |
| `--password`   | `SPLUNK_PASSWORD` (or `SPLUNK_ADMIN_PASS`)                    |
| `--ca-bundle`  | `SPLUNK_CA_BUNDLE`                                            |

To get a bearer token, use Settings → Tokens in Splunk Web, or `POST
/services/authorization/tokens` with `name`, `audience` and `expires_on`.

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
splunk-extract head --out runs/web-aug -n 5

# deeper validation any time later (re-reads files, recounts every chunk, re-extracts a sample)
splunk-extract validate --out runs/web-aug --level full --sample 5

# one file per day instead of one per chunk (gzip members are concatenated, not recompressed)
splunk-extract compact --out runs/web-aug --delete-parts
```

Time inputs accept epoch seconds, ISO-8601 (naive = UTC), or Splunk relative
expressions such as `-30d@d`. The range is half-open: `earliest <= _time < latest`.
In PowerShell, quote values that contain `@` (`'-30d@d'`): a bare `@d` is splatting
syntax and the argument silently disappears.

Stop a run with Ctrl-C and re-run the same command to resume. A run killed outright
(closed window, `kill -9`, `taskkill`) resumes the same way: interrupted chunks are
re-queued and leftover `.part` files are removed. One run per output directory: a
second `run` on the same `--out` stops with "another splunk-extract run is active".

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
atomically on completion, so any file without `.part` is complete. Data files,
`run.log`, the report, and `head` output are UTF-8 on every platform.

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

## Errors and exit codes

Every failure ends with an `ERROR` line saying what went wrong, followed by a
`->` line saying what to do, in the same timestamped format as the rest of the
log. `run.log` in the output directory holds the same text. A traceback appears
only for a genuine bug, and then under a line that says so.

```
2026-09-03 12:30:27,486 INFO    MainThread limits: maxresultrows=50000 ...
2026-09-03 12:30:27,490 ERROR   MainThread runs/last-day already holds a run for a different SPL
2026-09-03 12:30:27,490 ERROR   MainThread    -> each run needs its own directory: pass a new --out, or re-run the original SPL to resume
```

| exit code | meaning |
|---|---|
| 0 | success (for `run`: every chunk done and validation passed) |
| 1 | failure: cannot connect or authenticate, Splunk rejected a request, a chunk failed permanently, or validation failed |
| 2 | usage: bad or missing argument, SPL the tool refuses, an unknown time zone, no credentials |
| 130 | interrupted (Ctrl-C or SIGTERM); re-run the same command to resume |

An interactive shell does not display the exit code. Read it right after the
command with `$LASTEXITCODE` in PowerShell or `$?` in bash and zsh.

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
uv run pytest tests/integration -v       # needs SPLUNK_* (environment or .env); pins the measured Splunk behaviours
```

## Platform notes

* Exercised on macOS and on Windows 11 (ARM64, Windows PowerShell 5.1, uv-managed
  Python 3.14) against Splunk Enterprise 9.4.8: every command, plus overflow
  bisection down to the export fallback, resume after Ctrl-C and after a hard kill,
  and the run lock against a REST simulator of the same endpoints.
* Windows PowerShell 5.1 renders a native command's redirected stderr (`2>&1`) as
  red `NativeCommandError` records. That is PowerShell, not a failure; progress and
  log lines go to stderr and to `run.log` in the output directory.
