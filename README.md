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

# 2. get the code and its dependencies (uv installs a Python 3.11+ for you if needed)
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
uv run splunk-extract head --out runs/internal-yesterday -n 3
```

`--insecure` skips TLS verification for a self-signed lab certificate. Drop it and
use `--ca-bundle /path/to/ca.pem` against a real search head.

To get a bearer token from Splunk (Settings → Tokens in the UI, or over REST):

```bash
curl -sk -u admin https://127.0.0.1:8089/services/authorization/tokens \
  -d name=admin -d audience=splunk_rest_extractor \
  -d expires_on="$(date -v+30d +%Y-%m-%dT%H:%M:%S%z)" -d output_mode=json | python3 -m json.tool
```

## Quick start (Windows, PowerShell)

Exercised end to end on Windows 11 (ARM64, Windows PowerShell 5.1, uv-managed
Python 3.14) against a Splunk Enterprise 9.4.8 lab instance: `plan`, `run` in job
and export mode, `validate --level full --sample`, `compact`, `head`, and the full
integration suite. Overflow bisection down to the export fallback, resume after
Ctrl-C and after a hard kill, and the per-directory run lock were exercised on
Windows against a REST simulator of the same endpoints.

```powershell
# 1. prerequisites: Git and uv. `--source winget` skips the Microsoft Store source and its
#    terms prompt; uv also pulls in the VC++ runtime it needs. Then close and reopen
#    PowerShell: uv.exe lands in %LOCALAPPDATA%\Microsoft\WinGet\Links, which is only on
#    the PATH of new shells.
winget install --id Git.Git -e --source winget
winget install --id astral-sh.uv -e --source winget

# 2. get the code and its dependencies. On a private repo Git Credential Manager opens a
#    browser sign-in. uv downloads a Python if none is installed (the newest 3.x; on an
#    ARM64 machine it is the x86-64 build, which runs fine under emulation).
git clone https://github.com/jagalliers/splunk_rest_extractor.git
cd splunk_rest_extractor
uv sync

# 3. credentials: copy the template, fill in SPLUNK_URL plus a token or user/password,
#    then load it into this PowerShell session (blank values are treated as unset)
Copy-Item .env.example .env
notepad .env
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*([^#][^=]*)=(.*)$') { Set-Item -Path "Env:$($matches[1].Trim())" -Value $matches[2].Trim() }
}

# 4. sanity check: how would a range be chunked?  Quote the relative times: an unquoted
#    '@d' is splatting syntax to PowerShell and the argument silently disappears.
uv run splunk-extract plan --insecure --spl 'index=_internal' --earliest '-1d@d' --latest '@d'

# 5. extract it
uv run splunk-extract run --insecure --spl 'index=_internal' --earliest '-1d@d' --latest '@d' `
    --out runs\internal-yesterday --validate total

# 6. look at the result. Windows PowerShell 5.1 reads files as ANSI unless told otherwise,
#    so ask for UTF-8 or the report's dashes and check marks come out as mojibake.
uv run splunk-extract status --out runs\internal-yesterday
Get-Content runs\internal-yesterday\report.md -Encoding UTF8
uv run splunk-extract head --out runs\internal-yesterday -n 3
```

Windows notes:

* Progress and log lines go to stderr and to `run.log` in the output directory.
  Windows PowerShell 5.1 renders redirected stderr (`2>&1`) as red
  `NativeCommandError` records; that is PowerShell, not a failure. Read `run.log`.
* `head` output and the printed report are UTF-8 even when redirected to a file or
  piped, whatever the console code page. Read them back with
  `Get-Content -Encoding UTF8`.
* Non-UTC `--tz` values need the `tzdata` package (Windows has no system zoneinfo
  database); the project depends on it on Windows, so `uv sync` brings it in.
* Stop a run with Ctrl-C and re-run the same command to resume. A run killed
  outright (closed window, `taskkill`) resumes the same way: interrupted chunks are
  re-queued and leftover `.part` files are removed.
* One run per output directory: a second `run` on the same `--out` exits with
  "another splunk-extract run is active".

To mint a bearer token from PowerShell (use `curl.exe`; plain `curl` is an alias
for `Invoke-WebRequest` in Windows PowerShell 5):

```powershell
$exp = (Get-Date).AddDays(30).ToString('yyyy-MM-ddTHH:mm:sszz') + '00'
curl.exe -sk -u admin https://127.0.0.1:8089/services/authorization/tokens `
  -d name=admin -d audience=splunk_rest_extractor -d "expires_on=$exp" -d output_mode=json
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
splunk-extract head --out runs/web-aug -n 5

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
