# job-aggregator — Consumer Feedback Notes

**Living document.** Captures observations and improvement ideas about the upstream [`job-aggregator`](https://github.com/cbeaulieu-gt/job-aggregator) tool from career-ops's perspective as a consumer (Level 4 scan source).

The intent: keep a running list as career-ops keeps using the tool, then translate the highest-leverage items into upstream issues / PRs when the time is right. **Do not delete entries when they're addressed** — strike them through and add a "✅ Fixed in vX.Y" note for institutional memory.

**Last updated:** 2026-04-25 (initial entries from first production scan after PR #6/#7 merged the per-level title-filter override)

---

## How to use this doc

When career-ops runs `node scan-api.mjs` (or evaluates the records it produced) and you spot:

- A data-quality gap (missing field, wrong type, stale URL)
- An ergonomic friction point (CLI flag missing, behavior surprising)
- A schema gap (something we wish was returned that isn't)
- A retry / failure-mode oddity

…add a numbered entry below in the appropriate tier. Lead with the **observation** (what you saw), then the **proposal** (what we wish it did). Cite the run date so we can correlate with `data/scan-history.tsv`.

When ready to act on an item, file an issue on `cbeaulieu-gt/job-aggregator` and link it back here.

---

## Tier 1 — Highest leverage (data quality at source)

### 1. URL liveness verification during `hydrate`

**Observed:** 2026-04-25. Meta job arrived as a fresh record (`jobicy.com/jobs/141185-software-engineer-systems`), but the URL returned HTTP 410 + the canonical `metacareers.com` job ID 404'd. The aggregator surfaced a record that couldn't be acted on. Career-ops's dedup saved us this time (existing #100 was higher-scored), but a fresh role at a new company with a stale URL would have wasted an evaluator.

**Proposal:** Optional `--verify-liveness` flag on `hydrate` that issues a HEAD against `record.url` and surfaces records as `liveness: active|stale|unknown`. Consumer filters post-hoc.

**Impact:** Eliminates a class of false-positive records before they reach the evaluation stage.

### 2. Surface `accepts_query` per source in JSONL envelope

**Observed:** 2026-04-25. The stderr message `accepts_query=never, partial` shows that `jobicy`, `remoteok`, and `the_muse` ignored the `--query "software engineer"` parameter. They returned everything-remote regardless. The orchestrator has no programmatic way to know this — only stderr.

**Proposal:** In the envelope (`schema_version` + `jobs[]`), add a `query_behavior` map: `{ "jobicy": "ignored", "jooble": "honored", ... }`. Consumers can downweight or surface a warning when sources didn't honor the query.

**Impact:** Direct visibility into why a source returned what it returned.

### 3. Per-source rolling `source_quality_score`

**Observed:** Across the first production run, sources had wildly different signal-to-noise ratios:

| Source | Records returned | Made it past dedup | Notes |
|---|---|---|---|
| `remoteok` | 3 | 3 (100%) | High signal |
| `jobicy` | 5 | 4 (80%) | Snippets only, 1 stale URL (Meta 410) |
| `jooble` | ~80 | <handful> | Most missing `posted_at`, `source_id` empty |
| `jsearch` | small | clean | Worked well |
| `the_muse` | small | clean | Low volume but coherent |
| `adzuna` | 0 | 0 | HTTP 503 (transient) |

**Proposal:** Compute a rolling per-source score (records that passed the consumer's dedup + filters ÷ records returned, over the last N runs) and surface it in the envelope. Could even be opt-in via `--track-quality config.json`.

**Impact:** Lets the consumer make per-source decisions ("disable jooble for the next 7 days, signal too low").

---

## Tier 2 — Schema completeness

### 4. Date hygiene — `jooble` returns null `posted_at` / `created_at`

**Observed:** 2026-04-25. ~30 stderr warnings (one per record):

```
WARNING: record source='jooble' source_id='' has no parseable posted_at or created_at; posted_at will be null.
```

These records are unusable for any `--hours N` lookback filter. They get returned anyway (silent inclusion).

**Proposal:** Either (a) drop date-less records when `--hours N` is set, or (b) surface a `dropped_dateless: N` count in the envelope. Currently neither happens — they just leak through with `posted_at: null`.

**Impact:** Makes the `--hours` filter actually authoritative.

### 5. Empty `source_id` in `jooble` records

**Observed:** 2026-04-25. Every `jooble` warning shows `source_id=''`. Without a stable per-source ID, the aggregator can't dedup `jooble`-vs-`jooble` results across runs, and consumers can't either.

**Proposal:** Synthesize a stable `source_id` from URL hash (or title+company hash) when source-native ID is missing. Document this in the schema as "synthesized" so consumers don't assume it round-trips back to the source.

### 6. Salary extraction is uneven

**Observed:** `salary_min` / `salary_max` is populated by some sources (Adzuna typically does) but rarely by `remoteok`, `jobicy`, etc. JD bodies usually contain salary text — even regex-based extraction (`\$\d+K?\s*[-–]\s*\$?\d+K?` covers most US postings) would help downstream tools.

**Proposal:** Add an opt-in `--extract-salary` post-processor on `hydrate` that runs the regex against the description field and fills `salary_min` / `salary_max` when missing. Mark synthesized values with `salary_source: "extracted"` vs. `"native"`.

**Impact:** Career-ops Block C (Comp scoring) currently has to re-parse the JD body itself.

### 7. `remote_eligible` is sometimes `null`

**Observed:** Records arrive with `remote_eligible: null` even when the JD body explicitly says "Fully Remote – US." Career-ops uses `remote_eligible: false` as a hard filter, but `null` is treated as pass-through (per [scan-api.mjs:390](../scan-api.mjs)). So records with ambiguous-but-actually-remote signals leak through.

**Proposal:** When source data is missing this field, run a simple text classifier on `description` (`"remote"|"fully remote"|"hybrid"|"on-site"|"in-office"`) and fill the gap. Similar `remote_source: "extracted"|"native"` mark for transparency.

---

## Tier 3 — CLI ergonomics

### 8. `--limit-per-source N` flag

**Observed:** 2026-04-25. `jooble` returned ~80 of 129 records this run, dominating the result set. A `--limit-per-source 30` would give consumers a more diverse sample without disabling sources.

**Proposal:** `--limit-per-source N` (default unlimited) caps per-source contribution.

### 9. `--exclude-body-keyword` flag

**Observed:** 2026-04-25. Career-ops needs to filter out Rails-primary shops (3 of 8 surviving records this run were Rails — Tines, Chime ×2, Mighty Networks). Title filter doesn't catch them because the title says "Software Engineer" — the disqualifier is in the JD body. Aggregator could pre-filter on body keywords before returning to the consumer.

**Proposal:** `--exclude-body-keyword "Ruby on Rails,Rails,Ruby/Elixir"` (comma-separated). Drop records whose hydrated description contains any listed keyword.

**Impact:** Cuts evaluator burn rate further. Most useful for stack-mismatch filtering that the title doesn't expose.

### 10. `--retry N` for transient API failures

**Observed:** 2026-04-25. `adzuna` failed with HTTP 503 (transient Adzuna outage). The entire source dropped from the result set with no retry. A re-run 5 min later might have succeeded.

**Proposal:** `--retry N` (default 1) with exponential backoff (1s, 2s, 4s) for 5xx. Don't retry on 4xx.

### 11. Distinguish auth failures from server failures in error message

**Observed:** When `adzuna` failed, the message was `HTTP 503: Scrape failed for 'https://api.adzuna.com/...'`. Hard to tell whether this is an auth issue (revoke key + re-issue) vs. a real Adzuna outage (wait it out). The status code 503 strongly suggests upstream, but the wording doesn't make this distinction explicit.

**Proposal:** When the response status is 401/403, prefix the error with `AUTH ERROR (check credentials):`. For 5xx, prefix with `UPSTREAM ERROR (try later):`. For 4xx other than auth, prefix with `CLIENT ERROR:`.

### 12. Fold `hydrate` into `jobs` as `--hydrate-full`

**Observed:** Currently `scan-api.mjs` has to spawn `jobs` and pipe its output into a separate `hydrate` subprocess (see [scan-api.mjs:194-220](../scan-api.mjs)). It works but it's two processes for one logical operation.

**Proposal:** `--hydrate-full` flag on `jobs` runs hydration internally and emits hydrated records directly. Keep standalone `hydrate` for power users.

**Impact:** Simpler consumer code; one fewer pipe to manage.

---

## Tier 4 — Discovery & ops

### 13. `job-aggregator init <source>` — guided source setup

**Observed:** Adding USAJobs (currently in-config but not in `sources` list) requires:
1. Reading the docs to know required fields
2. Editing `creds.json` manually
3. Adding to `portals.yml` `sources:` list
4. Hoping you didn't typo

A typo silently degrades the source to "not used" without an error.

**Proposal:** `job-aggregator init usajobs` prompts for required fields, validates them with a 1-record test query, writes to `creds.json`, and prints the YAML snippet to add to the config.

### 14. `job-aggregator doctor` — diagnostic subcommand

**Proposal:** Pings each enabled source with a 1-record query and reports per-source status:

```
✓ remoteok      OK (2 records, 187ms)
✓ jooble        OK (1 record, 421ms)
✗ adzuna        HTTP 503 — upstream outage, retry later
✗ usajobs       AUTH ERROR — credentials missing or invalid
✓ jsearch       OK (1 record, 612ms)
```

**Impact:** Single command to triage scan failures.

### 15. Cross-source deduplication

**Observed:** `remoteok` and `jobicy` often both index the same Patreon listing with different URLs. Currently the consumer has to dedup post-hoc by `(company, title)`. Aggregator could do this upstream.

**Proposal:** `--dedup-strategy strict|loose|off` (default `loose`):
- `strict` — exact URL match
- `loose` — `(company.lower(), title.lower())` match, prefer the earliest `posted_at`
- `off` — return all records as-is

---

## Already-resolved items

*(none yet — strike through above when items get fixed upstream)*

---

## Use cases this would unlock

These are the consumer workflows that get easier or possible:

| Improvement | Career-ops workflow it unlocks |
|---|---|
| URL liveness | Skip stale aggregator entries before evaluation |
| `accepts_query` envelope | Auto-disable sources that ignore `--query` for the run's keyword set |
| Source quality score | Auto-rotate the `sources:` list weekly based on signal |
| Salary extraction | Block C (Comp) gets accurate floor/ceiling without re-parsing |
| Body keyword exclude | Stack-mismatch pre-screen at the aggregator level (saves evaluator tokens) |
| `--limit-per-source` | Diversity guarantee when one source dominates volume |
| `doctor` subcommand | Single-command triage in pre-scan health check |
