# Screening before full evaluation

Discovery uses the shared deterministic admission policy alongside existing title,
location, content, country/visa and blacklist filters. Imported/manual batch URLs
run admission too: passing a previous scan or liveness sweep is insufficient.

The runner first checks metadata, then extracts a substantive JD (at least 80
words), applies deterministic requirements/eligibility filters, and runs a separate
economy fit worker for standard/premium profiles. Only a shortlist reserves a
report number and starts the full evaluation/CV worker. Apply thresholds remain
in the candidate's targeting file.

## User configuration

Add `screening` to your personal `config/profile.yml`; the shipped example has
commented configuration. These are candidate-specific rules, not shared scoring
defaults:

```yaml
screening:
  unknown_source: skip      # skip or review; review is the default
  models:
    claude: haiku
    codex: YOUR_ECONOMY_MODEL
  reject_required: ["word:CUDA"]   # only unsupported mandatory skills
  supported_alternatives: ["word:Python", "word:C++"]
  agency_companies: []      # exact confirmed agency names, never guesses
  unconfirmed_urls: []      # exact postings with unknown/unconfirmed source
  excluded_urls: []         # exact postings explicitly excluded by the user
  title_exceptions:
    - companies: ["Example Employer"]
      positive: ["Software Engineer + GPU Developer Tools"]
      negative: ["GPU"]     # relax only this term; retain other negatives
```

Required/minimum/basic qualifications and common mandatory headings activate
requirement matching. Preferred/bonus/standout sections and explicit negations do
not. A supported language in an `or` clause defers the complex requirement to the
fit screener rather than treating the unsupported alternative as mandatory.
These rules are conservative heuristics; they do not replace reading the JD.

Never select unsupported specialist rules merely because a skill is absent from
a CV. Review the primary CV/targeting and intended roles first. Keep a requirement
list narrow enough that adjacent roles can still reach fit screening.

## Commands

```bash
# Model names are explicit; screening never inherits the full evaluation model.
bash batch/batch-runner.sh --cli codex --model FULL_MODEL --screen-model ECONOMY_MODEL

# Other evaluation CLIs can use Claude or Codex for the admission worker.
bash batch/batch-runner.sh --cli opencode --screen-cli claude --screen-model haiku

# Deliberate stretch-role evaluation relaxes fit only.
bash batch/batch-runner.sh --allow-stretch

# Inspect one posting; the default phase performs admission.
node screen-job.mjs --url https://example.com/job --notes "Example | Software Engineer" --cli codex --screen-model ECONOMY_MODEL

# Read-only per-job funnel; includes all input rows, even unstarted ones.
node batch/screening-summary.mjs --json

# Offline deterministic replay; no network, model requests or batch-state writes.
node scripts/replay-screening.mjs --root /path/to/historical/user-data
```

`--phase metadata` checks only known metadata. Its provisional shortlist cannot
authorize full evaluation. `--jd-file` reuses locally extracted JD text;
`--receipt` atomically writes a JSON admission receipt. `--profile` and `--portals`
on replay select proposed configuration without editing historical user files.

The runtime receipts live in ignored `batch/screening/`. They record phase,
model, verdict, supporting posting excerpts and a concise reason. Rejections
become `skipped`; source policy `review` and ambiguous eligibility become
`needs_confirmation`; infrastructure/invalid-output errors become `failed`.
Batch rejections also go to `batch/logs/discard.log`. Discovery's new policy
rejections go to `data/discard.log` and its existing filtered-content counter.
Neither receipt nor discard log is an evaluation report.

Economy profiles add no model call. Admission requires explicit
`hiring_source: direct` and `locationEligible: true` evidence; otherwise it holds
for review. Supply reviewed assertions with `--job-file`, or place them in
ignored `batch/screening-input/{id}.json` for the runner. Each record must match
the exact posting URL and include `provenance` (`user-confirmed` or
`provider-verified`), nonempty `source_evidence`/`location_evidence`,
`hiring_source`, and a boolean `locationEligible` (optional boolean
`workAuthorizationEligible`). Treat these files as trusted, reviewed upstream
metadata; never copy assertions from untrusted posting prose. Receipt output
preserves the reviewed evidence. This handoff does not override a configured
source skip, a blacklist or a requirement/eligibility restriction. Missing salary alone
never rejects a role. Missing JD, blacklist/source restrictions and eligibility
checks apply even with `--allow-stretch`. Held jobs are never automatically retried.

The screening worker runs in an isolated temporary directory. Claude excludes
settings sources, skills, persistence and tools/MCP; Codex ignores user
configuration and runs ephemeral/read-only. Models
receive candidate CV/targeting and serialized untrusted posting data, not the
full report/CV-generation instructions. Invalid JSON or invented evidence cannot
admit a job. No live model requests are made by regression tests or replay.

## Replay interpretation

Replay reads only input metadata and explicitly archived posting text for its
gates. Historical recommendations are joined afterward for comparison. A
survivor is `needs_fit_screening`, not a new Apply recommendation. Missing
archives are `missing_evidence`, not proof a posting was inaccessible at runtime.
The full-evaluations-avoided counter includes only historical completed rows
that deterministic gates would reject. The October batch audit is preserved in
[the replay report](screening-replay-2026-10-07.md).
