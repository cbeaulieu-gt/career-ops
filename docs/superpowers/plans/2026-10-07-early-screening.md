# Earlier job screening implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task in this session. Steps use checkbox syntax for tracking.

**Goal:** Filter unsuitable jobs before expensive evaluation workers, without weakening Apply criteria.

**Architecture:** Shared pure screening policy feeds discovery and a separate batch admission command. The runner records admission outcomes before reserving report numbers. A compact economy worker handles evidence-based fit judgments after deterministic gates.

**Tech Stack:** Node.js ESM, existing js-yaml, Bash orchestration, existing ATS/browser extraction.

**Spec:** `docs/superpowers/specs/2026-10-07-early-screening-design.md`.

## Global Constraints

- User-specific configuration stays in user-layer files; primary facts and Apply thresholds stay unchanged (issue #66; `AGENTS.md:Data Contract`).
- Shell argument arrays and validated JSON; no job text is executed as instructions (`AGENTS.md:Untrusted External Content`).
- Preserve Windows CRLF and the existing state/lock contract (`batch/batch-runner.sh:250-340`).
- No live paid evaluation is needed for replay; missing historical evidence must be disclosed (spec, Verification).

## Review Focus

- Missing metadata must not be inferred into eligibility.
- A preferred skill must not trigger mandatory-experience rejection.
- NVIDIA exceptions must not relax unrelated negatives or other employers.
- An expensive CLI default must not silently become the screening model.
- Stretch overrides must not bypass source/eligibility restrictions or retry holds.

## Task 1 — shared policy and admission command

**Files:** `job-screening.mjs`, `screen-job.mjs`, `tests/job-screening.test.mjs`, `tests/screen-job.test.mjs`.

**Interfaces:** `screenMetadata(job, options)` returns `{ decision, reason, evidence }`; `screenAdmission(job, context, dependencies)` additionally returns JD and screening model metadata. Verdicts are `shortlist`, `filtered`, `source_unconfirmed`, `inaccessible`, `needs_review`, or `error`.

- [x] Write failing tests for required/preferred requirements, scoped exceptions, location ambiguity, source confirmation and malformed worker JSON. Use `assert.equal(screenMetadata(job, options).decision, 'filtered')` and injected fetch/model spies for admission ordering.
- [x] Run `node --test tests/job-screening.test.mjs tests/screen-job.test.mjs`; confirm missing implementations fail.
- [x] Implement the pure predicates and compact read-only admission command using existing filtering/extraction helpers; validate schemas and models before dispatch.
- [x] Run the tests, including no fetch/model call after metadata rejection and no full-context/report/PDF prompt in the screening worker.
- [x] Commit this independently tested gate.

## Task 2 — enforce admission in batch and discovery

**Files:** `batch/batch-runner.sh`, `scan.mjs`, `scan-ats-full.mjs`, `tests/batch-screening.test.mjs`, relevant existing batch tests.

**Interfaces:** Node command emits an admission receipt; batch maps screening outcomes to existing state statuses before `reserve_report_num_retrying`. Discovery consumes the same pure policy.

- [x] Write a failing fixture test whose screening rejection makes the fake evaluation CLI and report reservation fail if called.
- [x] Implement two-phase screening around existing prefetch; retain state locks and expose fit-only stretch and explicit screening-model arguments.
- [x] Test malformed receipts, infrastructure failure, source skips and CLI model routing; keep legacy dispatch tests passing.
- [x] Run the related discovery/batch tests and commit integration.

## Task 3 — targeting, replay and documentation

**Files:** configuration example, README/mode instructions, `scripts/replay-screening.mjs`, `tests/screening-replay.test.mjs`, durable aggregate replay report under `docs/`.

- [x] Add tests that replay uses pre-evaluation metadata/JDs and never uses a historical recommendation as screening input.
- [x] Implement offline replay with missing-evidence counters and explicit dispositions for historical Apply jobs. Document configuration and command usage in README and modes.
- [x] Stage tightened user-layer criteria separately from reusable code and validate against the main checkout's historical batch, read-only.
- [x] Run `node test-all.mjs` with Git Bash available, syntax and diff checks. Audit every committed file reference against `git ls-tree HEAD` before publishing.
- [x] Commit final changes and create a PR with `Closes #66`; attach it to this chat. Keep the plan until the issue closes, then extract durable rationale and remove it.

Implementation published as [PR #67](https://github.com/cbeaulieu-gt/career-ops/pull/67).
Validation: 10,711 checks passed, zero failed; 11 environment/pre-existing
warnings reviewed. Artifact persistence verified with `git ls-tree HEAD` for
all new modules, replay audit, specification and plan. The plan remains until
issue #66 closes.
