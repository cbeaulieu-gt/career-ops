# Earlier job screening — issue #66

## Intent and boundary

Reduce unnecessary full evaluations while preserving plausible matches and the user's current Apply thresholds. This implements the user-authorized issue #66, including the instruction to skip unknown/unconfirmed hiring sources. Historical evidence and acceptance criteria are preserved in https://github.com/cbeaulieu-gt/career-ops/issues/66.

## Existing flow

The scanner applies title, location, content and blacklist predicates before discovery output (`scan.mjs:3640-3700`). The batch runner reserves a report number before fetching JD content and dispatching a complete evaluation (`batch/batch-runner.sh:832-1040`). The modes specify a separate economy pre-screen (`modes/batch.md:26-29`, `modes/pipeline.md:21-35`). These gates must also cover imported URLs, which are not necessarily scanner output.

## Design

1. A pure `job-screening.mjs` policy module checks title exceptions, confirmed source policy, mandatory specialist requirements and structured location evidence. It never scores or generates candidate content. Discovery uses available provider fields; missing descriptions remain explicitly unchecked until admission.
2. A `screen-job.mjs` admission command resolves user files through `path-resolver.mjs:1-37`, reuses existing scanner filters and blacklist matching, and obtains a substantive JD using the existing known-ATS/browser extraction flow (`fetch-jd.mjs:28-53`, `browser-extract.mjs:827-880`). Metadata can reject a job before extraction. No full worker runs on missing content.
3. Standard/premium runs use a separate read-only economy worker with a short prompt containing primary candidate evidence, targeting and the JD. It returns only a validated JSON verdict and evidence. Economy runs retain deterministic gates without an extra model call, matching `modes/batch.md:26-29`. An explicit screening model is required for CLIs without a concrete economy mapping (`modes/_shared.md:59-71`). Never reuse an expensive evaluation model silently.
4. Batch admission precedes report reservation, evaluation and CV generation. Every attempt records a screening receipt with reason, phase and model. Fit/source/access rejections become skips; screening infrastructure errors remain failures. Malformed output cannot admit a job.
5. An explicit stretch option relaxes only fit checks. Source, liveness, blacklist and location/work-authorization constraints still apply. Agency holds are not retried by this feature.
6. Candidate-specific NVIDIA and source policy live in user-layer configuration, not shared defaults (`AGENTS.md:Data Contract`). A committed example documents the configuration without personal facts. Apply thresholds remain in `_profile.md` unchanged.

## Verification

Test metadata rejection before fetch/model dispatch; imported URL filtering; scoped employer exceptions; required versus preferred specialist skills; ambiguous location; missing/unconfirmed source; malformed model verdicts and infrastructure errors; no report reservation on rejection; and stretch override boundaries. Reuse real archived JDs in a read-only replay of all 99 jobs, compare with historical outcomes, and preserve an aggregate audit plus all eight historical Apply dispositions. Missing archived JDs are reported as unavailable replay evidence, never reconstructed from their final recommendation.

## Non-goals

No new live scan, paid evaluation batch, candidate-fact changes, score-threshold changes, application submissions or retroactive report/tracker edits. No target pass-rate guarantee. The implementation and validation are preserved in [PR #67](https://github.com/cbeaulieu-gt/career-ops/pull/67). Issue #66 closed when that PR merged; the completed execution plan is retired under the repository lifecycle rule.
