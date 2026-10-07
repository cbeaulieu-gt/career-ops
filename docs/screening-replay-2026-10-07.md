# October batch admission replay — issue #66

This is an offline deterministic replay of the 99-job batch, recorded on
2026-10-07. The original batch totals and user instructions are preserved in
[issue #66](https://github.com/cbeaulieu-gt/career-ops/issues/66). The complete
per-job replay result is committed in [the audit JSON](screening-replay-2026-10-07.json).
Original operational files and candidate configuration remain private/runtime
data; they are not claimed as shipped artifacts.

## Result and limits

| Replay outcome | Jobs |
|---|---:|
| Discovered/input rows | 99 |
| Deterministically filtered | 18 |
| Known unconfirmed-source postings | 9 |
| Missing evidence after metadata gates | 28 |
| Survive deterministic checks; need economy fit screening | 44 |
| Historical completed full evaluations | 54 |
| Completed evaluations avoided by deterministic rules | 10 |

The disjoint outcomes total 99 (18 + 9 + 28 + 44). Forty-five inputs lack a
substantive archived JD, including 17 already rejected by metadata/source
rules. Missing archival evidence does not mean those postings were inaccessible
at runtime. Counts come from the committed audit JSON, `counts` and `jobs`.

The 10 avoided full evaluations are **18.5% of the 54 completed evaluations**.
Their batch IDs are **37, 39, 40, 41, 44, 46, 50, 61, 62, 63**. Nine historically
recommended Skip; one recommended Consider. All fail the proposed title policy
before fetching or dispatching a full evaluator. Source: audit JSON, those job IDs.
This measurement makes no claim about what a future economy model will reject,
how much money was spent, or a future Apply pass rate.

## All historical Apply recommendations

| Batch ID | Employer / role | Report | Deterministic disposition |
|---|---|---:|---|
| 1 | Microsoft — Sr. Software Engineering | 495 | Needs fit screening |
| 2 | Microsoft — Git Systems Engineering | 497 | Needs fit screening |
| 6 | GitHub — Data Engineering | 503 | Needs fit screening |
| 8 | GitHub — Billing | 505 | Needs fit screening |
| 12 | GitHub — Copilot Code Review | 509 | Needs fit screening |
| 13 | GitHub — Copilot Models Inference | 510 | Needs fit screening |
| 69 | Snowflake — Customer Experience Engineering | 569 | Needs fit screening |
| 84 | Close — Backend | 579 | Needs fit screening |

Source: audit JSON, `historical_apply`. No historical Apply is excluded by the
deterministic policy. Surviving checks are not a new recommendation or a source/
Florida eligibility confirmation: those judgments still belong to the admission
worker. Historical scores and recommendations were not inputs to these gates
(`scripts/replay-screening.mjs`, `replayScreening`; regression coverage in
`tests/screening-replay.test.mjs`).

## Criteria reviewed and applied

- Keep the current target archetypes, seniority exclusions, Florida/remote
  location policy, blacklist and country/visa rules. Keep every Apply cutoff,
  including preferred-employer overrides. Source: issue #66, Evidence/Constraints.
- Remove the blanket NVIDIA GPU/CUDA/HPC/AI Infrastructure relaxation. Relax
  only the GPU title term for `Software Engineer + GPU Developer Tools` and
  `Data Engineer + GPU Platform`; keep other global negatives. Tighten NVIDIA
  discovery queries toward backend/data/analytics/developer tools and exclude
  CUDA/HPC/research. Source: issue #66, NVIDIA finding/acceptance criteria;
  audit JSON IDs 37–63 demonstrate the observed title exclusions.
- Reject explicitly mandatory unsupported CUDA, Rust, OpenGL, Vulkan, GPU-kernel,
  kernel-driver and InfiniBand requirements. Retain preferred/standout skills
  and supported language alternatives. These proposed terms were reviewed
  against the current primary CV/targeting, which list application/backend,
  data, C#/Python/C++/SQL and deployment experience rather than those specialist
  requirements. This is a targeting policy, not a claim the candidate cannot
  learn a technology. Source: issue #66, unsupported-specialist constraint;
  implementation behavior verified by `tests/job-screening.test.mjs`.
- Keep job 68 explicitly excluded and nine known unconfirmed postings excluded
  by exact URL. Do not blacklist their entire companies or infer an agency
  relationship from an aggregator domain. Source: issue #66 and audit JSON IDs
  68, 73, 74, 75, 78, 82, 87, 92, 97, 98.

The reviewed policy was applied to the user's ignored configuration/portal files
on 2026-10-07; original copies were retained privately. The new admission
enforcement takes effect when the implementing PR is merged.
Reusable code and the configuration example contain no candidate-specific
defaults. The audit is a recorded historical result, not executable targeting.
