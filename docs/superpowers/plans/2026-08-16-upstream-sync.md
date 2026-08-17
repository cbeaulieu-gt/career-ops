# Upstream Synchronization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fork's tracked system layer with upstream while preserving and validating the user's local profiles, instructions, configuration, and job-search data.

**Architecture:** Merge upstream into the isolated issue branch for auditable ancestry, then make the staged system tree equal to upstream except for two ignore rules and the issue's design/plan documents. Keep user-owned files outside the commit; migrate only their procedural instructions into the upstream custom-instructions layer and validate configuration without printing personal values.

**Tech Stack:** Git, Node.js 18+, npm, PowerShell, GitHub MCP

**Spec:** `docs/superpowers/specs/2026-08-16-upstream-sync-design.md`

## Global Constraints

- Upstream owns application code, shared modes, templates, tests, documentation, workflows, and release metadata. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L12-L20`.
- User-owned profiles, instructions, configuration, trackers, reports, outputs, and interview artifacts must not enter a commit or be overwritten. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L14-L16`, `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L41-L45`.
- Fork API integrations, local application agents, obsolete integration docs, and fork PDF implementation changes are intentionally dropped. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L24-L29`.
- The only retained tracked deviations from upstream are ignore rules for the worktree directory and local profile variants, plus the issue's temporary design and plan documents. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L29` and #25.
- Do not invent missing profile values or change job-search preferences. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L31-L39`.
- Work only on `sync-upstream-issue-25` in `I:/career/career-ops/.worktrees/sync-upstream-issue-25`; never commit or push directly to `main`. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L41-L45`, `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L53-L55`.

---

### Task 1: Refresh and Pin the Integration Inputs

**Files:**
- Read: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md`
- Read: `.gitignore`

**Interfaces:**
- Consumes: clean issue branch at design commit `c098578`; remotes `origin` and `upstream`.
- Produces: verified local refs and a recorded upstream SHA for the merge.

- [ ] **Step 1: Confirm the isolated branch is clean**

```powershell
git status --short --branch
git branch --show-current
```

Expected: branch `sync-upstream-issue-25`; only this plan may be untracked before it is committed.

- [ ] **Step 2: Commit this plan before integration**

```powershell
git add docs/superpowers/plans/2026-08-16-upstream-sync.md
git commit -m "docs: add upstream sync implementation plan (#25)"
```

Expected: one documentation commit and a clean working tree.

- [ ] **Step 3: Refresh both remotes**

```powershell
git fetch origin
git fetch upstream
```

Expected: both fetches exit 0. If upstream moved beyond `22cbe88`, use the new `upstream/main` tip because the approved specification permits a verified newer tip. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L14-L16`.

- [ ] **Step 4: Reconfirm the branch base and capture the upstream SHA**

```powershell
git merge-base --is-ancestor main HEAD
git rev-parse main
git rev-parse origin/main
git rev-parse upstream/main
```

Expected: `main` is an ancestor of `HEAD`; local `main` equals `origin/main`; the upstream SHA is recorded in the execution notes.

### Task 2: Merge Upstream and Replace the System Tree

**Files:**
- Replace from upstream: all tracked system-layer paths
- Preserve: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md`
- Preserve: `docs/superpowers/plans/2026-08-16-upstream-sync.md`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: refreshed `upstream/main` from Task 1.
- Produces: a merge commit whose application/system tree matches upstream, plus the two approved ignore rules and planning documents.

- [ ] **Step 1: Start an auditable non-fast-forward merge without committing**

```powershell
git merge --no-ff --no-commit upstream/main
```

Expected: Git stops for conflicts. The design investigation predicted 27 tracked system-layer conflict paths. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L18-L22` and #25.

- [ ] **Step 2: Replace the staged system tree from upstream while preserving planning docs and `.gitignore`**

```powershell
git restore --source=upstream/main --staged --worktree -- . ':(exclude)docs/superpowers/**' ':(exclude).gitignore'
git restore --source=upstream/main --staged --worktree -- .gitignore
```

Expected: canonical system files and file modes now match upstream; fork-only tracked application files are staged for deletion. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L24-L29`.

- [ ] **Step 3: Add the two approved local-only ignore rules**

Use `apply_patch` to append these exact lines once, under the repository-local tooling/user customization portion of `.gitignore`:

```gitignore
.worktrees/
modes/_profile-*.md
```

Then stage the resolved tree:

```powershell
git add -A
```

- [ ] **Step 4: Prove all conflicts are resolved**

```powershell
git diff --name-only --diff-filter=U
git status --short
git ls-files -s -- .claude/skills/career-ops/SKILL.md
```

Expected: the unmerged-path command prints nothing; the skill entry uses the same file mode and blob as `upstream/main`.

- [ ] **Step 5: Prove the staged tree differs from upstream only where approved**

```powershell
git diff --cached --name-status upstream/main
git diff --cached --check
```

Expected paths: `.gitignore`, the design specification, and this plan only. Any other path must be restored from `upstream/main` before continuing.

- [ ] **Step 6: Commit the merge**

```powershell
git commit -m "chore: synchronize system layer with upstream/main (#25)"
```

Expected: a two-parent merge commit; `git merge-base --is-ancestor upstream/main HEAD` exits 0.

### Task 3: Preserve Relevant User Instructions Locally

**Files:**
- Create locally, never stage: `I:/career/career-ops/modes/_custom.md`
- Modify locally, never stage: `I:/career/career-ops/modes/_profile.md`
- Preserve unchanged: `I:/career/career-ops/modes/_profile-dotnet-azure.md`
- Preserve unchanged: `I:/career/career-ops/modes/_profile-ml-mlops.md`
- Read only: `I:/career/career-ops/config/profile.yml`

**Interfaces:**
- Consumes: the approved profile migration rules from the specification.
- Produces: an upstream-compatible local custom-instructions file and a factual profile without obsolete API workflow text.

- [ ] **Step 1: Create the upstream custom-instructions file with exact profile-selection behavior**

Use `apply_patch` in the main checkout to create `modes/_custom.md` with this structure:

```markdown
# Custom Workflow Rules

## Profile Selection

- Read `active_profile` from `config/profile.yml` before evaluating or tailoring.
- `default`: use `modes/_profile.md`.
- `dotnet-azure`: read `modes/_profile.md`, then `modes/_profile-dotnet-azure.md`; the variant overrides the base profile for that session.
- `ml-mlops`: read `modes/_profile.md`, then `modes/_profile-ml-mlops.md`; the variant overrides the base profile for that session.
- An explicit profile request from the user overrides `active_profile` for that session only.

## PDF Accuracy

- Before generation, verify every metric, tool, technology, and quantified outcome against `cv.md`; correct any unsupported claim before rendering.
- After generation, independently audit the rendered content against `cv.md` and do not finalize below 95% claim confidence.
- Never convert a job-description keyword into a claimed skill unless the User Layer supports it.
```

This preserves the still-relevant procedural intent without committing user data. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L31-L39`.

- [ ] **Step 2: Remove migrated and obsolete procedural sections from the factual profile**

Use `apply_patch` in the main checkout to remove the block beginning at `## PDF Generation — Accuracy Policy (MANDATORY)` through the end of the obsolete `## Scan Customization — Indeed RSS Feeds` block. Keep every preceding factual section unchanged.

Expected: targeting, framing, narrative, proof points, compensation, negotiation, deal-breaker, and location sections remain byte-for-byte unchanged; PDF rules now live in `_custom.md`; Indeed/API workflow text is absent. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L35-L39`.

- [ ] **Step 3: Confirm user files remain outside Git**

```powershell
git -C I:/career/career-ops check-ignore -v modes/_profile.md modes/_custom.md modes/_profile-dotnet-azure.md modes/_profile-ml-mlops.md config/profile.yml portals.yml
git -C I:/career/career-ops status --short
```

Expected: all listed user files are ignored after the synchronized `.gitignore` reaches `main`; before PR merge, the new custom file and variant files may remain untracked but must never be staged.

### Task 4: Install and Verify the Synchronized Application

**Files:**
- Read: `package.json`
- Read only: `I:/career/career-ops/config/profile.yml`
- Read only: `I:/career/career-ops/portals.yml`
- Read only: `I:/career/career-ops/data/applications.md`
- Read only: `I:/career/career-ops/reports/`

**Interfaces:**
- Consumes: merged system tree from Task 2 and local user compatibility from Task 3.
- Produces: fresh test, configuration, pipeline, and safety evidence.

- [ ] **Step 1: Install the upstream dependency set**

```powershell
npm install
```

Expected: dependency installation exits 0 and any tracked lockfile changes exactly match upstream. If installation changes a tracked system file beyond upstream, restore it and investigate before continuing.

- [ ] **Step 2: Run the upstream quick suite**

```powershell
node test-all.mjs --quick
```

Expected: 0 failures. The quick flag skips the unavailable Go dashboard compiler. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L47-L51`.

- [ ] **Step 3: Validate the existing portals without printing their contents**

```powershell
$env:CAREER_OPS_PORTALS = 'I:/career/career-ops/portals.yml'
node validate-portals.mjs
Remove-Item Env:CAREER_OPS_PORTALS
```

Expected: exit 0. If validation fails, change only schema structure required by the validator; do not change companies, search preferences, or credentials without user approval. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L37-L39`.

- [ ] **Step 4: Parse the existing profile without emitting values**

```powershell
node --input-type=module -e "import { readFileSync } from 'node:fs'; import yaml from 'js-yaml'; yaml.load(readFileSync('I:/career/career-ops/config/profile.yml','utf8')); console.log('profile YAML valid')"
```

Expected: `profile YAML valid` and exit 0.

- [ ] **Step 5: Run pipeline verification against the existing user tracker and reports**

```powershell
$env:CAREER_OPS_TRACKER = 'I:/career/career-ops/data/applications.md'
$env:CAREER_OPS_REPORTS = 'I:/career/career-ops/reports'
node verify-pipeline.mjs
Remove-Item Env:CAREER_OPS_TRACKER
Remove-Item Env:CAREER_OPS_REPORTS
```

Expected: 0 errors. Do not print report or tracker contents in diagnostic output. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L43-L51`.

- [ ] **Step 6: Recheck the branch after verification**

```powershell
git status --short --branch
git diff --check
```

Expected: no uncommitted tracked changes.

### Task 5: Audit, Push, and Open the Pull Request

**Files:**
- Audit: all files in `main...HEAD`
- Verify: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md`
- Verify: `docs/superpowers/plans/2026-08-16-upstream-sync.md`

**Interfaces:**
- Consumes: verified merge commit and local compatibility evidence.
- Produces: a pushed issue branch and draft PR closing #25.

- [ ] **Step 1: Reconcile the PR deliverables against the diff**

```powershell
git diff main...HEAD --stat
git diff --name-status upstream/main..HEAD
git diff --check main...HEAD
```

Expected: the branch incorporates upstream; its tree differs from upstream only by `.gitignore` and the two planning documents.

- [ ] **Step 2: Verify removed fork functionality is absent from the final tree**

```powershell
git ls-tree HEAD -- scan-api.mjs config/job-aggregator-creds.example.json docs/job-aggregator-feedback.md .claude/agents/career-ops-runner.md .claude/agents/job-scraper.md .claude/agents/jd-fetcher.md .claude/agents/portal-scanner.md
```

Expected: no output.

- [ ] **Step 3: Verify planning artifacts and every cited tracked path persist**

```powershell
git ls-tree HEAD -- docs/superpowers/specs/2026-08-16-upstream-sync-design.md docs/superpowers/plans/2026-08-16-upstream-sync.md DATA_CONTRACT.md AGENTS.md modes/_shared.md
git log --all -- docs/superpowers/specs/2026-08-16-upstream-sync-design.md docs/superpowers/plans/2026-08-16-upstream-sync.md
```

Expected: each path has a tree entry and history. User Layer paths are intentionally untracked and are covered by the committed Data Contract rather than the PR diff.

- [ ] **Step 4: Check GitHub for an existing open PR before pushing**

Use the GitHub MCP pull-request listing for `cbeaulieu-gt/career-ops`, filtered to head branch `sync-upstream-issue-25`.

Expected: no merged or closed PR owns the branch. If one exists and is closed, create a new branch before pushing.

- [ ] **Step 5: Push the issue branch**

```powershell
git push -u origin sync-upstream-issue-25
```

Expected: push exits 0.

- [ ] **Step 6: Open a draft PR through GitHub MCP**

Use:

- Title: `chore: synchronize fork system layer with upstream`
- Base: `main`
- Head: `sync-upstream-issue-25`
- Draft: `true`
- Body sections: Summary, Conflict Policy, User Data Safety, Verification, Environmental Limitation, and the exact closing directive `Closes #25`
- Final attribution: `> 🤖 _Generated by Codex on behalf of @cbeaulieu-gt_`

Expected: a draft PR URL. Do not merge until live review comments, requested reviews, and CI checks have been evaluated. Source: `docs/superpowers/specs/2026-08-16-upstream-sync-design.md:L53-L55`.
