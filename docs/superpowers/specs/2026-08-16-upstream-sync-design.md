# Upstream Synchronization Design

**Status:** Approved in conversation on 2026-08-16
**Tracking:** #25
**Fork baseline:** `5f40cea`
**Upstream baseline:** `22cbe88`

## Goal

Make the fork's tracked application and system layer follow upstream while retaining the user's profiles, persistent instructions, configuration, and job-search artifacts. The approved scope explicitly drops fork-added application functionality and API integrations. Source: #25.

## Source of Truth

- Upstream supplies application code, shared modes, templates, canonical agent instructions, tests, documentation, and release metadata. Upstream classifies these paths as its System Layer. Source: `22cbe88` (`DATA_CONTRACT.md`).
- The user's CV, profile, custom instructions, portals, trackers, reports, outputs, and interview artifacts remain the User Layer and must not be replaced by the synchronization. Source: `22cbe88` (`DATA_CONTRACT.md`) and #25.
- The integration starts from the fork's current `main` at `5f40cea` and incorporates upstream `22cbe88` or a verified newer upstream tip. Source: #25.

## Integration Strategy

Merge upstream into the issue branch so the fork retains an auditable relationship to both histories. Resolve every shared system conflict to upstream, then remove fork-only tracked application files that upstream does not contain. This avoids replaying obsolete updater commits and implements the approved upstream-owns-system boundary. Source: #25.

The dry merge evaluation identified 27 conflict paths, all in the tracked system layer. None of the current User Layer files participates in those conflicts. Source: #25.

## Conflict Policy

1. Accept upstream for canonical instructions, wrappers, modes, scripts, dashboard code, tests, templates, docs, workflow files, package metadata, and version metadata. Source: #25 and `22cbe88`.
2. Remove the fork's job-aggregator bridge, fork-local API configuration example, local Claude agent implementations, obsolete integration feedback document, and fork-specific PDF implementation changes. These originated in `864f63f`, `9294946`, `b4d5a19`, `9c4476b`, `820bd0f`, `61235d5`, and `93a0158`; the approved scope makes them non-requirements. Source: #25.
3. Use upstream's current deterministic tracker URL handling instead of retaining the fork implementation from `5f40cea`. Upstream documents URL-first matching in its current canonical instructions. Source: `22cbe88` (`AGENTS.md`) and #25.
4. Retain only two fork-specific tracked compatibility rules: ignore the project-local worktree directory and ignore local profile-variant files. Both protect local-only state required by the approved workflow and do not change runtime application behavior. Source: #25.

## User Configuration Compatibility

The existing primary profile remains usable because upstream continues to load the User Layer profile after shared defaults. Persistent procedural rules now have a dedicated User Layer custom-instructions file that is loaded after the factual profile. Source: `22cbe88` (`AGENTS.md` and `modes/_shared.md`).

The local `active_profile` selector is not an upstream system feature. Preserve its behavior as a user-owned procedural rule: the custom-instructions file maps each configured selector value to the corresponding local profile variant and allows an explicit per-session user request to override it. Source: #25.

Keep factual targeting, narrative, proof points, compensation, negotiation, deal-breaker, and location content in the primary and variant profiles. Move the profile-selection workflow and still-relevant PDF accuracy instructions to the custom-instructions file. Remove obsolete Indeed/API integration instructions. Source: #25.

Do not invent values for upstream profile fields that are absent locally. Preserve existing configuration values, run compatibility checks against the synchronized application, and make only structural changes needed to pass validation without changing user preferences. Source: #25.

## Data Safety

The merge and commits occur only in the isolated issue worktree. User Layer files remain in the main checkout and are not copied into commits. Before the PR is opened, compare the PR diff with the upstream tree and audit every referenced artifact. Source: #25.

No command may delete, overwrite, stage, or expose the contents of user-owned CV, profile, portals, tracker, report, output, or interview files. Compatibility checks should report structure and validation results without printing personal values. Source: #25 and `22cbe88` (`DATA_CONTRACT.md`).

## Verification

Verification must cover: no unresolved conflicts; the intended upstream commit is an ancestor; fork-only application files are absent; required ignore rules remain; repository tests pass; existing portals validate; pipeline health remains clean; and the PR diff contains no user data. Source: #25.

The local machine does not currently provide the Go executable, so dashboard compilation may remain an explicitly reported environmental limitation unless Go becomes available. The pre-sync full test run passed 68 checks and failed only the dashboard build because `go` was unavailable. Source: #25.

## Delivery

Commit the synchronization on the issue branch, push only after verifying the PR is not already closed or merged, and open a pull request against `main` whose body contains `Closes #25`. Do not update `main` directly. Source: #25.
