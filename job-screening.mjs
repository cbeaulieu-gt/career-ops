/** Shared, side-effect-free admission rules. User-specific rules are configuration. */
import { buildTitleFilter, compileContentKeyword, foldAccents } from './title-keywords.mjs';

const normalize = value => foldAccents(String(value ?? '').trim().toLowerCase());
const verdict = (decision, reason, evidence = []) => ({ decision, reason, evidence });

/** Relax only named negative terms, for named employers and narrowly matching titles. */
export function passesScreeningTitle(title, company, titleFilter, policy = {}) {
  if (buildTitleFilter(titleFilter)(title)) return true;
  for (const rule of policy.title_exceptions ?? []) {
    if (!Array.isArray(rule.companies) || !rule.companies.some(name => normalize(name) === normalize(company))) continue;
    if (!Array.isArray(rule.positive) || rule.positive.length === 0) continue;
    if (!buildTitleFilter({ positive: rule.positive })(title)) continue;
    const relaxed = new Set((rule.negative ?? []).map(normalize));
    const negative = (titleFilter?.negative ?? []).filter(term => !relaxed.has(normalize(term)));
    if (buildTitleFilter({ ...titleFilter, negative })(title)) return true;
  }
  return false;
}

/** Find an explicit mandatory requirement, never a preferred or negated skill. */
export function unsupportedRequirement(description, rules = []) {
  const matchers = rules.filter(rule => typeof rule === 'string' && rule.trim())
    .map(rule => ({ rule, matches: compileContentKeyword(normalize(rule)) }));
  let requiredSection = false;
  for (const raw of String(description ?? '').replace(/<[^>]*>/g, '\n').split(/\r?\n|(?<=[.!?])\s+/u)) {
    const text = raw.trim();
    const lower = normalize(text);
    if (/^(?:#{1,6}\s*)?(?:preferred|desired|bonus|nice.to.have|additional)\s+(?:qualifications|skills|experience)/.test(lower)) requiredSection = false;
    if (/^(?:#{1,6}\s*)?(?:required|minimum|basic)\s+(?:qualifications|skills|experience)/.test(lower)) requiredSection = true;
    if (/\b(?:preferred|optional|nice.to.have|bonus|not required)\b|\bno\b.*\brequired\b/.test(lower)) continue;
    if (!requiredSection && !/\b(?:required|must|mandatory|need to have)\b/.test(lower)) continue;
    const hit = matchers.find(({ matches }) => matches(lower));
    if (hit) return { requirement: hit.rule, evidence: text };
  }
  return null;
}

/** Deterministic gates that can run before extraction or any model invocation. */
export function screenMetadata(job, { portals = {}, policy = {}, allowStretch = false } = {}) {
  if (job.locationEligible === false || job.workAuthorizationEligible === false) {
    return verdict('filtered', 'Explicit location or work-authorization restriction', [job.location ?? 'Eligibility restriction']);
  }
  const agency = (policy.agency_companies ?? []).some(name => normalize(name) === normalize(job.company));
  if (agency || ['unknown', 'unconfirmed', 'agency'].includes(job.hiring_source)) {
    return verdict(policy.unknown_source === 'skip' ? 'source_unconfirmed' : 'needs_review', 'Hiring source is unknown or unconfirmed', [job.company || 'Unknown employer']);
  }
  if (!allowStretch && (job.titleEligible === false || (job.titleEligible !== true && job.title && !passesScreeningTitle(job.title, job.company, portals.title_filter, policy)))) {
    return verdict('filtered', 'Title does not meet configured targeting', [job.title]);
  }
  if (!allowStretch) {
    const unsupported = unsupportedRequirement(job.description, policy.reject_required);
    if (unsupported) return verdict('filtered', `Unsupported mandatory requirement: ${unsupported.requirement}`, [unsupported.evidence]);
  }
  return verdict('shortlist', 'Available metadata passes; JD and source still require admission checks');
}

/** Validate compact model output; unknown eligibility never silently passes. */
export function validateScreeningVerdict(value, jd, policy = {}) {
  const decisions = ['shortlist', 'filtered', 'source_unconfirmed', 'inaccessible', 'needs_review'];
  const sources = ['direct', 'agency', 'unknown', 'unconfirmed'];
  const locations = ['eligible', 'ineligible', 'unknown'];
  if (!value || !decisions.includes(value.decision) || !sources.includes(value.source)
      || !locations.includes(value.location) || typeof value.reason !== 'string' || !value.reason.trim()
      || !Array.isArray(value.evidence) || value.evidence.length === 0
      || value.evidence.some(text => typeof text !== 'string' || text.trim().length < 5
        || !normalize(jd).replace(/\s+/g, ' ').includes(normalize(text).replace(/\s+/g, ' ')))) {
    return verdict('error', 'Invalid screening verdict or evidence not found in JD');
  }
  if (value.source !== 'direct') {
    return verdict(policy.unknown_source === 'skip' ? 'source_unconfirmed' : 'needs_review', value.reason, value.evidence);
  }
  if (value.location === 'ineligible') return verdict('filtered', value.reason, value.evidence);
  if (value.location === 'unknown' && value.decision === 'shortlist') return verdict('needs_review', 'Location eligibility needs review: ' + value.reason, value.evidence);
  return verdict(value.decision, value.reason, value.evidence);
}
