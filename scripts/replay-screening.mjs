#!/usr/bin/env node
/** Offline deterministic replay. Historical recommendations are comparison output only. */
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import * as yaml from 'js-yaml';
import { screenMetadata } from '../job-screening.mjs';
import { readTSV } from '../batch/screening-summary.mjs';
import { isMainModule } from '../lib/is-main-module.mjs';

/** Only the explicitly archived JD section may supply posting evidence. */
export function archivedJD(report) {
  return String(report).match(/^## Job Description[^\r\n]*\r?\n([\s\S]*?)(?=^## |$(?![\s\S]))/m)?.[1]?.trim() || '';
}

/** No historical score, decision, rejection explanation or candidate claims enter these gates. */
export function replayScreening(jobs, { portals = {}, policy = {}, locationFilter, contentFilter, eligibilityFilter } = {}) {
  const counts = { discovered: jobs.length, filtered: 0, source_unconfirmed: 0, needs_review: 0, missing_evidence: 0, needs_fit_screening: 0, missing_jd_evidence: 0, historical_full_evaluations: 0, full_evaluations_avoided: 0 };
  const results = jobs.map(job => {
    const input = { url: job.url, company: job.company, title: job.title, location: job.location, description: job.description || '', hiring_source: job.hiring_source };
    const substantive = input.description.trim().split(/\s+/).length >= 80;
    if (!substantive) counts.missing_jd_evidence++;
    let result = screenMetadata(input, { portals, policy });
    if (result.decision === 'shortlist' && job.location && locationFilter && !locationFilter(job.location, job.url, job.title)) result = { decision: 'filtered', reason: 'Location fails configured filter', evidence: [job.location] };
    if (result.decision === 'shortlist' && !substantive) result = { decision: 'missing_evidence', reason: 'No substantive archived JD; cannot replay fit or source eligibility', evidence: [] };
    if (result.decision === 'shortlist' && eligibilityFilter && !eligibilityFilter(input.description)) result = { decision: 'filtered', reason: 'Archived JD fails configured country or visa eligibility filter', evidence: [] };
    if (result.decision === 'shortlist' && contentFilter && !contentFilter(input.description, job.title)) result = { decision: 'filtered', reason: 'Archived JD fails configured content filter', evidence: [] };
    if (result.decision === 'shortlist') result = { ...result, decision: 'needs_fit_screening', reason: 'Deterministic gates pass; source, location and plausible Apply fit still need economy screening' };
    counts[result.decision]++;
    if (job.status === 'completed') {
      counts.historical_full_evaluations++;
      if (['filtered', 'source_unconfirmed'].includes(result.decision)) counts.full_evaluations_avoided++;
    }
    return { id: job.id, company: job.company, title: job.title, ...result, evidence_path: job.evidence_path || null, historical_status: job.status, historical_report: job.report_num || null, historical_decision: job.historical_decision || null };
  });
  return { scope: 'Offline deterministic gates only; no model calls, live fetches or run-state writes', counts, historical_apply: results.filter(job => job.historical_decision === 'Apply'), jobs: results };
}

/** Join archival data without using the evaluator's narrative as original posting text. */
export function loadReplayJobs(root) {
  const inputs = readTSV(join(root, 'batch/batch-input.tsv'));
  const states = new Map(readTSV(join(root, 'batch/batch-state.tsv')).map(row => [row.id, row]));
  const reportFiles = existsSync(join(root, 'reports')) ? readdirSync(join(root, 'reports')) : [];
  const jdFiles = existsSync(join(root, 'jds')) ? readdirSync(join(root, 'jds')) : [];
  return inputs.map(input => {
    const state = states.get(input.id) || {}, fields = input.notes.split('|').map(value => value.trim());
    const reportName = /^\d+$/.test(state.report_num || '') ? reportFiles.find(file => file.endsWith('.md') && Number(file.match(/^(\d+)-/)?.[1]) === Number(state.report_num)) : null;
    const report = reportName ? readFileSync(join(root, 'reports', reportName), 'utf8') : '';
    let description = archivedJD(report), evidence_path = description ? `reports/${reportName}:Job Description` : null;
    if (!description && /^\d+$/.test(state.report_num || '')) {
      const capture = jdFiles.find(file => /\.(?:md|txt)$/.test(file) && Number(file.match(/^(\d+)-/)?.[1]) === Number(state.report_num));
      if (capture) { description = readFileSync(join(root, 'jds', capture), 'utf8'); evidence_path = `jds/${capture}`; }
    }
    const summary = report.match(/## Machine Summary[\s\S]*?```ya?ml\s*([\s\S]*?)```/i);
    let historical_decision = null;
    if (summary) { try { historical_decision = yaml.load(summary[1])?.final_decision || null; } catch { /* never synthesize a result */ } }
    return { ...input, ...state, company: fields[0] || '', title: fields[1] || '', description, evidence_path, historical_decision };
  });
}

if (isMainModule(import.meta.url)) {
  const args = process.argv.slice(2), values = {};
  for (let i = 0; i < args.length; i++) {
    if (!['--root', '--profile', '--portals'].includes(args[i]) || !args[i + 1]) throw new Error(`Unknown/incomplete argument: ${args[i]}`);
    values[args[i].slice(2)] = args[++i];
  }
  if (!values.root) throw new Error('--root is required (historical user data directory)');
  const root = resolve(values.root);
  const profile = yaml.load(readFileSync(values.profile || join(root, 'config/profile.yml'), 'utf8')) || {};
  const portals = yaml.load(readFileSync(values.portals || join(root, 'portals.yml'), 'utf8')) || {};
  const { buildLocationFilter, buildContentFilter, buildCountryEligibilityFilter, buildVisaFilter, matchedTitleKeywords } = await import('../scan.mjs');
  const result = replayScreening(loadReplayJobs(root), { portals, policy: profile.screening,
    locationFilter: buildLocationFilter(portals.location_filter),
    eligibilityFilter: description => buildCountryEligibilityFilter(portals.country_eligibility_filter, profile.location?.country)(description) && buildVisaFilter(portals.visa_filter)(description),
    contentFilter: (description, title) => buildContentFilter(portals.content_filter)(description, matchedTitleKeywords(title, portals.title_filter)),
  });
  console.log(JSON.stringify(result, null, 2));
}
