#!/usr/bin/env node
/** Read-only admission funnel, including input rows not yet present in state. */
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as yaml from 'js-yaml';
import { isMainModule } from '../lib/is-main-module.mjs';

export function readTSV(path) {
  if (!existsSync(path)) return [];
  const [header, ...lines] = readFileSync(path, 'utf8').trim().split(/\r?\n/);
  const keys = header.split('\t');
  return lines.filter(line => line.trim()).map(line => {
    const cells = line.split('\t');
    return Object.fromEntries(keys.map((key, i) => [key, cells[i] || '']));
  });
}

export function summarizeScreening(inputs, states, receipts, reportDecisions = new Map()) {
  const byId = new Map(states.map(row => [row.id, row]));
  const counts = { discovered: inputs.length, filtered: 0, source_unconfirmed: 0, inaccessible: 0, needs_review: 0, shortlisted: 0, fully_evaluated: 0, apply: 0, execution_failed: 0, awaiting_screening: 0, user_skipped: 0 };
  const jobs = inputs.map(input => {
    const state = byId.get(input.id), receipt = receipts.get(input.id);
    if (receipt?.decision === 'shortlist' && ['fit', 'economy', 'admission'].includes(receipt.phase)) counts.shortlisted++;
    else if (['filtered', 'source_unconfirmed', 'inaccessible', 'needs_review'].includes(receipt?.decision)) counts[receipt.decision]++;
    if (state?.status === 'completed') {
      counts.fully_evaluated++;
      if (reportDecisions.get(state.report_num) === 'Apply') counts.apply++;
    }
    if (state?.status === 'failed') counts.execution_failed++;
    if (!receipt && (!state || ['pending', 'processing'].includes(state.status))) counts.awaiting_screening++;
    if (!receipt && state?.status === 'skipped') counts.user_skipped++;
    return { id: input.id, url: input.url, status: state?.status || 'unstarted', decision: receipt?.decision || null, reason: receipt?.reason || state?.error || null };
  });
  return { counts, jobs };
}

export function readScreeningSummary(batchDir) {
  const inputs = readTSV(join(batchDir, 'batch-input.tsv'));
  const states = readTSV(join(batchDir, 'batch-state.tsv'));
  const receipts = new Map();
  for (const input of inputs) {
    if (!/^\d+$/.test(input.id)) continue;
    const file = join(batchDir, 'screening', `${input.id}.json`);
    if (existsSync(file)) {
      try { receipts.set(input.id, JSON.parse(readFileSync(file, 'utf8'))); }
      catch { receipts.set(input.id, { decision: 'error', reason: 'Unreadable screening receipt' }); }
    }
  }
  const reportDecisions = new Map(), reportsDir = join(batchDir, '..', 'reports');
  const files = existsSync(reportsDir) ? readdirSync(reportsDir) : [];
  for (const state of states.filter(row => row.status === 'completed' && /^\d+$/.test(row.report_num))) {
    const file = files.find(name => name.startsWith(state.report_num + '-') && name.endsWith('.md'));
    if (!file) continue;
    const match = readFileSync(join(reportsDir, file), 'utf8').match(/## Machine Summary[\s\S]*?```ya?ml\s*([\s\S]*?)```/i);
    if (match) { try { reportDecisions.set(state.report_num, yaml.load(match[1])?.final_decision); } catch { /* missing result is never an Apply */ } }
  }
  return summarizeScreening(inputs, states, receipts, reportDecisions);
}

if (isMainModule(import.meta.url)) {
  const args = process.argv.slice(2), index = args.indexOf('--batch-dir');
  const batchDir = index >= 0 ? resolve(args[index + 1]) : dirname(fileURLToPath(import.meta.url));
  const result = readScreeningSummary(batchDir);
  if (args.includes('--json')) console.log(JSON.stringify(result, null, 2));
  else console.log('Admission funnel: ' + Object.entries(result.counts).map(([key, value]) => `${key}=${value}`).join(' | '));
}
