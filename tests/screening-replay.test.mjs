import test from 'node:test';
import assert from 'node:assert/strict';
import { replayScreening, archivedJD } from '../scripts/replay-screening.mjs';

const description = 'Minimum Qualifications\nCUDA experience required.\n' + 'Build services for our engineering teams. '.repeat(20);
const options = { portals: { title_filter: { positive: ['Software Engineer'], negative: ['Principal'] } }, policy: { reject_required: ['CUDA'], unknown_source: 'skip' } };

test('offline replay never feeds historical recommendations or evaluator claims to the admission gate', () => {
  const text = '# Report\n## Job Description (archived verbatim)\nPython preferred.\n## B) CV Match\nCUDA is required.\n## Machine Summary\n```yaml\nfinal_decision: Skip\n```';
  assert.equal(archivedJD(text).trim(), 'Python preferred.');
  const job = { id: '1', title: 'Software Engineer', description, status: 'completed', historical_decision: 'Apply' };
  const result = replayScreening([job], options);
  assert.equal(result.jobs[0].decision, 'filtered');
  assert.equal(result.counts.full_evaluations_avoided, 1);
  const changed = replayScreening([{ ...job, historical_decision: 'Skip' }], options);
  assert.equal(changed.jobs[0].decision, result.jobs[0].decision);
  assert.equal(changed.jobs[0].reason, result.jobs[0].reason);
});

test('missing archives are counted explicitly and surviving jobs still require inexpensive fit screening', () => {
  const result = replayScreening([
    { id: '1', title: 'Software Engineer', description: '', status: 'failed' },
    { id: '2', title: 'Software Engineer', description: 'Python required. ' + 'Build backend services. '.repeat(40), status: 'completed', historical_decision: 'Apply' },
    { id: '3', title: 'Principal Software Engineer', description: '', status: 'failed' },
  ], options);
  assert.equal(result.counts.discovered, 3);
  assert.equal(result.counts.missing_jd_evidence, 2);
  assert.equal(result.counts.needs_fit_screening, 1);
  assert.equal(result.jobs[0].decision, 'missing_evidence');
  assert.equal(result.jobs[2].decision, 'filtered');
  assert.equal(result.historical_apply[0].decision, 'needs_fit_screening');
});
