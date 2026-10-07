import test from 'node:test';
import assert from 'node:assert/strict';
import { passesScreeningTitle, screenMetadata, validateScreeningVerdict, unsupportedRequirement } from '../job-screening.mjs';

const portals = { title_filter: { positive: ['Software Engineer', 'Data Engineer'], negative: ['GPU', 'CUDA', 'Principal', 'word:intern'] } };
const policy = {
  unknown_source: 'skip', agency_companies: ['Example Staffing'],
  reject_required: ['word:CUDA', 'word:Rust', 'GPU kernels'],
  title_exceptions: [{ companies: ['NVIDIA'], negative: ['GPU'], positive: ['Data Engineer + GPU Platform', 'Software Engineer + GPU Developer Tools'] }],
};
const options = { portals, policy };

test('standalone preferred/bonus headings terminate mandatory context', () => {
  for (const heading of ['Preferred', 'Preferred:', 'Nice to have:', 'Bonus points:']) assert.equal(unsupportedRequirement('Required Qualifications\nPython experience\n' + heading + '\nCUDA experience', ['CUDA']), null);
});

test('common JD headings distinguish required and standout skills and allow supported language alternatives', () => {
  assert.equal(unsupportedRequirement('**What we need to see:**\nCUDA programming experience', ['CUDA'])?.requirement, 'CUDA');
  assert.equal(unsupportedRequirement('**What we need to see:**\nPython\n**Ways to stand out from the crowd:**\nCUDA programming', ['CUDA']), null);
  assert.equal(unsupportedRequirement('Python or Rust is required.', ['Rust'], ['Python']), null);
  assert.equal(unsupportedRequirement('Python and Rust are required.', ['Rust'], ['Python'])?.requirement, 'Rust');
});

test('invalid policy fails closed and requirements sections do not leak into responsibilities', () => {
  assert.throws(() => screenMetadata({ title: 'Software Engineer' }, { policy: { reject_required: 'CUDA' } }), /reject_required/);
  const result = screenMetadata({ title: 'Software Engineer', description: 'Minimum Qualifications\nPython\nResponsibilities\nCollaborate with CUDA specialists' }, options);
  assert.equal(result.decision, 'shortlist');
});

test('a narrow employer exception never relaxes CUDA, seniority or another company', () => {
  assert.equal(passesScreeningTitle('Software Engineer, GPU Developer Tools', 'NVIDIA', portals.title_filter, policy), true);
  assert.equal(passesScreeningTitle('Software Engineer, CUDA Driver', 'NVIDIA', portals.title_filter, policy), false);
  assert.equal(passesScreeningTitle('Principal Software Engineer, GPU Developer Tools', 'NVIDIA', portals.title_filter, policy), false);
  assert.equal(passesScreeningTitle('Software Engineer, GPU Performance', 'NVIDIA', portals.title_filter, policy), false);
  assert.equal(passesScreeningTitle('Software Engineer, GPU Developer Tools', 'Other Employer', portals.title_filter, policy), false);
  assert.equal(passesScreeningTitle('Software Engineer, Internal Tools', 'Other Employer', portals.title_filter, policy), true);
});

test('mandatory unsupported specialists are filtered, preferred and negated skills are retained', () => {
  for (const description of ['CUDA experience is required.', 'Required qualifications:\n- Develop GPU kernels.\n- Python experience.']) {
    assert.equal(screenMetadata({ title: 'Software Engineer', company: 'Acme', description }, options).decision, 'filtered');
  }
  for (const description of ['Preferred qualifications:\n- CUDA experience.', 'Python required. Rust is a nice-to-have.', 'CUDA is not required.', 'No Rust experience required.']) {
    assert.equal(screenMetadata({ title: 'Software Engineer', company: 'Acme', description }, options).decision, 'shortlist', description);
  }
});

test('provider HTML descriptions honor mandatory sections', () => {
  assert.equal(screenMetadata({ title: 'Software Engineer', description: '<h2>Required qualifications</h2><ul><li>CUDA experience</li></ul>' }, options).decision, 'filtered');
});

test('source policy skips known agencies and explicit unknown sources without guessing from a URL', () => {
  assert.equal(screenMetadata({ company: 'Example Staffing', title: 'Software Engineer' }, options).decision, 'source_unconfirmed');
  assert.equal(screenMetadata({ company: 'Acme', hiring_source: 'unknown' }, options).decision, 'source_unconfirmed');
  assert.equal(screenMetadata({ url: 'https://aggregator.example/job/1', company: 'Acme' }, options).decision, 'shortlist');
});

test('stretch override relaxes fit only, never source or explicit eligibility failure', () => {
  const stretch = { ...options, allowStretch: true };
  assert.equal(screenMetadata({ title: 'Principal Software Engineer, CUDA Driver', company: 'NVIDIA' }, stretch).decision, 'shortlist');
  assert.equal(screenMetadata({ title: 'Software Engineer', company: 'Example Staffing' }, stretch).decision, 'source_unconfirmed');
  assert.equal(screenMetadata({ title: 'Software Engineer', locationEligible: false }, stretch).decision, 'filtered');
  assert.equal(screenMetadata({ title: 'Software Engineer', locationEligible: null }, options).decision, 'shortlist');
});

test('verdicts require real JD evidence, valid enums and no model-invented agency authorization', () => {
  const jd = 'Acme hires software engineers in the United States. Python experience required.';
  const verdict = { decision: 'shortlist', source: 'direct', location: 'eligible', reason: 'Relevant backend role', evidence: ['Python experience required.'] };
  assert.equal(validateScreeningVerdict(verdict, jd, policy).decision, 'shortlist');
  assert.equal(validateScreeningVerdict({ ...verdict, location: 'unknown' }, jd, policy).decision, 'needs_review');
  assert.equal(validateScreeningVerdict({ ...verdict, source: 'unconfirmed' }, jd, policy).decision, 'source_unconfirmed');
  assert.equal(validateScreeningVerdict({ ...verdict, source: 'agency' }, jd, policy).decision, 'source_unconfirmed');
  for (const value of [{ ...verdict, decision: 'approve' }, { ...verdict, evidence: ['Made-up requirement'] }, { ...verdict, evidence: [] }, null]) {
    assert.equal(validateScreeningVerdict(value, jd, policy).decision, 'error');
  }
});
