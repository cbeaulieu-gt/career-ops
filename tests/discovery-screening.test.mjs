import test from 'node:test';
import assert from 'node:assert/strict';
import { passesFilters } from '../scan-ats-full.mjs';
import { buildTitleFilter } from '../title-keywords.mjs';

test('reverse discovery applies shared source/mandatory rules and narrow employer exceptions', () => {
  const titleFilterConfig = { positive: ['Software Engineer'], negative: ['GPU', 'CUDA'] };
  const options = { titleFilter: buildTitleFilter(titleFilterConfig), titleFilterConfig, locationFilter: () => true,
    screeningPolicy: { unknown_source: 'skip', reject_required: ['Rust'], title_exceptions: [{ companies: ['NVIDIA'], negative: ['GPU'], positive: ['Software Engineer + GPU Developer Tools'] }] } };
  assert.equal(passesFilters({ title: 'Software Engineer', description: 'Rust is required.' }, options), false);
  assert.equal(passesFilters({ title: 'Software Engineer', hiring_source: 'unconfirmed' }, options), false);
  assert.equal(passesFilters({ title: 'Software Engineer, GPU Developer Tools', company: 'NVIDIA' }, options), true);
  assert.equal(passesFilters({ title: 'Software Engineer, GPU Developer Tools', company: 'Other' }, options), false);
  assert.equal(passesFilters({ title: 'Software Engineer', description: 'Rust preferred.' }, options), true);
});

test('a caller-provided title veto cannot be bypassed by an empty fallback config', () => {
  assert.equal(passesFilters({ title: 'Anything' }, { titleFilter: () => false, locationFilter: () => true }), false);
});
