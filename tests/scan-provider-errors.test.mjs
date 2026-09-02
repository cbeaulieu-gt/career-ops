import { test } from 'node:test';
import assert from 'node:assert/strict';

import { classifyFetchError } from '../verify-portals.mjs';
import * as scanner from '../scan.mjs';

test('HTTP 429 is classified as a rate-limit failure', () => {
  assert.equal(classifyFetchError({ status: 429 }), 'rate_limit');
  assert.equal(classifyFetchError(new Error('HTTP 429')), 'rate_limit');
  assert.equal(
    classifyFetchError({ status: 429, message: 'HTTP 429: network quota exceeded' }),
    'rate_limit',
  );
});

test('new slug diagnostics reserve ATS migration guidance for ATS providers', () => {
  assert.equal(typeof scanner.formatNewSlugFailureLines, 'function');
  assert.deepEqual(scanner.formatNewSlugFailureLines([
    {
      company: 'JSearch Software Engineer',
      providerId: 'jsearch',
      kind: 'slug_gone',
      error: 'HTTP 404',
    },
    {
      company: 'Acme ATS',
      providerId: 'greenhouse',
      kind: 'slug_gone',
      error: 'HTTP 404',
    },
  ]), [
    '   JSearch Software Engineer: HTTP 404',
    '   Acme ATS: HTTP 404 — run: node verify-portals.mjs',
  ]);
});

test('provider error records retain provider identity and actionable details', () => {
  assert.equal(typeof scanner.providerErrorRecord, 'function');
  const error = Object.assign(new Error('HTTP 429'), {
    status: 429,
    attempts: 3,
  });

  assert.deepEqual(scanner.providerErrorRecord('JSearch Software Engineer', 'jsearch', error), {
    company: 'JSearch Software Engineer',
    providerId: 'jsearch',
    error: 'HTTP 429 after 3 attempts',
    kind: 'rate_limit',
  });
});

test('persistent diagnostics show the latest error and scope ATS-slug guidance', () => {
  assert.equal(typeof scanner.formatPersistentFailureLines, 'function');
  const lines = scanner.formatPersistentFailureLines(
    ['JSearch Software Engineer', 'Acme ATS'],
    [
      {
        company: 'JSearch Software Engineer',
        providerId: 'jsearch',
        kind: 'rate_limit',
        error: 'HTTP 503 old failure',
      },
      {
        company: 'Acme ATS',
        providerId: 'greenhouse',
        kind: 'slug_gone',
        error: 'HTTP 404',
      },
      {
        company: 'JSearch Software Engineer',
        providerId: 'jsearch',
        kind: 'rate_limit',
        error: 'HTTP 429 after 3 attempts',
      },
    ],
  );

  assert.deepEqual(lines, [
    '   JSearch Software Engineer: HTTP 429 after 3 attempts',
    '   Acme ATS: HTTP 404',
    '      Run: node verify-portals.mjs to check if the ATS migrated, or update its board slug.',
  ]);
});
