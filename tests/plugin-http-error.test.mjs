import { test } from 'node:test';
import assert from 'node:assert/strict';

import * as pluginEngine from '../plugins/_engine.mjs';

test('plugin HTTP errors preserve Retry-After for provider retry policies', async () => {
  assert.equal(typeof pluginEngine.httpErrorFromResponse, 'function');
  const response = new Response('rate limited', {
    status: 429,
    headers: { 'retry-after': '7' },
  });

  const error = await pluginEngine.httpErrorFromResponse(response);

  assert.equal(error.status, 429);
  assert.equal(error.retryAfter, '7');
  assert.equal(error.message, 'HTTP 429: rate limited');
});
