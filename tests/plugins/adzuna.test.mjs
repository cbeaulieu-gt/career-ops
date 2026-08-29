import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

import adzuna from '../../plugins/adzuna/index.mjs';
import { discoverPlugins } from '../../plugins/_engine.mjs';

test('manifest declares the keyed provider security boundary', async () => {
  const manifestUrl = new URL('../../plugins/adzuna/manifest.json', import.meta.url);
  const manifest = JSON.parse(await readFile(manifestUrl, 'utf8'));

  assert.equal(manifest.id, 'adzuna');
  assert.equal(manifest.apiVersion, 1);
  assert.deepEqual(manifest.hooks, ['provider']);
  assert.deepEqual(manifest.requiredEnv, ['ADZUNA_APP_ID', 'ADZUNA_APP_KEY']);
  assert.deepEqual(manifest.allowedHosts, ['api.adzuna.com']);
  assert.equal(manifest.humanInTheLoop, true);

  const pluginsDir = fileURLToPath(new URL('../../plugins/', import.meta.url));
  const discovered = discoverPlugins([pluginsDir]).find((plugin) => plugin.id === 'adzuna');
  assert.ok(discovered, 'the plugin engine should accept and discover the manifest');
  assert.deepEqual(discovered.requiredEnv, ['ADZUNA_APP_ID', 'ADZUNA_APP_KEY']);
});

test('fetch requires country and uses scoped credentials for bounded v1 pagination', async () => {
  const requests = [];
  const pages = [
    {
      count: 3,
      results: [
        {
          id: 'adz-1',
          title: 'Machine Learning Lead',
          redirect_url: 'https://jobs.example.com/1',
          company: { display_name: 'Example AI' },
          location: { display_name: 'Toronto, Ontario' },
          description: 'Lead the applied ML group.',
          created: '2026-08-28T12:30:00Z',
        },
        {
          id: 'adz-2',
          title: 'Director of AI',
          redirect_url: 'https://jobs.example.com/2',
          company: { display_name: 'Example Labs' },
          location: { display_name: 'Remote' },
          description: 'Build an AI organization.',
          created: 'not-a-date',
        },
      ],
    },
    {
      count: 3,
      results: [
        {
          id: 'adz-3',
          title: 'Head of Data',
          redirect_url: 'http://jobs.example.com/3',
          company: { display_name: 'Example Data' },
          location: { display_name: 'Ottawa, Ontario' },
          description: 'Own data and analytics.',
          created: '2026-08-27T09:00:00Z',
        },
      ],
    },
  ];
  const ctx = {
    env: Object.freeze({
      ADZUNA_APP_ID: 'scoped-app-id',
      ADZUNA_APP_KEY: 'scoped-app-key',
    }),
    fetchJson: async (url, options) => {
      requests.push({ url: new URL(url), options });
      return pages[requests.length - 1];
    },
  };

  const jobs = await adzuna.provider.fetch({
    name: 'Adzuna Canada',
    provider: 'adzuna',
    country: 'ca',
    what: 'machine learning',
    where: 'Toronto',
    results_per_page: 2,
    max_pages: 5,
    max_days_old: 14,
  }, ctx);

  assert.equal(requests.length, 2);
  assert.equal(requests[0].url.href, 'https://api.adzuna.com/v1/api/jobs/ca/search/1?app_id=scoped-app-id&app_key=scoped-app-key&results_per_page=2&what=machine+learning&where=Toronto&max_days_old=14&content-type=application%2Fjson');
  assert.equal(requests[1].url.pathname, '/v1/api/jobs/ca/search/2');
  assert.deepEqual(requests[0].options, undefined);
  assert.deepEqual(jobs, [
    {
      id: 'adz-1',
      title: 'Machine Learning Lead',
      url: 'https://jobs.example.com/1',
      company: 'Example AI',
      location: 'Toronto, Ontario',
      description: 'Lead the applied ML group.',
      postedAt: Date.parse('2026-08-28T12:30:00Z'),
    },
    {
      id: 'adz-2',
      title: 'Director of AI',
      url: 'https://jobs.example.com/2',
      company: 'Example Labs',
      location: 'Remote',
      description: 'Build an AI organization.',
    },
    {
      id: 'adz-3',
      title: 'Head of Data',
      url: 'http://jobs.example.com/3',
      company: 'Example Data',
      location: 'Ottawa, Ontario',
      description: 'Own data and analytics.',
      postedAt: Date.parse('2026-08-27T09:00:00Z'),
    },
  ]);
});

test('fetch accepts query and location aliases and clamps request bounds', async () => {
  const requests = [];
  const ctx = {
    env: Object.freeze({ ADZUNA_APP_ID: 'id', ADZUNA_APP_KEY: 'key' }),
    fetchJson: async (url) => {
      requests.push(new URL(url));
      return { count: 0, results: [] };
    },
  };

  await adzuna.provider.fetch({
    name: 'Bounded search',
    country: 'CA',
    query: 'head of ai',
    location: 'Montreal',
    results_per_page: 500,
    max_pages: 500,
  }, ctx);

  assert.equal(requests.length, 1);
  assert.equal(requests[0].searchParams.get('what'), 'head of ai');
  assert.equal(requests[0].searchParams.get('where'), 'Montreal');
  assert.equal(requests[0].searchParams.get('results_per_page'), '50');
});

test('fetch drops malformed rows and unsafe result URLs without aborting the page', async () => {
  const ctx = {
    env: Object.freeze({ ADZUNA_APP_ID: 'id', ADZUNA_APP_KEY: 'key' }),
    fetchJson: async () => ({
      count: 5,
      results: [
        null,
        { id: 'missing-title', redirect_url: 'https://jobs.example.com/no-title' },
        { id: 'unsafe', title: 'Unsafe', redirect_url: 'javascript:alert(1)' },
        { title: 'Missing ID', redirect_url: 'https://jobs.example.com/no-id' },
        { id: 42, title: 'Valid Role', redirect_url: 'https://jobs.example.com/valid' },
      ],
    }),
  };

  const jobs = await adzuna.provider.fetch({ country: 'gb', results_per_page: 10 }, ctx);

  assert.deepEqual(jobs, [{
    id: '42',
    title: 'Valid Role',
    url: 'https://jobs.example.com/valid',
    company: '',
    location: '',
    description: '',
  }]);
});

test('fetch surfaces upstream failures without exposing credentials', async () => {
  const ctx = {
    env: Object.freeze({
      ADZUNA_APP_ID: 'sensitive-app-id',
      ADZUNA_APP_KEY: 'sensitive-app-key',
    }),
    fetchJson: async (url) => {
      const error = new Error(`HTTP 429 requesting ${url}`);
      error.status = 429;
      throw error;
    },
  };

  await assert.rejects(
    adzuna.provider.fetch({ country: 'us' }, ctx),
    (error) => {
      assert.equal(error.status, 429);
      assert.match(error.message, /HTTP 429/);
      assert.doesNotMatch(error.message, /sensitive-app-id|sensitive-app-key/);
      return true;
    },
  );
});

test('fetch requires scoped credentials and never falls back to process.env', async (t) => {
  const previousId = process.env.ADZUNA_APP_ID;
  const previousKey = process.env.ADZUNA_APP_KEY;
  t.after(() => {
    if (previousId === undefined) delete process.env.ADZUNA_APP_ID;
    else process.env.ADZUNA_APP_ID = previousId;
    if (previousKey === undefined) delete process.env.ADZUNA_APP_KEY;
    else process.env.ADZUNA_APP_KEY = previousKey;
  });
  process.env.ADZUNA_APP_ID = 'global-id-must-not-be-read';
  process.env.ADZUNA_APP_KEY = 'global-key-must-not-be-read';

  await assert.rejects(
    adzuna.provider.fetch(
      { country: 'ca' },
      { env: Object.freeze({}), fetchJson: async () => ({ results: [] }) },
    ),
    /ADZUNA_APP_ID and ADZUNA_APP_KEY/,
  );
});

test('fetch rejects missing or invalid country before making a request', async () => {
  let requests = 0;
  const ctx = {
    env: Object.freeze({ ADZUNA_APP_ID: 'id', ADZUNA_APP_KEY: 'key' }),
    fetchJson: async () => {
      requests += 1;
      return { results: [] };
    },
  };

  await assert.rejects(adzuna.provider.fetch({ name: 'No country' }, ctx), /requires a two-letter 'country' code/);
  await assert.rejects(adzuna.provider.fetch({ country: '../us' }, ctx), /requires a two-letter 'country' code/);
  assert.equal(requests, 0);
});

test('fetch rejects a malformed API response', async () => {
  const ctx = {
    env: Object.freeze({ ADZUNA_APP_ID: 'id', ADZUNA_APP_KEY: 'key' }),
    fetchJson: async () => ({ count: 1, results: null }),
  };

  await assert.rejects(
    adzuna.provider.fetch({ country: 'au' }, ctx),
    /malformed search response on page 1/,
  );
});

test('fetch enforces the twenty-page safety ceiling', async () => {
  const paths = [];
  const ctx = {
    env: Object.freeze({ ADZUNA_APP_ID: 'id', ADZUNA_APP_KEY: 'key' }),
    fetchJson: async (url) => {
      paths.push(new URL(url).pathname);
      return {
        results: [{
          id: `job-${paths.length}`,
          title: `Role ${paths.length}`,
          redirect_url: `https://jobs.example.com/${paths.length}`,
        }],
      };
    },
  };

  const jobs = await adzuna.provider.fetch({
    country: 'nz',
    results_per_page: 1,
    max_pages: 500,
  }, ctx);

  assert.equal(paths.length, 20);
  assert.equal(paths.at(-1), '/v1/api/jobs/nz/search/20');
  assert.equal(jobs.length, 20);
});
