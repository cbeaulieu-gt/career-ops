// @ts-check

const API_ROOT = 'https://api.adzuna.com/v1/api';

function positiveInteger(value, fallback, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) return fallback;
  return Math.min(parsed, maximum);
}

function requiredCountry(entry) {
  const country = typeof entry?.country === 'string' ? entry.country.trim().toLowerCase() : '';
  if (!/^[a-z]{2}$/.test(country)) {
    throw new Error(`adzuna: entry ${entry?.name || '(unnamed)'} requires a two-letter 'country' code`);
  }
  return country;
}

function validJobUrl(value) {
  try {
    const url = new URL(String(value));
    return url.protocol === 'https:' || url.protocol === 'http:';
  } catch {
    return false;
  }
}

function normalizeResult(result) {
  if (!result || typeof result !== 'object') return null;
  const id = result.id == null ? '' : String(result.id).trim();
  const title = typeof result.title === 'string' ? result.title.trim() : '';
  const url = typeof result.redirect_url === 'string' ? result.redirect_url.trim() : '';
  if (!id || !title || !validJobUrl(url)) return null;

  const job = {
    id,
    title,
    url,
    company: typeof result.company?.display_name === 'string' ? result.company.display_name.trim() : '',
    location: typeof result.location?.display_name === 'string' ? result.location.display_name.trim() : '',
    description: typeof result.description === 'string' ? result.description.trim() : '',
  };
  const postedAt = Date.parse(result.created);
  if (Number.isFinite(postedAt)) job.postedAt = postedAt;
  return job;
}

function searchUrl(entry, country, page, appId, appKey, resultsPerPage) {
  const url = new URL(`${API_ROOT}/jobs/${country}/search/${page}`);
  url.searchParams.set('app_id', appId);
  url.searchParams.set('app_key', appKey);
  url.searchParams.set('results_per_page', String(resultsPerPage));

  const whatValue = entry.what ?? entry.query;
  const whereValue = entry.where ?? entry.location;
  const what = typeof whatValue === 'string' ? whatValue.trim() : '';
  const where = typeof whereValue === 'string' ? whereValue.trim() : '';
  if (what) url.searchParams.set('what', what);
  if (where) url.searchParams.set('where', where);
  const maxDaysOld = positiveInteger(entry.max_days_old, null, Number.MAX_SAFE_INTEGER);
  if (maxDaysOld !== null) url.searchParams.set('max_days_old', String(maxDaysOld));
  url.searchParams.set('content-type', 'application/json');
  return url;
}

async function fetchPage(ctx, url, secrets) {
  try {
    return await ctx.fetchJson(url);
  } catch (error) {
    let message = String(error?.message || error)
      .replace(/([?&](?:app_id|app_key)=)[^&\s]+/gi, '$1«redacted»');
    for (const secret of secrets) {
      if (typeof secret === 'string' && secret) {
        message = message.split(secret).join('«redacted»');
        message = message.split(encodeURIComponent(secret)).join('«redacted»');
      }
    }
    const safeError = new Error(message);
    if (error?.status !== undefined) safeError.status = error.status;
    if (error?.retryAfter !== undefined) safeError.retryAfter = error.retryAfter;
    throw safeError;
  }
}

export default {
  provider: {
    id: 'adzuna',
    detect() { return null; },

    fetch: async (entry, ctx) => {
      const appId = ctx?.env?.ADZUNA_APP_ID;
      const appKey = ctx?.env?.ADZUNA_APP_KEY;
      if (!appId || !appKey) {
        throw new Error('adzuna: ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in .env');
      }
      if (typeof ctx?.fetchJson !== 'function') {
        throw new Error('adzuna: plugin context is missing fetchJson');
      }

      const country = requiredCountry(entry);
      const resultsPerPage = positiveInteger(entry.results_per_page, 50, 50);
      const maxPages = positiveInteger(entry.max_pages, 1, 20);
      const jobs = [];

      for (let page = 1; page <= maxPages; page += 1) {
        const url = searchUrl(entry, country, page, appId, appKey, resultsPerPage);
        const payload = await fetchPage(ctx, url, [appId, appKey]);
        if (!payload || typeof payload !== 'object' || !Array.isArray(payload.results)) {
          throw new Error(`adzuna: malformed search response on page ${page}`);
        }
        for (const result of payload.results) {
          const normalized = normalizeResult(result);
          if (normalized) jobs.push(normalized);
        }
        if (payload.results.length < resultsPerPage) break;
        if (Number.isFinite(payload.count) && page * resultsPerPage >= payload.count) break;
      }
      return jobs;
    },
  },
};
