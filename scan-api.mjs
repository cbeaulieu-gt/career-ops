#!/usr/bin/env node

/**
 * scan-api.mjs — job-aggregator bridge for career-ops
 *
 * Fetches job listings from the job-aggregator CLI (RemoteOK, Remotive,
 * Arbeitnow, Jobicy, The Muse, and more), applies career-ops title
 * filtering and deduplication, then adds new offers to pipeline.md
 * and scan-history.tsv.
 *
 * Configure via the `api_sources:` block in portals.yml.
 *
 * Usage:
 *   node scan-api.mjs                              # scan with portals.yml config
 *   node scan-api.mjs --dry-run                    # preview, no writes
 *   node scan-api.mjs --sources remoteok,remotive  # override source list
 *   node scan-api.mjs --hours 48                   # override lookback window
 *   node scan-api.mjs --query "data engineer"      # override search query
 */

import { readFileSync, writeFileSync, appendFileSync, existsSync, mkdirSync } from 'fs';
import { spawn } from 'child_process';
import yaml from 'js-yaml';

const parseYaml = yaml.load;

// ── Paths ────────────────────────────────────────────────────────────────

const PORTALS_PATH    = 'portals.yml';
const SCAN_HISTORY_PATH = 'data/scan-history.tsv';
const PIPELINE_PATH   = 'data/pipeline.md';
const APPLICATIONS_PATH = 'data/applications.md';

mkdirSync('data', { recursive: true });

// ── US-eligibility filter ─────────────────────────────────────────────────
//
// Keeps records where location is:
//   - null / empty string  (unknown → keep; remote boards often omit it)
//   - contains "remote"    (no geo restriction)
//   - contains "united states" or matches \bUSA?\b (whole word)
//   - contains a US state abbreviation (CA, TX, NY, etc.)
//
// Discards records explicitly located outside the US (Spain, UK, Ireland…).

const US_STATE_RE = /\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b/;

function isUsEligible(location) {
  if (!location || location.trim() === '') return true;
  const loc   = location.trim();
  const lower = loc.toLowerCase();
  return (
    lower.includes('remote') ||
    lower.includes('united states') ||
    /\busa?\b/i.test(loc) ||
    US_STATE_RE.test(loc)
  );
}

// ── Title filter (mirrors scan.mjs) ──────────────────────────────────────

function buildTitleFilter(titleFilter) {
  const positive = (titleFilter?.positive || []).map(k => k.toLowerCase());
  const negative = (titleFilter?.negative || []).map(k => k.toLowerCase());

  return (title) => {
    const lower = title.toLowerCase();
    const hasPositive = positive.length === 0 || positive.some(k => lower.includes(k));
    const hasNegative = negative.some(k => lower.includes(k));
    return hasPositive && !hasNegative;
  };
}

// ── Dedup (mirrors scan.mjs) ─────────────────────────────────────────────

function loadSeenUrls() {
  const seen = new Set();

  if (existsSync(SCAN_HISTORY_PATH)) {
    const lines = readFileSync(SCAN_HISTORY_PATH, 'utf-8').split('\n');
    for (const line of lines.slice(1)) {
      const url = line.split('\t')[0];
      if (url) seen.add(url);
    }
  }

  if (existsSync(PIPELINE_PATH)) {
    const text = readFileSync(PIPELINE_PATH, 'utf-8');
    for (const match of text.matchAll(/- \[[ x]\] (https?:\/\/\S+)/g)) {
      seen.add(match[1]);
    }
  }

  if (existsSync(APPLICATIONS_PATH)) {
    const text = readFileSync(APPLICATIONS_PATH, 'utf-8');
    for (const match of text.matchAll(/https?:\/\/[^\s|)]+/g)) {
      seen.add(match[0]);
    }
  }

  return seen;
}

function loadSeenCompanyRoles() {
  const seen = new Set();
  if (existsSync(APPLICATIONS_PATH)) {
    const text = readFileSync(APPLICATIONS_PATH, 'utf-8');
    for (const match of text.matchAll(/\|[^|]+\|[^|]+\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|/g)) {
      const company = match[1].trim().toLowerCase();
      const role    = match[2].trim().toLowerCase();
      if (company && role && company !== 'company') {
        seen.add(`${company}::${role}`);
      }
    }
  }
  return seen;
}

// ── Pipeline writer (mirrors scan.mjs) ───────────────────────────────────

function appendToPipeline(offers) {
  if (offers.length === 0) return;

  let text = readFileSync(PIPELINE_PATH, 'utf-8');
  const marker = '## Pendientes';
  const idx    = text.indexOf(marker);

  if (idx === -1) {
    const procIdx  = text.indexOf('## Procesadas');
    const insertAt = procIdx === -1 ? text.length : procIdx;
    const block    = `\n${marker}\n\n` + offers.map(o =>
      `- [ ] ${o.url} | ${o.company} | ${o.title}`
    ).join('\n') + '\n\n';
    text = text.slice(0, insertAt) + block + text.slice(insertAt);
  } else {
    const afterMarker = idx + marker.length;
    const nextSection = text.indexOf('\n## ', afterMarker);
    const insertAt    = nextSection === -1 ? text.length : nextSection;
    const block       = '\n' + offers.map(o =>
      `- [ ] ${o.url} | ${o.company} | ${o.title}`
    ).join('\n') + '\n';
    text = text.slice(0, insertAt) + block + text.slice(insertAt);
  }

  writeFileSync(PIPELINE_PATH, text, 'utf-8');
}

function appendToScanHistory(offers, date) {
  if (!existsSync(SCAN_HISTORY_PATH)) {
    writeFileSync(SCAN_HISTORY_PATH, 'url\tfirst_seen\tportal\ttitle\tcompany\tstatus\n', 'utf-8');
  }
  const lines = offers.map(o =>
    `${o.url}\t${date}\t${o.source}\t${o.title}\t${o.company}\tadded`
  ).join('\n') + '\n';
  appendFileSync(SCAN_HISTORY_PATH, lines, 'utf-8');
}

// ── Subprocess helpers ────────────────────────────────────────────────────

/**
 * Spawn a process, collect its stdout as a string, and resolve.
 * Rejects only if the process errored AND produced no output (some tools
 * emit warnings to stderr with a non-zero exit but still produce valid output).
 */
function spawnCollect(executable, args) {
  return new Promise((resolve, reject) => {
    const proc = spawn(executable, args, { windowsHide: true });
    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', chunk => { stdout += chunk; });
    proc.stderr.on('data', chunk => { stderr += chunk; });

    proc.on('close', (code) => {
      if (stderr.trim()) {
        process.stderr.write(`[job-aggregator] ${stderr.trim()}\n`);
      }
      if (code !== 0 && !stdout.trim()) {
        reject(new Error(`job-aggregator exited ${code} with no output.\n${stderr.trim()}`));
      } else {
        resolve(stdout);
      }
    });

    proc.on('error', (err) =>
      reject(new Error(`Failed to start '${executable}': ${err.message}\nCheck api_sources.executable in portals.yml`))
    );
  });
}

/**
 * Spawn a process with stdin data piped in, collect stdout.
 */
function spawnWithStdin(executable, args, stdinData) {
  return new Promise((resolve, reject) => {
    const proc = spawn(executable, args, { windowsHide: true });
    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', chunk => { stdout += chunk; });
    proc.stderr.on('data', chunk => { stderr += chunk; });

    proc.on('close', (code) => {
      if (stderr.trim()) {
        process.stderr.write(`[job-aggregator hydrate] ${stderr.trim()}\n`);
      }
      if (code !== 0 && !stdout.trim()) {
        reject(new Error(`job-aggregator hydrate exited ${code} with no output.\n${stderr.trim()}`));
      } else {
        resolve(stdout);
      }
    });

    proc.on('error', (err) =>
      reject(new Error(`Failed to start '${executable}' hydrate: ${err.message}`))
    );

    proc.stdin.write(stdinData, 'utf-8');
    proc.stdin.end();
  });
}

// ── JSONL parser ──────────────────────────────────────────────────────────

/**
 * Parse JSONL output from job-aggregator.
 * First line with schema_version + jobs[] = envelope; all others = records.
 */
function parseJsonl(jsonlText) {
  const lines   = jsonlText.split('\n').filter(l => l.trim());
  const records = [];
  let envelope  = null;

  for (const line of lines) {
    try {
      const obj = JSON.parse(line);
      if (obj.schema_version !== undefined && Array.isArray(obj.jobs)) {
        envelope = obj;
      } else {
        records.push(obj);
      }
    } catch {
      // skip malformed lines silently
    }
  }

  return { envelope, records };
}

// ── Main ──────────────────────────────────────────────────────────────────

async function main() {
  const args   = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');

  // CLI overrides
  const sourcesIdx = args.indexOf('--sources');
  const hoursIdx   = args.indexOf('--hours');
  const queryIdx   = args.indexOf('--query');
  const cliSources = sourcesIdx !== -1 ? args[sourcesIdx + 1] : null;
  const cliHours   = hoursIdx   !== -1 ? Number(args[hoursIdx + 1])  : null;
  const cliQuery   = queryIdx   !== -1 ? args[queryIdx + 1]   : null;

  // 1. Load portals.yml
  if (!existsSync(PORTALS_PATH)) {
    console.error('Error: portals.yml not found. Run onboarding first.');
    process.exit(1);
  }

  const config    = parseYaml(readFileSync(PORTALS_PATH, 'utf-8'));
  const apiConfig = config.api_sources;

  if (!apiConfig) {
    console.error(
      'Error: portals.yml has no api_sources block.\n' +
      'Add an api_sources: section to portals.yml to use scan-api.mjs.\n' +
      'See config/job-aggregator-creds.example.json for the credentials format.'
    );
    process.exit(1);
  }

  if (apiConfig.enabled === false) {
    console.log('api_sources.enabled is false — nothing to do.');
    process.exit(0);
  }

  // 2. Resolve config (CLI flags override portals.yml)
  const executable    = apiConfig.executable || 'job-aggregator';
  const useHydrate    = apiConfig.hydrate !== false; // default true
  const hours         = cliHours  ?? apiConfig.hours    ?? 168;
  const query         = cliQuery  ?? apiConfig.query    ?? null;
  const country       = apiConfig.country   ?? null;
  const location      = apiConfig.location  ?? null;
  const remoteOnly    = apiConfig.remote_only ?? false;
  const usOnly        = apiConfig.us_only     ?? false;
  const credentials   = apiConfig.credentials ?? null;

  const sources       = cliSources
    ? cliSources.split(',').map(s => s.trim()).filter(Boolean)
    : (apiConfig.sources || []);
  const excludeSources = apiConfig.exclude_sources || [];

  const titleFilter = buildTitleFilter(config.title_filter);

  // 3. Build job-aggregator args
  const jobsArgs = ['jobs', '--format', 'jsonl', '--hours', String(hours)];
  if (query)                jobsArgs.push('--query',           query);
  if (location)             jobsArgs.push('--location',        location);
  if (country)              jobsArgs.push('--country',         country);
  if (sources.length)       jobsArgs.push('--sources',         sources.join(','));
  if (excludeSources.length) jobsArgs.push('--exclude-sources', excludeSources.join(','));
  if (credentials)          jobsArgs.push('--credentials',     credentials);

  console.log(`API Scan via job-aggregator — Level 4`);
  console.log(`Sources:    ${sources.length ? sources.join(', ') : 'all registered'}`);
  console.log(`Hours:      ${hours}`);
  console.log(`Hydrate:    ${useHydrate}`);
  console.log(`Remote only: ${remoteOnly}`);
  console.log(`US only:     ${usOnly}`);
  if (dryRun) console.log('(dry run — no files will be written)');
  console.log('');

  // 4. Run job-aggregator jobs
  let jsonlText;
  try {
    console.log(`→ ${executable} ${jobsArgs.join(' ')}`);
    const jobsOutput = await spawnCollect(executable, jobsArgs);

    if (useHydrate) {
      console.log('→ Hydrating listings for full descriptions...');
      try {
        jsonlText = await spawnWithStdin(executable, ['hydrate'], jobsOutput);
      } catch (hydrateErr) {
        console.warn(`Warning: hydrate step failed — using snippet output. (${hydrateErr.message})`);
        console.warn('Affected records will fall back to Playwright at evaluation time.');
        jsonlText = jobsOutput;
      }
    } else {
      jsonlText = jobsOutput;
    }
  } catch (err) {
    console.error(`\nError running job-aggregator: ${err.message}`);
    process.exit(1);
  }

  // 5. Parse JSONL
  const { envelope, records } = parseJsonl(jsonlText);

  if (!records.length) {
    console.log('\nNo records returned from job-aggregator.');
    if (envelope?.sources_failed?.length) {
      console.log(`Sources that failed: ${envelope.sources_failed.join(', ')}`);
    }
    process.exit(0);
  }

  const sourcesUsed = envelope?.sources_used?.join(', ') ?? 'unknown';
  console.log(`\nReceived ${records.length} records from: ${sourcesUsed}`);
  if (envelope?.sources_failed?.length) {
    console.log(`Sources that failed: ${envelope.sources_failed.join(', ')}`);
  }

  // 6. Load dedup state
  const seenUrls        = loadSeenUrls();
  const seenCompanyRoles = loadSeenCompanyRoles();

  // 7. Filter + dedup
  const date = new Date().toISOString().slice(0, 10);
  let totalNoUrl          = 0;
  let totalRemoteFiltered = 0;
  let totalUsFiltered     = 0;
  let totalFiltered       = 0;
  let totalDupes          = 0;
  const newOffers         = [];

  for (const record of records) {
    const title      = record.title    || '';
    const url        = record.url      || '';
    const company    = record.company  || record.source || 'unknown';
    const location_  = record.location || '';
    const sourceKey  = `api-${record.source || 'unknown'}`;

    // Skip records with no URL (can't evaluate or dedup without one)
    if (!url) {
      totalNoUrl++;
      continue;
    }

    // Optional remote filter (keeps null/unknown = pass, only removes explicit false)
    if (remoteOnly && record.remote_eligible === false) {
      totalRemoteFiltered++;
      continue;
    }

    // Optional US-only location filter
    if (usOnly && !isUsEligible(location_)) {
      totalUsFiltered++;
      continue;
    }

    // Title relevance filter (uses portals.yml title_filter)
    if (!titleFilter(title)) {
      totalFiltered++;
      continue;
    }

    // URL dedup
    if (seenUrls.has(url)) {
      totalDupes++;
      continue;
    }

    // Company + role dedup (catches same role posted at a slightly different URL)
    const roleKey = `${company.toLowerCase()}::${title.toLowerCase()}`;
    if (seenCompanyRoles.has(roleKey)) {
      totalDupes++;
      continue;
    }

    // Register as seen to prevent intra-scan dupes
    seenUrls.add(url);
    seenCompanyRoles.add(roleKey);

    newOffers.push({
      title,
      url,
      company,
      location: location_,
      source: sourceKey,
      descriptionSource: record.description_source ?? 'none',
    });
  }

  // 8. Write to pipeline.md + scan-history.tsv
  if (!dryRun && newOffers.length > 0) {
    appendToPipeline(newOffers);
    appendToScanHistory(newOffers, date);
  }

  // 9. Summary
  console.log(`\n${'━'.repeat(50)}`);
  console.log(`API Source Scan (Level 4) — ${date}`);
  console.log(`${'━'.repeat(50)}`);
  console.log(`Records received:        ${records.length}`);
  if (totalNoUrl)          console.log(`Skipped (no URL):        ${totalNoUrl}`);
  if (remoteOnly)          console.log(`Filtered non-remote:     ${totalRemoteFiltered} removed`);
  if (usOnly)              console.log(`Filtered non-US:         ${totalUsFiltered} removed`);
  console.log(`Filtered by title:       ${totalFiltered} removed`);
  console.log(`Duplicates skipped:      ${totalDupes}`);
  console.log(`New offers added:        ${newOffers.length}`);

  if (newOffers.length > 0) {
    console.log('\nNew offers:');
    for (const o of newOffers) {
      const hydrationNote = o.descriptionSource === 'full'
        ? '[full JD]'
        : '[snippet — Playwright fallback at eval]';
      console.log(`  + ${o.company} | ${o.title} | ${o.location || 'N/A'} ${hydrationNote}`);
    }

    if (dryRun) {
      console.log('\n(dry run — run without --dry-run to save results)');
    } else {
      console.log(`\nResults saved to ${PIPELINE_PATH} and ${SCAN_HISTORY_PATH}`);
    }
  }

  console.log(`\n→ Run /career-ops pipeline to evaluate new offers.`);
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
