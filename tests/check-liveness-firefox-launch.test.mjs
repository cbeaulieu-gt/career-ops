// tests/check-liveness-firefox-launch.test.mjs — issue #37.
//
// check-liveness.mjs must launch headless Firefox by default, not Chromium —
// both for the primary lazy browser (ensureBrowser's `chromium.launch` at line
// 68) and for the headed Cloudflare-challenge retry it wires through
// liveness-browser.mjs's `createHeadedPageProvider` (`chromium.launch` inside
// that function, used only when check-liveness.mjs hands it a browser type).
//
// createHeadedPageProvider itself is already parameterized over whichever
// browser type its caller passes in, so the actual switch lives entirely in
// check-liveness.mjs's two call sites: `chromium.launch({headless:true})` in
// ensureBrowser, and `createHeadedPageProvider(chromium)`. This test observes
// both from the outside by stubbing the `playwright` module check-liveness.mjs
// imports, rather than asserting on liveness-browser.mjs's internals directly.
//
// A sandboxed copy of check-liveness.mjs + its liveness-*.mjs dependencies runs
// against a stubbed `playwright` module exporting BOTH `chromium` and
// `firefox`. Each browser type's launch() records which one fired (and
// whether headless or headed) instead of actually launching a browser. The
// stub page always answers HTTP 403, which liveness-core.mjs classifies as
// `access_blocked` -- a challenge code -- so a single URL check deterministically
// exercises BOTH the initial headless launch and the headed retry launch in
// one run.
import { spawnSync } from 'child_process';
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  writeFileSync,
} from 'fs';
import { join } from 'path';
import { pass, fail, rmSync, ROOT, NODE } from './helpers.mjs';

console.log('\ncheck-liveness.mjs — default browser engine (#37)');

const outputRoot = join(ROOT, 'output');
mkdirSync(outputRoot, { recursive: true });
const sandbox = mkdtempSync(join(outputRoot, 'liveness-firefox-test-'));
const script = join(sandbox, 'check-liveness.mjs');

copyFileSync(join(ROOT, 'check-liveness.mjs'), script);
copyFileSync(join(ROOT, 'liveness-browser.mjs'), join(sandbox, 'liveness-browser.mjs'));
copyFileSync(join(ROOT, 'liveness-core.mjs'), join(sandbox, 'liveness-core.mjs'));
copyFileSync(join(ROOT, 'liveness-api.mjs'), join(sandbox, 'liveness-api.mjs'));
copyFileSync(join(ROOT, 'user-agent.mjs'), join(sandbox, 'user-agent.mjs'));

const playwrightStub = join(sandbox, 'node_modules', 'playwright');
mkdirSync(playwrightStub, { recursive: true });
writeFileSync(join(playwrightStub, 'package.json'), JSON.stringify({
  name: 'playwright',
  type: 'module',
  exports: './index.js',
}), 'utf-8');
writeFileSync(join(playwrightStub, 'index.js'), `
import { appendFile } from 'fs/promises';

// Always answers HTTP 403, which liveness-core.mjs classifies as
// \`access_blocked\` -- a challenge code -- so checkUrlLivenessWithFallback
// always retries once through the headed browser. That makes both launch
// sites (the initial headless browser and the headed challenge retry)
// observable from a single URL check. No route/frames methods are defined:
// checkUrlLiveness() only uses them when present (typeof-guarded), so this
// stays a minimal double.
function makeStubPage() {
  return {
    async goto() { return { status: () => 403 }; },
    async waitForTimeout() {},
    url() { return 'https://example.com/careers/job-1'; },
    async evaluate(fn) {
      // extractApplyControls is assigned to a const in liveness-browser.mjs,
      // so JS name-inference gives it that .name; the bodyText probe is an
      // inline anonymous arrow passed directly as an argument, so its .name
      // is ''. That's enough to shape the two return types classifyLiveness
      // expects (an array of controls vs. a body-text string) without
      // actually running either callback against a real DOM.
      return typeof fn === 'function' && fn.name === 'extractApplyControls' ? [] : '';
    },
  };
}

function makeBrowserType(name) {
  return {
    async launch(options) {
      await appendFile('.launches', name + ':' + (options && options.headless) + '\\n');
      return {
        async newContext() {
          return { async newPage() { return makeStubPage(); }, async close() {} };
        },
        async newPage() { return makeStubPage(); },
        async close() {},
      };
    },
  };
}

export const chromium = makeBrowserType('chromium');
export const firefox = makeBrowserType('firefox');
`, 'utf-8');

function run(args) {
  const result = spawnSync(NODE, [script, ...args], {
    cwd: sandbox,
    encoding: 'utf-8',
    timeout: 30_000,
  });
  return `${result.stdout || ''}${result.stderr || ''}`;
}

try {
  const out = run(['https://example.com/careers/job-1']);
  const launchesFile = join(sandbox, '.launches');
  const launches = existsSync(launchesFile)
    ? readFileSync(launchesFile, 'utf-8').trim().split('\n').filter(Boolean)
    : [];

  // Browser NAME only for the first launch: firefox.launch() is headless by
  // default, so a correct fix that drops an explicit `{ headless: true }` and
  // just calls `firefox.launch()` would record 'firefox:undefined' -- still
  // correct, and must not be failed on the exact options object.
  if (launches.length >= 1 && launches[0].startsWith('firefox:')) {
    pass('check-liveness.mjs launches Firefox by default (ensureBrowser)');
  } else {
    fail(`check-liveness.mjs's default browser launch did not use Firefox: launches=${JSON.stringify(launches)}\n${out.trim()}`);
  }

  if (launches.length >= 2 && launches[1] === 'firefox:false') {
    pass('check-liveness.mjs retries the Cloudflare-challenge fallback in headed Firefox, not Chromium');
  } else {
    fail(`check-liveness.mjs's headed challenge-retry launch did not use Firefox: launches=${JSON.stringify(launches)}\n${out.trim()}`);
  }
} finally {
  rmSync(sandbox, { recursive: true, force: true });
}
