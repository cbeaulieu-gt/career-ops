// tests/browser-extract-firefox-launch.test.mjs — issue #37.
//
// browser-extract.mjs must launch headless Firefox by default, not Chromium
// (the `chromium.launch({ headless: true })` call inside main(), fed by a
// dynamic `await import('playwright')`).
//
// A sandboxed copy of browser-extract.mjs + its dependencies (liveness-browser.mjs
// for the SSRF guard/context options, lib/cli-flags.mjs for argv validation)
// runs against a stubbed `playwright` module exporting BOTH `chromium` and
// `firefox`. Each browser type's launch() throws a distinguishing marker error
// instead of launching a real browser; main()'s own catch reports it as a JSON
// `navigation_error`, so the marker surfaces on stdout without needing a working
// browser at all. Whichever marker appears names the browser type the real,
// non-injected code path actually invoked.
import { spawnSync } from 'child_process';
import {
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  writeFileSync,
} from 'fs';
import { join } from 'path';
import { pass, fail, rmSync, linkRepoPackage, ROOT, NODE } from './helpers.mjs';

console.log('\nbrowser-extract.mjs — default browser engine (#37)');

const outputRoot = join(ROOT, 'output');
mkdirSync(outputRoot, { recursive: true });
// realpathSync: browser-extract.mjs's own main-guard compares
// `import.meta.url` against `pathToFileURL(process.argv[1])`; a symlinked
// output/ makes them disagree and the spawned script silently no-ops (#3165),
// same as the generate-pdf sandboxes in this suite.
const sandbox = realpathSync(mkdtempSync(join(outputRoot, 'extract-firefox-test-')));
const script = join(sandbox, 'browser-extract.mjs');

copyFileSync(join(ROOT, 'browser-extract.mjs'), script);
copyFileSync(join(ROOT, 'liveness-browser.mjs'), join(sandbox, 'liveness-browser.mjs'));
copyFileSync(join(ROOT, 'liveness-core.mjs'), join(sandbox, 'liveness-core.mjs'));
copyFileSync(join(ROOT, 'user-agent.mjs'), join(sandbox, 'user-agent.mjs'));
mkdirSync(join(sandbox, 'lib'), { recursive: true });
copyFileSync(join(ROOT, 'lib', 'cli-flags.mjs'), join(sandbox, 'lib', 'cli-flags.mjs'));

// browser-extract.mjs does `import * as yaml from 'js-yaml'` at module scope
// (used by resolveExtractorMode); resolves by walking up into node_modules
// from the importer's REALPATH, same as the generate-pdf sandboxes.
linkRepoPackage(sandbox, 'js-yaml');

const playwrightStub = join(sandbox, 'node_modules', 'playwright');
mkdirSync(playwrightStub, { recursive: true });
writeFileSync(join(playwrightStub, 'package.json'), JSON.stringify({
  name: 'playwright',
  type: 'module',
  exports: './index.js',
}), 'utf-8');
writeFileSync(join(playwrightStub, 'index.js'), `
function makeBrowserType(name) {
  return { async launch() { throw new Error('LAUNCH_MARKER:' + name); } };
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
  if (/LAUNCH_MARKER:firefox/.test(out)) {
    pass('browser-extract.mjs launches Firefox by default');
  } else if (/LAUNCH_MARKER:chromium/.test(out)) {
    fail(`browser-extract.mjs still launches Chromium by default:\n${out.trim()}`);
  } else {
    fail(`browser-extract.mjs never reached the browser launch (test harness problem?):\n${out.trim()}`);
  }
} finally {
  rmSync(sandbox, { recursive: true, force: true });
}
