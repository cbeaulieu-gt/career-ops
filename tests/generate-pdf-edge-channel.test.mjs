// tests/generate-pdf-edge-channel.test.mjs — issue #37 (CodeRabbit follow-up).
//
// Playwright's `page.pdf()` is implemented only for the Chromium browser
// type -- Firefox's driver never implements PDF generation and throws when
// called. #37 originally switched generate-pdf.mjs to launch Firefox, which
// broke PDF rendering outright. The fix: launch Chromium via the `msedge`
// channel (`chromium.launch({ ...options, channel: 'msedge' })`). This still
// runs Microsoft Edge, not the bundled Chromium binary, but `page.pdf()`
// works because Edge-via-channel is still the `chromium` BROWSER TYPE under
// Playwright's API -- PDF support isn't gated to the bundled binary, it's
// implemented generically in Playwright's Chromium driver code path.
//
// generate-pdf.mjs must launch Chromium with `channel: 'msedge'` by default,
// on BOTH code paths that own the default (non-injected) launchBrowser seam:
//   - renderHtmlToPdf (single-CV render, `node generate-pdf.mjs in.html out.pdf`)
//   - renderBatch (`--batch=<manifest>`)
// A plain `chromium.launch(options)` WITHOUT `channel: 'msedge'` must NOT
// satisfy this test -- the whole point of the fix is Edge specifically, not
// generic (and license-encumbered) Chromium.
//
// Neither call site is exercised through the injectable opts.launchBrowser
// seam here (that seam is what generate-pdf-batch.test.mjs and
// generate-pdf-page-budget.test.mjs use to stub a *working* Chromium) — this
// test instead spies on the real, non-injected default by stubbing the
// `playwright` module itself, so the module-level `import { chromium } from
// 'playwright'` the default falls back to is observable regardless of what
// name the implementation eventually binds it under.
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

console.log('\ngenerate-pdf.mjs — default browser engine is Chromium via msedge channel (#37)');

const outputRoot = join(ROOT, 'output');
mkdirSync(outputRoot, { recursive: true });
// realpathSync: generate-pdf.mjs's `isMain` guard compares the script's own
// realpath against process.argv[1]; a symlinked output/ makes them disagree
// and the spawned script silently no-ops (#3165), matching the other
// generate-pdf sandboxes in this suite.
const sandbox = realpathSync(mkdtempSync(join(outputRoot, 'edge-channel-test-')));
const script = join(sandbox, 'generate-pdf.mjs');

mkdirSync(join(sandbox, 'data'), { recursive: true });
writeFileSync(join(sandbox, 'data', 'pdf-index.tsv'), '', 'utf-8');

copyFileSync(join(ROOT, 'generate-pdf.mjs'), script);
copyFileSync(join(ROOT, 'theme-style.mjs'), join(sandbox, 'theme-style.mjs'));
copyFileSync(join(ROOT, 'tracker-utils.mjs'), join(sandbox, 'tracker-utils.mjs'));
copyFileSync(join(ROOT, 'tracker-parse.mjs'), join(sandbox, 'tracker-parse.mjs'));
copyFileSync(join(ROOT, 'tracker-aliases.json'), join(sandbox, 'tracker-aliases.json'));
copyFileSync(join(ROOT, 'pipeline-lock.mjs'), join(sandbox, 'pipeline-lock.mjs'));

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
  return {
    async launch(options) {
      // Report which browser TYPE was invoked and what channel (if any) was
      // requested, without JSON.stringify-ing the whole options object --
      // launch options can carry values (e.g. functions) that are not
      // guaranteed serializable, and only the channel matters here.
      const channel = options && typeof options === 'object' ? options.channel : undefined;
      throw new Error('LAUNCH_MARKER:' + name + ':channel=' + String(channel));
    },
  };
}
export const chromium = makeBrowserType('chromium');
export const firefox = makeBrowserType('firefox');
`, 'utf-8');

function htmlDoc(body) {
  return `<!doctype html>\n<html>\n  <body>\n    <main>${body}</main>\n  </body>\n</html>\n`;
}
writeFileSync(join(sandbox, 'solo.html'), htmlDoc('Solo CV'), 'utf-8');

function run(args) {
  const result = spawnSync(NODE, [script, ...args], {
    cwd: sandbox,
    encoding: 'utf-8',
    timeout: 30_000,
  });
  return `${result.stdout || ''}${result.stderr || ''}`;
}

function assertLaunchedEdgeChannel(output, label) {
  // Anchored so 'msedge-beta' / 'msedge-dev' -- both real Playwright channels
  // distinct from stable 'msedge' -- do not also satisfy this branch.
  if (/LAUNCH_MARKER:chromium:channel=msedge(?![\w-])/.test(output)) {
    pass(`${label} launches Chromium with channel: 'msedge' by default`);
  } else if (/LAUNCH_MARKER:chromium:channel=undefined/.test(output)) {
    fail(`${label} launches plain Chromium without the msedge channel:\n${output.trim()}`);
  } else if (/LAUNCH_MARKER:firefox:/.test(output)) {
    fail(`${label} still launches Firefox by default (PDF generation is unsupported there):\n${output.trim()}`);
  } else {
    fail(`${label} never reached the browser launch (test harness problem?):\n${output.trim()}`);
  }
}

try {
  // --- single-CV render path: renderHtmlToPdf's default launchBrowser ---
  const single = run(['solo.html', 'out/solo.pdf']);
  assertLaunchedEdgeChannel(single, 'generate-pdf single-CV render (renderHtmlToPdf)');

  // --- --batch path: renderBatch's default launchBrowser ---
  const manifest = join(sandbox, 'batch.json');
  writeFileSync(manifest, JSON.stringify([
    { input: 'solo.html', output: 'out/batch-solo.pdf' },
  ]), 'utf-8');
  const batch = run([`--batch=${manifest}`]);
  assertLaunchedEdgeChannel(batch, 'generate-pdf --batch render (renderBatch)');
} finally {
  rmSync(sandbox, { recursive: true, force: true });
}
