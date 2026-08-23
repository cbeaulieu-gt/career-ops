// tests/scan-interamt-chromium-regression.test.mjs — issue #37 guard rail.
//
// #37 switches generate-pdf.mjs, check-liveness.mjs, liveness-browser.mjs and
// browser-extract.mjs from Chromium to Firefox. scan-interamt.mjs is explicitly
// OUT of scope: Interamt.de is an Apache Wicket portal scanned with a dedicated
// Playwright browser session, and there is no requirement or evidence Firefox
// behaves the same there. This is a regression guard against the fix
// over-applying to a file the issue never asked it to touch.
//
// A static source check, not a launch spy: scan-interamt.mjs's own browser
// session isn't part of the seam under test elsewhere in this suite, so
// asserting on the source text is enough to catch an over-broad find/replace
// while staying decoupled from its (unrelated) runtime behavior.
import { readFileSync } from 'fs';
import { join } from 'path';
import { pass, fail, ROOT } from './helpers.mjs';

console.log('\nscan-interamt.mjs — stays on Chromium (#37 regression guard)');

const source = readFileSync(join(ROOT, 'scan-interamt.mjs'), 'utf-8');

if (/import\s*\{\s*chromium\s*\}\s*from\s*['"]playwright['"]/.test(source)) {
  pass('scan-interamt.mjs still imports { chromium } from playwright');
} else {
  fail('scan-interamt.mjs no longer imports { chromium } from playwright -- Interamt.de is out of scope for #37');
}

if (/chromium\.launch\(/.test(source)) {
  pass('scan-interamt.mjs still launches chromium.launch(...)');
} else {
  fail('scan-interamt.mjs no longer calls chromium.launch(...) -- Interamt.de is out of scope for #37');
}

// Scoped to actual USAGE, not the bare word: a correct fix to the four
// sibling files may legitimately leave a comment here explaining why this
// file was deliberately left alone (e.g. "stays on Chromium -- Wicket portal,
// see #37"), and that prose must not fail this guard.
if (!/\bimport\s*\{[^}]*\bfirefox\b[^}]*\}\s*from\s*['"]playwright['"]/.test(source)) {
  pass("scan-interamt.mjs does not import { firefox } from playwright");
} else {
  fail('scan-interamt.mjs now imports firefox from playwright -- #37 explicitly excludes this file');
}

if (!/\bfirefox\.launch\(/.test(source)) {
  pass('scan-interamt.mjs does not call firefox.launch(...)');
} else {
  fail('scan-interamt.mjs now calls firefox.launch(...) -- #37 explicitly excludes this file');
}
