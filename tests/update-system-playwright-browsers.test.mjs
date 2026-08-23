// tests/update-system-playwright-browsers.test.mjs — issue #37 follow-up.
//
// package.json's `postinstall` script installs the chromium, firefox, and
// msedge Playwright binaries (see tests/postinstall-playwright-browsers.test.mjs).
// But update-system.mjs's `apply` step has ITS OWN separate, hardcoded
// Playwright install call (step "5b", around line 2037) that runs when the
// updater does its own `npm install` and then explicitly triggers a browser
// install. CodeRabbit flagged (PR #38) that this call was never updated
// alongside the postinstall script and still installs only chromium -- so a
// user who updates via this path does not get the other browsers, even
// though:
//   - check-liveness.mjs, liveness-browser.mjs and browser-extract.mjs now
//     default to launching Firefox
//   - generate-pdf.mjs now defaults to launching Chromium via the msedge
//     channel (page.pdf() is Chromium-only in Playwright; plain Firefox
//     broke PDF rendering outright, a second CodeRabbit follow-up on the
//     same PR)
// scan-interamt.mjs stays on plain Chromium, so chromium must keep being
// installed too. All three browsers -- chromium, firefox, msedge -- must be
// requested, additively.
//
// This test inspects the SOURCE TEXT of update-system.mjs rather than
// executing the updater (the "5b" step runs mid-way through a real `apply`
// that also touches git/npm/file state — exercising it end to end belongs to
// a full updater-integration test, not this narrow regression check). It
// pins down two things: the actual `execSync(...)` install call, and the
// manual-install fallback message logged when that call throws — both must
// name all three browsers, not just chromium.
import { readFileSync } from 'fs';
import { join } from 'path';
import { pass, fail, ROOT } from './helpers.mjs';

console.log('\nupdate-system.mjs playwright install — installs chromium, firefox, and msedge (#37)');

const source = readFileSync(join(ROOT, 'update-system.mjs'), 'utf-8');

// Locate the step 5b block specifically, so this test fails for the right
// reason if the browser-install call is ever moved/renamed rather than
// silently matching some unrelated execSync call elsewhere in the file.
const stepMatch = source.match(
  /\/\/ 5b\. Ensure Playwright browser binary is up to date after npm install\s*\n\s*try\s*\{([\s\S]*?)\}\s*catch\s*\{([\s\S]*?)\}/,
);

if (stepMatch) {
  pass('update-system.mjs still has the "5b. Ensure Playwright browser binary" step');
} else {
  fail('update-system.mjs is missing the "5b. Ensure Playwright browser binary" step (moved or renamed?)');
}

const tryBlock = stepMatch ? stepMatch[1] : '';
const catchBlock = stepMatch ? stepMatch[2] : '';

// The try block's execSync call is the one that actually runs on a normal
// update. It must request ALL THREE browsers, additively — chromium and
// firefox must keep being installed, and msedge must be added for
// generate-pdf.mjs's switch to the msedge channel.
const execSyncCallMatch = tryBlock.match(/execSync\(\s*(['"`])(.*?)\1/);
const execSyncArg = execSyncCallMatch ? execSyncCallMatch[2] : '';

if (/\bplaywright\s+install\b/.test(execSyncArg) && /\bchromium\b/.test(execSyncArg)) {
  pass('the execSync call still installs the chromium binary (scan-interamt.mjs still needs it)');
} else {
  fail(`the execSync call in step 5b no longer installs chromium: "${execSyncArg}"`);
}

if (/\bplaywright\s+install\b/.test(execSyncArg) && /\bfirefox\b/.test(execSyncArg)) {
  pass('the execSync call installs the firefox binary (#37: check-liveness/liveness-browser/browser-extract launch Firefox)');
} else {
  fail(`the execSync call in step 5b does not install firefox: "${execSyncArg}"`);
}

if (/\bplaywright\s+install\b/.test(execSyncArg) && /\bmsedge\b/.test(execSyncArg)) {
  pass('the execSync call installs the msedge binary (#37 follow-up: generate-pdf.mjs launches Chromium via the msedge channel)');
} else {
  fail(`the execSync call in step 5b does not install msedge: "${execSyncArg}"`);
}

// The catch block's console.log tells the user how to install manually if
// the automatic install failed. That instruction is stale (and misleading)
// if it omits any of the three browsers now in use.
const manualInstallMatch = catchBlock.match(/console\.log\(\s*(['"`])(.*?)\1/);
const manualInstallMessage = manualInstallMatch ? manualInstallMatch[2] : '';

if (/\bchromium\b/.test(manualInstallMessage)) {
  pass('the manual-install fallback message still mentions chromium');
} else {
  fail(`the manual-install fallback message no longer mentions chromium: "${manualInstallMessage}"`);
}

if (/\bfirefox\b/.test(manualInstallMessage)) {
  pass('the manual-install fallback message mentions firefox (#37)');
} else {
  fail(`the manual-install fallback message does not mention firefox: "${manualInstallMessage}"`);
}

if (/\bmsedge\b/.test(manualInstallMessage)) {
  pass('the manual-install fallback message mentions msedge (#37 follow-up)');
} else {
  fail(`the manual-install fallback message does not mention msedge: "${manualInstallMessage}"`);
}
