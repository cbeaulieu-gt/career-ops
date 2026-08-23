// tests/postinstall-playwright-browsers.test.mjs — issue #37.
//
// package.json's `postinstall` script must install THREE Playwright browser
// binaries, additively (`npx playwright install chromium --with-deps`,
// doubled as its own retry fallback via `||`, was the pre-#37 baseline):
//   - chromium: scan-interamt.mjs
//   - firefox:  check-liveness.mjs / liveness-browser.mjs / browser-extract.mjs
//   - msedge:   generate-pdf.mjs (Chromium browser TYPE via the msedge
//               CHANNEL -- page.pdf() is Chromium-only in Playwright, and
//               plain Firefox broke PDF rendering outright; CodeRabbit
//               follow-up on PR #38)
// This is additive, not a replacement: chromium and firefox must keep being
// installed alongside msedge.
import { readFileSync } from 'fs';
import { join } from 'path';
import { pass, fail, ROOT } from './helpers.mjs';

console.log("\npackage.json postinstall — installs chromium, firefox, and msedge (#37)");

const pkg = JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf-8'));
const postinstall = pkg.scripts?.postinstall || '';

if (postinstall) {
  pass('package.json declares a postinstall script');
} else {
  fail('package.json has no postinstall script to inspect');
}

// The existing script is `<install> || <same install again>` -- a same-command
// retry, not two different alternatives. Checking "chromium/firefox/msedge
// appear SOMEWHERE in the whole string" would go green for
// `playwright install chromium firefox --with-deps || playwright install msedge --with-deps`,
// which installs msedge only as a FALLBACK when the chromium+firefox install
// fails -- the opposite of additive, and a plausible mis-edit of the doubled-
// `||` idiom already in this file. Only the primary (pre-`||`) attempt has to
// actually succeed on a normal run, so all three browsers must be requested
// THERE.
const primaryAttempt = postinstall.split('||')[0];
const hasPlaywrightInstall = /playwright\s+install\b/.test(primaryAttempt);
const installsChromium = hasPlaywrightInstall && /\bchromium\b/.test(primaryAttempt);
const installsFirefox = hasPlaywrightInstall && /\bfirefox\b/.test(primaryAttempt);
const installsMsedge = hasPlaywrightInstall && /\bmsedge\b/.test(primaryAttempt);

if (installsChromium) {
  pass('postinstall still installs the chromium binary (scan-interamt.mjs still needs it)');
} else {
  fail(`postinstall no longer installs chromium: "${postinstall}"`);
}

if (installsFirefox) {
  pass('postinstall installs the firefox binary (#37: check-liveness/liveness-browser/browser-extract launch Firefox)');
} else {
  fail(`postinstall does not install firefox: "${postinstall}"`);
}

if (installsMsedge) {
  pass('postinstall installs the msedge binary (#37 follow-up: generate-pdf.mjs launches Chromium via the msedge channel)');
} else {
  fail(`postinstall does not install msedge: "${postinstall}"`);
}
