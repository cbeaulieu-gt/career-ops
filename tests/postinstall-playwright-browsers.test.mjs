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
// `||` idiom already in this file. The primary (pre-`||`) attempt has to
// succeed on a normal run, so all three browsers must be requested there --
// but the retry/fallback attempt(s) matter too: a future edit could weaken
// ONLY the segment after `||` (e.g. to `... || playwright install msedge
// --with-deps`) and still install all three browsers on the happy path,
// silently dropping chromium/firefox the moment the retry is actually
// needed. So every `||`-separated attempt is checked, not just the first.
const attempts = postinstall.split('||').map((attempt) => attempt.trim()).filter(Boolean);

// An attempt that doesn't even run `playwright install` can't possibly
// install any of the three browsers, so it's reported as missing all three
// below rather than as its own separate assertion (matching the original
// single-attempt gating behavior).
const isPlaywrightInstall = (attempt) => /playwright\s+install\b/.test(attempt);

const attemptsMissingChromium = attempts.filter((attempt) => !isPlaywrightInstall(attempt) || !/\bchromium\b/.test(attempt));
const attemptsMissingFirefox = attempts.filter((attempt) => !isPlaywrightInstall(attempt) || !/\bfirefox\b/.test(attempt));
const attemptsMissingMsedge = attempts.filter((attempt) => !isPlaywrightInstall(attempt) || !/\bmsedge\b/.test(attempt));

if (attempts.length > 0 && attemptsMissingChromium.length === 0) {
  pass('every postinstall attempt still installs the chromium binary (scan-interamt.mjs still needs it)');
} else {
  fail(`attempt(s) no longer installing chromium: ${JSON.stringify(attemptsMissingChromium)} in "${postinstall}"`);
}

if (attempts.length > 0 && attemptsMissingFirefox.length === 0) {
  pass('every postinstall attempt installs the firefox binary (#37: check-liveness/liveness-browser/browser-extract launch Firefox)');
} else {
  fail(`attempt(s) not installing firefox: ${JSON.stringify(attemptsMissingFirefox)} in "${postinstall}"`);
}

if (attempts.length > 0 && attemptsMissingMsedge.length === 0) {
  pass('every postinstall attempt installs the msedge binary (#37 follow-up: generate-pdf.mjs launches Chromium via the msedge channel)');
} else {
  fail(`attempt(s) not installing msedge: ${JSON.stringify(attemptsMissingMsedge)} in "${postinstall}"`);
}
