// tests/postinstall-playwright-browsers.test.mjs — issue #37.
//
// package.json's `postinstall` script currently installs only the Chromium
// Playwright binary (`npx playwright install chromium --with-deps`, doubled as
// its own retry fallback via `||`). After #37, generate-pdf.mjs,
// check-liveness.mjs, liveness-browser.mjs and browser-extract.mjs switch to
// Firefox, while scan-interamt.mjs stays on Chromium -- so postinstall must
// install BOTH browser binaries. This is additive, not a replacement: Chromium
// must keep being installed for scan-interamt.mjs's sake.
import { readFileSync } from 'fs';
import { join } from 'path';
import { pass, fail, ROOT } from './helpers.mjs';

console.log("\npackage.json postinstall — installs both Chromium and Firefox (#37)");

const pkg = JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf-8'));
const postinstall = pkg.scripts?.postinstall || '';

if (postinstall) {
  pass('package.json declares a postinstall script');
} else {
  fail('package.json has no postinstall script to inspect');
}

// The existing script is `<install> || <same install again>` -- a same-command
// retry, not two different alternatives. Checking "chromium and firefox appear
// SOMEWHERE in the whole string" would go green for
// `playwright install chromium --with-deps || playwright install firefox --with-deps`,
// which installs Firefox only as a FALLBACK when the Chromium install fails --
// the opposite of additive, and a plausible mis-edit of the doubled-`||` idiom
// already in this file. Only the primary (pre-`||`) attempt has to actually
// succeed on a normal run, so both browsers must be requested THERE.
const primaryAttempt = postinstall.split('||')[0];
const hasPlaywrightInstall = /playwright\s+install\b/.test(primaryAttempt);
const installsChromium = hasPlaywrightInstall && /\bchromium\b/.test(primaryAttempt);
const installsFirefox = hasPlaywrightInstall && /\bfirefox\b/.test(primaryAttempt);

if (installsChromium) {
  pass('postinstall still installs the chromium binary (scan-interamt.mjs still needs it)');
} else {
  fail(`postinstall no longer installs chromium: "${postinstall}"`);
}

if (installsFirefox) {
  pass('postinstall installs the firefox binary (#37: generate-pdf/check-liveness/browser-extract now launch Firefox)');
} else {
  fail(`postinstall does not install firefox: "${postinstall}"`);
}
