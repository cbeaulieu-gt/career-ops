import test from 'node:test';
import assert from 'node:assert/strict';
import { copyFileSync, mkdirSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { spawnSync } from 'node:child_process';
import { HEADING_BEFORE, HEADING_AFTER, renderRows, renderSection } from '../.github/scripts/sponsors.mjs';

test('sponsor CLI accepts LF and CRLF READMEs while rejecting actual content drift', () => {
  const root = mkdtempSync(join(tmpdir(), 'career-ops-sponsor-eol-'));
  const sponsors = [{ id: 'example', name: 'Example', url: 'https://example.com',
    logo: 'docs/sponsors/example.svg', description: 'Synthetic sponsor', order: 1 }];
  try {
    mkdirSync(join(root, '.github/scripts'), { recursive: true });
    mkdirSync(join(root, 'docs/sponsors'), { recursive: true });
    copyFileSync(new URL('../.github/scripts/sponsors.mjs', import.meta.url), join(root, '.github/scripts/sponsors.mjs'));
    writeFileSync(join(root, '.github/sponsors.json'), JSON.stringify({ sponsors }));
    writeFileSync(join(root, 'docs/sponsors/example.svg'), '<svg/>');
    const english = `# Fixture\n\n${HEADING_BEFORE}\n\nCommunity\n\n${renderSection(sponsors)}\n\n${HEADING_AFTER}\n\nFeatures\n`;
    const translated = `## Translated sponsors\n\n${renderRows(sponsors)}\n\n> Translated note\n`;
    for (const eol of ['\n', '\r\n']) {
      writeFileSync(join(root, 'README.md'), english.replace(/\n/g, eol));
      writeFileSync(join(root, 'README.fr.md'), translated.replace(/\n/g, eol));
      const result = spawnSync(process.execPath, [join(root, '.github/scripts/sponsors.mjs'), '--check'], { encoding: 'utf8' });
      assert.equal(result.status, 0, result.stderr || result.stdout);
    }
    writeFileSync(join(root, 'README.md'), english.replace('Synthetic sponsor', 'Unapproved copy').replace(/\n/g, '\r\n'));
    const drift = spawnSync(process.execPath, [join(root, '.github/scripts/sponsors.mjs'), '--check'], { encoding: 'utf8' });
    assert.equal(drift.status, 1);
    assert.match(drift.stderr, /drifted/);
  } finally {
    rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});
