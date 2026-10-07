import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { join, delimiter } from 'node:path';
import { tmpdir } from 'node:os';
import { ROOT, getBash, rmSync } from './helpers.mjs';

function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'screen-batch-'));
  const batch = join(root, 'batch'), bin = join(root, 'bin');
  mkdirSync(batch); mkdirSync(bin);
  copyFileSync(join(ROOT, 'batch/batch-runner.sh'), join(batch, 'batch-runner.sh'));
  writeFileSync(join(batch, 'batch-prompt.md'), 'Test evaluation prompt');
  writeFileSync(join(batch, 'batch-input.tsv'), 'id\turl\tsource\tnotes\n1\thttps://example.com/jobs/1\tmanual\tAcme | Software Engineer\n');
  writeFileSync(join(root, 'screen-job.mjs'), `
    import fs from 'node:fs'; import path from 'node:path';
    const args=process.argv.slice(2), phase=args[args.indexOf('--phase')+1];
    if(process.env.SCREEN_DECISION==='malformed' && phase!=='metadata') { console.log('not JSON'); }
    else {
    const decision=phase==='metadata' ? (process.env.METADATA_DECISION||'shortlist') : process.env.SCREEN_DECISION;
    const receipt={decision,reason:'Fixture screening reason',evidence:['Fixture JD evidence'],phase};
    const file=args[args.indexOf('--receipt')+1]; fs.mkdirSync(path.dirname(file),{recursive:true});
    fs.writeFileSync(file,JSON.stringify(receipt)); console.log(JSON.stringify(receipt));
    }
  `);
  for (const script of ['merge-tracker.mjs', 'reconcile-pipeline.mjs', 'verify-pipeline.mjs']) writeFileSync(join(root, script), '');
  writeFileSync(join(root, 'reserve-report-num.mjs'), `import fs from 'node:fs';fs.writeFileSync(${JSON.stringify(join(root,'reserved'))},'yes');process.exitCode=99;`);
  for (const command of ['claude', 'curl']) writeFileSync(join(bin, command), `#!/usr/bin/env bash\nprintf invoked > '${join(root, command + '-called').replaceAll('\\','/')}'\nexit 99\n`, { mode: 0o755 });
  return { root, batch, run: env => spawnSync(getBash(), ['-o','igncr',join(batch,'batch-runner.sh'),'--model','evaluation-model'], {
    cwd: root, encoding: 'utf8', timeout: 15000, env: { ...process.env, ...env, PATH: bin + delimiter + process.env.PATH },
  }) };
}

test('metadata rejection stops before curl, reservation and evaluation dispatch', () => {
  const f=fixture();
  try {
    const result=f.run({METADATA_DECISION:'source_unconfirmed'});
    assert.ifError(result.error); assert.equal(result.status,0,result.stdout+result.stderr);
    assert.match(readFileSync(join(f.batch,'batch-state.tsv'),'utf8'), /\tskipped\t/);
    for(const marker of ['reserved','curl-called','claude-called']) assert.equal(existsSync(join(f.root,marker)),false,marker);
  } finally { rmSync(f.root,{recursive:true,force:true}); }
});

test('fit rejection and malformed screening output never reserve reports or launch evaluation', () => {
  for(const [decision,status] of [['filtered','skipped'],['needs_review','needs_confirmation'],['malformed','failed']]) {
    const f=fixture();
    try {
      const result=f.run({SCREEN_DECISION:decision});
      assert.ifError(result.error); assert.equal(result.status,0,result.stdout+result.stderr);
      assert.match(readFileSync(join(f.batch,'batch-state.tsv'),'utf8'),new RegExp('\\t'+status+'\\t'));
      assert.equal(existsSync(join(f.root,'reserved')),false);
      assert.equal(existsSync(join(f.root,'claude-called')),false);
    } finally { rmSync(f.root,{recursive:true,force:true}); }
  }
});
