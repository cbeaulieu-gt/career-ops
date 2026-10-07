import test from 'node:test';
import assert from 'node:assert/strict';
import { summarizeScreening } from '../batch/screening-summary.mjs';

test('summary counts every input and separates rejection reasons from execution failure', () => {
  const inputs=[1,2,3,4,5,6,7].map(id=>({id:String(id),url:`https://example.com/${id}`}));
  const states=[{id:'1',status:'completed',report_num:'1'},{id:'2',status:'skipped'},{id:'3',status:'skipped'},{id:'4',status:'skipped'},{id:'5',status:'failed'},{id:'6',status:'needs_confirmation'}];
  const receipts=new Map([['1',{decision:'shortlist',phase:'fit'}],['2',{decision:'filtered'}],['3',{decision:'source_unconfirmed'}],['4',{decision:'inaccessible'}],['5',{decision:'error'}],['6',{decision:'needs_review'}]]);
  const result=summarizeScreening(inputs,states,receipts,new Map([['1','Apply']]));
  assert.deepEqual(result.counts,{discovered:7,filtered:1,source_unconfirmed:1,inaccessible:1,needs_review:1,shortlisted:1,fully_evaluated:1,apply:1,execution_failed:1,awaiting_screening:1,user_skipped:0});
});
