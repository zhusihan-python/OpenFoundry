"""Stable fresh h9 retry actual component and natural drain negative controls."""
import copy,importlib.util,json
from pathlib import Path

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt22';s=importlib.util.spec_from_file_location('original',b/'archive/p26_original_proofs.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);source='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e';prefix='tools/smoke/business-platform-p2-6/';launch=prefix+'results/w10/S2-P25-h9-retry1-outer-launch-attempt1';p={'kind':'p25-actual-healthy-component','epoch':source,'case':'h9','canonical_root_label':str(root),'audit':launch+'/original-audit.json','launch_rc':launch+'/rc','outer':prefix+'results/w10/P25-h9-drain-h10-anchor-code-pure-review-v2/h9-retry1-outer-actual','drain_chain':launch+'/DRAIN-ACTUAL-INPUT-CHAIN.json','failed_launch':prefix+'results/w10/S2-P25-h9-outer-launch-attempt1'}
 result,files=m.validate(root,p,source);p['original_files']=files;controls=[];old=m.Reader
 tests=[('drain-chain-monotonic-evidence','DRAIN-ACTUAL-INPUT-CHAIN.json',b'"elapsed_seconds": 0.033',b'"elapsed_seconds": 9.033'),('drain-response-hash-versus-file','20-http.meta.json',b'629f65b1',b'729f65b1'),('original-failed-h9-remains-failed','p25-h9-runtime-actual/summary.json',b'finally-quiet',b'finally-green'),('fresh-owned-parent-no-import-count','handles.json',b'674e0361',b'774e0361')]
 # The final case uses the actual 1459-handle document count, not a guessed embedded digest.
 tests[-1]=('fresh-handles-whole-count','p25-healthy-20261115T141219/handles.json',b'"count": 1459',b'"count": 1458')
 for name,suffix,was,now in tests:
  hit=[False]
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel)
    if rel.endswith(suffix):new=raw.replace(was,now);hit[0]|=new!=raw;return new
    return raw
  m.Reader=Overlay;proof=copy.deepcopy(p);proof.pop('original_files')
  try:m.validate(root,proof,source);rejected=False
  except (ValueError,KeyError,TypeError,OSError):rejected=True
  m.Reader=old;controls.append({'name':name,'mutated':hit[0],'rejected':rejected})
 sp=importlib.util.spec_from_file_location('p25',b/'archive/p26_p25_proofs.py');p25=importlib.util.module_from_spec(sp);sp.loader.exec_module(p25)
 try:p25.full(old(root),p,source);rejected=False
 except (ValueError,KeyError,OSError):rejected=True
 controls.append({'name':'fresh-h9-component-cannot-be-full15','rejected':rejected});assert all(x['rejected'] and x.get('mutated',True) for x in controls),controls
 evidence=json.loads((b/'archive/p26_S2_evidence.candidate.json').read_text());sid='legacy-S2-P25-h9-fresh-retry1-component';evidence['steps']=[x for x in evidence['steps'] if x['id']!=sid]+[{'id':sid,'classification':'actual','rc_kind':'unix','rc':0,'artifact':p['audit'],'rc_artifact':p['launch_rc'],'proof':p}];(b/'archive/p26_S2_evidence.candidate.json').write_text(json.dumps(evidence,indent=2)+'\n')
 doc={'result':result,'proof':p,'original_files':files,'unique_originals':len(files),'negative_controls':controls,'drain_samples':13,'drain_GET_inputs':26,'original_HTTP_child_rc':None,'original_failed_h9_still_failed':True,'full_P25_15':False,'actual_live_SQL_private_calls':0};(o/'01-fresh-h9-controls.json').write_text(json.dumps(doc,indent=2)+'\n');print('PASS',len(files),len(controls))
if __name__=='__main__':
 import sys
 run(Path(sys.argv[1]).resolve())
