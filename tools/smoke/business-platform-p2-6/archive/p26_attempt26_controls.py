"""Independent accepted h13-ledger reader and strict pending quiet barriers."""
import copy,importlib.util,json
from pathlib import Path

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt26';sp=importlib.util.spec_from_file_location('original',b/'archive/p26_original_proofs.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m);source='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e';prefix='tools/smoke/business-platform-p2-6/';launch=prefix+'results/w10/S2-P25-h13-ledger-outer-launch-attempt1';p={'kind':'p25-h13-ledger-component','epoch':source,'canonical_root_label':str(root),'audit':launch+'/original-audit.json','launch_rc':launch+'/rc','outer':prefix+'results/w10/S2-seven-stage-ready-attempt1/p25-h13-ledger-outer-actual'}
 result,files=m.validate(root,p,source);p['original_files']=files;controls=[];old=m.Reader
 for name,suffix,was,now in [('verifier-no-fabricated-unix-rc','original-audit.json',b'"separate_process_rc": null',b'"separate_process_rc": 0'),('actual-ledger-token','read/body.json',b'p25h13-tok-0',b'p25h13-tok-X'),('actual-run-window-binding','h13-ledger-proof.json',b'"expected_pages": 1',b'"expected_pages": 2')]:
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
 for name,proof in [('quiet-missing-actual13source-contract',{'kind':'p25-global-quiet-component'}),('component-cannot-be-full15',dict(p,kind='p25-actual-full'))]:
  proof.pop('original_files',None)
  try:m.validate(root,proof,source);rejected=False
  except (ValueError,KeyError,TypeError,OSError):rejected=True
  controls.append({'name':name,'rejected':rejected})
 assert all(x['rejected'] and x.get('mutated',True) for x in controls),controls
 doc={'result':result,'proof':p,'original_files':files,'unique_originals':len(files),'negative_controls':controls,'global_quiet_closed':False,'healthy_bundle_closed':False,'full_P25_15':False,'live_calls':0};(o/'01-h13-ledger-controls.json').write_text(json.dumps(doc,indent=2)+'\n');print('PASS',len(files),len(controls))
if __name__=='__main__':
 import sys
 run(Path(sys.argv[1]).resolve())
