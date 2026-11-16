"""Offline failed h9 and later CURRENT-only original readers/controls."""
from pathlib import Path
import copy,importlib.util,json

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt19';sp=importlib.util.spec_from_file_location('op',b/'archive/p26_original_proofs.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
 source='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e';prefix='tools/smoke/business-platform-p2-6/';launch=prefix+'results/w10/S2-P25-h9-outer-launch-attempt1';current=prefix+'results/w10/P25-h9-failed-CURRENT-quiet-attempt1';summary=json.loads((root/launch/'FAILED-SUMMARY.json').read_text())
 proofs=[{'kind':'p25-failed-h9-history','epoch':source,'classification':'failed-history-only','canonical_root_label':str(root),'launch':launch,'outer':prefix+'results/w10/S2-seven-stage-ready-attempt1/p25-h9-outer-actual'}, {'kind':'p25-h9-current-quiet-only','epoch':source,'classification':'CURRENT-readonly-only','canonical_root_label':str(root),'audit':current+'/original-audit.json','base':current,'failed_summary':summary['summary']}]
 results=[];files={}
 for p in proofs:
  result,read=m.validate(root,p,source);p['original_files']=read;results.append(result);files.update(read);print(p['kind'],len(read),flush=True)
 old=m.Reader;controls=[]
 for name,index,suffix,was,now in [('original-failed-verdict',0,'FAILED-SUMMARY.json',b'"original_launch_rc": 1',b'"original_launch_rc": 0'),('original-active1',0,'p25-h9-runtime-actual/summary.json',b'"active": 1',b'"active": 0'),('CURRENT-no-POST',1,'final-quiet-attempt-0001.json',b'"GET"',b'"POST"'),('CURRENT-did-not-amend-failure',1,'p25-h9-runtime-actual/summary.json',b'finally-quiet',b'finally-green')]:
  hit=[False]
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel)
    if rel.endswith(suffix):new=raw.replace(was,now);hit[0]|=new!=raw;return new
    return raw
  m.Reader=Overlay;p=copy.deepcopy(proofs[index]);p.pop('original_files')
  try:m.validate(root,p,source);rejected=False
  except (ValueError,KeyError,TypeError,OSError):rejected=True
  m.Reader=old;controls.append({'name':name,'mutated':hit[0],'rejected':rejected})
 for proof in proofs:
  p=copy.deepcopy(proof);p.pop('original_files');p['classification']='actual-success'
  try:m.validate(root,p,source);rejected=False
  except ValueError:rejected=True
  controls.append({'name':proof['kind']+'-cannot-claim-success','rejected':rejected})
 assert all(x['rejected'] and x.get('mutated',True) for x in controls),controls
 result={'results':results,'proofs':proofs,'original_files':files,'unique_originals':len(files),'negative_controls':controls,'live_calls':0,'CURRENT_SQL_calls':0,'formal_h9_pass':False,'full_P25_15':False}
 (o/'h9-failed-and-CURRENT-contracts.json').write_text(json.dumps(result,indent=2)+'\n');print('PASS',len(controls),'originals',len(files))
if __name__=='__main__':
 import sys
 run(Path(sys.argv[1]).resolve())
