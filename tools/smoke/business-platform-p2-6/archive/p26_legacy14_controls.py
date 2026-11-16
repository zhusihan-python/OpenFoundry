"""Stable stage original baselines and in-memory mutation controls only."""
import copy,importlib.util,json
from pathlib import Path

def run(root):
 b=Path(__file__).parents[1];sp=importlib.util.spec_from_file_location('o',b/'archive/p26_original_proofs.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m);path=b/'results/w9/2026-11-15-w9-attempt14/S2-legacy-stages-contract.json';doc=json.loads(path.read_text());files={};results=[]
 for proof in doc['proofs']:
  result,read=m.validate(root,proof,doc['source']);results.append(result);files.update(read)
 controls=[];old=m.Reader
 for name,index,suffix,mutation in [('P1-full-named9',0,'p1-report.json',lambda b:b.replace(b'"pass": 9',b'"pass": 8')),('P22-L-entered-gate',1,'original-result_p22.json',lambda b:b.replace(b'"gate_entered": true',b'"gate_entered": false')),('P22-outer-child-rc',1,'child/rc',lambda b:b'1'),('P23-holder-stream-never-normalize0',2,'358-holder-acquire.rc',lambda b:b'0'),('P23-original-NOWAIT55P03',2,'362-delegate.meta.json',lambda b:b.replace(b'55P03',b'55P04')),('P23-real-backend-PID',2,'358-holder-acquire.raw',lambda b:b.replace(b'30191',b'30192')),('P23-owned-rollback',2,'368-holder-release.cmd.json',lambda b:b.replace(b'ROLLBACK',b'COMMIT')),('P23-source-snapshot',2,'before-o-reject.json',lambda b:b+b' ')]:
  hit=[False]
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel)
    if rel.endswith(suffix):changed=mutation(raw);hit[0]|=changed!=raw;return changed
    return raw
  m.Reader=Overlay;p=copy.deepcopy(doc['proofs'][index]);p.pop('original_files',None)
  try:m.validate(root,p,doc['source']);rejected=False
  except (ValueError,TypeError,KeyError,OSError):rejected=True
  controls.append({'name':name,'mutated':hit[0],'rejected':rejected});m.Reader=old
 assert all(x['mutated'] and x['rejected'] for x in controls),controls
 return {'status':'S2-P1pre9-P22-P23-ORIGINAL-STAGES-DRAFT','results':results,'unique_original_files':len(files),'original_files':files,'negative_controls':controls,'full_legacy_complete':False,'live_calls':0}
if __name__=='__main__':
 import sys
 print(json.dumps(run(sys.argv[1]),indent=2))
