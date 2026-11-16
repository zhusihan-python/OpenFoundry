"""Actual h1/h2 components and targeted byte/scope mutation controls only."""
import copy,importlib.util,json
from pathlib import Path

def mod(name):
 s=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt16';doc=json.loads((o/'P25-healthy-components-contract.json').read_text());m=mod('p26_original_proofs');p25=mod('p26_p25_proofs');results=[];files={};controls=[]
 for proof in doc['proofs']:
  result,read=m.validate(root,proof,doc['source']);results.append(result);files.update(read)
 old=m.Reader
 for name,index,suffix,mutation in [('structured-audit-member',0,'original-audit.json',lambda b:b.replace(b'"original_child_rc": 0',b'"original_child_rc": 1')),('original-engine-rc',0,'runtime-20261115T125507Z-024de6/RESULT.json',lambda b:b.replace(b'"original_engine_or_runner_rc": 0',b'"original_engine_or_runner_rc": 1')),('original-h1-three-run-gates',0,'p25-h1-runtime-actual/summary.json',lambda b:b.replace(b'"ok": true',b'"ok": false',1)),('original-download-observed-SHA',0,'h1-attempt-0016.json',lambda b:b.replace(b'b7a09405',b'c7a09405')),('original-h2-owned-handle-scope',1,'p25-healthy-20261115T125823/handles.json',lambda b:b.replace(b'"count": 7',b'"count": 6')),('original-h1-source-parent',0,'h1-attempt-0020.json',lambda b:b.replace(b'fdba9b77',b'edba9b77'))]:
  hit=[False]
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel)
    if rel.endswith(suffix):changed=mutation(raw);hit[0]|=changed!=raw;return changed
    return raw
  m.Reader=Overlay;p=copy.deepcopy(doc['proofs'][index]);p.pop('original_files',None)
  try:m.validate(root,p,doc['source']);rejected=False
  except (ValueError,TypeError,OSError,KeyError):rejected=True
  controls.append({'name':name,'mutated':hit[0],'rejected':rejected});m.Reader=old
 for name,p in [('h1-component-cannot-be-full15',doc['proofs'][0]),('h2-component-cannot-be-full15',doc['proofs'][1])]:
  try:p25.full(old(root),p,doc['source']);rejected=False
  except (ValueError,KeyError,OSError):rejected=True
  controls.append({'name':name,'rejected':rejected})
 assert all(x['rejected'] and x.get('mutated',True) for x in controls),controls
 ep=b/'archive/p26_S2_evidence.candidate.json';e=json.loads(ep.read_text())
 for proof in doc['proofs']:
  sid='legacy-S2-P25-'+proof['case']+'-component';step={'id':sid,'classification':'actual','rc_kind':'unix','rc':0,'artifact':proof['audit'],'rc_artifact':proof['launch_rc'],'proof':proof};e['steps']=[x for x in e['steps'] if x['id']!=sid]+[step]
 ep.write_text(json.dumps(e,indent=2)+'\n')
 return {'status':'S2-P25-ACTUAL-h1-h2-COMPONENTS-VERIFIED-DRAFT','results':results,'unique_original_files':len(files),'original_files':files,'negative_controls':controls,'full_healthy12':False,'full_P25_15':False,'live_calls':0,'CURRENT_parent_SQL_calls':0}
if __name__=='__main__':
 import sys
 print(json.dumps(run(sys.argv[1]),indent=2))
