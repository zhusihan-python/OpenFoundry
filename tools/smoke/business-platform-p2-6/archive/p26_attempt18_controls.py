"""Bounded stable h7/h8 original component intake; offline only."""
import copy,importlib.util,json
from pathlib import Path

def mod(name):
 s=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt18';m=mod('p26_original_proofs');p25=mod('p26_p25_proofs');results=[];files={};controls=[]
 prior=json.loads((b/'results/w9/2026-11-15-w9-attempt16/P25-healthy-components-contract.json').read_text());source=prior['source'];proofs=[]
 evidence=json.loads((b/'archive/p26_S2_evidence.candidate.json').read_text());compat=next(x['proof'] for x in evidence['steps'] if x['id']=='legacy-S2-P22-full11')
 for sid in ('h7','h8'):
  p={'kind':'p25-actual-healthy-component','epoch':source,'case':sid,'canonical_root_label':str(Path(root).resolve()),'audit':str(b.relative_to(Path(root))/'results/w10'/('S2-P25-'+sid+'-outer-launch-attempt1')/'original-audit.json'),'outer':str(b.relative_to(Path(root))/'results/w10/S2-seven-stage-ready-attempt1'/('p25-'+sid+'-outer-actual')),'launch_rc':str(b.relative_to(Path(root))/'results/w10'/('S2-P25-'+sid+'-outer-launch-attempt1')/'rc')}
  if sid=='h5':p['h5_legacy_compatibility']=compat
  result,read=m.validate(root,p,source);p['original_files']=read;proofs.append(p);results.append(result);files.update(read)
  print(sid,'original files',len(read),'gates',result['gate_assertions'],flush=True)
 (o/'P25-healthy-components-contract.json').write_text(json.dumps({'source':source,'proofs':proofs,'full_P25_complete':False},indent=2)+'\n')
 old=m.Reader
 mutations=[('h7-binding-stale-original',0,'h7-attempt-0030.json',b'sync_incremental_binding_stale',b'sync_incremental_binding_badxx'),('h8-whole-batch-original',1,'h8-attempt-0014.json',b'since_token',b'wrong_token')]
 for name,index,suffix,was,now in mutations:
  hit=[False]
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel)
    if (suffix is None or rel.endswith(suffix)) and was in raw:
     changed=raw.replace(was,now);hit[0]=changed!=raw;return changed
    return raw
  p=copy.deepcopy(proofs[index]);p.pop('original_files');m.Reader=Overlay
  try:m.validate(root,p,source);rejected=False
  except (ValueError,TypeError,KeyError,OSError):rejected=True
  m.Reader=old;controls.append({'name':name,'mutated':hit[0],'rejected':rejected})
 for p in proofs:
  try:p25.full(old(root),p,source);rejected=False
  except (ValueError,KeyError,OSError):rejected=True
  controls.append({'name':p['case']+'-component-cannot-be-full15','rejected':rejected})
 assert all(x['rejected'] and x.get('mutated',True) for x in controls),controls
 for p in proofs:
  sid='legacy-S2-P25-'+p['case']+'-component';step={'id':sid,'classification':'actual','rc_kind':'unix','rc':0,'artifact':p['audit'],'rc_artifact':p['launch_rc'],'proof':p};evidence['steps']=[x for x in evidence['steps'] if x['id']!=sid]+[step]
 (b/'archive/p26_S2_evidence.candidate.json').write_text(json.dumps(evidence,indent=2)+'\n')
 return {'status':'S2-P25-ACTUAL-h7-h8-COMPONENTS-VERIFIED-DRAFT','results':results,'unique_original_files':len(files),'original_files':files,'negative_controls':controls,'full_healthy12':False,'full_P25_15':False,'live_calls':0,'CURRENT_parent_SQL_calls':0}
if __name__=='__main__':
 import sys
 root=Path(sys.argv[1]).resolve();result=run(root);out=Path(__file__).parents[1]/'results/w9/2026-11-15-w9-attempt18/01-component-controls.json';out.write_text(json.dumps(result,indent=2)+'\n');print('PASS',len(result['negative_controls']),result['unique_original_files'])
