"""Stable h12/h13/h15 original semantic component readers, offline only."""
import copy,importlib.util,json
from pathlib import Path

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt25';sp=importlib.util.spec_from_file_location('original',b/'archive/p26_original_proofs.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m);source='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e';prefix='tools/smoke/business-platform-p2-6/';proofs=[];results=[];files={}
 for sid in ('h12','h13','h15'):
  launch=prefix+'results/w10/S2-P25-'+sid+'-outer-launch-attempt1';p={'kind':'p25-actual-healthy-component','epoch':source,'case':sid,'canonical_root_label':str(root),'audit':launch+'/original-audit.json','launch_rc':launch+'/rc','outer':prefix+'results/w10/S2-seven-stage-ready-attempt1/p25-'+sid+'-outer-actual'}
  result,read=m.validate(root,p,source);p['original_files']=read;proofs.append(p);results.append(result);files.update(read);print(sid,len(read),result['gate_assertions'],flush=True)
 old=m.Reader;controls=[]
 for name,index,suffix,was,now in [('h12-rollback-real-name',0,'new-normal.raw',b'--- PASS: TestPGCheckpointAdvanceAllOrNothingRollback',b'--- SKIP: TestPGCheckpointAdvanceAllOrNothingRollback'),('h13-original-busy-SQL',1,'sql-facts-attempt-0134.json',b'skipped_incremental_busy',b'skipped_other_busy'),('h15-original-busy-code',2,'h15-attempt-0009.json',b'sync_incremental_busy',b'sync_incremental_wrong')]:
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
 sp=importlib.util.spec_from_file_location('p25',b/'archive/p26_p25_proofs.py');p25=importlib.util.module_from_spec(sp);sp.loader.exec_module(p25)
 for p in proofs:
  try:p25.full(old(root),p,source);rejected=False
  except (ValueError,KeyError,OSError):rejected=True
  controls.append({'name':p['case']+'-component-cannot-be-full15','rejected':rejected})
 assert all(x['rejected'] and x.get('mutated',True) for x in controls),controls
 e=json.loads((b/'archive/p26_S2_evidence.candidate.json').read_text())
 for p in proofs:
  sid='legacy-S2-P25-'+p['case']+'-component';e['steps']=[x for x in e['steps'] if x['id']!=sid]+[{'id':sid,'classification':'actual','rc_kind':'unix','rc':0,'artifact':p['audit'],'rc_artifact':p['launch_rc'],'proof':p}]
 (b/'archive/p26_S2_evidence.candidate.json').write_text(json.dumps(e,indent=2)+'\n');doc={'results':results,'proofs':proofs,'original_files':files,'unique_originals':len(files),'negative_controls':controls,'all_healthy_components_observed':True,'full_healthy12_bundle_closed':False,'full_P25_15':False,'actual_calls':0};(o/'01-late-healthy-controls.json').write_text(json.dumps(doc,indent=2)+'\n');print('PASS',len(files),len(controls))
if __name__=='__main__':
 import sys
 run(Path(sys.argv[1]).resolve())
