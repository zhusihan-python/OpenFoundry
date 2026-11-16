"""New failed-history/current-parent CODE controls; no lifecycle or private IO."""
import copy,hashlib,importlib.util,json,tempfile,datetime
from pathlib import Path

def mod(name):
 s=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt15';original=mod('p26_original_proofs');failed=mod('p26_failed_history_proofs');legacy=mod('p26_legacy_proofs');collector=mod('p26_collect_owned_parents');p=json.loads((o/'P24-retry1-failed-contract.json').read_text());S='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e';r=original.Reader(root);failed.p24_retry1(r,p,S);controls=[]
 def bad(name,fn):
  try:fn();rejected=False
  except (ValueError,KeyError,OSError,TypeError):rejected=True
  controls.append({'name':name,'rejected':rejected})
 for name,suffix,change in [('failed-P24-not-green','FAILED-SUMMARY.json',lambda b:b.replace(b'"original_launch_rc": 1',b'"original_launch_rc": 0')),('failed-original-raw-byte','original-result_p24-20181377b5.json',lambda b:b+b' '),('failed-cleanup-owner-complete','original-result_p24-20181377b5.json',lambda b:b.replace(b'"grant_gone": true',b'"grant_gone": false'))]:
  hit=[False]
  class Overlay(original.Reader):
   def read(self,rel):
    raw=super().read(rel)
    if rel.endswith(suffix):altered=change(raw);hit[0]|=altered!=raw;return altered
    return raw
  bad(name,lambda:failed.p24_retry1(Overlay(root),p,S));controls[-1]['mutated']=hit[0]
 pp=json.loads((o/'P24-retry2-passed-contract.json').read_text());result,files=original.validate(root,pp,S)
 plan=json.loads((o/'exact-current-parent-plan.CANDIDATE.json').read_text());rows=[{'def_id':e['def_id'],'org_id':e['expected_tenant'],'source_id':e['source_id'],'output_dataset_id':e['output_dataset_id'],'conn_id':e['source_id'],'conn_org_id':e['expected_tenant'],'conn_owner_id':e['expected_actor'],'source_name':'NONSECRET-fixture-only','connector_type':'rest_api'} for e in plan['entries']];collector.compare(plan,rows)
 for name,change in [('current-parent-owner-mismatch',lambda x:x[0].update(conn_owner_id='00000000-0000-0000-0000-000000000000')),('current-parent-source-mismatch',lambda x:x[0].update(conn_id='00000000-0000-0000-0000-000000000000')),('current-extra-ID-not-adopted',lambda x:x.append(dict(x[0]))),('unsafe-current-column',lambda x:x[0].update(config='MUST-NOT-BE-SELECTED'))]:
  rr=copy.deepcopy(rows);change(rr);bad(name,lambda rr=rr:collector.compare(plan,rr))
 argv=collector.sql_argv({'pg_pin':{'cid':'a'*64}},plan);assert 'BEGIN READ ONLY;' in argv and all(term not in argv[-3].lower() for term in ['config','credential','password','publish_body','lease_token']);assert argv[-3].count('::uuid')==len(plan['entries'])
 with tempfile.TemporaryDirectory(prefix='NONSECRET-parent-code-') as temp:
  file=Path(temp)/'plan.json';file.write_text(json.dumps(plan));release={'schema':'p26-owned-parent-current-release-v1','phase':collector.PHASE,'root_accepted':True,'expires_utc':(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=2)).isoformat(),'collector_sha256':collector.sha(collector.__file__),'plan_path':str(file),'plan_sha256':collector.sha(file),'source_sha256':S,'pg_pin':{'cid':'a'*64,'image':'sha256:'+'b'*64}};collector.validate(release)
  q=copy.deepcopy(release);q['plan_sha256']='0'*64;bad('Root-exact-plan-SHA',lambda:collector.validate(q))
  q=copy.deepcopy(plan);q['protected_exclusions'].append(q['entries'][0]['def_id']);file.write_text(json.dumps(q));release['plan_sha256']=collector.sha(file);bad('protected-def-exclusion',lambda:collector.validate(release))
 assert all(x['rejected'] and x.get('mutated',True) for x in controls),controls
 return {'status':'NEW CODE/ORIGINALS SLICE ONLY','P24_retry2_result':result,'P24_retry2_original_files':files,'failed_history_and_parent_negative_controls':controls,'parent_SQL_plan':{'exact_Def_IDs':len(plan['entries']),'safe_fields':sorted(collector.FIELDS),'SQL_executed':False},'private_reads':0,'live_calls':0,'actual_package_ops':0}
if __name__=='__main__':
 import sys
 print(json.dumps(run(sys.argv[1]),indent=2))
