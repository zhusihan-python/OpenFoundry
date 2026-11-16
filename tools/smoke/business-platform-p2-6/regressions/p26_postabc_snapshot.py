#!/usr/bin/env python3
"""Fixed actual NEW ABC B/C snapshot; Root phase, SELECT READ ONLY and GET only."""
import argparse,hashlib,json,subprocess,sys
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(P26/'regressions/adapters'))
import w10_guard_shim as H
S='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
LAUNCH=P26/'results/w10/S2-final-ABC3-outer-launch-attempt1'
INPUTS={str(LAUNCH/'ACTUAL-CREATORS-AND-BC-TARGETS.json'):'464edc4e80c3439d2e350556876237b0d2db94ad177ef9ba0efe8ef35946b812',str(LAUNCH/'ORIGINAL-AUDIT.json'):'68ebb478c9afe4d2db3f65e340636d4b21653b2e56c304ab0305aec49882962d'}
ADAPTER=P26/'results/w10/S2-final-sequence-ready-attempt1/p21-ABC-final-commands-actual/adapter.json'
INPUTS[str(ADAPTER)]='0e049cd5ef679d1385bcbc005eb2012d4e6483dec95119f994ce164222e19958'
C_LAST=ADAPTER.parent/'suite-commands-20261115T172350Z-56988b/66-delegate'
for suffix in ['.cmd.json','.raw','.rc','.meta.json']:
 p=Path(str(C_LAST)+suffix)
 INPUTS[str(p)]=json.loads((LAUNCH/'ORIGINAL-AUDIT.json').read_text())['files'][str(p.relative_to(P26))]['sha256']
PARTIAL=P26/'results/w10/S2-postABC-BC-snapshot-outer-launch-attempt1/FAILED-PARTIAL-AUDIT.json'
INPUTS[str(PARTIAL)]='f585a22daa3fa232c8fd58809f3e141dac23f03bc666314f26c285c9a7fb8709'
FIRST_CURRENT=Path(json.loads(PARTIAL.read_text())['completed_SQL_steps'][0]['raw']['path'])
for suffix in ['.cmd.json','.raw','.rc','.meta.json']:
 p=Path(str(FIRST_CURRENT).removesuffix('.raw')+suffix)
 INPUTS[str(p)]=json.loads(PARTIAL.read_text())['files'][str(p.relative_to(P26))]['sha256']

def read(p):return json.loads(Path(p).read_text())
def ref(p):return {'path':str(p),'sha256':H.sha(p)}
def pins():return {str(Path(p).resolve()):H.sha(p) for p in [__file__,H.__file__,H.W.__file__,H.G.__file__,H.W.C.__file__]}
def targets():return read(LAUNCH/'ACTUAL-CREATORS-AND-BC-TARGETS.json')['targets']

CP404={'error':'incremental checkpoint not initialized for this sync definition'}
def semantic_cp(status,body):
 if status==404:
  if body!=CP404:raise ValueError('Actual ordinary CP404 error body contract drift')
  return {} # semantic absence only, NEVER the original HTTP body
 if status!=200 or not isinstance(body,dict):raise ValueError('Checkpoint response invalid')
 return body

def compare_old_view(view,status,body,runs_status,runs):
 semantic=semantic_cp(status,body)
 if status!=view['checkpoint_status'] or semantic!=view['baseline_cp'] or runs!=view['baseline_runs']:raise ValueError('Immutable old typed7 public basis drift')
 compared=H.W.compare_public_view(view['def_id'],view['baseline_cp'],view['baseline_runs'],200 if status==404 else status,semantic,runs_status,runs)
 if not compared['ok']:raise ValueError('Existing public comparator refused observation')
 return compared

def ordinary_public_view(view,status,body,runs,cp_step,run_step):
 def receipt(step):return {suffix:ref(Path(str(step)+suffix)) for suffix in ['.cmd.json','.raw','.rc','.meta.json']}
 return {'def_id':view['def_id'],'actor':view['actor'],'tenant':view['tenant'],'checkpoint_status':status,'baseline_cp':semantic_cp(status,body),'baseline_runs':runs,'actual_checkpoint_body':body,'checkpoint_step':cp_step,'runs_step':run_step,'checkpoint_receipt':receipt(cp_step),'runs_receipt':receipt(run_step),'checkpoint_semantics':'baseline_cp {} is semantic absence; actual HTTP404 error body and original receipts are preserved separately'}

def original_C_fields():
 if Path(str(C_LAST)+'.rc').read_text().strip()!='0' or read(str(C_LAST)+'.meta.json').get('scrub_hits')!=0:raise ValueError('Original C SELECT rc nonzero or scrubbed')
 command=read(str(C_LAST)+'.cmd.json')['replacement']
 if not any("WHERE id = 'df354604-b389-45a1-ad5b-da7481543606'::uuid" in x for x in command):raise ValueError('Original C SELECT target drift')
 values=Path(str(C_LAST)+'.raw').read_text().strip().split('|')
 if len(values)!=5 or values[1] not in ('t','f') or values[4] not in ('t','f'):raise ValueError('Actual original C five-column shape required')
 return dict(zip(['status','has_holder','heartbeat_at','lease_expires_at','has_token'],[values[0],str(values[1]=='t').lower(),values[2],values[3],str(values[4]=='t').lower()]))

def recorded_get(att,tag,url,token=None,transport=None):
 step=att.step_begin(tag,{'method':'GET','url':url})
 try:resp=(transport or H.W.http_call)(url,auth=token)
 except Exception as exc:
  att.step_finish(tag,step,type(exc).__name__,'unavailable',{'original_child_rc':None,'transport_outcome':1,'status':None,'http_status':None,'failure_kind':type(exc).__name__},kind='http');raise
 outcome=0 if resp.status is not None and not resp.error else 1
 att.step_finish(tag,step,resp.body,'unavailable',{'original_child_rc':None,'transport_outcome':outcome,'status':resp.status,'http_status':resp.status,'error':resp.error,'body_sha256':hashlib.sha256(resp.body).hexdigest(),'body_bytes':len(resp.body)},kind='http')
 att.last_http_step=step
 if outcome:raise ValueError('Snapshot GET transport failure')
 return resp.status,json.loads(resp.body)

def gate(path):
 if Path(path).is_symlink() or Path(path).stat().st_mode & 0o777 != 0o600:raise ValueError('Private Root file mode/type required')
 r=H.validate_release(path,stage='postABC-BC-readonly-snapshot',suite='final')
 if r.get('candidate_source')!=S or H.G.candidate_hash(H.G.OPENFOUNDRY)!=S or r.get('snapshot_tools')!=pins() or r.get('snapshot_inputs')!=INPUTS or r.get('snapshot_targets')!=targets():raise ValueError('Root exact snapshot source/tools/origins/targets required')
 for p,h in INPUTS.items():
  if H.sha(p)!=h:raise ValueError('ABC actual origin drift')
 for key in ['typed_roster','deployment_record']:
  if H.sha(r[key])!=r[key+'_sha256']:raise ValueError('Root snapshot basis drift')
 if r['typed_roster_sha256']!='52a66a0e66fbc18b5062ef8740f25fd1698de3470b2d81f991c266c105170fd1' or r['deployment_record_sha256']!='d34c1c63bd539b9069e1c85d6d57ce4623b1843b4c82ef2a1bf5aed60969f5aa':raise ValueError('Only actual main5 and immutable original typed7 permitted')
 if not Path(r['snapshot_output']).resolve().is_relative_to(P26/'results/w10'):raise ValueError('NEW output required')
 return r

def sql_plan(pg,ts):
 ids=','.join("'"+x['run_id']+"'::uuid" for x in ts)
 full=H.W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'][-1]
 predicate="r.control_mode = 'controlled' AND r.status IN ('pending','running')"
 if full.count(predicate)!=1:raise ValueError('safe27 predicate drift')
 full=full.replace(predicate,'r.id IN ('+ids+')')
 cp=H.W.C.OLDPG_READONLY_ARGV['cp_owners'][-1]
 predicate='WHERE c.active_run_id IS NOT NULL'
 if cp.count(predicate)!=1:raise ValueError('CP predicate drift')
 cp=cp.replace(predicate,'WHERE c.active_run_id IN ('+ids+')')
 aggregate='SELECT json_agg(row_to_json(t))'
 if cp.count(aggregate)!=1:raise ValueError('CP aggregate shape drift')
 cp=cp.replace(aggregate,"SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json)",1)
 parents=H.W.build_parents_argv({x['def_id'] for x in ts})[-1]
 protocol="SELECT json_agg(row_to_json(t)) FROM (SELECT id::text,recovery_protocol,(lease_expires_at IS NOT NULL AND lease_expires_at<=clock_timestamp()) AS expired FROM sync_runs WHERE id IN ("+ids+")) t;"
 if pg!={'cid':'604060baaa88095d64f00ae055f60a6bf1cdc48011db650f2498d506979967e8','user':'openfoundry','database':'connectors'}:raise ValueError('Only fixed actual PG observer')
 base=['docker','exec',pg['cid'],'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','openfoundry','-d','connectors']
 return {tag:base+['-c','BEGIN READ ONLY;','-c',"SET TimeZone='UTC'; SET DateStyle='ISO, MDY';",'-c',sql,'-c','COMMIT;'] for tag,sql in [('safe27',full),('CP',cp),('parents',parents),('protocol-clock',protocol)]}

def validate_rows(ts,rows,parents,protocol,cps):
 if len(rows)!=2 or len(parents)!=2 or len(protocol)!=2:raise ValueError('Exact two BC rows/parents/protocol required')
 H.exact(json.loads(FIRST_CURRENT.read_text()),rows,'id') # immutable 17:46 CURRENT, never historical ABC full27
 for t in ts:
  found=[r for r in rows if r['id']==t['run_id']];ps=[p for p in parents if p['def_id']==t['def_id']];pr=[p for p in protocol if p['id']==t['run_id']]
  if len(found)!=1 or len(ps)!=1 or len(pr)!=1:raise ValueError('BC binding missing/duplicate')
  row,p=found[0],ps[0];H.W.C._check_row_schema(row,'actual ABC',0)
  want={'sync_def_id':t['def_id'],'source_id':t['source_id'],'output_dataset_id':t['output_dataset_id'],'org_id':t['tenant'],'owner_id':t['actor'],'run_owner_id':t['actor'],'requested_by':t['actor'],'status':'running','control_mode':'controlled','source_name':t['source_name'],'fixture_src':t['fixture_src']}
  if any(row[k]!=v for k,v in want.items()) or any(p[k]!=v for k,v in {'source_id':t['source_id'],'output_dataset_id':t['output_dataset_id'],'conn_id':t['source_id'],'org_id':t['tenant'],'conn_org_id':t['tenant'],'conn_owner_id':t['actor'],'source_name':t['source_name']}.items()):raise ValueError('Actual BC parent/actor/status drift')
  if t['case']=='B':
   frozen=next(iter(read(ADAPTER)['runtime_frozen_B'].values()))
   H.exact([frozen['safe_projection_27']],[row],'id')
   if pr[0]!={'id':t['run_id'],'recovery_protocol':None,'expired':True}:raise ValueError('Original frozen B protocol/expiry drift')
  else:
   if pr[0]['recovery_protocol'] is not None:raise ValueError('Original legacy C must retain NULL recovery protocol')
   if any(row[k]!=v for k,v in original_C_fields().items()):raise ValueError('C actual original five-column projection drift')
 if cps:raise ValueError('Ordinary snapshot B/C must not acquire incremental CP owners')

def parse_snapshot_rows(tag,stdout):
 if tag!='CP':return H.W.C._parse_rows_stdout(stdout,tag)
 rows=json.loads(stdout)
 if not isinstance(rows,list):raise ValueError('CP SQL must explicitly return a JSON list')
 return rows

def run(release_path):
 r=gate(release_path);out=Path(r['snapshot_output']).resolve();out.mkdir(parents=True,exist_ok=False);att=H.W.Attempt(out,'snapshot');ts=targets();data={};receipts={}
 def current():
  if gate(release_path)!=r:raise ValueError('Root phase changed')
 def command(tag,argv):
  current();step=att.step_begin(tag,argv)
  try:cp=subprocess.run(argv,capture_output=True,text=True,timeout=30)
  except Exception as exc:
   att.step_finish(tag,step,type(exc).__name__,'unavailable',{'original_child_rc':None,'failure_kind':'spawn-or-timeout'});raise
  att.step_finish(tag,step,cp.stdout,cp.returncode,{'original_child_rc':cp.returncode,'stderr':cp.stderr})
  if cp.returncode:raise ValueError('Actual command nonzero; no retry')
  return cp,step
 for tag,argv in sql_plan(r['w7_pg_observer'],ts).items():
  cp,step=command(tag,argv)
  data[tag]=parse_snapshot_rows(tag,cp.stdout);receipts[tag]={'prefix':step,'original_rc':cp.returncode}
 validate_rows(ts,data['safe27'],data['parents'],data['protocol-clock'],data['CP'])
 secret=H.W.read_jwt_secret();att.add_secret(secret)
 def get(tag,url,token=None):
  current();return recorded_get(att,tag,url,token)
 old=read(r['typed_roster']);public=[]
 views=old['public_views']+[{'def_id':t['def_id'],'actor':t['actor'],'tenant':t['tenant'],'case':t['case']} for t in ts]
 for v in views:
  token=H.W.sign_testing_jwt(secret,v['actor'],v['tenant']);att.add_secret(token);url=H.W.ORIGINS['connector']+'/api/v1/data-connection/syncs/'+v['def_id'];cs,c=get('CP',url+'/checkpoint',token);cp_step=att.last_http_step;rs,runs=get('runs',url+'/runs',token);run_step=att.last_http_step;runs=runs.get('runs') if isinstance(runs,dict) else runs
  if rs!=200 or not isinstance(runs,list):raise ValueError('Full Run list failed')
  if 'case' not in v:
   compare_old_view(v,cs,c,rs,runs)
  else:
   target=next(t for t in ts if t['def_id']==v['def_id'])
   if cs!=404 or c!={'error':'incremental checkpoint not initialized for this sync definition'} or len(runs)!=1 or runs[0]['id']!=target['run_id'] or runs[0]['sync_def_id']!=target['def_id'] or runs[0]['status']!='running':raise ValueError('BC public scope/CP drift')
   if any(runs[0].get(k)!=v for k,v in {'org_id':target['tenant'],'owner_id':target['actor'],'requested_by':target['actor'],'control_mode':'controlled'}.items()):raise ValueError('BC public Run actor/tenant/protocol drift')
   public.append(ordinary_public_view(v,cs,c,runs,cp_step,run_step))
 entries=[]
 for target in ts:
  row=next(x for x in data['safe27'] if x['id']==target['run_id']);parent=next(x for x in data['parents'] if x['def_id']==target['def_id']);protocol=next(x for x in data['protocol-clock'] if x['id']==target['run_id']);path=out/('safe27-'+target['case']+'.json');path.write_text(json.dumps(row,indent=2)+'\n')
  entries.append({**{k:target[k] for k in ('run_id','def_id','source_id','actor','tenant')},'control_mode':row['control_mode'],'protocol':'legacy-null' if protocol['recovery_protocol'] is None else protocol['recovery_protocol'],'purpose':'actual final ABC '+target['case']+' unknown preserved; no adoption/clear','expected_state':row['status'],'pubentered':row['gate_entered'],'safe_projection_27':row,'parent':parent,'evidence':{**ref(path),'json_pointer':''}})
 current();result={'ok':True,'entries':entries,'public_views':public,'SQL':data,'SQL_receipts':receipts,'old_typed7_unchanged':ref(r['typed_roster']),'needs_independent_Root_whole_guard':True};att.finish(result);print(json.dumps({'ok':True,'snapshot':str(att.dir/'RESULT.json')}));return 0

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);args=p.parse_args()
 try:sys.exit(run(args.release_file))
 except Exception as exc:print('postABC snapshot refused: '+type(exc).__name__,file=sys.stderr);sys.exit(1)
