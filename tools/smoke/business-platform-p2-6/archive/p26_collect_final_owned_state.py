"""Root-released final exact-ID readonly state; no discovery, mutation or private reads."""
import argparse,datetime,hashlib,importlib.util,json,re,subprocess,sys
from pathlib import Path
UUID=re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
PHASE='p26-final-owned-state-readonly'
DEF_FIELDS={'requested_id','def_id','org_id','source_id','output_dataset_id','conn_id','conn_org_id','conn_owner_id','source_name','connector_type','schedule_enabled','schedule_next_fire_at'}
SOURCE_FIELDS={'requested_id','source_id','owner_id','org_id','source_name','connector_type'}
def need(ok,why='final readonly collector refused'):
 if not ok:raise ValueError(why)
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def validate_plan(plan):
 need(plan['schema']=='p26-final-owned-state-plan-v1' and plan['after_ABC_cutoff_complete'] is True and plan['ABC_creation_proof'].keys()>={'path','sha256'},'actual ABC-LAST cutoff proof required; interim plan cannot execute')
 entries,sources=plan['entries'],plan['source_entries'];ids={x['def_id'] for x in entries};sids={x['source_id'] for x in sources};need(entries and sources and len(ids)==len(entries) and len(sids)==len(sources) and len(plan['protected_exclusions'])==19 and not ids&set(plan['protected_exclusions']),'exact known Def/Source set/protected19')
 for row in entries:
  need(all(isinstance(row[k],str) and UUID.fullmatch(row[k]) for k in ('def_id','source_id','output_dataset_id','expected_actor')) and row['source_id'] in sids,'known owned parent IDs only')
 for row in entries+sources:need(UUID.fullmatch(row.get('source_id','')) and UUID.fullmatch(row.get('expected_actor','')) and (row['expected_tenant'] is None or UUID.fullmatch(row['expected_tenant'])) and row['expected_presence'] in ('must_exist','observe_present_or_absent'),'known ownership/presence policy')
 return plan

def validate(release):
 need(release['schema']=='p26-final-owned-state-release-v1' and release['phase']==PHASE and release['root_accepted'] is True,'exact separate Root final readonly authority')
 need(datetime.datetime.now(datetime.timezone.utc)<datetime.datetime.fromisoformat(release['expires_utc'].replace('Z','+00:00')),'Root readonly phase expired')
 need(sha(__file__)==release['collector_sha256'] and sha(release['plan_path'])==release['plan_sha256'],'exact Root collector/plan bytes')
 plan=validate_plan(json.loads(Path(release['plan_path']).read_bytes()));need(str(plan['ABC_creation_proof']['path']).endswith('.json') and '.env' not in str(plan['ABC_creation_proof']['path']) and plan['source_sha256']==release['source_sha256'] and sha(plan['ABC_creation_proof']['path'])==plan['ABC_creation_proof']['sha256'],'exact actual ABC creation closure/S2')
 need(re.fullmatch('[0-9a-f]{64}',release['pg_pin']['cid']) and re.fullmatch('sha256:[0-9a-f]{64}',release['pg_pin']['image']),'exact PG identity pin');return plan

def sql_argv(release,plan):
 defs=','.join("('"+e['def_id']+"'::uuid)" for e in sorted(plan['entries'],key=lambda e:e['def_id']));sources=','.join("('"+e['source_id']+"'::uuid)" for e in sorted(plan['source_entries'],key=lambda e:e['source_id']))
 query="WITH known_defs(requested_id) AS (VALUES "+defs+"), known_sources(requested_id) AS (VALUES "+sources+") SELECT json_build_object('definitions',(SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT k.requested_id::text,d.id::text AS def_id,d.org_id::text,d.source_id::text,d.output_dataset_id::text,c.id::text AS conn_id,c.org_id::text AS conn_org_id,c.owner_id::text AS conn_owner_id,c.name AS source_name,c.connector_type,d.schedule_enabled,d.schedule_next_fire_at::text FROM known_defs k LEFT JOIN batch_sync_defs d ON d.id=k.requested_id LEFT JOIN connections c ON c.id=d.source_id ORDER BY k.requested_id) t),'sources',(SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT k.requested_id::text,c.id::text AS source_id,c.owner_id::text,c.org_id::text,c.name AS source_name,c.connector_type FROM known_sources k LEFT JOIN connections c ON c.id=k.requested_id ORDER BY k.requested_id) t),'grants_count',(SELECT count(*) FROM sync_schedule_grants));"
 return ['docker','exec',release['pg_pin']['cid'],'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','openfoundry','-d','connectors','-c','BEGIN READ ONLY;','-c',query,'-c','COMMIT;']

def compare(plan,actual):
 need(set(actual)=={'definitions','sources','grants_count'} and type(actual['grants_count']) is int and actual['grants_count']==0,'fresh whole schedule grants count must be zero')
 out={}
 for key,entries,idkey,fields in (('definitions',plan['entries'],'def_id',DEF_FIELDS),('sources',plan['source_entries'],'source_id',SOURCE_FIELDS)):
  rows=actual[key];by={r['requested_id']:r for r in rows};need(len(by)==len(rows)==len(entries) and set(by)=={e[idkey] for e in entries},'exact requested ID set, including absent observations')
  for expected in entries:
   row=by[expected[idkey]];need(set(row)==fields,'fixed safe fields only')
   if row[idkey] is None:
    need(expected['expected_presence']=='observe_present_or_absent' and all(v is None for k,v in row.items() if k!='requested_id'),'only explicitly permitted actual absence, never infer original deletion');continue
   need(row[idkey]==expected[idkey],'exact original known ID')
   if key=='definitions':need(row['source_id']==row['conn_id']==expected['source_id'] and row['output_dataset_id']==expected['output_dataset_id'] and row['conn_owner_id']==expected['expected_actor'] and row['org_id']==row['conn_org_id']==expected['expected_tenant'] and row['schedule_enabled'] is False and row['schedule_next_fire_at'] is None,'present owned Def exact parents/identity and schedulefalse/nextNULL')
   else:need(row['owner_id']==expected['expected_actor'] and row['org_id']==expected['expected_tenant'],'present known Source exact actor/tenant')
  out[key]=by
 # A present Def must have a present, same-owner Source in the separate full Source observation.
 for did,row in out['definitions'].items():
  if row['def_id'] is not None:
   src=out['sources'][row['source_id']];need(src['source_id']==row['source_id'] and src['owner_id']==row['conn_owner_id'] and src['org_id']==row['conn_org_id'],'Def-parent JOIN and independent Source row agree')
 return out

def run(path):
 release_bytes=Path(path).read_bytes();release=json.loads(release_bytes);plan=validate(release);output=Path(release['output_dir']);need(output.is_absolute() and not output.exists(),'exclusive original output required');output.mkdir(parents=True,exist_ok=False)
 p26=Path(__file__).resolve().parents[1];need(sha(p26/'common/p26_gates.py')==release['source_gate_sha256']);spec=importlib.util.spec_from_file_location('G',p26/'common/p26_gates.py');G=importlib.util.module_from_spec(spec);spec.loader.exec_module(G)
 def gate():need(Path(path).read_bytes()==release_bytes and G.candidate_hash(p26.parents[2])==release['source_sha256']);validate(release)
 def command(name,argv):
  gate();(output/(name+'.cmd.json')).write_text(json.dumps(argv,indent=2)+'\n')
  try:cp=subprocess.run(argv,capture_output=True,timeout=120)
  except BaseException:
   (output/(name+'.rc')).write_text('unavailable\n');(output/(name+'.meta.json')).write_text(json.dumps({'original_child_rc':None,'failure_kind':'spawn-or-timeout'})+'\n');raise
  (output/(name+'.stdout')).write_bytes(cp.stdout);(output/(name+'.stderr')).write_bytes(cp.stderr);(output/(name+'.rc')).write_text(str(cp.returncode)+'\n');(output/(name+'.meta.json')).write_text(json.dumps({'kind':'Unix-child','original_child_rc':cp.returncode})+'\n');need(cp.returncode==0,'original readonly child nonzero');return cp.stdout.decode()
 try:
  gate();(output/'source-before.json').write_text(json.dumps({'source':release['source_sha256']}))
  fmt='{"cid":{{json .Id}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}}}';before=json.loads(command('PG-before',['docker','inspect',release['pg_pin']['cid'],'--format',fmt]));need(before==dict(release['pg_pin'],running=True),'PG exact pin before')
  raw=command('CURRENT-final-owned-state-SELECT',sql_argv(release,plan));values=[json.loads(line) for line in raw.splitlines() if line.startswith('{')];need(len(values)==1,'one exact original JSON state');state=values[0];by=compare(plan,state);after=json.loads(command('PG-after',['docker','inspect',release['pg_pin']['cid'],'--format',fmt]));need(after==before,'PG unchanged after')
  current={'scope':'DERIVED CURRENT exact-ID presence/identity/schedules; not historical POST or DELETE proof','definitions':by['definitions'],'sources':by['sources'],'grants_count':state['grants_count']};(output/'current-state.json').write_text(json.dumps(current,indent=2)+'\n')
  origin={'schema':'p26-final-owned-current-origin-ledger-v1','plan':{'path':release['plan_path'],'sha256':release['plan_sha256']},'ABC_creation_proof':plan['ABC_creation_proof'],'original_SELECT':{'path':str(output/'CURRENT-final-owned-state-SELECT.stdout'),'sha256':sha(output/'CURRENT-final-owned-state-SELECT.stdout'),'original_rc':0,'rc_file':str(output/'CURRENT-final-owned-state-SELECT.rc')},'current_state':{'path':str(output/'current-state.json'),'sha256':sha(output/'current-state.json')},'definitions':{e['def_id']:{'historical_basis':e,'current_present':by['definitions'][e['def_id']]['def_id'] is not None,'current_pointer':'/definitions/'+e['def_id'],'absence_does_not_assert_original_DELETE':True} for e in plan['entries']},'sources':{e['source_id']:{'historical_basis':e,'current_present':by['sources'][e['source_id']]['source_id'] is not None,'current_pointer':'/sources/'+e['source_id'],'absence_does_not_assert_original_DELETE':True} for e in plan['source_entries']}}
  (output/'origin-ledger.json').write_text(json.dumps(origin,indent=2)+'\n');gate();(output/'source-after.json').write_text(json.dumps({'source':release['source_sha256']}));counts={}
  for kind,key in (('Def','definitions'),('Source','sources')):
   idkey='def_id' if kind=='Def' else 'source_id';rows=list(by[key].values());counts[kind]={'requested':len(rows),'present':sum(r[idkey] is not None for r in rows),'absent':sum(r[idkey] is None for r in rows)}
  standalone=sorted(set(by['sources'])-{e['source_id'] for e in plan['entries']});result={'schema':'p26-final-owned-state-proof-v1','ok':True,'source_sha256':release['source_sha256'],'plan':origin['plan'],'ABC_creation_proof':plan['ABC_creation_proof'],'cutoff':plan['cutoff'],'counts':counts,'standalone_known_Source_ids':standalone,'grants_count':0,'all_present_owned_schedules_disabled_nextNULL':True,'PG_unchanged':True,'PG_before':before,'PG_after':after,'current_state':origin['current_state'],'origin_ledger':{'path':str(output/'origin-ledger.json'),'sha256':sha(output/'origin-ledger.json')},'original_Unix_rcs':[0,0,0],'HTTP_calls':0,'business_mutations':0,'historical_POST_or_DELETE_bodies_reconstructed':False};(output/'RESULT.json').write_text(json.dumps(result,indent=2)+'\n');return 0
 except BaseException:
  (output/'FAILED.json').write_text(json.dumps({'ok':False,'scope':'final readonly exact-ID state collection failed','auto_retry':False})+'\n');return 1
if __name__=='__main__':
 ap=argparse.ArgumentParser();ap.add_argument('--release-file',required=True);args=ap.parse_args()
 try:rc=run(args.release_file)
 except BaseException:print('final owned readonly collection refused; no private data reported',file=sys.stderr);rc=1
 sys.exit(rc)
