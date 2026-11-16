"""Original independent natural DB-expiry freeze versus immutable h10 anchor."""
import hashlib,json,importlib.util
from pathlib import Path
from datetime import datetime,timezone

def need(ok,why):
 if not ok:raise ValueError(why)
def mod(name):
 sp=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m);return m

def transition(anchor,post,frozen):
 need(post['lease_expired_by_DB_clock'] is False and post['frozen_for_next_phase'] is False,'freeze historical post must remain unexpired/unfrozen')
 need(frozen['row']==post['row']==anchor['row'] and frozen['parent']==post['parent']==anchor['parent'],'freeze full27 and parent cannot change')
 expected=dict(post['cp_owner_row_full']);expected['blocked_reason']='requires_reconciliation'
 need(post['cp_owner_row_full']['blocked_reason']=='' and frozen['cp_owner_row_full']==expected and frozen['lease_expired_by_DB_clock'] is True and frozen['frozen_for_next_phase'] is True,'freeze only computed CP reason transition with actual DB expiry')

def freeze(r,p,source):
 h=mod('p26_h10_actual_reader');ar=mod('p26_h10_anchor_reader');a=r.doc(p['audit']);need(len(a['files'])==245,'freeze exact245 original members')
 for path,pin in a['files'].items():
  need(not path.endswith('.env'),'freeze private env forbidden');raw=r.read('tools/smoke/business-platform-p2-6/'+path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'freeze original SHA/size')
 r.rc(p['launch_rc']);outer=p['outer'];r.rc(outer+'/child/rc')
 for side in ('source-before','source-after'):
  r.rc(outer+'/'+side+'/rc');need(source in r.text(outer+'/'+side+'/stdout.log'),'freeze original source before/after')
 tools=r.doc(outer+'/tools-before.json');need(tools==r.doc(outer+'/tools-after.json'),'freeze actual tools stable')
 for name,digest in tools.items():need(hashlib.sha256(r.read(outer+'/tools-before/'+name)).hexdigest()==digest,'freeze historical imported source')
 rt=json.loads(h.ref(r,p,a['runtime_result']));need(rt['source_S']==source and rt['mode']=='freeze-held' and rt['original_engine_or_runner_rc']==0 and rt['wrapper_exception'] is None and rt['created_safe']=={} and rt['h10_command_counts']=={'kill':0,'start':0},'freeze actual pure-readonly runtime source/rc/no creations/lifecycle')
 actualaudit=json.loads(h.ref(r,p,p['h10_original_audit']));need(a['pre_kill_anchor']==actualaudit['anchor'] and a['old_post_observation_unchanged']==actualaudit['typed_afterpost'],'freeze same original h10 anchor and historical post refs')
 post=json.loads(h.ref(r,p,a['old_post_observation_unchanged']));h.post_state_semantics(post)
 previous=json.loads(h.ref(r,p,p['accepted_previous_CURRENT']));baseline={}
 for record in previous['initial_whole_guard']['records']:
  argv=record.get('argv',[])
  if 'psql' not in argv:continue
  rows=h.rows(record['stdout'].encode());query=argv[-1]
  if "r.control_mode = 'controlled'" in query:baseline['controlled']=rows
  elif "r.control_mode = 'legacy'" in query:baseline['legacy']=rows
  elif 'c.active_run_id IS NOT NULL' in query:baseline['CP']=rows
  elif ' AS def_id' in query:baseline['parents']=rows
  elif rows and set(rows[0])==set(ar.SAFE27):baseline['typed']=rows
  elif rows and set(rows[0])=={'id','recovery_protocol'}:baseline['typed_protocol']=rows
 need(set(baseline)=={'controlled','legacy','CP','parents','typed','typed_protocol'},'freeze complete earlier immutable old/typed source')
 ap=dict(p,anchor={k:a['pre_kill_anchor'][k] for k in ('path','sha256')},baseline_controlled_rows=baseline['controlled'],baseline_CP_rows=baseline['CP'],baseline_parent_rows=baseline['parents']);anchor=ar.anchor(r,ap,source);need(rt['pre_kill_anchor']==ap['anchor'],'freeze runtime carries same immutable prekill anchor')
 frozen=json.loads(h.ref(r,p,a['frozen_typed']));ar.observation(r,ap,anchor,a['frozen_typed'],True);need(frozen['stage']=='freeze-1' and a['actual_samples']==1 and frozen['pre_kill_anchor']==post['pre_kill_anchor'] and frozen['row']==post['row']==anchor['row'] and frozen['parent']==post['parent']==anchor['parent'],'freeze one actual sample/full27/parent immutable across pre/post/freeze')
 transition(anchor,post,frozen)
 receipts=frozen['actual_observation_sql_receipts'];need(len(receipts)==len(a['actual_sql_receipts'])==7 and all(a['actual_sql_receipts'][key]['receipt']==value for key,value in receipts.items()),'freeze exact7 original SQL receipts');parsed={key:h.sql(r,p,receipt) for key,receipt in receipts.items()}
 for key,value in parsed.items():need(len(value)==a['actual_sql_receipts'][key]['observed_row_count'],'freeze raw original row counts')
 for suffix,key in (('Root-typed-full27','typed'),('Root-typed-protocol','typed_protocol'),('whole-legacy','legacy')):
  values=[v for k,v in parsed.items() if k.endswith('-'+suffix)];need(len(values)==1 and values[0]==baseline[key],'freeze RootN/protocol/legacy full immutable originals')
 clock=next(v for k,v in parsed.items() if k.endswith('-actual-protocol-clock'));need(clock==[{'id':anchor['row']['id'],'recovery_protocol':None,'expired':True}] and anchor['protocol_clock']['expired'] is False and post['lease_expired_by_DB_clock'] is False,'freeze actual DB false-to-true sequence; not a fabricated local clock')
 clockreceipt=next(v for k,v in receipts.items() if k.endswith('-actual-protocol-clock'));need('lease_expires_at<=clock_timestamp()' in clockreceipt['argv'][-1],'freeze actual DB-clock query')
 meta=json.loads(h.ref(r,p,clockreceipt['files']['.meta.json']));observed=datetime.strptime(meta['utc'],'%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc);expiry=datetime.fromisoformat(anchor['row']['lease_expires_at']);need(observed>=expiry,'freeze observation follows original immutable lease expiry')
 views={};did=anchor['parent']['def_id'];rid=anchor['row']['id']
 for path in a['files']:
  if '/held-public/' not in path or not path.endswith('.json') or path.endswith('/handles.json'):continue
  event=r.doc('tools/smoke/business-platform-p2-6/'+path);raw=event['resp_body'].encode();need(event['method']=='GET' and event['status']==200 and hashlib.sha256(raw).hexdigest()==event['resp_sha256'] and len(raw)==event['resp_bytes'],'freeze original held public GET body/status');views[event['path'].rsplit('/',1)[-1]]=json.loads(raw)
 need(views=={'checkpoint':frozen['public_view']['baseline_cp'],'runs':frozen['public_view']['baseline_runs']} and len(views['runs'])==1 and views['runs'][0]['id']==rid and views['runs'][0]['status']=='running' and views['checkpoint']['owner']['blocked_reason']=='requires_reconciliation','freeze coherent original public CP/fullRun retains held')
 need(json.loads(h.ref(r,p,frozen['typed_entry']['evidence']))==anchor['row'],'freeze actual exported safe27 original')
 record=json.loads(h.ref(r,p,a['current_runtime_record']));need(a['current_runtime_record']==actualaudit['runtime15_candidate'] and record['source_sha256']==source and len(record['containers'])==15,'freeze current unchanged accepted runtime15')
 runtime_dir=h.local(p,str(Path(a['runtime_result']['path']).parent))+'/';inspections={}
 for path in a['files']:
  rel='tools/smoke/business-platform-p2-6/'+path
  if not rel.startswith(runtime_dir) or not rel.endswith('.cmd.json'):continue
  cmd=r.doc(rel)
  if isinstance(cmd,list) and cmd[:2]==['docker','inspect']:
   name=cmd[2];stem=rel.removesuffix('.cmd.json');r.rc(stem+'.rc');need(r.doc(stem+'.meta.json')['original_rc']==0,'freeze original identity Unix rc');inspections[name]=r.text(stem+'.raw').strip().splitlines()
 need(set(inspections)==set(record['containers']),'freeze exact15 original identity observations')
 for name,pin in record['containers'].items():need(inspections[name]==[pin['cid'],pin['image'],pin['started_at'],'true'],'freeze original runtime unchanged')
 union=json.loads(h.ref(r,p,p['typed_N_plus_frozen_h10']));need(union['schema']=='p26-w10-typed-v1' and len(union['runs'])==len(union['public_views'])==2,'freeze declared exact N+held typed union2');entries={e['run_id']:e for e in union['runs']};need(entries[rid]==frozen['typed_entry'],'freeze declared held entry is actual export');other=[e for id,e in entries.items() if id!=rid];need(len(other)==1 and other[0]['safe_projection_27']==baseline['typed'][0],'freeze N full27 is unchanged original typed entry');held_views=[v for v in union['public_views'] if v['def_id']==did];need(held_views==[frozen['public_view']],'freeze declared held public view exact export')
 return {'kind':p['kind'],'natural_DB_expiry_verified':True,'immutable_anchor_unchanged':True,'pre_and_post_expired':False,'frozen_expired':True,'only_CP_computed_reason_changed':True,'whole_controlled':10,'whole_CP':9,'whole_legacy':3,'typed_N_plus_held':2,'actual_samples':1,'lifecycle_actions':0,'full_P25_15':False}

def h10_complete(r,p,source):
 need(p['crash']['kind']=='p25-h10-actual-crash-component' and p['freeze']['kind']=='p25-h10-natural-freeze-component','h10 final exact two independent component kinds');h=mod('p26_h10_actual_reader');h.actual(r,p['crash'],source);freeze(r,p['freeze'],source);audit=r.doc(p['crash']['audit']);need(p['freeze']['h10_original_audit']['path']==p['canonical_root_label']+'/'+p['crash']['audit'],'h10 freeze belongs to same actual crash audit');runner=json.loads(h.ref(r,p,audit['runner_result']));binding=h.docpath(r,p,runner['tail_known_binding_file']);need(binding['known_bindings']=={'h10-held':audit['new_held']} and binding['post_summary_sha256']==audit['children'][1]['summary']['sha256'],'h10 known tail binding exact actual held/post original');return {'kind':p['kind'],'case':'h10','crash_and_natural_freeze_verified':True,'full_P25_15':False,'binding':binding}
