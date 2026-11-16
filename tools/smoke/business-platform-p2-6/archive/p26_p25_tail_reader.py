"""Independent actual tail: complete gates, whole public rosters and SQL exports."""
import hashlib,json,importlib.util
from pathlib import Path

def need(ok,why):
 if not ok:raise ValueError(why)
def mod(n):
 s=importlib.util.spec_from_file_location(n,Path(__file__).with_name(n+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def cases(summary,registry):
 need(summary['phase']=='tail' and summary['failed']==[] and len(summary['scenarios'])==2 and {x['id'] for x in summary['scenarios']}=={'h11','h14'},'tail mandatory exact two cases')
 for row in summary['scenarios']:
  mod('p26_p25_proofs').good_case(row,registry[row['id']]);need(len(row['gates'])=={'h11':4,'h14':5}[row['id']],'tail mandatory complete gate count')

def whole(summary,obs,defs,bindings):
 need(set(obs)=={(d,s) for d in defs for s in ('','checkpoint','runs')},'tail actual whole28 Def/checkpoint/fullRun observations')
 owners=[];live=[]
 for did in defs:
  de,cp,runs=(obs[(did,k)] for k in ('','checkpoint','runs'));need(de['id']==did and isinstance(runs,list) and len({r['id'] for r in runs})==len(runs),'tail whole Def and full unique Run shape')
  owner=cp.get('owner')
  if owner:
   owners.append({'def_id':did,'source_id':de['source_id'],'run_id':owner['run_id'],'run_status':owner['run_status'],'blocked_reason':owner['blocked_reason'],'source':'finally-actual'})
   need(any(r['id']==owner['run_id'] and r['status']==owner['run_status'] for r in runs),'tail retained owner must appear in actual fullRun')
  for run in runs:
   if run['status'] in ('pending','running'):
    live.append(dict(def_id=did,source_id=de['source_id'],run_id=run['id'],source='finally-actual',**{k:run.get(k) for k in ('sync_def_id','status','generation','attempt','control_mode','publish_entered_at')}))
 sort=lambda xs:sorted(xs,key=lambda x:x['run_id']);need(sort(owners)==sort(summary['final_owner_roster']) and sort(live)==sort(summary['final_run_roster']),'tail exact whole original owner/live dual roster')
 expected={v['run_id'] for k,v in bindings.items() if v['kind']=='owner'};held=bindings['h10-held'];need(len(owners)==6 and {x['run_id'] for x in owners}==expected and len(live)==1 and live[0]['run_id']==held['run_id'] and live[0]['publish_entered_at'] is None,'tail exact6 unresolved owners/one known live prepublish')
 for owner in owners:need(owner['blocked_reason']=='requires_reconciliation' and owner['run_status']==('running' if owner['run_id']==held['run_id'] else 'failed'),'tail no owner clear/adoption or terminal-state relabel')
 return owners,live

def tail(r,p,source):
 h=mod('p26_h10_actual_reader');sem=mod('p26_p25_proofs');a=r.doc(p['audit']);need(len(a['files'])==1022,'tail exact1022 original members')
 for path,pin in a['files'].items():
  need(not path.endswith('.env'),'tail private inputs forbidden');raw=r.read('tools/smoke/business-platform-p2-6/'+path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'tail original SHA/bytes')
 r.rc(p['launch_rc']);outer=p['outer'];r.rc(outer+'/child/rc')
 for side in ('source-before','source-after'):r.rc(outer+'/'+side+'/rc');need(source in r.text(outer+'/'+side+'/stdout.log'),'tail actual source original before/after')
 tools=r.doc(outer+'/tools-before.json');need(tools==r.doc(outer+'/tools-after.json'),'tail original tool bytes stable')
 for path,digest in tools.items():need(hashlib.sha256(r.read(outer+'/tools-before/'+path)).hexdigest()==digest,'tail actual imported tool snapshot')
 registry=sem.specs(r.read(p['engine_source']));rt=json.loads(h.ref(r,p,a['runtime_result']));s=json.loads(h.ref(r,p,a['summary']));cases(s,registry);need(rt['source_S']==source and rt['mode']=='engine' and rt['original_engine_or_runner_rc']==0 and rt['wrapper_exception'] is None and rt['h10_command_counts']=={'kill':0,'start':0} and rt['runtime_sha256']==tools['regressions/p25/p26_p25_runtime.py'],'tail runtime source/engine/rc/no lifecycle')
 ed=s['attempts']['evidence_dir'];events={};obs={};original_creations={};quiet={}
 for path in a['files']:
  if '/engine-evidence/' not in path or not path.endswith('.json') or 'attempt-' not in path:continue
  e=r.doc('tools/smoke/business-platform-p2-6/'+path);events[Path(path).name]=e
  if 'method' not in e:continue
  raw=e.get('resp_body','').encode()
  if e['resp_bytes']==len(raw):need(hashlib.sha256(raw).hexdigest()==e['resp_sha256'],'tail actual recorded HTTP raw body hash')
  else:need('/download' in e['path'] and e['method']=='GET','tail only historical binary download is digest/length-only')
  if e['method']=='POST' and e['status'] in (201,202):
   body=json.loads(raw);original_creations[body['id']]=body
  if Path(path).name.startswith('finally-attempt-') and e['method']=='GET':
   need(e['status']==200,'tail original finally GET status');body=json.loads(raw)
   if '/syncs/' in e['path']:
    did,_,suffix=e['path'].split('/syncs/',1)[1].partition('/');obs[(did,suffix)]=body
   elif e['path'].endswith('/admin/stats'):quiet['proxy']=body
   elif '/admin/src/' in e['path']:quiet.setdefault('fixture',{})[e['path'].rsplit('/',1)[-1]]=body
 for row in s['scenarios']:need(events[{'h11':'h11-attempt-0041.json','h14':'h14-attempt-0056.json'}[row['id']]]==row,'tail original recorder equals summary exact')
 bindings=json.loads(h.ref(r,p,a['tail_bindings']));decl=json.loads(h.ref(r,p,p['declaration']));canon=lambda x:hashlib.sha256(json.dumps(x,sort_keys=True).encode()).hexdigest();need(decl['schema']=='p25-tail-roster-v1' and decl['ok'] is True and canon(decl['spec'])==decl['declaration_sha256']==bindings['declaration_sha256'] and canon(bindings['bindings'])==bindings['bindings_sha256'] and bindings['bindings']==s['tail_bindings'] and bindings['summary_sha256']==a['summary']['sha256'] and bindings['problems']==[],'tail original declaration and actual binding adjudication')
 oldbinding=json.loads(h.ref(r,p,decl['source_proofs']['h10_binding']));frozen=json.loads(h.ref(r,p,decl['source_proofs']['actual_frozen_held']));need(decl['known_bindings']==oldbinding['known_bindings']==p['known_h10_binding'] and frozen['frozen_for_next_phase'] is True,'tail declaration exact actual frozen h10 known binding')
 need({k:{f:v[f] for f in ('def_id','source_id','run_id')} for k,v in bindings['bindings'].items() if k=='h10-held'}==p['known_h10_binding'],'tail actual known held unchanged')
 handles=json.loads(h.ref(r,p,a['actual_handles']));inputs=json.loads(h.ref(r,p,a['input_handles']));need(handles['count']==len(handles['handles'])==65 and inputs['count']==46,'tail exact65 handles from46 source input')
 defs={row['ref'] for row in handles['handles'] if row.get('ok') and row['kind'] in ('def','h10-def')};need(len(defs)==28,'tail union whole28 Def scope');owners,live=whole(s,obs,defs,bindings['bindings']);final=json.loads(h.ref(r,p,a['actual_whole_def_roster']));need(set(final['defs'])==defs and final['owner_roster']==s['final_owner_roster'] and final['run_roster']==s['final_run_roster'] and final['problems']==[],'tail whole roster original artifact linkage')
 created=rt['created_safe'];need(set(created)==set(original_creations) and len(created)==20 and {v['kind'] for v in created.values()}=={'def','source','dataset','run'},'tail exactly20 actual POST creators, not imported handles');need(all(sum(v['kind']==kind for v in created.values())==5 for kind in ('def','source','dataset','run')),'tail actual five creation chains')
 for i,mode in enumerate(('no_txn_200','not_committed','wrong_target','http_500')):
  before=json.loads(events[f'h11-attempt-{5+10*i:04d}.json']['resp_body']);after=json.loads(events[f'h11-attempt-{8+10*i:04d}.json']['resp_body']);delta=after[len(before):];b=bindings['bindings']['h11-'+mode];target=original_creations[b['def_id']]['output_dataset_id'];delta=[e for e in delta if e.get('request_target')==target and e.get('method')=='POST' and e.get('path','').endswith('/outputs:commit')];need(len(delta)==1 and delta[0]['mode']==mode and delta[0]['request_target']==target and delta[0]['forwarded'] is False and delta[0]['fabricated_status']==(502 if mode=='http_500' else 200),'tail four strict local ACK failures original proxy delta, never forwarded')
 need(quiet['fixture']==s['quiet']['fixture'] and quiet['proxy']==s['quiet']['proxy'] and s['quiet']['ok'] is True and s['quiet']['problems']==[],'tail exact actual quiet GETs')
 for v in quiet['fixture'].values():need(v['active']==0 and v['inventory_started']==v['inventory_finished'],'tail fixture strict quiet')
 need(quiet['proxy']['commit_started']==quiet['proxy']['commit_finished'] and quiet['proxy']['commit_inflight']==0,'tail proxy strict quiet')
 sqls={}
 for path in a['files']:
  if not path.endswith('.cmd.json') or '/runtime-' not in path:continue
  rel='tools/smoke/business-platform-p2-6/'+path;cmd=r.doc(rel)
  if not isinstance(cmd,list) or 'psql' not in cmd:continue
  stem=rel.removesuffix('.cmd.json');r.rc(stem+'.rc');need(r.doc(stem+'.meta.json')['original_rc']==0,'tail export original SQL rc0');sqls[Path(stem).name.split('-export-',1)[1]]=h.rows(r.read(stem+'.raw'))
 need(set(sqls)=={'whole-controlled','whole-cp','actual-new-safe27','parents','protocol'},'tail five original export SQL groups')
 initial={}
 for q in rt['initial_guard']['records']:
  if 'psql' not in q.get('argv',[]):continue
  need(q['rc']==0,'tail original initial SQL rc0');query=q['argv'][-1];values=h.rows(q['stdout'].encode())
  if "r.control_mode = 'controlled'" in query:initial['controlled']=values
  elif 'c.active_run_id IS NOT NULL' in query:initial['CP']=values
  elif ' AS def_id' in query:initial['parents']=values
 baseline={}
 for key,receipt in frozen['actual_observation_sql_receipts'].items():
  if key.endswith('-whole-controlled'):baseline['controlled']=h.sql(r,p,receipt)
  elif key.endswith('-whole-CP'):baseline['CP']=h.sql(r,p,receipt)
  elif key.endswith('-parents'):baseline['parents']=h.sql(r,p,receipt)
 need(initial==baseline,'tail initial controlled/CP/parents exact accepted immutable natural freeze')
 record=json.loads(h.ref(r,p,a['current_runtime_unchanged']));inspections={q['argv'][2]:q['stdout'].strip().splitlines() for q in rt['initial_guard']['records'] if q.get('argv',[])[:2]==['docker','inspect']}
 need(set(inspections)==set(record['containers']),'tail initial original15 runtime identities')
 for name,pin in record['containers'].items():need(inspections[name]==[pin['cid'],pin['image'],pin['started_at'],'true'],'tail same original runtime identity')
 need(len(initial['controlled'])==10 and len(initial['CP'])==9 and sqls['whole-controlled']==initial['controlled'],'tail preserves all10 controlled full27')
 cps={v['sync_def_id']:v for v in sqls['whole-cp']};need(len(cps)==14 and all(cps[v['sync_def_id']]==v for v in initial['CP']),'tail original9 CP fullfields immutable; five new owners only')
 te=json.loads(h.ref(r,p,a['typed_export']));need(te['schema']=='p26-p25-typed-export-v1' and len(te['entries'])==len(te['public_views'])==5 and te['whole_controlled_count']==10 and te['whole_cp_count']==14,'tail exact5 terminal typed export')
 newrows={v['id']:v for v in sqls['actual-new-safe27']};parents={v['def_id']:v for v in sqls['parents']};need(len(newrows)==5 and len(parents)==15 and all(parents[v['def_id']]==v for v in initial['parents']) and sqls['protocol']==[{'id':x['id'],'recovery_protocol':None} for x in sqls['actual-new-safe27']],'tail old parents unchanged/new5 NULL protocol original SQL')
 for e in te['entries']:
  row=e['safe_projection_27'];did=e['def_id'];rid=e['run_id'];need(e['expected_state']=='failed' and row==newrows[rid]==json.loads(h.ref(r,p,e['evidence'])) and row['status']=='failed' and set(row)==set(mod('p26_h10_anchor_reader').SAFE27) and row['gate_entered']=='true' and row['has_version']=='false' and e['cp_owner_row_full']==cps[did] and e['parent']==parents[did],'tail exact safe27/parent/CP original SQL and exported evidence');need(e['actual_creation']==created[did],'tail typed actual creator identity')
 for v in te['public_views']:need(v['baseline_cp']==obs[(v['def_id'],'checkpoint')] and v['baseline_runs']==obs[(v['def_id'],'runs')] and v['checkpoint_status']==200 and v['baseline_cp']['confirmed_epoch']==0,'tail typed full public baseline exact originals')
 return {'kind':p['kind'],'cases':['h11','h14'],'gates':{'h11':4,'h14':5},'whole_Defs':28,'final_owners':6,'final_live_runs':1,'new_creations':20,'new_Def_Source':[5,5],'whole_controlled':10,'whole_CP':14,'typed_new_terminal_failed':5,'historical_download_raw_archived':False,'independent_h14_fullbytes_required':True,'full_P25_15':False}
