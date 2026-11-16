"""Independent original h14 GET/ACK/transaction/full-file reader; no live clients."""
import hashlib,json,importlib.util
from pathlib import Path
from urllib.parse import urlsplit

def need(ok,why):
 if not ok:raise ValueError(why)
def mod(n):
 s=importlib.util.spec_from_file_location(n,Path(__file__).with_name(n+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def semantic(identity,bodies,entry,download):
 did,sid,rid,target,txid,fid=[identity[k] for k in ('def_id','source_id','run_id','target_id','transaction_id','file_id')];de,run,cp,tx,files=[bodies[k] for k in ('definition','run','checkpoint','transaction','files')]
 need(de['id']==did and de['source_id']==sid and de['output_dataset_id']==target and run['id']==rid and run['sync_def_id']==did and run['status']=='failed' and run['dataset_version_id'] is None,'h14 exact actual identity/local lost ACK outcome')
 need(cp['confirmed_epoch']==0 and cp['owner']['run_id']==rid and cp['owner']['run_status']=='failed' and cp['owner']['blocked_reason']=='requires_reconciliation','h14 local retained unknown owner/epoch0')
 need(tx['id']==txid and tx['dataset_id']==target and tx['status']=='COMMITTED' and tx['tx_type']=='SNAPSHOT' and tx['branch_name']=='main','h14 independent direct transaction COMMITTED exact target')
 meta=tx['metadata'];need(meta['rows']==24 and meta['bytes']==meta['size_bytes']==len(download)==558 and meta['pages']==2 and meta['file_count']==1 and meta['lineage']=={'source_id':sid,'sync_def_id':did,'sync_run_id':rid},'h14 direct transaction full24row558byte lineage')
 expected=('\n'.join(json.dumps(f'a-{i:03d}|name-a{i}|qty-{i}') for i in range(1,25))+'\n').encode();need(download==expected and hashlib.sha256(download).hexdigest()==meta['content_hash'],'h14 full bytes equal actual fixture A24, never preview/digest-only')
 need(files['total']==len(files['files'])==1 and files['branch']=='main','h14 complete one-file roster');file=files['files'][0];need(file['id']==fid and file['dataset_id']==target and file['transaction_id']==txid and file['logical_path']==f'sync-{rid}.ndjson' and file['size_bytes']==len(download),'h14 file bound to actual transaction/Run/size')
 need(entry['mode']=='drop_after_verify' and entry['method']=='POST' and entry['forwarded'] is True and entry['verified_committed'] is True and entry['upstream_status']==201 and entry['request_target']==entry['ack_dataset_id']==target and entry['ack_txn_id']==txid and entry['ack_txn_status']=='COMMITTED' and entry['error'] is None,'h14 original proxy forwarded verified201 ACK identity')
 raw=entry['upstream_ack_raw'].encode();need(hashlib.sha256(raw).hexdigest()==entry['upstream_ack_sha256'],'h14 original upstream raw ACK SHA');ack=json.loads(raw);need(ack['dataset_id']==target and ack['transaction']==tx and len(ack['files'])==1 and ack['files'][0]['metadata']['transaction_id']==txid and ack['files'][0]['size_bytes']==len(download),'h14 original raw ACK vs independent remote transaction')

def independent(r,p,source):
 h=mod('p26_h10_actual_reader');a=r.doc(p['audit']);need(len(a['files'])==315,'h14 exact315 original members')
 for path,pin in a['files'].items():
  need(not path.endswith('.env'),'h14 private env forbidden');raw=r.read('tools/smoke/business-platform-p2-6/'+path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'h14 original SHA/bytes')
 r.rc(p['launch_rc']);outer=p['outer'];r.rc(outer+'/child/rc')
 for side in ('source-before','source-after'):r.rc(outer+'/'+side+'/rc');need(source in r.text(outer+'/'+side+'/stdout.log'),'h14 original source before/after')
 tools=r.doc(outer+'/tools-before.json');need(tools==r.doc(outer+'/tools-after.json'),'h14 tool bytes stable')
 for path,digest in tools.items():need(hashlib.sha256(r.read(outer+'/tools-before/'+path)).hexdigest()==digest,'h14 actual imported tool snapshots')
 rt=json.loads(h.ref(r,p,a['runtime_result']));proof=json.loads(h.ref(r,p,a['proof']));need(rt['mode']=='h14-read' and rt['source_S']==proof['source_S']==source and rt['original_engine_or_runner_rc']==0 and rt['wrapper_exception'] is None and rt['created_safe']=={} and rt['h10_command_counts']=={'kill':0,'start':0},'h14 actual readonly helper/runtime rc0/source/no creates/lifecycle')
 need(proof['schema']=='p26-p25-h14-independent-read-v1' and proof['identity']==a['actual_identity'],'h14 actual independent proof identity');tailaudit=json.loads(h.ref(r,p,p['tail_audit']));need(proof['inputs']['tail_summary']==tailaudit['summary'] and proof['inputs']['bindings']==tailaudit['tail_bindings'],'h14 exact accepted actual tail originals');summary=json.loads(h.ref(r,p,proof['inputs']['tail_summary']));bindings=json.loads(h.ref(r,p,proof['inputs']['bindings']));declaration=json.loads(h.ref(r,p,proof['inputs']['declaration']));need({k:proof['identity'][k] for k in ('def_id','source_id','run_id','kind','mode')}==bindings['bindings']['h14-drop']==summary['tail_bindings']['h14-drop'] and declaration['declaration_sha256']==bindings['declaration_sha256'],'h14 tail drop binding exact identity')
 before,after=[json.loads(h.ref(r,p,e)) for e in proof['original_ledgers']];ledgers=[]
 for e in (before,after):
  raw=e['resp_body'].encode();need(e['method']=='GET' and e['status']==200 and hashlib.sha256(raw).hexdigest()==e['resp_sha256'] and len(raw)==e['resp_bytes'],'h14 original proxy ledgers GET raw SHA');ledgers.append(json.loads(raw))
 select=lambda xs:[e for e in xs if e.get('mode')=='drop_after_verify' and e.get('request_target')==proof['identity']['target_id'] and e.get('method')=='POST'];need(select(ledgers[0])==[] and select(ledgers[1])==[proof['actual_entry']],'h14 exactly target0-to1 actual forwarded dispatch')
 download=h.ref(r,p,proof['bodies']['download']);bodyraw={k:h.ref(r,p,e) for k,e in proof['bodies'].items() if k!='download'};bodies={k:json.loads(v) for k,v in bodyraw.items()};semantic(proof['identity'],bodies,proof['actual_entry'],download)
 events=[]
 for path in sorted(a['files']):
  if '/h14-reads/' in path and 'attempt-' in path and path.endswith('.json'):events.append(r.doc('tools/smoke/business-platform-p2-6/'+path))
 need(len(events)==6 and [e['method'] for e in events]==['GET']*6 and all(e['status']==200 and e.get('error') is None for e in events),'h14 six original independent GET status200')
 did=proof['identity']['def_id'];target=proof['identity']['target_id'];txid=proof['identity']['transaction_id'];rid=proof['identity']['run_id'];fid=proof['identity']['file_id'];paths=[f'http://127.0.0.1:17100/api/v1/data-connection/syncs/{did}',f'http://127.0.0.1:17100/api/v1/data-connection/syncs/{did}/runs/{rid}',f'http://127.0.0.1:17100/api/v1/data-connection/syncs/{did}/checkpoint',f'http://127.0.0.1:17078/v1/datasets/{target}/branches/main/transactions/{txid}',f'http://127.0.0.1:17078/v1/datasets/{target}/files',f'http://127.0.0.1:17078/v1/datasets/{target}/files/{fid}/download'];need([e['path'] for e in events]==paths,'h14 independent original exact GET URLs')
 runtimebase=str(Path(a['runtime_result']['path']).parent)
 for i,(key,event) in enumerate(zip(('definition','run','checkpoint','transaction','files','download'),events),1):
  raw=download if key=='download' else bodyraw[key];need(event['resp_sha256']==hashlib.sha256(raw).hexdigest() and event['resp_bytes']==len(raw) and (key=='download' or event['resp_body'].encode()==raw),'h14 recorder links exact original full body bytes');stem=h.local(p,runtimebase+f'/{i:02d}-http');cmd=r.doc(stem+'.cmd.json');meta=r.doc(stem+'.meta.json');need(cmd['method']=='GET' and cmd['url']=={'query_digest':None,'url':paths[i-1]} and meta['original_rc'] is None and meta['http_status']==200 and meta['error'] is None and meta['response_sha256']==event['resp_sha256'] and r.text(stem+'.rc').strip()=='unavailable','h14 original HTTP has no invented subprocess Unix rc');runtime_raw=r.read(stem+'.raw');need(runtime_raw in (raw,raw+b'\n'),'h14 original raw recorder newline transparent')
  if i==6:
   redirect=meta['redirect'];safe=redirect['safe_url'];url=urlsplit(safe['url']);need(redirect['policy']=='one anonymous same-origin local-fs follow' and url.scheme=='http' and url.netloc=='127.0.0.1:17078' and url.path==f'/v1/_internal/local-fs/datasets/{target}/transactions/{txid}/sync-{rid}.ndjson' and url.query=='' and set(safe)=={'url','query_digest'},'h14 validated sameorigin anonymous follow metadata; no query credentials')
 union=json.loads(h.ref(r,p,p['typed_union']));need(union['schema']=='p26-w10-typed-v1' and len(union['runs'])==len(union['public_views'])==7,'h14 exact original RootN+held+tail5 typed union7');tailte=json.loads(h.ref(r,p,tailaudit['typed_export']));frozen=json.loads(h.ref(r,p,p['frozen_typed']));want={e['run_id']:e for e in tailte['entries']+[frozen['typed_entry']]};actual={e['run_id']:e for e in union['runs']};need(all(actual[k]==v for k,v in want.items()),'h14 declared actual frozenheld and tail5 entries exact original')
 # Full initial SQL guard independently binds the typed union and whole rosters.
 groups={}
 for q in rt['initial_guard']['records']:
  if 'psql' not in q.get('argv',[]):continue
  need(q['rc']==0,'h14 initial original SQL rc0');query=q['argv'][-1];values=h.rows(q['stdout'].encode())
  if "r.control_mode = 'controlled'" in query:groups['controlled']=values
  elif "r.control_mode = 'legacy'" in query:groups['legacy']=values
  elif 'c.active_run_id IS NOT NULL' in query:groups['CP']=values
  elif ' AS def_id' in query:groups['parents']=values
  elif values and set(values[0])==set(mod('p26_h10_anchor_reader').SAFE27):groups['typed']=values
  elif values and set(values[0])=={'id','recovery_protocol'}:groups['protocol']=values
 need(set(groups)=={'controlled','legacy','CP','parents','typed','protocol'} and len(groups['controlled'])==10 and len(groups['CP'])==14 and len(groups['legacy'])==3 and {v['id']:v for v in groups['typed']}=={rid:e['safe_projection_27'] for rid,e in actual.items()} and {v['id']:v['recovery_protocol'] for v in groups['protocol']}=={rid:None for rid in actual},'h14 full27 typed7/NULLprotocol/whole10CP14legacy3 exact SQL')
 previous=json.loads(h.ref(r,p,p['previous_typed_union']));prior={e['run_id']:e for e in previous['runs']};need(all(actual[k]==v for k,v in prior.items()) and union['public_views']==previous['public_views']+tailte['public_views'] and union['legacy_whole_baseline_27']==previous['legacy_whole_baseline_27'] and {v['id']:v for v in groups['legacy']}=={v['id']:v for v in union['legacy_whole_baseline_27']+[e['safe_projection_27'] for e in union['runs'] if e['control_mode']=='legacy']},'h14 RootN exact previous entry/public and whole legacy immutable; tail5 actual public append')
 need(bodies['checkpoint']==next(v['baseline_cp'] for v in union['public_views'] if v['def_id']==did) and bodies['run']['incremental']=={'mode':'cursor','base_epoch':0} and [{k:v for k,v in bodies['run'].items() if k!='incremental'}]==next(v['baseline_runs'] for v in union['public_views'] if v['def_id']==did),'h14 independent local actual body equals declared full public baseline')
 tail_sql={}
 for path in tailaudit['files']:
  if '/runtime-' in path and '-export-' in path and path.endswith('.raw'):
   key=Path(path).name.removesuffix('.raw').split('-export-',1)[1];tail_sql[key]=h.rows(r.read('tools/smoke/business-platform-p2-6/'+path))
 need(groups['controlled']==tail_sql['whole-controlled'] and groups['CP']==tail_sql['whole-cp'] and groups['parents']==tail_sql['parents'],'h14 whole controlled/CP/parent exactly unchanged from actual tail exports')
 guard_public={}
 for path in a['files']:
  if '/guard-' not in path or not path.endswith('.cmd.json'):continue
  rel='tools/smoke/business-platform-p2-6/'+path;cmd=r.doc(rel)
  if not isinstance(cmd,dict) or cmd.get('method')!='GET':continue
  stem=rel.removesuffix('.cmd.json');r.rc(stem+'.rc');meta=r.doc(stem+'.meta.json');raw=r.read(stem+'.raw');did0,_,suffix=cmd['url'].split('/syncs/',1)[1].partition('/');guard_public[(did0,suffix)]=(meta['status'],json.loads(raw))
 for view in union['public_views']:
  did0=view['def_id'];need(guard_public[(did0,'checkpoint')][0]==view['checkpoint_status'] and ((view['checkpoint_status']==404 and view['baseline_cp']=={}) or guard_public[(did0,'checkpoint')][1]==view['baseline_cp']) and guard_public[(did0,'runs')]==(200,view['baseline_runs']),'h14 original raw typed public guard exactly declared checkpoint/fullRun')
 return {'kind':p['kind'],'independent_GETs':6,'HTTP_original_Unix_rc':None,'runtime_original_Unix_rc':0,'validated_anonymous_sameorigin_follows':1,'original_dispatch_counts':[0,1],'remote_COMMITTED':True,'local_Run_failed_owner_retained':True,'download_bytes':len(download),'download_rows':24,'typed_union':7,'whole_controlled':10,'whole_CP':14,'whole_legacy':3,'full_P25_15':False}
