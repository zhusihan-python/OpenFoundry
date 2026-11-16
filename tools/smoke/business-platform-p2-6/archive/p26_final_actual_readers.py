"""Read Root-released final-phase originals, without executing deployment helpers."""
import hashlib,json,importlib.util
from pathlib import Path

def need(ok,why):
 if not ok:raise ValueError(why)
def mod(n):
 s=importlib.util.spec_from_file_location(n,Path(__file__).with_name(n+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def exact_members(r,p,a,count):
 h=mod('p26_h10_actual_reader');need(len(a['files'])==count,'final actual exact original member count')
 for path,pin in a['files'].items():
  need(not path.endswith('.env') and '.env.' not in path and not path.endswith('.body'),'final private file forbidden');h.ref(r,p,dict(pin,path=path if path.startswith('/') else p['canonical_root_label']+'/tools/smoke/business-platform-p2-6/'+path))

def command(r,p,base):
 h=mod('p26_h10_actual_reader');stem=h.local(p,base);cmd=r.doc(stem+'.cmd.json');meta=r.doc(stem+'.meta.json');raw=r.read(stem+'.raw');rc=r.text(stem+'.rc').strip()
 if isinstance(cmd,list):need(meta['original_child_rc']==0 and rc=='0','final original subprocess Unix0')
 else:need(rc=='0' and meta['original_child_rc'] is None and meta['transport_outcome']==0 and meta.get('status',meta.get('http_status'))==200,'final original HTTP transport/status, Unix unavailable')
 return cmd,meta,raw

def schedule_rows(rows,expected,disabled):
 need(len(rows)==len(expected)==294 and {x['id'] for x in rows}==set(expected),'final owned schedules exact294 fixed IDs')
 if disabled:need(all(x['schedule_enabled'] is False and x['schedule_next_fire_at'] is None for x in rows),'final all294 disabled/NULL nextfire')

def guard_sql(guard):
 h=mod('p26_h10_actual_reader');result={}
 for q in guard['records']:
  if 'psql' not in q.get('argv',[]):continue
  need(q['rc']==0,'final original full SQL guard rc0');query=q['argv'][-1];values=h.rows(q['stdout'].encode())
  if "r.control_mode = 'controlled'" in query:result['controlled']=values
  elif "r.control_mode = 'legacy'" in query:result['legacy']=values
  elif 'c.active_run_id IS NOT NULL' in query:result['CP']=values
  elif ' AS def_id' in query:result['parents']=values
  elif values and set(values[0])==set(mod('p26_h10_anchor_reader').SAFE27):result['typed']=values
  elif values and set(values[0])=={'id','recovery_protocol'}:result['protocol']=values
 need(set(result)=={'controlled','legacy','CP','parents','typed','protocol'},'final guard six original full SQL groups');return result

def guards(before,after,prior,typed,stopnames):
 gb,ga,gp=[guard_sql(x) for x in (before,after,prior)];need(gb==ga==gp and len(gb['controlled'])==10 and len(gb['CP'])==14 and len(gb['legacy'])==3,'final before/after all immutable controlled10CP14legacy3,parent and full typed SQL equals pre-quiesce actual')
 need({x['id']:x for x in gb['typed']}=={e['run_id']:e['safe_projection_27'] for e in typed['runs']} and len(typed['runs'])==7,'final Root typed7 full27 immutable')
 pubs=lambda x:[q for q in x['records'] if 'checkpoint_body_sha256' in q];need(pubs(before)==pubs(after)==pubs(prior),'final all old/typed public fullCP/fullRun hashes remain unchanged')
 for guard,stopped in ((before,False),(after,True)):
  ins={q['argv'][2]:q['stdout'].strip().splitlines() for q in guard['records'] if q.get('argv',[])[:2]==['docker','inspect']};old={q['argv'][2]:q['stdout'].strip().splitlines() for q in prior['records'] if q.get('argv',[])[:2]==['docker','inspect']};need(set(ins)==set(old) and len(ins)==15,'final exact original runtime15')
  for n,values in ins.items():need(values[:3]==old[n][:3] and values[3]==('false' if stopped and n in stopnames else 'true'),'final only5 owned processes stop; no CID/image/boot drift')

def quiesce(r,p,source):
 h=mod('p26_h10_actual_reader');a=r.doc(p['audit']);exact_members(r,p,a,613);need(a['source_sha256']==source and a['outer_original_child_rc']==0,'final quiesce original source/outer0');outer=p['outer'];r.rc(outer+'/original-child.rc');outerresult=r.doc(outer+'/RESULT.json');inner=a['actual_inner'];need(outerresult['original_child_rc']==0 and outerresult['inner_output']==[inner],'final quiesce original outer/inner binding');result=h.docpath(r,p,inner+'/RESULT.json');release=r.doc(outer+'/Root-release.snapshot.json');need(release['root_accepted'] is True and release['candidate_source']==source and result['schema']=='p26-final-quiesce-proof-v1' and result['ok'] is True,'final accepted quiesce release/result schema')
 expected=json.loads(h.ref(r,p,p['through_P25_plan']));owned=result['owned_definitions'];expectedrows={e['def_id']:e for e in expected['entries']};need(len(owned)==294 and {e['def_id'] for e in owned}==set(expectedrows) and len({e['source_id'] for e in owned})==280,'final actual owned294Def280Source exact throughP25 creation cutoff');need(not set(expectedrows)&set(expected['protected_exclusions']) and len(expected['protected_exclusions'])==19,'final protected19 excluded')
 coverage=json.loads(h.ref(r,p,result['owned_definition_coverage']));need(set(coverage['definition_ids'])==set(expectedrows) and coverage['definition_count']==294 and coverage['source_count']==280 and set(coverage['protected_exclusions'])==set(expected['protected_exclusions']) and coverage['historical_POST_bodies_reconstructed'] is False,'final Rootcoverage exact historical scope, no invented original POST bodies')
 ledger=json.loads(h.ref(r,p,owned[0]['provenance_ledger']));need(set(ledger['definitions'])==set(expectedrows) and ledger['original_historical_plan']==p['through_P25_plan'] and ledger['historical_unique_ref_count']==len(ledger['historical_unique_refs'])==516 and ledger['definition_count']==294 and ledger['source_count']==280,'final full historical/CURRENT provenance ledger exact scope')
 for pin in ledger['historical_unique_refs']:h.ref(r,p,pin)
 currentaudit=json.loads(h.ref(r,p,ledger['original_current_audit']));need(currentaudit['PG_before']==currentaudit['PG_after'] and currentaudit['PG_unchanged'] is True and currentaudit['raw_Def_count']==294 and currentaudit['raw_Source_count']==280 and currentaudit['business_mutations']==0 and currentaudit['historical_POST_bodies_reconstructed'] is False,'final original CURRENT exact readonly parent proof')
 for path,pin in currentaudit['raw_members'].items():h.ref(r,p,dict(pin,path=path if path.startswith('/') else p['canonical_root_label']+'/tools/smoke/business-platform-p2-6/'+path))
 rawpath=next(path for path in currentaudit['raw_members'] if path.endswith('/CURRENT-exact-parent-SELECT.stdout'));rawrows=h.rows(r.read(h.local(p,rawpath)));need(len(rawrows)==294 and {x['def_id'] for x in rawrows}==set(expectedrows) and all(set(x)==set(currentaudit['exact_field_names']) for x in rawrows),'final CURRENT original raw294 exact9 fields')
 for x in rawrows:
  e=expectedrows[x['def_id']];need((x['source_id'],x['conn_id'],x['output_dataset_id'],x['org_id'],x['conn_org_id'],x['conn_owner_id'])==(e['source_id'],e['source_id'],e['output_dataset_id'],e['expected_tenant'],e['expected_tenant'],e['expected_actor']),'final actual CURRENT owner/org/parent matches whole historical plan, including four early gaps')
 need(r.doc(outer+'/before.json')['file_sha256']==r.doc(outer+'/after.json')['file_sha256'],'final original outer same imported code/data hashes')
 for row in owned:
  provenance=ledger['definitions'][row['def_id']];need(provenance['definition_creation']==row['definition_creation'] and provenance['source_creation']==row['source_creation'] and provenance['historical_basis']==row['historical_basis'],'final per-Def complete original provenance ledger')
  old=expectedrows[row['def_id']];need(row['source_id']==old['source_id'] and row['output_dataset_id']==old['output_dataset_id'] and row['actor_id']==old['expected_actor'] and row['org_id']==old['expected_tenant'] and row['historical_basis']==old and row['instance']=='p26connectorc','final historical creation provenance vs exact CURRENT actor/tenant; all routing C')
  for kind,refkey in (('definition','definition_creation'),('source','source_creation')):
   e=row[refkey];doc=json.loads(h.ref(r,p,e));value=doc
   for part in e['json_pointer'].strip('/').split('/'):value=value[part.replace('~1','/').replace('~0','~')]
   if kind=='definition':need(value['id']==row['def_id'] and value['source_id']==row['source_id'] and value['output_dataset_id']==row['output_dataset_id'] and value['org_id']==row['org_id'],'final CURRENT definition identity origin')
   else:need(value['id']==row['source_id'] and value['owner_id']==row['actor_id'] and value['org_id']==row['org_id'],'final CURRENT source actor/org origin')
 steps={}
 for path in a['files']:
  if str(Path(path).parent)==inner and path.endswith('.cmd.json'):
   stem=path.removesuffix('.cmd.json');steps[Path(stem).name]=command(r,p,stem)
 need(len(steps)==47,'final exact47 inner original operations');unix=[v for v in steps.values() if isinstance(v[0],list)];http=[v for v in steps.values() if isinstance(v[0],dict)];need(len(unix)==28 and len(http)==19,'final original28 Unix0/19 HTTP status200 transport0, no invented HTTP Unix')
 schedules={};grants=[]
 for name,(cmd,meta,raw) in steps.items():
  if isinstance(cmd,list) and 'psql' in cmd:
   query=' '.join(cmd);rows=h.rows(raw)
   if 'sync_schedule_grants' in query:need('WHERE' not in query.upper() and rows==[],'final whole schedule grants zero, not an allowlist');grants.append(rows)
   elif 'schedule_next_fire_at' in query:schedules[name]=rows;schedule_rows(rows,expectedrows,not name.startswith('17-'))
 need(len(grants)==3 and len(schedules)==3,'final three original full grants/schedule observations')
 before=schedules['17-owned-def-schedules-disabled'];changed={x['id'] for x in before if x['schedule_enabled'] is True or x['schedule_next_fire_at'] is not None};patches=[v for v in http if v[0]['method']=='PATCH'];need(len(changed)==len(patches)==1,'final only actually enabled owned Def patched once')
 for cmd,meta,raw in patches:
  need(cmd['body']=={'schedule_enabled':False} and cmd['url'].rsplit('/',1)[-1] in changed,'final minimal exact owned schedulefalse PATCH')
 need(all(v[0]['method'] in ('GET','PATCH') for v in http),'final no run cancellation/adoption/arbitrary API mutation')
 stops={};identities={}
 for name,(cmd,meta,raw) in steps.items():
  if isinstance(cmd,list) and cmd[:2]==['docker','inspect']:identities[name]=json.loads(raw)
  if isinstance(cmd,list) and cmd[:2]==['docker','stop']:
   need(cmd[:4]==['docker','stop','--time','120'] and len(cmd)==5,'final owned stop exact argv');stops[cmd[4]]=stops.get(cmd[4],0)+1
 names={'p26connectora','p26connectora2','p26connectorb','p26connectorb2','p26connectorc'};stopped=result['stopped_connectors'];need(set(stopped)==names and set(stops)=={v['cid'] for v in stopped.values()} and all(v==1 for v in stops.values()),'final exact owned5 each stopped once')
 for name,pin in stopped.items():
  role='connector_'+name.removeprefix('p26connector').rstrip('2');need(pin['running'] is False and pin['ownership']=={'of-p26-owner':'p26-http','of-p26-role':role,'of-p26-stage':'p2-6'},'final stopped ledger owned labels')
  observations=[v for k,v in identities.items() if k.endswith('-'+name)];need(len(observations)==3 and [v['running'] for v in observations]==[True,True,False] and all(v['cid']==pin['cid'] and v['image']==pin['image'] and v['started_at']==pin['started_at'] and v['name']=='/'+name and v['labels']==observations[0]['labels'] and {k:v['labels'][k] for k in pin['ownership']}==pin['ownership'] for v in observations),'final original pre/stop/post owned labels/CID/image/boot and stopped false')
 oldcounters={};newcounters={}
 for name,(cmd,meta,raw) in steps.items():
  if isinstance(cmd,dict) and cmd['method']=='GET':
   need(meta['auth_attached'] is False and hashlib.sha256(raw[:-1]).hexdigest()==meta['body_sha256'] and len(raw)-1==meta['bytes'],'final anonymous old-counter GET body/hash with capture LF');(oldcounters if int(name[:2])<39 else newcounters)[cmd['url']]=raw
 need(oldcounters==newcounters and len(oldcounters)==9,'final exact9 old fixture counters byte-for-byte unchanged')
 for guard in (result['guard_before'],result['guard_after']):
  originals={}
  for path in a['files']:
   if str(Path(path).parent)!=guard['evidence_dir'] or not path.endswith('.cmd.json'):continue
   rel=h.local(p,path);cmd=r.doc(rel);stem=rel.removesuffix('.cmd.json');r.rc(stem+'.rc');meta=r.doc(stem+'.meta.json');raw=r.text(stem+'.raw')
   if isinstance(cmd,list):need(meta['kind']=='call' and meta['stderr']=='','final guard original subprocess metadata shape');originals[json.dumps(cmd)]=raw
  for q in guard['records']:
   if 'argv' in q:need(q['stdout']+'\n'==originals[json.dumps(q['argv'])],'final fullguard embedded rows bound to independent original command/raw/rc')
 prior=json.loads(h.ref(r,p,p['accepted_h14_runtime']));typed=json.loads(h.ref(r,p,p['typed_union']));guards(result['guard_before'],result['guard_after'],prior['initial_guard'],typed,names)
 need(result['typed_roster_sha256']==p['typed_union']['sha256'] and result['typed_held_preserved'] is True and result['unknown_or_old_rows_cancelled_adopted_cleared'] is False and result['whole_schedule_grants_count']==result['owned_def_next_fire_count']==0 and result['worker_processes_stopped'] is True,'final result exact preservation/quiescence facts')
 return {'kind':p['kind'],'owned_Def_count':294,'owned_Source_count':280,'protected_Def_exclusions':19,'stopped_owned_connectors':5,'stop_each':1,'original_Unix0_operations':28,'HTTP200_transport0_operations':19,'HTTP_original_Unix_rc':None,'actual_PATCHes':1,'whole_grants':0,'whole_nextfire':0,'typed7_controlled10_CP14_legacy3_unchanged':True,'old_fixture_counter_pairs':9,'restart_receipt_or_final_P26':False}

def baseline_snapshot_semantics(result,docs,download,cp,constants):
 c=constants;run=docs['run'];receipt=docs['receipt'];tx=receipt['transaction'];branch=docs['branch'];files=docs['files'];versions=docs['versions']
 need(result['selection_run']==c['RUN'] and result['body_sha256']==c['BODY_SHA'] and result['body_bytes']==1804,'receipt baseline original private intent hash/length only')
 need(run['id']==c['RUN'] and run['sync_def_id']==c['DEF'] and run['dataset_version_id']==c['TXN'] and run['status']=='succeeded' and run['recovery']['protocol']=='p26-v1' and run['recovery']['state']=='finalized' and run['recovery']['transaction_id']==c['TXN'],'receipt baseline selected original successful finalized p26-v1 Run')
 need(receipt['idempotency_key']=='of-p26-pub-v1:'+c['RUN'] and receipt['request_fingerprint']==c['BODY_SHA'] and receipt['dataset_id']==c['DATASET'] and tx['id']==c['TXN'] and tx['dataset_id']==c['DATASET'] and tx['status']=='COMMITTED' and tx['tx_type']=='SNAPSHOT','receipt baseline exact formal receipt/key/fingerprint/committed transaction')
 meta=tx['metadata'];need(meta['rows']==32 and meta['bytes']==meta['size_bytes']==len(download)==827 and meta['file_count']==1 and meta['lineage']=={'source_id':c['SOURCE_ID'],'sync_def_id':c['DEF'],'sync_run_id':c['RUN']} and hashlib.sha256(download).hexdigest()==meta['content_hash']==c['DOWNLOAD_SHA'],'receipt baseline full827B32row download and actual lineage/hash')
 lines=download.decode().splitlines();need(len(lines)==32 and len(set(lines))==32 and all(isinstance(json.loads(line),str) for line in lines),'receipt baseline full32 unique actual download rows, no preview/truncation')
 need(branch['dataset_id']==c['DATASET'] and branch['name']=='main' and branch['head_transaction_id']==c['TXN'] and branch['version']==1 and versions['has_more'] is False and len(versions['data'])==1 and versions['data'][0]['transaction_id']==c['TXN'] and versions['data'][0]['version']==1,'receipt baseline unchanged branch head and complete version1 roster')
 need(files['total']==len(files['files'])==1 and files['files'][0]['dataset_id']==c['DATASET'] and files['files'][0]['transaction_id']==c['TXN'] and files['files'][0]['logical_path']=='sync-'+c['RUN']+'.ndjson' and files['files'][0]['size_bytes']==len(download),'receipt baseline full file target/transaction/Run')
 need(cp['status']==404 and cp['body']=={'error':'incremental checkpoint not initialized for this sync definition'},'receipt baseline ordinary snapshot CP404, no fabricated incremental epoch')

def receipt_baseline(r,p,source):
 import ast
 from urllib.parse import urlsplit
 h=mod('p26_h10_actual_reader');a=r.doc(p['audit']);exact_members(r,p,a,49);need(a['source_S']==source and a['original_launch_rc']==0 and a['HTTP_original_child_rc'] is None and a['POST_count']==a['restart_count']==a['Source_service_call_count']==0,'receipt baseline actual readonly/source/HTTP Unix null/no mutations')
 outer=p['outer'];r.rc(outer+'/rc');argv=r.doc(outer+'/cmd.json');release=r.doc(p['release_snapshot']);need(hashlib.sha256(r.read(p['release_snapshot'])).hexdigest()==a['release']['sha256'] and release['root_accepted'] is True and release['candidate_source']==source and argv[:3]==['python3','-I','-B'] and argv[4]=='baseline' and argv[argv.index('--release-file')+1]==a['release']['path'],'receipt baseline exact original separately released mode and Root JSON snapshot')
 for name,digest in release['final_receipt_tool_shas'].items():need(hashlib.sha256(r.read(p['producer_snapshots']+'/'+name)).hexdigest()==digest,'receipt baseline exact actually used producer code snapshots; never imported')
 code=r.read(p['producer_snapshots']+'/deployment/final_dataset_receipt_replay.py');tree=ast.parse(code.decode());constants={}
 for node in tree.body:
  if isinstance(node,ast.Assign) and len(node.targets)==1 and isinstance(node.targets[0],ast.Name) and node.targets[0].id in {'RUN','DEF','SOURCE_ID','DATASET','ORG','ACTOR','TXN','BODY_SHA','DOWNLOAD_SHA'}:constants[node.targets[0].id]=ast.literal_eval(node.value)
 need(len(constants)==9,'receipt baseline original static exact selection constants')
 for path,digest in release['final_receipt_selection_shas'].items():
  need(path.startswith('results/w7/attempts/'),'receipt baseline exact original public selection path scope');raw=r.read('tools/smoke/business-platform-p2-6/'+path);need(hashlib.sha256(raw).hexdigest()==digest,'receipt baseline original S2 formal Run selection provenance')
  if path.endswith('/RESULT.json'):
   selected=json.loads(raw);need(selected['outcome']=='pass' and selected['run_id']==constants['RUN'] and selected['def_id']==constants['DEF'] and selected['dataset_id']==constants['DATASET'] and selected['body_sha256']==constants['BODY_SHA'],'receipt baseline selected original successful HTTP regression, no new creation')
 result=json.loads(h.ref(r,p,a['baseline_result']));need(result['ok'] is True and result['source_S']==source and result['dataset_identity']==a['dataset_identity'],'receipt baseline actual result source/identity');base=str(Path(a['baseline_result']['path']).parent);docs={};responses={};headers={}
 for i in range(2,10):
  stem=h.local(p,base+f'/{i:02d}-http-get');cmd=r.doc(stem+'.cmd.json');meta=r.doc(stem+'.meta.json');raw=r.read(stem+'.raw');r.rc(stem+'.rc');need(raw.endswith(b'\n'),'receipt baseline original capture LF');body=raw[:-1];need(cmd['method']=='GET' and meta['kind']=='http' and meta['original_child_rc'] is None and meta['transport_outcome']==0 and meta['bytes']==len(body) and meta['body_sha256']==hashlib.sha256(body).hexdigest(),'receipt baseline exact original HTTP body/capture hash distinction and Unix semantics');responses[i]=(cmd,meta,body)
  if i in (2,3,4,5,6):need(meta['status']==200,'receipt baseline mandatory public200');docs[{2:'receipt',3:'branch',4:'versions',5:'files',6:'run'}[i]]=json.loads(body)
 dvs=f"http://127.0.0.1:17078/api/v1/datasets/{constants['DATASET']}";conn=f"http://127.0.0.1:17088/api/v1/data-connection/syncs/{constants['DEF']}"
 expected_urls={2:dvs+'/outputs/commits/of-p26-pub-v1:'+constants['RUN'],3:dvs+'/branches/main',4:dvs+'/versions',5:dvs+'/files',6:conn+'/runs/'+constants['RUN'],7:dvs+'/files/'+docs['files']['files'][0]['id']+'/download',9:conn+'/checkpoint'}
 need(all(responses[i][0]['url']==url for i,url in expected_urls.items()),'receipt baseline all public GET targets exact original selected IDs')
 cp={'status':responses[9][1]['status'],'body':json.loads(responses[9][2]),'body_sha256':responses[9][1]['body_sha256']};download=responses[8][2];baseline_snapshot_semantics(result,docs,download,cp,constants);snapshot=result['public_snapshot'];need(snapshot['docs']==docs and snapshot['checkpoint']==cp and snapshot['download_sha256']==hashlib.sha256(download).hexdigest() and snapshot['download_bytes']==len(download) and snapshot['receipt_bytes_sha256']==hashlib.sha256(responses[2][2]).hexdigest(),'receipt baseline full safe snapshot rebuilt from actual raw originals')
 file=docs['files']['files'][0];need(responses[7][1]['status']==302 and responses[7][2]==b'' and responses[8][1]['status']==200 and responses[8][0]['auth_attached'] is False and responses[7][1]['location']['url']==responses[8][0]['url'] and responses[7][1]['location']['query_digest']==responses[8][0]['query_digest'],'receipt baseline original302-to-anonymous200 download safe URL/query digest linkage')
 follow=urlsplit(responses[8][0]['url']);need(follow.scheme=='http' and follow.netloc=='127.0.0.1:17078' and follow.query=='' and follow.path==f"/v1/_internal/local-fs/datasets/{constants['DATASET']}/transactions/{constants['TXN']}/sync-{constants['RUN']}.ndjson",'receipt baseline validated sameorigin local-fs follow, no presigned query persisted')
 identitystem=h.local(p,base+'/01-dataset-identity');cmd=r.doc(identitystem+'.cmd.json');meta=r.doc(identitystem+'.meta.json');r.rc(identitystem+'.rc');raw=r.text(identitystem+'.raw');expected=result['dataset_identity'];need(cmd[:3]==['docker','inspect','openfoundry-platform-p1-q3v9-dataset-1'] and meta['original_child_rc']==0 and raw==expected['cid']+'\n'+expected['image']+'\n'+expected['started_at']+'\ntrue\n\n','receipt baseline original Dataset inspect Unix0 and exact runtime boot')
 record=json.loads(h.ref(r,p,{'path':release['deployment_record'],'sha256':release['deployment_record_sha256']}));pin=record['containers']['openfoundry-platform-p1-q3v9-dataset-1'];need(all(expected[k]==pin[k] for k in ('cid','image','started_at')),'receipt baseline Dataset matches actual post-quiesce runtime15 record')
 private=h.local(p,base+'/10-private-intent-select');cmd=r.doc(private+'.cmd.json');meta=r.doc(private+'.meta.json');r.rc(private+'.rc');need(meta['kind']=='private-projection' and meta['original_child_rc']==0 and r.read(private+'.raw')==b'PRIVATE stdout/stderr omitted by projection policy\n' and 'sync_run_publish_intents' in ' '.join(cmd) and all(constants[k] in ' '.join(cmd) for k in ('RUN','DEF','SOURCE_ID','DATASET','ACTOR','ORG','TXN','BODY_SHA')),'receipt baseline actual private SELECT Unix0/memory-only omission marker; no private bytes archived')
 material=h.local(p,base+'/11-private-material-hash');metadata=r.doc(material+'.meta.json');r.rc(material+'.rc');need(r.doc(material+'.cmd.json')=={'selection':constants['RUN']} and r.read(material+'.raw')==b'PRIVATE publish_body omitted; SHA/length verified\n' and metadata['body_sha256']==result['body_sha256']==a['private_projection']['body_sha256'] and metadata['body_bytes']==result['body_bytes']==a['private_projection']['body_bytes']==1804,'receipt baseline private body SHA/len projection only, no independent hash-step Unix claimed')
 return {'kind':p['kind'],'safe_original_members':49,'original_launch_Unix_rc':0,'private_SELECT_original_Unix_rc':0,'archive_reader_private_body_values_read':False,'private_body_values_archived':False,'producer_private_body_memory_only':True,'private_body_sha_length_only':True,'private_body_bytes':1804,'public_GETs':8,'HTTP_original_Unix_rc':None,'public_status_counts':{'200':6,'302':1,'404':1},'full_download_bytes':827,'full_download_rows':32,'capture_extra_LF_transparent':True,'ordinary_snapshot_CP404':True,'POSTs':0,'restarts':0,'Source_service_calls':0,'final_receipt_post_or_P26':False}

def auth_metadata_semantics(metadata):
 need(metadata['identity']=={'actor':'1c0e238a-3657-4978-98dc-d71a342c3fa1','tenant':'a792b91e-dab5-4739-9344-2848c1e83487'} and metadata['roles']==[],'writer renewal metadata exact existing actor/tenant/roles')
 for key in ('original_signature_verified','new_signature_verified','header_bytes_equal','non_time_claims_equal','permissions_equal','other_env_bytes_equal','private_mode_0600','backup_mode_0600','old_writer_backup_created','atomic_replace'):need(metadata[key] is True,'writer renewal Root helper metadata '+key+' must be true')
 need(metadata['duration_seconds']==10800 and metadata['time_after']=={'iat':1794759501,'exp':1794770301} and metadata['time_before']=={'iat':1794737643,'exp':1794752043} and metadata['time_after']['exp']-metadata['time_after']['iat']==metadata['duration_seconds'],'writer renewal exact Root-approved time-only expiry metadata')

def auth_metadata(r,p,source):
 h=mod('p26_h10_actual_reader');a=r.doc(p['audit']);exact_members(r,p,a,6);need(a['schema']=='p26-http-writer-refresh-safe-original-audit-v1' and a['original_rc']==0 and source in a['source_gate'],'writer renewal actual safe metadata/source attestation schema');r.rc(p['outer']+'/rc');cmd=r.doc(p['outer']+'/cmd.json');need(cmd[:3]==['python3','-I','-B'] and '--release-file' in cmd,'writer renewal original helper CLI, no private values supplied inline');metadata=json.loads(h.ref(r,p,a['metadata']));auth_metadata_semantics(metadata)
 return {'kind':p['kind'],'safe_original_members':6,'original_helper_Unix_rc':0,'Root_helper_attests_both_signatures_verified_and_claims_preserved':True,'offline_cryptographic_reverification':False,'private_values_unavailable_to_offline_reader':True,'actor_tenant_roles_metadata_exact':True,'new_expiry_UTC':'2026-11-15T19:18:21Z','old_writer_backup_registry_candidate':22,'backup_presence_and_mode':'Root helper actual metadata only; archive did not stat/open/hash private path','archive_private_values_read':False,'business_calls':0,'secret_scan_package_or_fullP26':False}
