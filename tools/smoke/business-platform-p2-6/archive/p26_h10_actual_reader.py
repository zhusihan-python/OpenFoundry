"""Actual h10 retry crash + immutable SQL/public/whole import preservation."""
import hashlib,json,importlib.util,re,shlex
from pathlib import Path
from datetime import datetime,timezone

def need(ok,why):
 if not ok:raise ValueError(why)
def local(p,path):
 prefix=p['canonical_root_label']+'/';need(path.startswith(prefix),'h10 actual local original');return path[len(prefix):]
def ref(r,p,e):
 raw=r.read(local(p,e['path']));need(hashlib.sha256(raw).hexdigest()==e['sha256'],'h10 actual original SHA')
 if 'bytes' in e:need(len(raw)==e['bytes'],'h10 actual original size')
 return raw
def docpath(r,p,path):return json.loads(r.read(local(p,path)))
def mod(name):
 sp=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m);return m
def rows(raw):
 values=[json.loads(line) for line in raw.decode().splitlines() if line.startswith('[') or line=='null'];need(len(values)==1,'h10 SQL exact original JSON rows');return values[0] or []
def sql(r,p,receipt):
 need(receipt['original_rc']==0 and set(receipt['files'])=={'.cmd.json','.raw','.rc','.meta.json'},'h10 actual SQL complete original four receipts')
 raw={k:ref(r,p,v) for k,v in receipt['files'].items()};need(json.loads(raw['.cmd.json'])==receipt['argv'] and raw['.rc'].strip()==b'0' and json.loads(raw['.meta.json'])['original_rc']==0,'h10 actual SQL argv/Unixrc');return rows(raw['.raw'])
def projections(r,p,a):
 need(len(a['import_projections'])==2,'h10 exact healthy and retained-failure import inputs');eligible={};all_defs=set()
 for index,ip in enumerate(a['import_projections']):
  origraw=ref(r,p,{'path':ip['original_file'],'sha256':ip['original_file_sha256']});semanticraw=ref(r,p,{'path':ip['semantic_file'],'sha256':ip['semantic_file_sha256']});ledger=json.loads(ref(r,p,{'path':ip['ledger_file'],'sha256':ip['ledger_file_sha256']}));original=json.loads(origraw);semantic=json.loads(semanticraw);need(original['count']==semantic['count']==len(original['handles'])==len(semantic['handles'])==len(ledger['rows'])==ip['rows'],'h10 projection full row count')
  need(ledger['schema']=='p26-h10-import-projection-v1' and ledger['original_file']==ip['original_file'] and ledger['original_file_sha256']==ip['original_file_sha256'] and ledger['semantic_file']==ip['semantic_file'] and ledger['semantic_file_sha256']==ip['semantic_file_sha256'],'h10 projection ledger exact original source/semantic binding')
  for i,(before,after,entry) in enumerate(zip(original['handles'],semantic['handles'],ledger['rows'])):
   removed=sorted(k for k in before if k in ('bundle_origin','bundle_origins'));want={k:v for k,v in before.items() if k not in ('bundle_origin','bundle_origins')};canon=lambda row:json.dumps(row,sort_keys=True,separators=(',',':')).encode();need(after==want and entry=={'original_file_sha256':ip['original_file_sha256'],'original_index':i,'original_row_sha256':hashlib.sha256(canon(before)).hexdigest(),'semantic_row_sha256':hashlib.sha256(canon(after)).hexdigest(),'removed_keys':removed},'h10 projection must preserve every semantic field; exact two metadata keys only')
   if after.get('ok') and after.get('kind') in ('source','def','h10-def'):need((after['kind'],after['ref']) not in eligible,'h10 import identity duplicated across healthy/failure inputs');eligible[(after['kind'],after['ref'])]=after
   if after.get('ok') and after.get('kind') in ('def','h10-def'):all_defs.add(after['ref'])
  need(ip['rows']==(96 if index==0 else 4),'h10 projected original96 healthy/full4 failed creations retained')
 need(len(eligible)==42 and len(all_defs)==22,'h10 actual import42 parents /22 preexisting Defs');return eligible,all_defs

def action(r,p,c):
 need(c['rc']==0,'h10 actual runner action original rc0');r.rc(local(p,c['rc_file']));need(hashlib.sha256(r.read(local(p,c['cmd_file']))).hexdigest()==c['cmd_sha256'] and shlex.split(c['cmd'])==docpath(r,p,c['argv_file']),'h10 actual action argv/cmd original')
 for pathkey,hashkey in (('artifact','artifact_sha256'),('stderr_artifact','stderr_sha256')):need(hashlib.sha256(r.read(local(p,c[pathkey]))).hexdigest()==c[hashkey],'h10 actual action raw stdout/stderr')
 return docpath(r,p,c['argv_file'])

def parent_snapshot(r,p,refs,defs):
 observations={}
 for entry in refs:
  event=json.loads(ref(r,p,entry))
  if event.get('method')!='GET' or event.get('status')!=200 or 'resp_body' not in event:continue
  raw=event['resp_body'].encode();need(hashlib.sha256(raw).hexdigest()==event['resp_sha256'] and len(raw)==event['resp_bytes'],'h10 original snapshot HTTP body/hash');parts=event['path'].split('/syncs/',1)
  if len(parts)!=2:continue
  did,_,suffix=parts[1].partition('/')
  if did in defs and suffix in ('','checkpoint','runs'):observations[(did,suffix)]=json.loads(raw)
 need(set(observations)=={(did,suffix) for did in defs for suffix in ('','checkpoint','runs')},'h10 parent complete22 Def/CP/fullRun original observations')
 projected={}
 fields=('id','sync_def_id','status','generation','attempt','control_mode','publish_entered_at')
 for did in sorted(defs):
  definition=observations[(did,'')];cp=observations[(did,'checkpoint')];runs=observations[(did,'runs')];need(isinstance(runs,list) and definition['id']==did,'h10 parent original Def/Run shape');projected[did]={'source_id':definition['source_id'],'checkpoint':cp,'runs':[{k:row.get(k) for k in fields} for row in runs]}
 return observations,projected

def child_semantics(case,stage,registry,semantic):
 need(case.get('id')=='h10' and stage in ('pre','post'),'h10 mandatory child identity/stage')
 semantic.good_case(case,registry['h10'])
 need(len(case['gates'])==({'pre':6,'post':12}[stage]),'h10 actual full mandatory pre/post gate count')

def post_state_semantics(post):
 need(post['lease_expired_by_DB_clock'] is False and post['frozen_for_next_phase'] is False,'h10 original post live lease/nonfrozen remains historical')


def actual(r,p,source):
 a=r.doc(p['audit']);need(len(a['files'])==3282,'h10 actual exact3282 original members')
 for path,pin in a['files'].items():
  need(not path.endswith('.env'),'h10 no private env');raw=r.read('tools/smoke/business-platform-p2-6/'+path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'h10 actual original member changed')
 r.rc(p['launch_rc']);outer=p['outer'];r.rc(outer+'/child/rc')
 for side in ('source-before','source-after'):
  r.rc(outer+'/'+side+'/rc');need(source in r.text(outer+'/'+side+'/stdout.log'),'h10 actual source before/after')
 tools=r.doc(outer+'/tools-before.json');need(tools==r.doc(outer+'/tools-after.json'),'h10 actual toolmap stable')
 for name,digest in tools.items():need(hashlib.sha256(r.read(outer+'/tools-before/'+name)).hexdigest()==digest,'h10 actual imported historical tool source')
 runtime=json.loads(ref(r,p,a['runtime_result']));runner=json.loads(ref(r,p,a['runner_result']));need(runtime['source_S']==source and runtime['original_engine_or_runner_rc']==0 and runtime['wrapper_exception'] is None and runtime['runtime_sha256']==tools['regressions/p25/p26_p25_runtime.py'] and runtime['h10_command_counts']==a['kill_start_counts']=={'kill':1,'start':1} and runner['ok'] is True,'h10 actual runner/runtime source/rc/lifecycle')
 eligible,parentdefs=projections(r,p,a);merged=json.loads(ref(r,p,a['merged_handles']));need(merged['count']==len(merged['handles'])==46,'h10 actual merged46 roster');created=runtime['created_safe'];need(len(created)==4 and {v['kind'] for v in created.values()}=={'source','def','run','dataset'},'h10 actual new four creators only')
 imports=[h for h in merged['handles'] if h.get('imported_from')];new=[h for h in merged['handles'] if not h.get('imported_from')];need({(h['kind'],h['ref']) for h in imports}==set(eligible) and len(imports)==42 and {h['ref'] for h in new}==set(created) and len(new)==4,'h10 merged original healthy40/failed2/new4 exact scope')
 for h in imports:
  want=eligible[(h['kind'],h['ref'])];need({k:v for k,v in h.items() if k not in ('imported_from','imported_sha256')}==want,'h10 imported parent semantic fields unchanged')
 before=json.loads(ref(r,p,a['baseline_before']));after=json.loads(ref(r,p,a['baseline_after']));need(set(before['defs'])==set(before['snapshot'])==parentdefs and len(before['defs'])==22 and before['snapshot']==after['snapshot'],'h10 exact parent22 original preservation snapshot')
 full_before,snap_before=parent_snapshot(r,p,before['raw_records'],parentdefs);full_after,snap_after=parent_snapshot(r,p,after['raw_records'],parentdefs);need(full_before==full_after and snap_before==before['snapshot'] and snap_after==after['snapshot'],'h10 parent full original Def/CP/Run bodies preserved')
 crash=docpath(r,p,runner['crash_record']);need(crash['schema']=='p25-crash-record-v1' and crash['method']=='sigkill+start-same-image' and crash['held']==runner['held']==a['new_held'],'h10 actual crash method/held identity')
 beforeC,afterC=a['C_before'],a['C_after'];cid=beforeC['cid'];need(cid==afterC['cid']==crash['container']==crash['container_id'] and beforeC['image']==afterC['image']==crash['image_before']==crash['image_after']==crash['image'] and beforeC['started_at']==crash['started_at_before'] and afterC['started_at']==crash['started_at_after'] and beforeC['started_at']!=afterC['started_at'],'h10 actual sameCID/image/newboot')
 need(crash['image']=='sha256:ea8244f03a3b07e72b77bd7539e13ae1d39cf91bf90a26d01675f008c64685d6','h10 frozen S2 Connector image')
 need(action(r,p,crash['kill'])==['docker','kill','-s','KILL',cid] and action(r,p,crash['start'])==['docker','start',cid],'h10 actual exact owned SIGKILL/start argv')
 for label in ('inspect_before','inspect_after'):
  action(r,p,crash[label]);info=json.loads(r.read(local(p,crash[label]['artifact'])))[0];pin=beforeC if label=='inspect_before' else afterC;need(info['Id']==pin['cid'] and info['Image']==pin['image'] and info['State']['StartedAt']==pin['started_at'] and info['State']['Running'] is True,'h10 actual independent inspect identities')
 action(r,p,crash['health']);need(0<a['kill_elapsed_seconds']==runner['kill_elapsed_seconds']<5,'h10 original monotonic admission-to-kill under5s')
 utc=lambda s:datetime.fromisoformat(s.replace('Z','+00:00'));need(0<(utc(crash['kill']['finished_at'])-utc(runner['held_trigger_at'])).total_seconds()<5 and utc(crash['timestamps']['pre_at'])<=utc(crash['kill']['started_at'])<utc(crash['kill']['finished_at'])<=utc(crash['start']['started_at'])<utc(crash['start']['finished_at'])<=utc(crash['timestamps']['post_at']),'h10 original UTC action order and completion under5s')
 for c in runner['commands']:action(r,p,c)
 priorpath=str(Path(runner['baseline_file']).parent/'prior.json');priorraw=r.read(local(p,priorpath));prior=json.loads(priorraw);held=a['new_held'];did=held['def_id'];rid=held['run_id'];sid=held['source_id'];need(hashlib.sha256(priorraw).hexdigest()==crash['pre_snapshot_sha256'] and prior['held_def']==did and prior['defs']==[runner['quiet']['def_id'],did] and len(prior['defs'])==2,'h10 official prior remains exact original quiet+newheld2 scope')
 need(runner['quiet']['def_id'] in parentdefs and did not in parentdefs,'h10 official quiet and newheld distinction')
 semantic=mod('p26_p25_proofs');registry=semantic.specs(r.read(outer+'/tools-before/regressions/vendor/p25-runtime/smoke_p25.py'))
 for child in a['children']:
  stage=child['stage'];summary=json.loads(ref(r,p,child['summary']));need(summary['failed']==[] and len(summary['scenarios'])==1 and summary['scenarios'][0]['id']=='h10','h10 exact successful original pre/post case');case=summary['scenarios'][0];child_semantics(case,stage,registry,semantic)
  roster=json.loads(ref(r,p,child['whole_roster']));need(set(roster['defs'])==parentdefs|{did} and len(roster['defs'])==23 and roster['problems']==[] and roster['owner_roster']==child['owner_roster']==summary['final_owner_roster'] and roster['run_roster']==child['run_roster']==summary['final_run_roster'],'h10 child actual full23 Def CP/Run scope')
  need(len(roster['owner_roster'])==len(roster['run_roster'])==1 and roster['owner_roster'][0]['run_id']==roster['run_roster'][0]['run_id']==rid and roster['owner_roster'][0]['def_id']==roster['run_roster'][0]['def_id']==did and roster['owner_roster'][0]['run_status']==roster['run_roster'][0]['status']=='running' and roster['run_roster'][0]['publish_entered_at'] is None,'h10 only registered unknown owner/Run retained')
  if stage=='pre':need(summary['quiet']=={'ok':True,'scope':'h10-pre-expected-inflight','global_quiet':False},'h10 pre expected inflight not globalquiet')
  else:need(summary['quiet']['ok'] is True and summary['quiet']['problems']==[] and after['rosters']==roster,'h10 post actual quiet/full-roster preservation')
  command=next(c for c in runner['commands'] if c['cmd_file'].endswith('/'+('03-pre' if stage=='pre' else '08-post')+'/cmd.txt'));argv=docpath(r,p,command['argv_file']);need(argv[argv.index('--h10-defs')+1].split(',')==prior['defs'] and argv[argv.index('--h10-held-def')+1]==did,'h10 official child exact2 defs unchanged')
 # Immutable anchor and all current SQL tuples use earlier accepted old9/CP8/N observations as baseline, not later self-baselines.
 anchor_reader=mod('p26_h10_anchor_reader');anchor_p=dict(p,anchor={k:a['anchor'][k] for k in ('path','sha256')});accepted=json.loads(ref(r,p,p['accepted_previous_CURRENT']));baselines={}
 for record in accepted['initial_whole_guard']['records']:
  argv=record.get('argv',[])
  if 'psql' not in argv:continue
  parsed=rows(record['stdout'].encode());query=argv[-1]
  if "r.control_mode = 'controlled'" in query:baselines['controlled']=parsed
  elif "r.control_mode = 'legacy'" in query:baselines['legacy']=parsed
  elif 'c.active_run_id IS NOT NULL' in query:baselines['CP']=parsed
  elif ' AS def_id' in query:baselines['parents']=parsed
  elif parsed and set(parsed[0])==set(anchor_reader.SAFE27):baselines['typed']=parsed
  elif parsed and set(parsed[0])=={'id','recovery_protocol'}:baselines['typed_protocol']=parsed
 need(set(baselines)=={'controlled','legacy','CP','parents','typed','typed_protocol'} and len(baselines['controlled'])==9 and len(baselines['CP'])==8,'h10 earlier accepted full old/typed baseline source')
 anchor_p.update(baseline_controlled_rows=baselines['controlled'],baseline_CP_rows=baselines['CP'],baseline_parent_rows=baselines['parents']);anchor=anchor_reader.anchor(r,anchor_p,source);need(anchor['sql_bindings']==a['prekill_sql_original_receipts'] and runtime['pre_kill_anchor']==anchor_p['anchor'],'h10 immutable prekill anchor exact actual SQL/terminal binding')
 for receipt in anchor['sql_bindings'].values():
  meta=json.loads(ref(r,p,receipt['files']['.meta.json']));observed=datetime.strptime(meta['utc'],'%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc);need(observed<=utc(crash['kill']['finished_at']),'h10 prekill raw SQL observed within guarded kill-wrapper completion')
 runtime_prefix=local(p,str(Path(a['runtime_result']['path']).parent))+'/';actual_kills=[]
 for path in a['files']:
  rel='tools/smoke/business-platform-p2-6/'+path
  if rel.startswith(runtime_prefix) and rel.endswith('.cmd.json') and r.doc(rel)==['docker','kill','-s','KILL',cid]:actual_kills.append(rel)
 need(len(actual_kills)==1,'h10 single actual runtime kill command receipt');kill_event=actual_kills[0];need(int(Path(kill_event).name.split('-',1)[0])>max(int(Path(x['files']['.cmd.json']['path']).name.split('-',1)[0]) for x in anchor['sql_bindings'].values()),'h10 actual command ordinal follows every immutable prekill SQL receipt');r.rc(kill_event.removesuffix('.cmd.json')+'.rc');need(r.doc(kill_event.removesuffix('.cmd.json')+'.meta.json')['original_rc']==0,'h10 actual runtime kill original Unix rc')
 post=json.loads(ref(r,p,a['typed_afterpost']));anchor_reader.observation(r,anchor_p,anchor,a['typed_afterpost'],False);post_state_semantics(post);need(a['frozen_for_next_phase'] is False,'h10 audit historical post frozen flag')
 receipts=post['actual_observation_sql_receipts'];need(len(receipts)==7 and receipts==a['afterpost_sql_original_receipts'],'h10 all7 original post SQL receipt scope')
 parsed={k:sql(r,p,v) for k,v in receipts.items()}
 for suffix,key in (('Root-typed-full27','typed'),('Root-typed-protocol','typed_protocol'),('whole-legacy','legacy')):
  matches=[v for k,v in parsed.items() if k.endswith('-'+suffix)];need(len(matches)==1 and matches[0]==baselines[key],'h10 full Root typed N/protocol/legacy original invariants')
 for label,entries in [('pre',anchor['sql_bindings']),('post',{k:next(v for key,v in receipts.items() if key.endswith('-'+suffix)) for k,suffix in anchor_reader.SUFFIX.items()})]:
  query=entries['controlled']['argv'][-1];need("r.control_mode = 'controlled' AND r.status IN ('pending','running')" in query and 'r.id IN (' not in query and 'c.active_run_id IS NOT NULL' in entries['CP']['argv'][-1],'h10 unfiltered whole controlled/CP observations')
 # Original parent runtime public responses after ALL7 post SQL reads bind the typed exported public view.
 high=max(int(Path(v['files']['.cmd.json']['path']).name.split('-',1)[0]) for v in receipts.values());views={};runtime_dir=str(Path(a['runtime_result']['path']).parent);prefix=local(p,runtime_dir)+'/'
 for path in a['files']:
  rel='tools/smoke/business-platform-p2-6/'+path
  if not rel.startswith(prefix) or not rel.endswith('.cmd.json') or int(Path(rel).name.split('-',1)[0])<=high:continue
  command=r.doc(rel);url=command.get('url',{});url=url.get('url') if isinstance(url,dict) else url
  if command.get('method')!='GET' or url not in ['http://127.0.0.1:17100/api/v1/data-connection/syncs/'+did+'/checkpoint','http://127.0.0.1:17100/api/v1/data-connection/syncs/'+did+'/runs']:continue
  stem=rel.removesuffix('.cmd.json');meta=r.doc(stem+'.meta.json');need(meta['http_status']==200 and meta['original_rc'] is None and r.text(stem+'.rc').strip()=='unavailable','h10 exported post public original HTTP status/rc');views[url.rsplit('/',1)[-1]]=json.loads(r.read(stem+'.raw'))
 need(views.get('checkpoint')==post['public_view']['baseline_cp'] and views.get('runs')==post['public_view']['baseline_runs'],'h10 typed public export binds actual original GETs after post SQL')
 need(len(views['runs'])==1 and views['runs'][0]['id']==rid and views['runs'][0]['status']=='running','h10 current original held full Run list exact1')
 entry=post['typed_entry'];need(json.loads(ref(r,p,entry['evidence']))==anchor['row'],'h10 actual exported safe27 evidence exact immutable prekill')
 candidate=json.loads(ref(r,p,a['runtime15_candidate']));previous=json.loads(ref(r,p,p['previous_runtime15']));need(candidate['source_sha256']==previous['source_sha256']==source and beforeC==previous['containers']['p26connectorc'],'h10 exact prior S2 C/source pin');old_failed=json.loads(ref(r,p,a['original_failed_h10_preserved']));need(old_failed['ok'] is False,'h10 first failure retained as failure');want=json.loads(json.dumps(previous));want['containers']['p26connectorc']['started_at']=afterC['started_at'];need(candidate['containers']==want['containers'] and len(candidate['containers'])==15 and candidate['containers']['p26connectorc']['cid']==cid and candidate['containers']['p26connectorc']['image']==beforeC['image'] and candidate['containers']['p26connectorc']['started_at']==afterC['started_at'],'h10 runtime15 exact C boot-only update')
 return {'kind':p['kind'],'actual_h10_crash_verified':True,'kill_start':{'kill':1,'start':1},'kill_elapsed_seconds':a['kill_elapsed_seconds'],'full_parent_snapshot_Defs':22,'child_whole_Defs':23,'official_prior_Defs':2,'pre_gates':6,'post_gates':12,'pre_SQL_originals':4,'post_SQL_originals':7,'post_lease_expired':False,'post_frozen':False,'public_original_HTTP_binding_closed':True,'full_P25_15':False}
