"""Independent main3 restart originals, exact order and complete fixed invariants."""
import hashlib,json,importlib.util
from pathlib import Path
from datetime import datetime

def need(ok,why):
 if not ok:raise ValueError(why)
def mod(n):
 s=importlib.util.spec_from_file_location(n,Path(__file__).with_name(n+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def transition(previous,current):
 need(set(previous)==set(current)=={'cid','image','started_at'} and previous['cid']==current['cid'] and previous['image']==current['image'] and datetime.fromisoformat(previous['started_at'].replace('Z','+00:00'))<datetime.fromisoformat(current['started_at'].replace('Z','+00:00')),'main restart exact same CID/image, actual boot changes only')

def restart(r,p,source):
 h=mod('p26_h10_actual_reader');f=mod('p26_final_actual_readers');a=r.doc(p['audit']);f.exact_members(r,p,a,839);need(a['source_sha256']==source and a['outer_original_child_rc']==0,'main restart source/original outer0');outer=p['outer'];r.rc(outer+'/original-child.rc');release=r.doc(outer+'/Root-release.snapshot.json');need(release['root_accepted'] is True and release['candidate_source']==source,'main restart original Root release/source');outerresult=r.doc(outer+'/RESULT.json');need(outerresult['original_child_rc']==0 and len(outerresult['inner_output'])==1,'main restart original child/inner');inner=outerresult['inner_output'][0];result=h.docpath(r,p,inner+'/RESULT.json');need(result['ok'] is True and result['source_S']==source and result['completed']==['dataset','pipeline','connector'] and result['rollback_executed'] is False,'main restart result exact completed order/no rollback')
 need(r.doc(outer+'/before.json')['file_sha256']==r.doc(outer+'/after.json')['file_sha256'],'main restart actual input code/data hashes stable')
 for path,digest in release['final_main_tools_sha256'].items():need(hashlib.sha256(r.read(p['producer_snapshots']+'/'+path)).hexdigest()==digest,'main restart exact Root-pinned producer code snapshot, never imported')
 steps={}
 for path in a['files']:
  if str(Path(path).parent)==inner and path.endswith('.cmd.json'):
   stem=path.removesuffix('.cmd.json');steps[Path(stem).name]=f.command(r,p,stem)
 need(len(steps)==120 and sum(isinstance(v[0],list) for v in steps.values())==81 and sum(isinstance(v[0],dict) for v in steps.values())==39,'main restart exact120 originals:81 Unix0/39HTTP200 transport0, no HTTP Unix claim')
 ordered=sorted(steps,key=lambda s:int(s.split('-',1)[0]));actual_restarts=[name for name in ordered if isinstance(steps[name][0],list) and steps[name][0][:2]==['docker','restart']];need(actual_restarts==['33-restart-only-dataset','60-restart-only-pipeline','87-restart-only-connector'],'main restart exact actual command ordinal DVS then Pipeline then Connector, each once')
 output=release['main_restart_output'];proof=h.docpath(r,p,output+'/restart-transition-proof.json');need(proof['schema']=='p26-verified-main-boot-chain-v1' and proof['derived_only_fields']==['containers.main3.started_at'] and [x['service'] for x in proof['steps']]==['dataset','pipeline','connector'],'main original verified transition chain/only boot fields')
 for path,pin in proof['original_artifact_sha256'].items():h.ref(r,p,dict(pin,path=path))
 base=json.loads(h.ref(r,p,p['base_runtime15']));derived=h.docpath(r,p,output+'/derived-deployment-record.json');mainnames={'openfoundry-platform-p1-q3v9-'+s+'-1' for s in ('dataset','pipeline','connector')};main5=mainnames|{'openfoundry-platform-p1-q3v9-postgres-1','openfoundry-platform-p1-q3v9-fixture-1'};need(set(derived['containers'])==main5 and len(base['containers'])==15 and derived['source_sha256']==source,'main derived record explicit5 projection, not silently runtime15')
 newboots={}
 for index,step in enumerate(proof['steps']):
  service=step['service'];before,after=step['previous'],step['current'];transition(before,after);name='openfoundry-platform-p1-q3v9-'+service+'-1';need(all(before[k]==base['containers'][name][k] for k in before),'main immutable original preboot anchor');newboots[name]=after['started_at'];boot=h.docpath(r,p,step['proof']);need(boot['previous']==before and boot['current']==after and boot['service']==service,'main per-service boot ledger exact original transition')
  restart=steps[Path(boot['restart_step']).name][0];need(restart==['docker','restart','--time','120',before['cid']],'main exact sameCID restart argv')
  for role,identity in (('before',before),('after',after)):
   references=boot[role];cmd,meta,raw=steps[Path(references['identity']).name];need(cmd[:3]==['docker','inspect',identity['cid']] and raw.decode()==identity['cid']+'\n'+identity['image']+'\n'+identity['started_at']+'\ntrue\n\n','main exact original boot identity raw')
  for field in ('mounts','ports','env_names'):
   cb,_,rb=steps[Path(boot['before'][field]).name];ca,_,ra=steps[Path(boot['after'][field]).name];need(cb==ca and rb==ra and cb[:3]==['docker','inspect',before['cid']],'main mounts/ports/envnames unchanged, no env value reads');
   if field=='env_names':need('(index (split . "=") 0)' in cb[-1],'main env names only projection')
  need(derived['containers'][name]==dict(after,expected_running=True),'main derived main3 exact actual newboot')
 for name in main5-mainnames:need(derived['containers'][name]=={k:base['containers'][name][k] for k in ('cid','image','started_at')}|{'expected_running':True},'main PG/oldfixture derived exact unchanged pin')
 # Each fixed PG/oldfixture field has four actual observations, never restarted.
 fixed={}
 for name,(cmd,meta,raw) in steps.items():
  if '-fixed-' not in '-'+name or not isinstance(cmd,list):continue
  suffix=next((s for s in ('identity','mounts','ports','env_names') if name.endswith('-'+s)),None)
  if suffix:fixed.setdefault((cmd[2],suffix),[]).append((cmd,raw))
 need(len(fixed)==8 and all(len(v)==4 for v in fixed.values()),'main PG/oldfixture four full observations of each field')
 for (cid,field),samples in fixed.items():
  need(all(x[0]==samples[0][0] for x in samples),'main fixed original argv unchanged')
  values=[sorted(json.loads(x[1]),key=lambda m:m['Destination']) if field=='mounts' else x[1] for x in samples];need(all(x==values[0] for x in values),'main fixed full original fields unchanged; mount arrays compared by Destination only, raw order retained')
 # Old fixture inventories are nine exact URLs sampled four times unchanged.
 counters={};health=[]
 for name in ordered:
  cmd,meta,raw=steps[name]
  if isinstance(cmd,dict):
   need(cmd['method']=='GET' and meta['original_child_rc'] is None and meta['transport_outcome']==0 and meta.get('status',meta.get('http_status'))==200,'main only original readonly HTTP200/transport0')
   if '/admin/src/' in cmd['url']:
    need(cmd['auth_attached'] is False and raw.endswith(b'\n') and hashlib.sha256(raw[:-1]).hexdigest()==meta['body_sha256'] and len(raw)-1==meta['bytes'],'main oldfixture rawbody/captureLF original hash');counters.setdefault(cmd['url'],[]).append(raw)
   else:health.append(cmd['url'])
 need(len(counters)==9 and all(len(v)==4 and len(set(v))==1 for v in counters.values()) and health==['http://127.0.0.1:17078/healthz','http://127.0.0.1:17081/healthz','http://127.0.0.1:17088/healthz'],'main strict9×4 counter raw equality and service health original order')
 aux=r.doc(outer+'/ACTUAL-AUX10-UNCHANGED.json')['containers'];prioraux=json.loads(h.ref(r,p,{'path':release['final_aux_ledger'],'sha256':release['final_aux_ledger_sha256']}));need(aux==prioraux['containers'] and set(aux)|main5==set(base['containers']) and not set(aux)&main5,'main exact prior actual aux10 plus main5 complete runtime15, no merge assumptions');need(len(aux)==10 and sum(v['running'] is False for v in aux.values())==5,'main exact actual aux10=5stopped5running');quiesce=json.loads(h.ref(r,p,p['quiesce_result']));need({k:v for k,v in aux.items() if not v['running']}==quiesce['stopped_connectors'],'main exact accepted quiesce5 remain stopped')
 for name,pin in aux.items():
  need(all(pin[k]==base['containers'][name][k] for k in ('cid','image','started_at')),'main aux actual original runtime15 identity unchanged')
  pairs=[v for k,v in steps.items() if k.endswith('-quiet-owned-'+name)];need(len(pairs)==2 and pairs[0][0]==pairs[1][0] and pairs[0][2]==pairs[1][2],'main actual aux10 each exact before+after originalinspect');lines=pairs[0][2].decode().splitlines();need(lines[:4]==[pin['cid'],pin['image'],pin['started_at'],str(pin['running']).lower()] and {k:json.loads(lines[4])[k] for k in pin['ownership']}==pin['ownership'],'main actual aux identity/state/owned labels ledger exact')
 gb,ga=result['guard_before']['result'],result['guard_after']['result'];need(f.guard_sql(gb)==f.guard_sql(ga)==f.guard_sql(quiesce['guard_after']),'main exact full safe27/CP/parents/protocol/old+typed unchanged across restart');pub=lambda g:{q['def_id']:q for q in g['records'] if 'checkpoint_body_sha256' in q};need(pub(gb)==pub(ga)==pub(quiesce['guard_after']),'main full old+typed CP/fullRun hashes unchanged')
 for guard,after in ((gb,False),(ga,True)):
  ins={q['argv'][2]:q['stdout'].strip().splitlines() for q in guard['records'] if q.get('argv',[])[:2]==['docker','inspect']};need(len(ins)==5 and set(ins)==main5,'main originalguard explicitly main5; aux10 independently observed before/after')
  for name,values in ins.items():
   old=base['containers'][name];need(values[:2]==[old['cid'],old['image']] and values[2]==(newboots[name] if after and name in newboots else old['started_at']) and values[3]==('false' if name in quiesce['stopped_connectors'] else 'true'),'main original15 actual observations exact allowed3boot transitions/5stopped')
  for path in a['files']:
   if str(Path(path).parent)!=guard['evidence_dir'] or not path.endswith('.cmd.json'):continue
   rel=h.local(p,path);cmd=r.doc(rel);stem=rel.removesuffix('.cmd.json');r.rc(stem+'.rc');raw=r.text(stem+'.raw')
   if isinstance(cmd,list):need(any(q.get('argv')==cmd and q['stdout']+'\n'==raw and q['rc']==0 for q in guard['records']),'main guard full original command/raw/rc linkage')
 need(all(result[k] is True for k in ('PG_oldfixture_unchanged','fixed_oldfixture_counters_unchanged','wholetyped_preserved')),'main original final facts')
 return {'kind':p['kind'],'restart_order':['dataset','pipeline','connector'],'restart_each':1,'sameCID_image':True,'actual_main3_newboots':newboots,'original_Unix0_operations':81,'HTTP_GET200_transport0_operations':39,'HTTP_original_Unix_rc':None,'PG_oldfixture_four_samples_unchanged':True,'old_fixture_counter_URLs_samples':[9,4],'aux_actual_before_after_inspects':20,'aux_running_stopped':[5,5],'main5_derived_projection_explicit':True,'main5_guard_plus_actual_aux10_complete':True,'typed7_controlled10_CP14_legacy3_unchanged':True,'postP1_receipt_ABC_or_fullP26':False}
