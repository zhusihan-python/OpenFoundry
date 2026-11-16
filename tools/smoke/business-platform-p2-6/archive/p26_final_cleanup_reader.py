"""Independent exact owned10 cleanup after rebuilt final readonly scopes."""
import hashlib,json,importlib.util
from pathlib import Path

def need(ok,why):
 if not ok:raise ValueError(why)
def mod(n):
 s=importlib.util.spec_from_file_location(n,Path(__file__).with_name(n+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
ORDER=['p26connectora','p26connectorb','p26connectorc','p26connectora2','p26connectorb2','p26p25proxy','p26p25fixture','p26p22proxy','p26proxy','p26fixture']
PORTS={'p26connectora':(17097,50088),'p26connectorb':(17098,50088),'p26connectorc':(17100,50088),'p26connectora2':(17101,50088),'p26connectorb2':(17102,50088),'p26fixture':(17095,8090),'p26proxy':(17096,8091),'p26p22proxy':(17099,8091),'p26p25fixture':(17103,8090),'p26p25proxy':(17104,8091)}
FMT='{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}},"ports":{{json .HostConfig.PortBindings}}}'
def cleanup(r,p,S):
 h=mod('p26_h10_actual_reader');f=mod('p26_final_actual_readers');fc=mod('p26_final_composites');proof=p['final_readonly_contract'];fc.component(r,proof,S,'final-db-preservation-abc');a=r.doc(p['audit']);f.exact_members(r,p,a,587);need(a['outer_original_child_rc']==0,'cleanup outer actual Unix0');outer=p['outer'];r.rc(outer+'/original-child.rc');launch=r.doc(outer+'/command.json');need(launch[:3]==['python3','-I','-B'] and launch[-2]=='--release-file','cleanup original released helper CLI');beforepins=r.doc(outer+'/before.json')['file_sha256'];need(beforepins==r.doc(outer+'/after.json')['file_sha256'],'cleanup actual source7tools7proof pins stable');
 for path,digest in beforepins.items():
  if path==launch[-1]:need(hashlib.sha256(r.read(outer+'/Root-release.snapshot.json')).hexdigest()==digest,'cleanup original Rootphase bytepin')
  else:h.ref(r,p,{'path':path,'sha256':digest})
 release=r.doc(outer+'/Root-release.snapshot.json');need(release['root_accepted'] is True and release['candidate_source']==S and release['cleanup_order']==ORDER and release['final_readonly_accepted']==p['final_readonly_proof']==proof['Root_readonly_composite'] and release['final_quiesce_proof']==proof['fresh_quiesce_composite'],'cleanup actual release binds same already independently rebuilt fresh finalreadonly/quiesce')
 need({'path':release['deployment_record'],'sha256':release['deployment_record_sha256']}==proof['proofs']['H_typed']['current_main_record'] and {'path':release['typed_roster'],'sha256':release['typed_roster_sha256']}==proof['typed_union'] and release['final_owned_creation_append']==proof['final_plan'],'cleanup main5/typed9/all310creation scope exact prior actual')
 for name,digest in release['cleanup_tools_sha256'].items():need(hashlib.sha256(r.read(p['producer_snapshots']+'/'+name)).hexdigest()==digest,'cleanup actual frozen source provenance only')
 abc=json.loads(h.ref(r,p,release['ABC_last_write_accepted']));need(abc['ok'] is True,'cleanup original accepted ABC metadata retained');main=json.loads(h.ref(r,p,{'path':release['deployment_record'],'sha256':release['deployment_record_sha256']}));aux=json.loads(h.ref(r,p,{'path':release['final_aux_ledger'],'sha256':release['final_aux_ledger_sha256']}));pins=aux['containers'];need(set(pins)==set(ORDER) and len({v['cid'] for v in pins.values()})==10 and len(main['containers'])==5 and not {v['cid'] for v in pins.values()}&{v['cid'] for v in main['containers'].values()},'cleanup exact owned10 disjoint main5');inner=a['actual_inner'];result=h.docpath(r,p,inner+'/RESULT.json');need(result['ok'] is True and result['source_S']==S and result['oldmain_fixture_PG_unchanged'] is True and result['rows_volumes_networks_images_deleted'] is False,'cleanup original scope/source/result')
 commands={}
 for path in a['files']:
  absolute=path if path.startswith('/') else p['canonical_root_label']+'/tools/smoke/business-platform-p2-6/'+path
  if str(Path(absolute).parent)!=inner or not absolute.endswith('.cmd.json'):continue
  stem=h.local(p,absolute.removesuffix('.cmd.json'));cmd=r.doc(stem+'.cmd.json');meta=r.doc(stem+'.meta.json');r.rc(stem+'.rc');raw=r.read(stem+'.raw');need(isinstance(cmd,list) and meta['original_child_rc']==0 and meta['stderr']=='','cleanup immediate actual Unix0/stderr');commands[Path(stem).name]=(cmd,raw)
 need(len(commands)==53,'cleanup exactly53 original Unix0 commands');ordered=sorted(commands,key=lambda k:int(k.split('-',1)[0]));used=set()
 def one(suffix):
  names=[k for k in commands if k.endswith('-'+suffix)];need(len(names)==1,'cleanup missing/duplicate step '+suffix);name=names[0];used.add(name);return name,commands[name]
 before={}
 for name,pin in main['containers'].items():
  _,(cb,rb)=one('main-before-'+name);_,(ca,ra)=one('main-after-'+name);need(cb==ca==['docker','inspect',pin['cid'],'--format',FMT] and rb==ra,'cleanup main5 fullsafe facts raw byte identical before/after');fact=json.loads(rb);need(all(fact[k]==pin[k] for k in ('cid','image','started_at')) and fact['running'] is True,'cleanup main5 same actual fullCID/image/newboots d34, all running');before[name]=fact
 def target(name,tag,running):
  ordinal,(cmd,raw)=one(tag+'-'+name);pin=pins[name];fact=json.loads(raw);host,inside=PORTS[name];need(cmd==['docker','inspect',pin['cid'],'--format',FMT] and fact['name']=='/'+name and all(fact[k]==pin[k] for k in ('cid','image','started_at')) and fact['running'] is running and pin['ownership']['of-p26-owner']=='p26-http' and pin['ownership']['of-p26-stage']=='p2-6' and all(fact['labels'].get(k)==v for k,v in pin['ownership'].items()) and fact['ports'][str(inside)+'/tcp']==[{'HostIp':'127.0.0.1','HostPort':str(host)}],'cleanup actual owned label/fullCID/image/boot/name/exact loopbackports');return ordinal
 removal=[];stops=[]
 for name in ORDER:
  pin=pins[name];target(name,'owned-before',pin['running']);immediate=target(name,'owned-immediate',pin['running']);rm,(cmd,raw)=one('remove-exact-owned-'+name);need(cmd==['docker','rm',pin['cid']] and raw.decode().strip()==pin['cid'] and int(immediate.split('-')[0])<int(rm.split('-')[0]),'cleanup one exact rm after immediate ownership recheck');removal.append((rm,{'name':name,'cid':pin['cid']}))
  if pin['running']:
   stop,(cmd,raw)=one('stop-exact-owned-'+name);stopped=target(name,'owned-stopped',False);need(cmd==['docker','stop','--time','120',pin['cid']] and raw.decode()=='Flag --time has been deprecated, use --timeout instead\n'+pin['cid']+'\n\n' and int(immediate.split('-')[0])<int(stop.split('-')[0])<int(stopped.split('-')[0])<int(rm.split('-')[0]),'cleanup each running owned server stop120 once before rm');stops.append(name)
  else:need(name in ORDER[:5],'cleanup only connector5 already stopped, no repeated Stop writes')
 need([name for _,name in sorted(removal)]==[{'name':n,'cid':pins[n]['cid']} for n in ORDER] and stops==ORDER[5:],'cleanup exact original Root remove order/5serverstop only');removed=[x for _,x in removal];need(removed==result['removed']==h.docpath(r,p,inner+'/removed-ledger.json'),'cleanup original ledger exact10 observed removals, no inferred deletion')
 _,(cmd,raw)=one('inventory-after-cleanup');need(cmd==['docker','ps','-a','--no-trunc','--format','{{.ID}}'] and not {v['cid'] for v in pins.values()}&set(raw.decode().splitlines()) and {v['cid'] for v in main['containers'].values()}<=set(raw.decode().splitlines()),'cleanup exact original complete inventory excludes10removedCIDs')
 acceptedH=proof['proofs']['H_typed'];ha=r.doc(acceptedH['audit']);expected=json.loads(h.ref(r,p,ha['actual_guard_RESULT']));expected_sql=f.guard_sql(expected);expectedpub={q['def_id']:q for q in expected['records'] if 'checkpoint_body_sha256' in q};httpcount=0
 for side in ('before','after'):
  g=result['guard_'+side];_,(cmd,raw)=one('whole-old-plus-typed-'+side);need(cmd==release['cleanup_typed_guard']['argv'] and json.loads(raw.decode().strip().splitlines()[-1])==g and g['ok'] is True and g['controlled_count']==12 and g['cp_count']==14,'cleanup original subprocess Hguard returns exact embedded full result');need(f.guard_sql(g)==expected_sql and {q['def_id']:q for q in g['records'] if 'checkpoint_body_sha256' in q}==expectedpub,'cleanup before/after fullsafe27/CP/parents/protocol/wholelegacy/public hashes match independent finalH actual')
  original={};responses={};unix=0
  for path in a['files']:
   absolute=path if path.startswith('/') else p['canonical_root_label']+'/tools/smoke/business-platform-p2-6/'+path
   if str(Path(absolute).parent)!=g['evidence_dir'] or not absolute.endswith('.cmd.json'):continue
   stem=h.local(p,absolute.removesuffix('.cmd.json'));cmd=r.doc(stem+'.cmd.json');meta=r.doc(stem+'.meta.json');r.rc(stem+'.rc');raw=r.read(stem+'.raw')
   if isinstance(cmd,list):original[json.dumps(cmd)]=raw;unix+=1
   else:
    need(cmd['method']=='GET' and raw.endswith(b'\n') and meta['status'] in (200,404),'cleanup H HTTP original sentinel0/status, no Unix child');did,suffix=cmd['url'].split('/syncs/',1)[1].split('/',1);need((did,suffix) not in responses,'cleanup duplicate HTTP');body=raw[:-1];responses[(did,suffix)]=(meta['status'],hashlib.sha256(body).hexdigest());
    if meta['status']==404:need(json.loads(body)=={'error':'incremental checkpoint not initialized for this sync definition'},'cleanup H true404 body distinct from semantic{}')
    httpcount+=1
  need(unix==11 and len(responses)==34,'cleanup each H main5+6SQL Unix0 and34publicGET original sentinel0')
  for q in g['records']:
   if 'argv' in q:need(q['rc']==0 and original[json.dumps(q['argv'])]==(q['stdout']+'\n').encode(),'cleanup H original command/raw/Unix0 binding')
  ins={q['argv'][2]:q['stdout'].splitlines() for q in g['records'] if q.get('argv',[])[:2]==['docker','inspect']};need(set(ins)==set(main['containers']),'cleanup H guard main5 only, aux10 actual cleanup receipts separate')
  for n,pin in main['containers'].items():need(ins[n]==[pin['cid'],pin['image'],pin['started_at'],'true'],'cleanup H currentmain5 same d34')
  need(set(responses)=={(did,s) for did in expectedpub for s in ('checkpoint','runs')} and all(responses[(did,'checkpoint')]==(v['checkpoint_status'],v['checkpoint_body_sha256']) and responses[(did,'runs')]==(v['runs_status'],v['runs_body_sha256']) for did,v in expectedpub.items()),'cleanup all68 actual raw GET bodies equal independent finalH CP/fullRun baselines')
 need(used==set(commands) and httpcount==68,'cleanup no extra immediate command or hidden operation')
 return {'kind':p['kind'],'safe_original_members':587,'removed_exact_owned':10,'main5_preserved':True,'original_cleanup_Unix0':53,'server_Stop120_eachonce':5,'already_stopped_connectors_rm_only':5,'H_HTTP_GETs_original_UnixNone':68,'final_readonly_rebuilt':True,'typed_union_preserved':True,'row_volume_network_image_deletions':False}
