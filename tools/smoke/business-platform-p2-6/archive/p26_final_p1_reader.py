"""Independent final P1post9 original two-layer execution and creation projection."""
import hashlib,json,importlib.util,re
from pathlib import Path

def need(ok,why):
 if not ok:raise ValueError(why)
def mod(n):
 s=importlib.util.spec_from_file_location(n,Path(__file__).with_name(n+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
NAMES={'dataset_isolation','pipeline_isolation','connector_isolation','legacy_regression','sync_real_rest','hash_change','sync_failures','build_positive','build_isolation'}
def named(report,stdout):
 found=re.findall(r'^\[smoke\] PASS ([A-Za-z0-9_]+) \(',stdout,re.M);need(len(found)==9 and set(found)==NAMES and not re.search(r'^\[smoke\] (FAIL|UNCOVERED)',stdout,re.M),'P1post original exact9 stdout PASS/zeroFAILuncovered')
 need(set(report['results'])==set(report['evidence'])==NAMES and report['counts']=={'pass':9,'fail':0,'uncovered':0} and report['uncovered']==[] and all(v['status']=='pass' for v in report['results'].values()),'P1post complete mandatory9 report cases/evidence and statuses')

def post(r,p,source):
 h=mod('p26_h10_actual_reader');f=mod('p26_final_actual_readers');a=r.doc(p['audit']);f.exact_members(r,p,a,529);r.rc(p['launch_rc']);outer,inner=p['outer'],p['inner'];out=json.loads(h.ref(r,p,a['outer_result']));inside=json.loads(h.ref(r,p,a['inner_result']));need(out['schema']=='p26-legacy-actual-outer-v1' and out['stage']=='p1post9' and out['suite']=='p1' and out['candidate_source']==source and out['ok'] is True and out['error'] is None and inside['schema']=='p26-p1-actual-outer-v1' and inside['stage']=='p1post9' and inside['candidate_source']==source and inside['ok'] is True,'P1post actual two-layer source/stage/status')
 need(out['links']['p1_outer']=={k:a['inner_result'][k] for k in ('path','sha256')} and inside['p1_report_sha256']==a['report']['sha256'] and inside['adapter_record']=={k:a['adapter'][k] for k in ('path','sha256')},'P1post original outer/inner/report/adapter hash links')
 for base,result in ((outer,out),(inner,inside)):
  need(result['child_original_rc']==result['source_before_original_rc']==result['source_after_original_rc']==0,'P1post original four-layer rc0')
  tools=r.doc(base+'/tools-before.json');need(tools==r.doc(base+'/tools-after.json'),'P1post original per-layer tool bytes stable')
  if base==outer:
   for name,digest in tools.items():need(hashlib.sha256(r.read(base+'/tools-before/'+name)).hexdigest()==digest,'P1post actual frozen imported tool bytes')
  else:need(all(r.doc(outer+'/tools-before.json').get(name)==digest for name,digest in tools.items()),'P1post inner actual29 tools present in actual outer30 source snapshot')
  for side in ('source-before','source-after'):
   stem=base+'/'+side+('/' if base==outer else '.')
   r.rc(stem+'rc');cmd=r.doc(stem+'cmd.json');raw=r.read(stem+('stdout.log' if base==outer else 'stdout'));need(source.encode() in raw and b'CANDIDATE_VERIFY=OK' in raw and '--expect' in cmd and source in cmd,'P1post both original source gates before and after')
 r.rc(outer+'/child/rc');r.rc(inner+'/p1-child.rc');report=json.loads(h.ref(r,p,a['report']));named(report,r.text(inner+'/p1-child.stdout'));need(report['evidence']==p['business_assertion_inputs'],'P1post original full business assertion inputs')
 ad=json.loads(h.ref(r,p,a['adapter']));need(ad['schema']=='p26-w10-execution-v1' and ad['suite']=='p1' and ad['suite_rc']==0 and ad['raised'] is None and ad['abort_reason'] is None and ad['count_ok'] is True and ad['guard_calls']==ad['expected_guard_calls']==0 and ad['cleanup_ok'] is True and ad['pending_holders']==0 and ad['pending_sql_cleanup']==[],'P1post suite adapter original rc/temporary holder+SQL cleanup')
 need(ad['suite_sha256']==r.doc(inner+'/tools-before.json')['regressions/vendor/tree/business-platform-p1/smoke/smoke.py'],'P1post actual unchanged official smoke source SHA')
 events=[];delegates=[];steps={};folder=str(Path(a['adapter']['path']).parent)
 for path in sorted(a['files']):
  if '/suite-commands-' not in path or not path.endswith('.cmd.json'):continue
  rel='tools/smoke/business-platform-p2-6/'+path;stem=rel.removesuffix('.cmd.json');cmd=r.doc(rel);meta=r.doc(stem+'.meta.json');raw=r.read(stem+'.raw');r.rc(stem+'.rc');name=Path(stem).name;steps[name]=(cmd,meta,raw)
  if 'actual-http-owned-create' in name:
   e=json.loads(raw);need(cmd=={'method':e['method'],'path':e['path']} and e['method']=='POST' and e['status']==201 and meta['http_status']==201 and meta['original_rc'] is None,'P1post actual creation HTTP201; no invented original Unix');events.append(e)
  elif name.endswith('-delegate'):delegates.append((name,cmd,meta,raw))
 need(len(steps)==73 and len(events)==57 and len(delegates)==len(ad['commands'])==14,'P1post exact73 four-receipt steps:57 creations+14delegates+2 temporary checks')
 for (_,cmd,meta,raw),record in zip(delegates,ad['commands']):
  need(record['kind']=='delegate' and record['rc']==0 and cmd=={'original':record['original_argv'],'replacement':record['replacement_argv']} and (record['original_argv']==record['replacement_argv'] or (record['original_argv'][-1].lstrip().upper().startswith('SELECT ') and record['replacement_argv']==record['original_argv'][:-1]+['BEGIN READ ONLY;','-c',record['original_argv'][-1],'-c','COMMIT;'])) and raw.endswith(b'\n') and hashlib.sha256(raw[:-1]).hexdigest()==record['stdout_sha256'] and hashlib.sha256(meta['stderr'].encode()).hexdigest()==record['stderr_sha256'],'P1post exact original SQL delegate unchanged argv/rc/stdout/stderr')
 creators=json.loads(h.ref(r,p,a['actual_creators']));need(creators['source_adapter']==a['adapter'] and creators['receipts']==events==ad['actual_owned_receipts'],'P1post full57 actual creator raw/adapter/index identity')
 counts={kind:sum(e['kind']==kind for e in events) for kind in ('dataset','source','def')};need(counts=={'dataset':33,'source':11,'def':13},'P1post actual creation counts; imports not creators')
 sourceids={e['safe_actual_creation']['id'] for e in events if e['kind']=='source'};datasetids={e['safe_actual_creation']['id'] for e in events if e['kind']=='dataset'};defs=[e['safe_actual_creation'] for e in events if e['kind']=='def'];need(len(sourceids)==11 and len(datasetids)==33 and len({d['id'] for d in defs})==13 and all(d['source_id'] in sourceids and d['output_dataset_id'] in datasetids for d in defs),'P1post actual own parent creation chains')
 cmd,meta,raw=steps['44-temporary-object-absent'];need('pg_constraint' in cmd[-1] and 'p1_smoke_block_publish' in cmd[-1] and raw.decode().splitlines()[:2]==['0','public'] and meta['original_rc']==0,'P1post actual temporary CHECK catalog preexist0/public schema')
 cmd,meta,raw=steps['45-owned-inflight-zero'];ids=set(re.findall(r"'([0-9a-f-]{36})'::uuid",cmd[-1]));need(ids and ids<={d['id'] for d in defs} and raw.decode().strip()=='0' and meta['original_rc']==0,'P1post original then-owned inflight zero observation, not invented final13 deletion proof')
 encoded=json.dumps([x[1] for x in delegates]);need(encoded.count('ADD CONSTRAINT p1_smoke_block_publish')==2 and 'DROP CONSTRAINT IF EXISTS p1_smoke_block_publish' in encoded,'P1post exact one unchanged CHECK install (original/replacement) and original DROP cleanup preserved')
 prior=json.loads(h.ref(r,p,p['main_restart_result']));guard=ad['initial_guard'];need(f.guard_sql(guard)==f.guard_sql(prior['guard_after']['result']) and len(f.guard_sql(guard)['controlled'])==10 and len(f.guard_sql(guard)['CP'])==14,'P1post actual whole10CP14/typed7/fullparents/legacy3 matches accepted post-main original')
 pubs=lambda g:{q['def_id']:q for q in g['records'] if 'checkpoint_body_sha256' in q};need(pubs(guard)==pubs(prior['guard_after']['result']),'P1post original old+typed CP/fullRun body hashes unchanged at initial guard')
 originals={}
 for path in a['files']:
  if str(Path('tools/smoke/business-platform-p2-6/'+path).parent)!=h.local(p,guard['evidence_dir']) or not path.endswith('.cmd.json'):continue
  rel='tools/smoke/business-platform-p2-6/'+path;cmd=r.doc(rel);stem=rel.removesuffix('.cmd.json');r.rc(stem+'.rc');raw=r.text(stem+'.raw')
  if isinstance(cmd,list):originals[json.dumps(cmd)]=raw
 for q in guard['records']:
  if 'argv' in q:need(q['rc']==0 and q['stdout']+'\n'==originals[json.dumps(q['argv'])],'P1post actual initial guard SQL/identity original raw/command/rc linkage')
 main=json.loads(h.ref(r,p,a['current_main5']));ins={q['argv'][2]:q['stdout'].strip().splitlines() for q in guard['records'] if q.get('argv',[])[:2]==['docker','inspect']};need(set(ins)==set(main['containers']) and len(ins)==5,'P1post guard is explicit main5, not full config/counter proof')
 for name,values in ins.items():pin=main['containers'][name];need(values==[pin['cid'],pin['image'],pin['started_at'],'true'],'P1post original main5 post-restart identity')
 return {'kind':p['kind'],'named_pass':9,'failed':0,'uncovered':0,'original_rc_layers':[0,0,0,0],'source_gates_two_layers_before_after':[[0,0],[0,0]],'actual_creator_counts':counts,'Def_parent_unique_Sources':len({d['source_id'] for d in defs}),'standalone_created_Source_count':len(sourceids-{d['source_id'] for d in defs}),'temporary_SQL_holders_cleanup':True,'persisted_Source_Def_deletion_claimed':False,'initial_throughP25_294_modified':False,'full_legacy_aggregate_or_final_P26':False}
