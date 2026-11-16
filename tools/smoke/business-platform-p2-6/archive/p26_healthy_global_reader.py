"""Independent original healthy-global compiler. No producer imports/live calls."""
import hashlib,json,re,importlib.util
from pathlib import Path

def need(ok,why):
 if not ok:raise ValueError(why)
def absolute_read(r,p,path):
 prefix=p['canonical_root_label']+'/'
 if path.startswith(prefix):rel=path[len(prefix):]
 else:
  need(path in p['external_json_map'] and path.endswith('.json') and not path.endswith('.env'),'unknown external/private reference forbidden');entry=p['external_json_map'][path];rel=entry['workspace_path'];need(rel.startswith('tools/smoke/business-platform-p2-6/results/w9/') and rel.endswith('.json') and not rel.endswith('.env'),'explicit workspace Root JSON snapshot mapping')
 return r.read(rel)
def ref(r,p,entry):
 raw=absolute_read(r,p,entry['path']);need(hashlib.sha256(raw).hexdigest()==entry['sha256'],'healthy original reference SHA')
 if 'bytes' in entry:need(len(raw)==entry['bytes'],'healthy original reference size')
 return raw

def case_semantics(case,sid,registry,semantic):
 need(case.get('id')==sid,'mandatory actual case ID')
 try:semantic.healthy_case(case,registry)
 except ValueError as exc:raise ValueError('mandatory healthy semantic: '+str(exc)) from exc

def selected_case_semantics(selected,source_case,recorder,sid,registry,semantic):
 need(selected==source_case==recorder,'mandatory actual source/recorder/selected case triplet')
 case_semantics(source_case,sid,registry,semantic)


def compile_bundle(r,p,source):
 need(p['epoch']==source,'healthy full source epoch')
 audit=json.loads(ref(r,p,p['closure_audit']));bundle=json.loads(ref(r,p,p['bundle']));handles_raw=ref(r,p,p['handles']);manifest=json.loads(ref(r,p,bundle['source_manifest']));closure=audit['hashed_reference_closure'];need(len(closure)==1191,'healthy exact1191 reference closure')
 for path,pin in closure.items():ref(r,p,dict(pin,path=path))
 need(bundle['schema']=='p26-p25-healthy-bundle-v1' and bundle['phase']=='healthy' and bundle['released'] is True and bundle['failed']==[],'healthy original bundle schema/status')
 registry_path=bundle['sources'][0]['step_dir'].rsplit('/',1)[0]+'/tools-before/regressions/vendor/p25-runtime/smoke_p25.py';registry_raw=absolute_read(r,p,registry_path);need(hashlib.sha256(registry_raw).hexdigest()=='f931dca20e5be294e250f551063f282c97233fe812ed54ced56d32809a6e254d','mandatory frozen engine registry source')
 spec=importlib.util.spec_from_file_location('mandatoryhealthy',Path(__file__).with_name('p26_p25_proofs.py'));semantic=importlib.util.module_from_spec(spec);spec.loader.exec_module(semantic);registry=semantic.specs(registry_raw)
 expected={**{'h'+str(i):i-1 for i in range(1,9)},'h9':9,'h12':10,'h13':11,'h15':12};need(manifest['selection']==audit['selection']==expected and len(manifest['sources'])==len(bundle['sources'])==13 and manifest['final_source_index']==12,'healthy exact13source/12selection/final12')
 # Compile actual original source rows and handle origins; each historical runtime keeps its own tool/Root phase.
 loaded=[];allhandles=[];failures=[];case_sources={};counts={};quiet_inputs=[]
 for index,(src,desc) in enumerate(zip(manifest['sources'],bundle['sources'])):
  need(src['summary']==desc['summary_path'] and src['step']==desc['step_dir'] and src['original_rc']==desc['original_rc'],'healthy source descriptor exact actual manifest linkage')
  raw=absolute_read(r,p,src['summary']);need(hashlib.sha256(raw).hexdigest()==desc['summary_sha256'],'healthy original source summary SHA');doc=json.loads(raw);sid=src['case'];need([x['id'] for x in doc['scenarios']]==[sid],'healthy exact original single case source')
  need(set(desc['raw_members'])=={'cmd.json','stdout.log','stderr.log','rc'},'healthy actual runtime step complete4 originals');members={k:ref(r,p,v) for k,v in desc['raw_members'].items()};need(members['rc'].strip()==str(src['original_rc']).encode(),'healthy actual original child rc')
  argv=json.loads(members['cmd.json']);need(argv.count('--release-file')==1 and '--' in argv and argv[argv.index('--release-file')+1]==desc['root_phase']['path'],'healthy original argv/Root phase binding')
  release=json.loads(ref(r,p,desc['root_phase']));runtime=json.loads(ref(r,p,desc['runtime_result']));need(release['schema']=='p26-w10-release-v1' and release['root_accepted'] is True and release['candidate_source']==runtime['source_S']==source and release['p25_mode']==runtime['mode']=='engine' and release['p25_argv']==argv[argv.index('--')+1:] and release['p25_runtime_sha256']==runtime['runtime_sha256'] and runtime['original_engine_or_runner_rc']==src['original_rc'] and runtime['wrapper_exception'] is None and runtime['initial_guard']['ok'] is True,'healthy each original same-S historical runtime/tool/phase')
  evprefix=doc['attempts']['evidence_dir']+'/';records=desc['evidence_records'];paths=[x['path'] for x in records];expectedpaths=sorted(path for path in closure if path.startswith(evprefix) and re.search(r'-attempt-[0-9]+\.json$',path));need(paths==expectedpaths,'healthy complete actual recorder set')
  values=[json.loads(ref(r,p,x)) for x in records];case=doc['scenarios'][0];need(set(desc['scenario_records'])=={sid} and json.loads(ref(r,p,desc['scenario_records'][sid]))==case and sum(x==case for x in values)==1,'healthy actual scenario recorder exact')
  if index==8:
   need(sid=='h9' and src.get('history_only') is True and src['original_rc']==1 and doc['failed']==['finally-quiet'] and doc['quiet']['ok'] is False and doc['final_owner_roster']==doc['final_run_roster']==[] and len(case['gates'])==8 and all(x['ok'] is True for x in case['gates']),'healthy first failedh9 retained/unselected only')
   failures.append({'source_index':index,'summary':src['summary'],'summary_sha256':desc['summary_sha256'],'original_rc':1,'failed':['finally-quiet'],'history_only':True})
  else:need(src.get('history_only') is not True and src['original_rc']==0 and doc['failed']==[] and doc['quiet']['ok'] is True,'healthy selected actual source rc0/quiettrue')
  case_semantics(case,sid,registry,semantic)
  if sid=='h5':need(case['gates']==[] and case['owner_roster']==case['run_roster']==[],'healthy h5 attribution-only without invented assertions')
  need(desc['handles_file']==doc['attempts']['handles_file'],'healthy actual source handles pointer');h=json.loads(ref(r,p,{'path':desc['handles_file'],'sha256':desc['handles_sha256']}));need(h['count']==len(h['handles']),'healthy complete source handle rows');counts[str(index)]=h['count'];quiet_inputs.append({'path':desc['handles_file'],'sha256':desc['handles_sha256']})
  for row in h['handles']:
   origin={'source_index':index,'summary_path':src['summary'],'summary_sha256':desc['summary_sha256'],'handles_path':desc['handles_file'],'handles_sha256':desc['handles_sha256']};allhandles.append(dict(row,bundle_origin=origin))
  loaded.append((doc,case,values))
 unique=[];indexed={}
 for h in allhandles:
  key=(h.get('kind'),h.get('ref')) if h.get('ref') else ('failed',len(unique))
  if key in indexed:unique[indexed[key]]['bundle_origins'].append(h['bundle_origin'])
  else:indexed[key]=len(unique);unique.append(dict(h,bundle_origins=[h['bundle_origin']]))
 need(handles_raw==(json.dumps({'handles':unique,'count':len(unique)},indent=2)+'\n').encode() and len(unique)==96 and len(allhandles)==11739 and counts==audit['source_handle_origin_occurrences'],'healthy96 handles and EVERY11739 origin independently rebuilt')
 selected=[]
 for sid,index in expected.items():
  doc,case,values=loaded[index];need(case['id']==sid and index!=8,'history-only cannot satisfy actual selected case');desc=bundle['sources'][index];selected_case_semantics(next(x for x in bundle['scenarios'] if x['id']==sid),case,json.loads(ref(r,p,desc['scenario_records'][sid])),sid,registry,semantic);selected.append(case);case_sources[sid]={'source_index':index,'summary_path':desc['summary_path'],'summary_sha256':desc['summary_sha256'],'original_rc':manifest['sources'][index]['original_rc'],'scenario_record':desc['scenario_records'][sid]}
 finaldoc,_,finalrecords=loaded[12];rosters=[x for x in finalrecords if isinstance(x,dict) and set(x)=={'defs','owner_roster','run_roster','problems'}];defs={x['ref'] for x in unique if x.get('ok') and x.get('kind') in ('def','h10-def')};need(len(defs)==21 and len(rosters)==1 and len(rosters[0]['defs'])==21 and set(rosters[0]['defs'])==defs and rosters[0]['owner_roster']==rosters[0]['run_roster']==rosters[0]['problems']==[] and finaldoc['final_owner_roster']==finaldoc['final_run_roster']==[],'healthy full final21 Def roster and empty owner/run state')
 # Re-read both accepted independent supplement components, without executing their producers.
 spec=importlib.util.spec_from_file_location('supplement',Path(__file__).with_name('p26_p25_supplement_readers.py'));supp=importlib.util.module_from_spec(spec);spec.loader.exec_module(supp);supp.ledger(r,p['ledger_component'],source);supp.quiet(r,p['quiet_component'],source)
 quiet=json.loads(ref(r,p,{'path':manifest['quiet_proof'],'sha256':closure[manifest['quiet_proof']]['sha256']}));need(quiet['handles_inputs']==quiet_inputs,'healthy globalquiet exact original13 handle order')
 ledgeraudit=r.doc(p['ledger_component']['audit']);need(ledgeraudit['ledger_proof']['path']==p['ledger_proof'],'healthy actual h13 proof path');ledger=json.loads(ref(r,p,ledgeraudit['ledger_proof']));need(bundle['h13_ledger_supplement']==ledger,'healthy original independent h13 supplement linkage')
 rebuilt={'schema':'p26-p25-healthy-bundle-v1','phase':'healthy','released':True,'failed':[],'scenarios':selected,'quiet':quiet['quiet'],'final_owner_roster':[],'final_run_roster':[],'retained_owner_origins':[],'original_failures_preserved':failures,'case_sources':case_sources,'sources':bundle['sources'],'h13_ledger_supplement':ledger,'source_manifest':bundle['source_manifest'],'attempts':{'handles_file':manifest['handles_output']}}
 need(rebuilt==bundle and manifest['handles_output']==p['handles']['path'],'healthy entire original bundle exactly rebuilt')
 for name in ('build-actual','verify-actual'):
  base=p['directory']+'/'+name;r.rc(base+'/rc');cmd=r.doc(base+'/cmd.json');stdout=r.doc(base+'/stdout.log');r.read(base+'/stderr.log');need(cmd[:3]==['python3','-I','-B'] and cmd[3].endswith('/p26_healthy_bundle.py') and cmd[cmd.index('--manifest')+1]==bundle['source_manifest']['path'] and cmd[cmd.index('--output')+1]==p['bundle']['path'] and ('--verify' in cmd)==(name=='verify-actual') and stdout=={'ok':True,'selected':12,'retained_failed':0},'healthy original pure build/verify child receipts')
 return {'kind':'p25-healthy-global','full_healthy12':True,'full_P25_15':False,'source_count':13,'selected_cases':12,'handle_count':96,'preserved_origin_occurrences':11739,'full_owned_Def_roster':21,'failed_h9_history_only':True,'h5_assertion_coverage':False,'build_original_Unix_rc':0,'verify_original_Unix_rc':0,'external_Root_JSON_snapshots':13}
