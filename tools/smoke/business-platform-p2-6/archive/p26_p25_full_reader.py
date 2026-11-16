"""Compose independently read P25 originals; full P26/package remains separate."""
import hashlib,json,importlib.util
from pathlib import Path

def need(ok,why):
 if not ok:raise ValueError(why)
def mod(n):
 s=importlib.util.spec_from_file_location(n,Path(__file__).with_name(n+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def full(r,p,source):
 need(p['contract_version']=='p26-actual-P25-full15-v1' and p['epoch']==source and p['sidecar_bases']=={'fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'},'full P25 exact current epoch/base/version')
 sem=mod('p26_p25_proofs');engine=r.read(p['engine_source']);need(hashlib.sha256(engine).hexdigest()==p['engine_sha256']=='f931dca20e5be294e250f551063f282c97233fe812ed54ced56d32809a6e254d','full P25 frozen original registry source');registry=sem.specs(engine)
 hg=p['healthy'];need(hg['kind']=='p25-healthy-global','full P25 mandatory original healthyglobal compiler');healthy=mod('p26_healthy_global_reader').compile_bundle(r,hg,source);need(healthy['full_healthy12'] is True,'full P25 complete healthy12')
 components=p['healthy_components'];ids=[c['case'] for c in components];expected=set(sem.HEALTHY_GATE_COUNTS);need(len(ids)==12 and set(ids)==expected,'full P25 mandatory all12 deep component readers; attribution h5 explicit')
 for c in components:
  need(c['kind']=='p25-actual-healthy-component' and c['epoch']==source,'full P25 healthy actual component kind/epoch');sem.healthy_one(r,c,source)
 need(p['h10']['kind']=='p25-h10-actual-complete' and p['tail']['kind']=='p25-tail-actual-component' and p['h14']['kind']=='p25-h14-independent-component','full P25 mandatory actual crash+freeze/tail/independent remote fullbytes components')
 crash=mod('p26_h10_freeze_reader').h10_complete(r,p['h10'],source);tail=mod('p26_p25_tail_reader').tail(r,p['tail'],source);remote=mod('p26_h14_independent_reader').independent(r,p['h14'],source)
 need(p['tail']['known_h10_binding']==crash['binding']['known_bindings'],'full P25 tail uses same independent frozen crash owner')
 need(set(p['supplements'])=={'h12','h13','h14','quiet','typed_export'},'full P25 mandatory supplement exact scope')
 supplements=p['supplements'];need(supplements['h12']==next(c for c in components if c['case']=='h12') and supplements['h13']==hg['ledger_component'] and supplements['quiet']==hg['quiet_component'] and supplements['h14']==p['h14'] and supplements['typed_export']==p['h14']['typed_union'],'full P25 exact original supplement/component linkage')
 need(next(c for c in components if c['case']=='h5')['h5_legacy_compatibility']==p['h5_legacy_compatibility'],'full P25 h5 attribution original same-S2 P22 proof linkage')
 # All original failures remain classified as history; neither can satisfy a case.
 history=p['history'];need(set(history)=={'h9_failed','h10_failed','h10_later_CURRENT'},'full P25 retain actual failures and later CURRENT separately')
 mod('p26_h9_history_proofs').failed(r,history['h9_failed'],source);mod('p26_failed_h10_history').history(r,history['h10_failed'],source);mod('p26_h10_current_reader').current(r,history['h10_later_CURRENT'],source)
 # Exact nonsecret Root phase snapshots remain relocatable; never read referenced env files.
 phases=p['additional_Root_JSON_map'];required_phases=set()
 for component in (history['h10_failed'],p['h10']['crash'],p['h10']['freeze'],p['tail'],p['h14']):
  audit=r.doc(component['audit']);entry=audit.get('release') or audit.get('root_release');required_phases.add(entry['path']);pin=phases[entry['path']];need(entry['path'].endswith('.json') and pin['workspace_path'].startswith('tools/smoke/business-platform-p2-6/results/w9/') and pin['workspace_path'].endswith('.json'),'full P25 explicit Root JSON copy mapping only')
  raw=r.read(pin['workspace_path']);need(hashlib.sha256(raw).hexdigest()==entry['sha256']==pin['sha256'],'full P25 exact original Root JSON bytes');release=json.loads(raw);runtime=json.loads(mod('p26_h10_actual_reader').ref(r,p,audit['runtime_result']));argv=r.doc(component['outer']+'/child/cmd.json');need(release['schema']=='p26-w10-release-v1' and release['root_accepted'] is True and release['candidate_source']==source and release['p25_mode']==runtime['mode'] and release['p25_runtime_sha256']==runtime['runtime_sha256'] and argv[argv.index('--release-file')+1]==entry['path'] and argv[argv.index('--')+1:]==release['p25_argv'],'full P25 each actual historical Root/runtime/argv contract')
 need(set(phases)==required_phases and len(required_phases)==5,'full P25 exact five additional historical Root JSON mappings')
 coverage=expected|{'h10','h11','h14'};need(coverage==set(registry) and len(coverage)==15 and len(expected)==12,'full P25 exact15 original cases, no synthetic IDs')
 # Final active scope equals the complete imported/new actual creation union.
 h=mod('p26_h10_actual_reader');healthyhandles=json.loads(h.ref(r,p,hg['handles']));ha=r.doc(p['h10']['crash']['audit']);ta=r.doc(p['tail']['audit']);finalhandles=json.loads(h.ref(r,p,ta['actual_handles']));defs=lambda data:{x['ref'] for x in data['handles'] if x.get('ok') and x['kind'] in ('def','h10-def')};healthydefs=defs(healthyhandles);faileddefs={x['ref'] for x in json.loads(h.ref(r,p,p['retained_failed_creations']))['handles'] if x['kind']=='def'};newheld={ha['new_held']['def_id']};summary=json.loads(h.ref(r,p,ta['summary']));taildefs={v['def_id'] for k,v in summary['tail_bindings'].items() if v['kind']=='owner' and k!='h10-held'};need(len(healthydefs)==21 and len(faileddefs)==1 and len(taildefs)==5 and defs(finalhandles)==healthydefs|faileddefs|newheld|taildefs and len(defs(finalhandles))==28,'full P25 exact final28 Def creation/import scope retains failed original')
 return {'kind':'p25-actual-full','full_P25_15':True,'cases':sorted(coverage,key=lambda x:int(x[1:])),'healthy_selected':12,'h5_assertion_coverage':False,'h5_same_S2_P22_compatibility':True,'healthy_h9_failed_history_only':True,'failed_h10_history_only':True,'actual_h10_crash_and_natural_freeze':True,'tail_whole_Def_count':28,'final_owner_count':6,'final_live_known_held':1,'typed_N_plus_held_plus_tail':7,'h14_independent_full_download_bytes':558,'h14_independent_full_download_rows':24,'supplements':['h12','h13','h14','quiet','typed_export'],'full_P26':False,'actual_package_or_secret_scan':False}
