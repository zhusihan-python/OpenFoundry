#!/usr/bin/env python3
"""Pure P1 candidate input preparation from actual accepted W8 originals."""
import argparse,hashlib,json,sys,re,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_guard_shim as H
P26=H.P26; S='b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c'

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def put(path,obj):path.write_text(json.dumps(obj,indent=2)+'\n')
def main():
    p=argparse.ArgumentParser();p.add_argument('--w8-original',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    origin=Path(a.w8_original).resolve();out=Path(a.output).resolve()
    if not out.is_relative_to(P26/'results/w10') or out.exists():raise ValueError('exclusive NEW results/w10 directory required')
    result=json.loads((origin/'RESULT.json').read_text())
    if result.get('ok') is not True or result.get('live') is not True or result.get('verdict')!='pass' or result.get('scope')!='current_core_preservation':raise ValueError('actual W8 current-core pass required')
    source=H.G.candidate_hash(H.G.OPENFOUNDRY)
    if source!=S:raise ValueError('actual source not frozen S')
    refs={}
    def read(name):
        data=(origin/name).read_bytes();refs[name]={'path':str(origin/name),'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)};return data.decode()
    read('RESULT.json')
    def step(stem):
        if read(stem+'.rc').strip()!='0':raise ValueError('original rc nonzero '+stem)
        command=json.loads(read(stem+'.cmd.json'));read(stem+'.meta.json');return command['argv'],read(stem+'.raw')
    _,catalog,derived=H.W.load_baseline(H.G.P25_FINAL_DIR)
    controlled_cmd,controlled=step('73-sql-controlled-9');cp_cmd,cp=step('75-sql-cp-owners')
    if controlled_cmd!=H.W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'] or cp_cmd!=H.W.C.OLDPG_READONLY_ARGV['cp_owners']:raise ValueError('original whole SQL argv drift')
    control_check=H.W.compare_controlled9(derived['protected9'],controlled);cp_check=H.W.compare_cp_owners(derived['cp8'],cp)
    if not control_check['ok'] or not cp_check['ok']:raise ValueError('original whole sets not exact old9/8; cannot declare NEW empty')
    for stem in ('74-controlled9-gate','76-cp8-gate','78-cp-parents-gate'):
        if read(stem+'.gate.rc').strip()!='0' or json.loads(read(stem+'.gate.json'))['ok'] is not True:raise ValueError('original gate not0 '+stem)
    catalog['public_views']=H.W.public_view_baselines(derived,catalog)
    _,parents=step('77-sql-cp-parents');ps=next(s for s in H.W.build_step_specs(catalog) if s['id']=='sql-cp-parents')
    parent_check=ps['gate'][1](parents,{'catalog':catalog,'baseline_cp8':derived['cp8']})
    if not parent_check['ok']:raise ValueError('original safe parents not preserved')
    containers={}
    for service,stem in (('dataset','31-inspect-dataset-1-identity'),('pipeline','43-inspect-pipeline-1-identity'),('connector','55-inspect-connector-1-identity'),('fixture','19-inspect-fixture-1-identity')):
        command,raw=step(stem);lines=raw.strip().splitlines();name=getattr(H.W,'Q3_'+service.upper())
        if len(lines)!=7 or lines[4]!='true' or not re.fullmatch('[0-9a-f]{64}',lines[0]) or not re.fullmatch('sha256:[0-9a-f]{64}',lines[1]) or command[:3]!=['docker','inspect',name]:raise ValueError('actual identity raw malformed '+service)
        if service=='fixture' and lines[3]!=H.W.EXPECTED_FIXTURE_STARTED_AT:raise ValueError('old fixture boot changed')
        containers[name]={'cid':lines[0],'image':lines[1],'started_at':lines[3]}
    envdir=Path('/private/tmp/openfoundry-p26-p1pre9-env');data=dict(line.split('=',1) for line in (envdir/'.env').read_text().splitlines() if '=' in line)
    required=('OF_P1_PG_PASSWORD','OF_P1_JWT_SECRET','OF_P1_SERVICE_JWT')
    if any(not data.get(k) for k in required):raise ValueError('required private env key missing')
    defaults={'dataset':'http://127.0.0.1:17078','pipeline':'http://127.0.0.1:17081','connector':'http://127.0.0.1:17088','fixture':'http://127.0.0.1:17090'}
    if any(data.get('OF_P1_'+key.upper()+'_BASE',value)!=value for key,value in defaults.items()):raise ValueError('P1 public endpoint overrides drift from MAIN stage')
    out.mkdir(parents=True)
    typed={'schema':'p26-w10-typed-v1','runs':[],'public_views':[],'root_review_status':'pending','scope':'NEW nonterminal controlled/live and NEW active CP owner preservation only; terminal W7 history without CP owners is outside this retention cohort','actual_empty_NEW_live_evidence':{'basis':'actual post-ALL10 whole SQL original rows equal sealed old9/oldCP8 by full per-field NEW W8 comparators','controlled_counts':control_check['counts'],'cp_counts':cp_check['counts'],'comparisons':{'controlled':control_check,'cp_owners':cp_check,'old_safe_parents':parent_check},'originals':refs}}
    deployment={'source_sha256':S,'containers':containers,'root_review_status':'pending','derived_from_actual_W8':{'path':str(origin),'result_sha256':sha(origin/'RESULT.json')},'original_identity_evidence':{k:v for k,v in refs.items() if 'identity' in k}}
    typed_path=out/'typed-new-live-candidate.json';dep_path=out/'deployment-candidate.json';put(typed_path,typed);put(dep_path,deployment)
    accepted_s=P26/'results/w9/2026-11-15-w9-attempt1/04-s-parity.stdout'
    release={'schema':'p26-w10-release-v1','root_accepted':False,'root_review_status':'pending; Root must fill actual release expiry/authority','stage':'p1pre9','suite':'p1','expires_utc':None,'candidate_source':S,'suite_argv':['--env-dir',str(envdir)],'expect_guard_calls':0,'evidence_root':str(out/'actual'),'deployment_record':str(dep_path),'deployment_record_sha256':sha(dep_path),'typed_roster':str(typed_path),'typed_roster_sha256':sha(typed_path),'owned_sql_templates':['p1-new-target-check'],'serial_owned_window':True}
    wrapper=P26/'regressions/p1/p26_p1_outer.py'
    spec=importlib.util.spec_from_file_location('p1_outer_pure',wrapper);outer=importlib.util.module_from_spec(spec);spec.loader.exec_module(outer)
    release.update(p1_private_env_sha256=sha(envdir/'.env'),p1_env_locator_sha256=sha(envdir/'compose.yaml'),p1_tools_sha256={path:sha(P26/path) for path in sorted(outer.active_paths())},p1_outer_output=str(out/'outer-actual'),p1_candidate_source_file_sha256=sha(accepted_s))
    rp=out/'root-release-candidate.json';put(rp,release)
    argv=['python3','-I','-B',str(P26/'regressions/adapters/w10_suite_adapter.py'),'--stage','p1pre9','--suite','p1','--release-file',str(rp),'--candidate-source-file',str(accepted_s),'--env-dir',str(envdir),'--expect-guard-calls','0','--record',str(out/'actual/adapter.json'),'--','--env-dir',str(envdir)];put(out/'inner-argv.json',argv)
    outer_argv=['python3','-I','-B',str(wrapper),'--release-file',str(rp),'--candidate-source-file',str(accepted_s),'--output',release['p1_outer_output']]
    put(out/'argv.json',outer_argv)
    tools={str(f.relative_to(P26)):sha(f) for f in (P26/'regressions').rglob('*') if f.is_file() and '__pycache__' not in f.parts}
    manifest={'schema':'p26-p1pre9-review-ready-v1','root_accepted':False,'source_S':S,'actual_resource_calls_by_preparer':0,'source_W8_originals':refs,'checked_counts':{'controlled':9,'CPowners':8,'NEW_nonterminal_controlled':0,'NEW_active_CPowners':0},'private_env_keys_present':list(required),'public_bases':defaults,'candidate_inputs':{str(f):sha(f) for f in (typed_path,dep_path,rp,out/'argv.json',accepted_s)},'current_NEW_tools_and_templates':tools,'frozen_P1_active_tools':release['p1_tools_sha256'],'P25_independent_files_may_continue_CODE_ONLY':True,'active_copy_ledger':{'path':str(P26/'regressions/vendor/closure-ledger.json'),'sha256':sha(P26/'regressions/vendor/closure-ledger.json')},'boundary_note':'Current guard will recollect actual state before business writes. No foundation mutation may run concurrently. Terminal W7 history is not erased or adopted.'}
    put(out/'READY-PACKAGE.json',manifest);print(json.dumps({'ready':str(out/'READY-PACKAGE.json'),'NEW_live_controlled':0,'NEW_active_CPowners':0,'review':'pending','live_calls':0}))
if __name__=='__main__':main()
