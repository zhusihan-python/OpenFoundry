#!/usr/bin/env python3
"""One explicitly released legacy stage; original child rc/logs and frozen bytes.

P1 delegates to its reviewed nine-case outer. P22/23/24/ABC delegate to the
copied-suite facade. P25 delegates to the executable copied runtime. No retry.
"""
import argparse, hashlib, importlib.util, json, os, re, subprocess, sys
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[1]; REPO=P26.parents[2]
sys.path.insert(0,str(P26/'regressions/adapters'))
import w10_guard_shim as H
STAGES={'p1':{'p1pre9','p1post9'},'p22':{'p22-full'},'p23':{'p23-full'},'p24':{'p24-full'},'p21':{'p21-ABC-final'},'p25':{'p25-healthy','p25-h10','p25-tail','p25-quiet','p25-h13-ledger','p25-freeze-held','p25-h14-read'}}

def put(path,value): path.write_text(json.dumps(value,indent=2)+'\n')
def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def active_paths(r):
    fixed={'regressions/p26_legacy_outer.py','regressions/p1/p26_p1_outer.py','regressions/vendor/closure-ledger.json','preflight/p26_current_preflight.py','common/p26_gates.py','common/p26_source_hash.py','cohort/p26_cohort_cli.py'}
    fixed.update('regressions/adapters/'+n for n in ('w10_suite_adapter.py','w10_guard_shim.py','w10_owned_sql.py','w10_holder_seam.py','w10_abc_held.py','w10_boot_transition.py'))
    ledgers=[P26/'regressions/vendor/closure-ledger.json']
    if r['suite']=='p25':
        fixed.add('regressions/vendor/p25-closure-ledger.json');ledgers.append(P26/'regressions/vendor/p25-closure-ledger.json')
        fixed.update('regressions/p25/'+n for n in ('p26_p25_runtime.py','p26_p25_transport.py','p26_p25_typed_guard.py','p26_p25_export.py','p26_p25_supplements.py','p26_healthy_bundle.py','p26_h13_ledger_read.py'))
    for ledger in ledgers:
        for item in json.loads(ledger.read_text())['files']:
            path=Path(item['copy']).resolve()
            if not path.is_relative_to(P26/'regressions/vendor'):raise ValueError('active copy outside NEW vendor')
            fixed.add(str(path.relative_to(P26)))
    for binding in r.get('driver_commands',{}).values():
        path=Path(binding['tool']).resolve()
        if not path.is_relative_to(P26/'deployment'):raise ValueError('driver outside NEW deployment')
        fixed.add(str(path.relative_to(P26)))
    for field in ('driver_config_shas',):
        for path in r.get(field,{}):
            path=Path(path);path=path if path.is_absolute() else P26/'deployment'/path
            path=path.resolve()
            if not path.is_relative_to(P26/'deployment'):raise ValueError('driver config outside NEW deployment')
            fixed.add(str(path.relative_to(P26)))
    if r.get('aux_compose'):fixed.add(str(Path(r['aux_compose']).resolve().relative_to(P26)))
    return fixed

def child_environment_bindings(r):
    if r['suite']!='p24': return {}
    if r.get('p24_connector2_base')!='http://127.0.0.1:17097':
        raise ValueError('Root fixed P24 connector2 origin required')
    return {'OF_P24_CONNECTOR2_BASE':r['p24_connector2_base']}


def child_argv(r,release_path,candidate):
    py=['python3','-I','-B'];suite=r['suite']
    if suite=='p1':return py+[str(P26/'regressions/p1/p26_p1_outer.py'),'--release-file',str(release_path),'--candidate-source-file',str(candidate),'--output',r['p1_outer_output']]
    if suite=='p25':return py+[str(P26/'regressions/p25/p26_p25_runtime.py'),'--release-file',str(release_path),'--mode',r['p25_mode'],'--output',r['p25_output'],'--']+r['p25_argv']
    return py+[str(P26/'regressions/adapters/w10_suite_adapter.py'),'--stage',r['stage'],'--suite',suite,'--release-file',str(release_path),'--candidate-source-file',str(candidate),'--env-dir',r['legacy_env_dir'],'--expect-guard-calls',str(r['expect_guard_calls']),'--record',str(Path(r['evidence_root'])/'adapter.json'),'--']+r['suite_argv']

def report_result(r,out,stdout,prior):
    suite=r['suite']
    if suite=='p1':
        p=Path(r['p1_outer_output'])/'RESULT.json';value=json.loads(p.read_text());return value.get('ok') is True,{'p1_outer':{'path':str(p),'sha256':sha(p)}}
    if suite=='p25':
        paths=list(Path(r['p25_output']).glob('runtime-*/RESULT.json'))
        if len(paths)!=1:raise ValueError('one actual runtime terminal receipt required')
        p=paths[0];value=json.loads(p.read_text());ok=value.get('wrapper_exception') is None and value.get('original_engine_or_runner_rc')==0
        return ok,{'runtime_result':{'path':str(p),'sha256':sha(p)}}
    env=Path(r['legacy_env_dir']).resolve()
    if suite=='p24':
        found=re.findall(r'^RESULT_FILE=(.+)$',stdout,re.M)
        if len(found)!=1:raise ValueError('one actual P24 RESULT_FILE required')
        p=Path(found[0]).resolve()
        if p.parent!=env or p.name in prior:raise ValueError('P24 report must be fresh NEW env result')
    else:p=env/('result_'+suite+'.json')
    raw=p.read_bytes()
    if prior.get(p.name)==hashlib.sha256(raw).hexdigest():raise ValueError('unchanged prior report cannot prove this invocation')
    (out/('original-'+p.name)).write_bytes(raw);value=json.loads(raw)
    names=set(r['legacy_expected_names']);rows=value.get('results',{})
    ok=set(rows)==names and all(x.get('status')=='pass' for x in rows.values()) and value.get('counts')=={'pass':len(names),'fail':0} and value.get('official') is True
    adapter=Path(r['evidence_root'])/'adapter.json';a=json.loads(adapter.read_text())
    ok=ok and a.get('cleanup_ok') is True and a.get('pending_sql_cleanup')==[] and a.get('pending_holders')==0 and a.get('count_ok') is True and a.get('abort_reason') is None
    return ok,{'original_report':{'path':str(p),'sha256':sha(p)},'adapter':{'path':str(adapter),'sha256':sha(adapter)}}

def main(argv=None):
    ap=argparse.ArgumentParser();ap.add_argument('--release-file',required=True);ap.add_argument('--candidate-source-file',required=True);ap.add_argument('--output',required=True);a=ap.parse_args(argv)
    rp=Path(a.release_file).resolve();r=H.validate_release(rp)
    if r['suite'] not in STAGES or r['stage'] not in STAGES[r['suite']]:raise ValueError('explicit legacy stage scope required')
    candidate=Path(a.candidate_source_file).resolve();source=r['candidate_source']
    if not re.fullmatch('[a-f0-9]{64}',source) or int(source,16)==0 or sha(candidate)!=r['legacy_candidate_source_file_sha256']:raise ValueError('Root actual candidate artifact/source required')
    if set(r['legacy_active_tools_sha256'])!=active_paths(r):raise ValueError('exact active legacy closure required')
    def tools():
        actual={k:sha(P26/k) for k in r['legacy_active_tools_sha256']}
        if actual!=r['legacy_active_tools_sha256']:raise ValueError('active legacy tool bytes differ from Root')
        return actual
    before=tools();release_sha=sha(rp)
    def phase():
        if H.validate_release(rp)!=r or sha(rp)!=release_sha:raise ValueError('Root phase changed')
        tools()
    out=Path(a.output).resolve()
    if out!=Path(r['legacy_outer_output']).resolve() or not out.is_relative_to(P26/'results/w10') or out.exists():raise ValueError('exclusive Root-pinned outer output required')
    cmd=child_argv(r,rp,candidate)
    if r['legacy_child_argv']!=cmd:raise ValueError('Root exact derived child argv required')
    child_bindings=child_environment_bindings(r)
    out.mkdir(parents=True);put(out/'tools-before.json',before)
    put(out/'child-environment-bindings.json',child_bindings)
    # Byte snapshots preserve what this invocation actually imported, regardless
    # of later permitted tool changes. No credentials/config .env in this map.
    for rel in before:
        dest=out/'tools-before'/rel;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes((P26/rel).read_bytes())
    env=dict(os.environ);env.update(child_bindings);secrets=[v for k,v in env.items() if any(w in k for w in ('JWT','TOKEN','PASSWORD','SECRET','CREDENTIAL'))];original_rc=None;error=None;links={};ok=False;after=None;source_after=None
    def redact(raw):return H.W.redact_text(raw.decode(errors='replace'),secrets)[0].encode()
    def run(directory,argv,childenv=None):
        directory.mkdir(exist_ok=False);put(directory/'cmd.json',argv)
        try:cp=subprocess.run(argv,capture_output=True,env=childenv)
        except BaseException as exc:
            (directory/'rc').write_text('unavailable\n');put(directory/'meta.json',{'original_rc':None,'failure_kind':type(exc).__name__});raise
        (directory/'stdout.log').write_bytes(redact(cp.stdout));(directory/'stderr.log').write_bytes(redact(cp.stderr));(directory/'rc').write_text(str(cp.returncode)+'\n');put(directory/'meta.json',{'original_rc':cp.returncode,'stdout_sha256':sha(directory/'stdout.log'),'stderr_sha256':sha(directory/'stderr.log')});return cp
    src=['python3','-I','-B',str(P26/'common/p26_source_hash.py'),'--repo',str(REPO),'--expect',source]
    first=run(out/'source-before',src)
    if first.returncode:raise ValueError('source-before failed; zero suite calls')
    prior={}
    if r['suite'] in ('p22','p23','p24','p21'):
        ep=Path(r['legacy_env_dir']);prior={p.name:sha(p) for p in ep.glob('result_*.json')}
    put(out/'prior-result-hashes.json',prior)
    try:
        phase()
        secret=H.W.read_jwt_secret();secrets.append(secret)
        private_env=Path(r.get('legacy_env_dir',r.get('p1_env_dir','/private/tmp/no-p26-env')))/'.env'
        if private_env.exists():
            for line in private_env.read_text().splitlines():
                if '=' in line and any(w in line.split('=',1)[0] for w in ('JWT','TOKEN','PASSWORD','SECRET','CREDENTIAL')):secrets.append(line.split('=',1)[1])
        if r['suite']=='p25':
            for key,identity in [('OPENFOUNDRY_SMOKE_JWT',r['p25_identity']),('OPENFOUNDRY_SMOKE_JWT_ORG2',r['p25_org2_identity'])]:
                token=H.W.sign_testing_jwt(secret,identity['actor'],identity['tenant']);env[key]=token;secrets.append(token)
            env['OPENFOUNDRY_SMOKE_ARM_JWT']=env['OPENFOUNDRY_SMOKE_JWT'];env['P25_CONFIRM']='yes'
        phase();cp=run(out/'child',cmd,env);original_rc=cp.returncode
        ok,links=report_result(r,out,redact(cp.stdout).decode(),prior);ok=ok and original_rc==0
    except BaseException as exc:error=type(exc).__name__+': '+str(exc)
    finally:
        try:
            source_after=run(out/'source-after',src).returncode;after=tools();put(out/'tools-after.json',after)
            # Expiry after a completed child is recorded as failure; it never
            # prevents the child's bounded owned-object cleanup from running.
            phase()
        except BaseException as exc:error=(error+'; ' if error else '')+'after-check '+type(exc).__name__+': '+str(exc)
        ok=ok and error is None and source_after==0 and before==after
        result={'schema':'p26-legacy-actual-outer-v1','ok':ok,'stage':r['stage'],'suite':r['suite'],'candidate_source':source,'child_original_rc':original_rc,'source_before_original_rc':first.returncode,'source_after_original_rc':source_after,'tools_before_after_equal':before==after,'release_sha256':release_sha,'error':error,'links':links,'child_step':str(out/'child')}
        (out/'RESULT.json').write_bytes(redact((json.dumps(result,indent=2)+'\n').encode()))
    print(json.dumps({'ok':ok,'output':str(out),'child_original_rc':original_rc}))
    return 0 if ok else original_rc if isinstance(original_rc,int) and original_rc>0 else 2

if __name__=='__main__':
    try:sys.exit(main())
    except (ValueError,KeyError,OSError) as exc:print('legacy outer refused: '+type(exc).__name__+': '+str(exc),file=sys.stderr);sys.exit(2)
