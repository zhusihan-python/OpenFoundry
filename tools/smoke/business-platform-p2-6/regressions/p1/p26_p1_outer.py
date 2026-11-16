#!/usr/bin/env python3
"""P1 actual outer wrapper: pinned active tools/source before+after, real rc/logs.

No Root release => zero child/resource calls. Independent P25 copy directories
are deliberately outside this frozen P1 import closure.
"""
import argparse,hashlib,json,re,subprocess,sys
from datetime import datetime,timezone
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[2];ROOT=P26.parents[2]
NAMES={'build_isolation','build_positive','connector_isolation','dataset_isolation','hash_change','legacy_regression','pipeline_isolation','sync_failures','sync_real_rest'}

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def put(path,obj):path.write_text(json.dumps(obj,indent=2)+'\n')
def active_paths():
    fixed={'regressions/p1/p26_p1_outer.py','regressions/vendor/closure-ledger.json','preflight/p26_current_preflight.py','common/p26_gates.py','common/p26_source_hash.py','cohort/p26_cohort_cli.py'}
    fixed.update('regressions/adapters/'+n for n in ('w10_suite_adapter.py','w10_guard_shim.py','w10_owned_sql.py','w10_holder_seam.py','w10_abc_held.py','w10_boot_transition.py'))
    for item in json.loads((P26/'regressions/vendor/closure-ledger.json').read_text())['files']:
        path=Path(item['copy']).resolve()
        if not path.is_relative_to(P26):raise ValueError('active copy outside NEW P26')
        fixed.add(str(path.relative_to(P26)))
    return fixed

def main():
    p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);p.add_argument('--candidate-source-file',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    release_path=Path(a.release_file).resolve();r=json.loads(release_path.read_text())
    if r.get('schema')!='p26-w10-release-v1' or r.get('root_accepted') is not True or r.get('stage') not in ('p1pre9','p1post9') or r.get('suite')!='p1':raise ValueError('Root P1pre/post9 release required; no actual calls')
    expiry=datetime.fromisoformat(r['expires_utc'].replace('Z','+00:00'))
    if expiry.tzinfo is None or datetime.now(timezone.utc)>=expiry:raise ValueError('P1 phase expired')
    expected=r['p1_tools_sha256']
    ledger='regressions/vendor/closure-ledger.json'
    if expected.get(ledger)!=sha(P26/ledger):raise ValueError('Root active copy ledger SHA mismatch')
    if set(expected)!=active_paths():raise ValueError('Root tool map must cover exact active P1 closure')
    def tool_check():
        actual={path:sha(P26/path) for path in expected}
        if actual!=expected:raise ValueError('Root P1 source/tool closure drift')
        return actual
    env=Path(r['p1_env_dir']).resolve()
    source=r['candidate_source']
    if not re.fullmatch('[a-f0-9]{64}',source) or int(source,16)==0:raise ValueError('full actual Root candidate source required')
    if env.parent!=Path('/private/tmp') or not re.fullmatch('openfoundry-p26-[A-Za-z0-9_-]+-'+r['stage']+'-env',env.name):raise ValueError('exclusive named NEW pre/post envdir required; old-S env must not be reused')
    def private_check():
        if sha(env/'.env')!=r['p1_private_env_sha256'] or sha(env/'compose.yaml')!=r['p1_env_locator_sha256'] or (env/'.env').stat().st_mode & 0o777 != 0o600 or env.stat().st_mode & 0o777 != 0o700 or (env/'compose.yaml').stat().st_mode & 0o777 != 0o600:
            raise ValueError('private NEW env/locator bytes or mode drift')
    private_check()
    before=tool_check();out=Path(a.output).resolve()
    if not out.is_relative_to((P26/'results/w10').resolve()) or out.exists() or out!=Path(r['p1_outer_output']).resolve():raise ValueError('exclusive Root-pinned outer output required')
    if r['expect_guard_calls']!=0 or r['suite_argv']!=['--env-dir',str(env)]:raise ValueError('exact P1 stage args/source/count required')
    if sha(a.candidate_source_file)!=r['p1_candidate_source_file_sha256']:raise ValueError('accepted S artifact pin drift')
    out.mkdir(parents=True);put(out/'tools-before.json',before)
    commands=[]
    def run(name,argv):
        put(out/(name+'.cmd.json'),argv)
        try:cp=subprocess.run(argv,capture_output=True)
        except Exception as exc:
            (out/(name+'.rc')).write_text('unavailable\n');put(out/(name+'.exception.json'),{'original_rc':None,'error':type(exc).__name__+': '+str(exc)});raise
        (out/(name+'.stdout')).write_bytes(cp.stdout);(out/(name+'.stderr')).write_bytes(cp.stderr);(out/(name+'.rc')).write_text(str(cp.returncode)+'\n');commands.append({'name':name,'original_rc':cp.returncode});return cp
    source_argv=['python3','-I','-B',str(P26/'common/p26_source_hash.py'),'--repo',str(ROOT),'--expect',source]
    first=run('source-before',source_argv)
    if first.returncode:raise ValueError('source-before mismatch; no suite calls')
    for filename in ('result.json','last-identities.json'):
        if (env/filename).exists():(out/('prior-'+filename)).write_bytes((env/filename).read_bytes())
    argv=['python3','-I','-B',str(P26/'regressions/adapters/w10_suite_adapter.py'),'--stage',r['stage'],'--suite','p1','--release-file',str(release_path),'--candidate-source-file',str(Path(a.candidate_source_file).resolve()),'--env-dir',str(env),'--expect-guard-calls','0','--record',str(Path(r['evidence_root'])/'adapter.json'),'--']+r['suite_argv']
    child=run('p1-child',argv)
    after_source=run('source-after',source_argv);after=tool_check();private_check();put(out/'tools-after.json',after)
    if (env/'last-identities.json').exists():(out/'p1-last-identities.json').write_bytes((env/'last-identities.json').read_bytes())
    original=env/'result.json';raw=original.read_bytes() if original.exists() else b'';(out/'p1-report.json').write_bytes(raw)
    adapter_path=Path(r['evidence_root'])/'adapter.json';adapter=json.loads(adapter_path.read_text()) if adapter_path.exists() else {}
    report=json.loads(raw) if raw else {};cases=report.get('results',{})
    passed=set(cases)==NAMES and all(x.get('status')=='pass' for x in cases.values()) and report.get('counts')=={'pass':9,'fail':0,'uncovered':0} and report.get('uncovered')==[]
    cleaned=adapter.get('cleanup_ok') is True and adapter.get('pending_sql_cleanup')==[] and adapter.get('pending_holders')==0 and adapter.get('count_ok') is True and adapter.get('guard_calls')==0
    ok=child.returncode==0 and after_source.returncode==0 and passed and cleaned
    result={'schema':'p26-p1-actual-outer-v1','stage':r['stage'],'candidate_source':source,'p1_env_dir':str(env),'ok':ok,'child_original_rc':child.returncode,'source_before_original_rc':first.returncode,'source_after_original_rc':after_source.returncode,'tools_before_after_equal':before==after,'named9_pass0fail_uncovered0':passed,'owned_temporary_cleanup_ok':cleaned,'commands':commands,'release_sha256':sha(release_path),'adapter_record':{'path':str(adapter_path),'sha256':sha(adapter_path) if adapter_path.exists() else None},'p1_report_sha256':sha(out/'p1-report.json'),'outer_return_value':(child.returncode if child.returncode>0 else 1 if child.returncode<0 else 0 if ok else 2)}
    put(out/'RESULT.json',result);print(json.dumps({'ok':ok,'output':str(out),'child_original_rc':child.returncode,'named9_pass0fail_uncovered0':passed,'owned_temporary_cleanup_ok':cleaned}))
    return result['outer_return_value']
if __name__=='__main__':
    try:sys.exit(main())
    except (ValueError,KeyError,OSError) as exc:
        print('P1 outer refused/failed: '+str(exc),file=sys.stderr);sys.exit(2)
