"""Generate reviewable sandbox commands only. This phase never executes isolation."""
import json
import os
import shlex
import sys

def profile(package,python,runtime_dirs,forbidden_dirs):
    paths=[package,python,*runtime_dirs,*forbidden_dirs]
    for value in paths:
        if os.path.abspath(value)!=os.path.realpath(value):raise ValueError('noncanonical/symlink alias isolation path')
    if not all(isinstance(p,str) and p.startswith('/') and '\n' not in p for p in paths):raise ValueError('absolute paths required')
    for path in forbidden_dirs:
        if any(os.path.commonpath([path,allow])==path or os.path.commonpath([path,allow])==allow for allow in [package,*runtime_dirs]):raise ValueError('allowed/denied tree overlap')
    q=json.dumps
    # Resource-focused offline boundary, not a general OS application sandbox.
    # Apple installed profile grammar was checked; actual fixture probes remain mandatory.
    lines=['(version 1)','(allow default)']
    lines += ['(deny file-read* (subpath '+q(p)+'))' for p in forbidden_dirs]
    lines += ['(deny network*)','(deny process-fork)','(deny process-exec (require-not (literal '+q(python)+')))','(deny file-write*)']
    return '\n'.join(lines)+'\n'

def plan(config):
    if not os.path.abspath(config['probe_config']).startswith(os.path.abspath(config['package_copy'])+'/'):raise ValueError('probe config must be a manifest member inside package')
    text=profile(config['package_copy'],config['python_realpath'],config['runtime_read_roots'],config['forbidden_roots'])
    command=['/usr/bin/sandbox-exec','-f',config['profile_path'],config['python_realpath'],'-I','-B',config['package_copy']+'/tools/p26_offline_probe.py','--config',config['probe_config']]
    return {'status':'PLAN ONLY; actual profile viability/deny probes NOT EXECUTED','profile':text,'argv':command,'command':shlex.join(command),'environment':'env -i with PATH limited to Python runtime; no inherited DSN/JWT/credential env; close_fds=True','required_probes':['nonsecret existing sentinel under original workspace denied','nonsecret sentinel under each private forbidden root denied (not actual env file)','network connect denied','external executable/service CLI denied','stdlib verifier runs from independent copied package, no writes'],'capture':'outer stdout/stderr/original Unix rc and config/profile SHA outside package; all probes plus verifier must succeed','known_limit':'macOS27 installed sandbox-exec(1) is deprecated; installed Apple SBPL is private interface; actual future successful deny probes are mandatory, unavailable backend => NOTREADY'}

def selftest():
    config={'package_copy':'/private/tmp/pkg','python_realpath':'/runtime/python','runtime_read_roots':['/runtime','/System/Library','/usr/lib'],'forbidden_roots':['/original/workspace','/private/credentials'],'profile_path':'/private/tmp/offline.sb','probe_config':'/private/tmp/pkg/offline-probe-config.json'}
    x=plan(config);assert '(deny network*)' in x['profile'] and '(deny file-write*)' in x['profile'] and '/original/workspace' in x['profile']
    for field,value in [('package_copy','/original/workspace/pkg'),('runtime_read_roots',['/'])]:
        bad=dict(config,**{field:value})
        try:plan(bad)
        except ValueError:pass
        else:raise AssertionError('overlap accepted')
    print('PASS isolation-plan-only + deny-tree overlap controls=3');return 0

if __name__=='__main__':
    if sys.argv[1:]==['selftest']:raise SystemExit(selftest())
    if len(sys.argv)!=2:raise SystemExit('usage: isolation_plan.py CONFIG.json | selftest')
    try:print(json.dumps(plan(json.load(open(sys.argv[1]))),indent=2))
    except (ValueError,KeyError,OSError) as e:print(json.dumps({'status':'NOTREADY','problem':str(e)}));raise SystemExit(1)
