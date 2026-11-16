"""Actual NONSECRET backend test only; no business package or credential reads."""
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def run(capture,workspace,python_override=None,allow_default_control=False):
    capture=Path(capture).resolve();capture.mkdir(exist_ok=False);work=Path(workspace).resolve();temporary=Path(tempfile.mkdtemp(prefix='p26-isolation-nonsecret-',dir='/private/tmp')).resolve();package=temporary/'copied-payload';package.mkdir();(package/'tools').mkdir();private=temporary/'private-fixture';private.mkdir();sentinel_name='.p26-offline-read-deny-sentinel';workspace_marker=capture/sentinel_name;workspace_marker.write_text('NONSECRET workspace isolation fixture\n');(private/sentinel_name).write_text('NONSECRET private-directory fixture\n')
    payload=b'NONSECRET standalone isolation payload\n';(package/'nonsecret-fixture.txt').write_bytes(payload);script=Path(__file__).with_name('p26_offline_probe.py');shutil.copyfile(script,package/'tools/p26_offline_probe.py')
    config={'package_copy':str(package),'fixture_only':True,'canonical_root_label':str(work),'fixture_payload_sha256':hashlib.sha256(payload).hexdigest(),'deny_sentinels':[{'path':str(workspace_marker),'existed_before_isolation':True},{'path':str(private/sentinel_name),'existed_before_isolation':True}]};(package/'fixture-config.json').write_text(json.dumps(config,indent=2))
    spec=importlib.util.spec_from_file_location('isolation',script.with_name('p26_offline_isolation_plan.py'));isolation=importlib.util.module_from_spec(spec);spec.loader.exec_module(isolation);python=str(Path(python_override or sys.executable).resolve());runtime=['/Library/Developer/CommandLineTools','/opt/homebrew',str(Path(sys.base_prefix).resolve()),'/System/Library','/usr/lib','/Library/Apple/System/Library'];profile=isolation.profile(str(package),python,runtime,[str(work),str(private),'/Users/allen/.codex/visualizations/2026/11/13/01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs','/Users/allen/.ssh','/Users/allen/.aws']);
    if allow_default_control:profile='(version 1)\n(allow default)\n'
    profile_path=temporary/'fixture.sb';profile_path.write_text(profile)
    argv=['/usr/bin/sandbox-exec','-f',str(profile_path),python,'-I','-B',str(package/'tools/p26_offline_probe.py'),'--config',str(package/'fixture-config.json'),'--fixture-backend-test'];env={'PATH':str(Path(python).parent),'PYTHONDONTWRITEBYTECODE':'1','PYTHONNOUSERSITE':'1'}
    (capture/'command.json').write_text(json.dumps({'argv':argv,'cwd':str(package),'env_keys':sorted(env),'close_fds':True,'sentinels_existed_before':config['deny_sentinels'],'profile':profile,'profile_sha256':hashlib.sha256(profile.encode()).hexdigest(),'probe_sha256':hashlib.sha256(script.read_bytes()).hexdigest(),'fixture_only':True},indent=2))
    try:
        child=subprocess.run(argv,cwd=package,env=env,close_fds=True,capture_output=True,timeout=30);(capture/'stdout').write_bytes(child.stdout);(capture/'stderr').write_bytes(child.stderr);(capture/'rc').write_text(str(child.returncode)+'\n')
    except subprocess.TimeoutExpired as e:
        (capture/'stdout').write_bytes(e.stdout or b'');(capture/'stderr').write_bytes(e.stderr or b'');(capture/'timeout.json').write_text('{"timeout":30,"original_rc":"unavailable"}');return {'status':'NOTREADY','reason':'sandbox timeout','capture':str(capture)}
    if child.returncode!=0:return {'status':'NOTREADY','original_rc':child.returncode,'capture':str(capture),'fixture':str(temporary)}
    result=json.loads(child.stdout);return {'status':result['status'],'original_rc':0,'result':result,'capture':str(capture),'fixture':str(temporary),'final_business_package':False}

if __name__=='__main__':
    x=run(sys.argv[1],sys.argv[2],sys.argv[3] if len(sys.argv)>3 else None,len(sys.argv)>4 and sys.argv[4]=='allow-default-control');print(json.dumps(x,indent=2));raise SystemExit(x['status']!='ISOLATION-FIXTURE-DENY-PROBES-VERIFIED')
