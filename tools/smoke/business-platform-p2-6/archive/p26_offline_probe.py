"""Future isolated entrypoint: run only after Root permits independent-copy phase.
Probes only explicitly prepared nonsecret sentinels, never credential env files.
"""
import argparse
import importlib.util
import json
import os
import socket
import subprocess
from pathlib import Path


def deny_probes(config):
    root=Path(__file__).resolve().parent.parent
    if Path(config['package_copy']).resolve()!=root:raise ValueError('must execute from independent copied package')
    results=[]
    for entry in config['deny_sentinels']:
        p=Path(entry['path'])
        if p.name!='.p26-offline-read-deny-sentinel' or entry.get('existed_before_isolation') is not True:raise ValueError('only prepared nonsecret existing sentinel probe allowed')
        try:
            with open(p,'rb') as f:f.read(1)
        except PermissionError:results.append({'kind':'original-read-denied','path':str(p),'ok':True})
        except OSError:raise ValueError('probe missing/error does not prove permission denial')
        else:raise ValueError('original path still readable')
    if len(results)<2:raise ValueError('workspace and private-root sentinel probes both required')
    try:
        with socket.create_connection(('127.0.0.1',9),timeout=1):pass
    except PermissionError:results.append({'kind':'network-denied','ok':True})
    except OSError:raise ValueError('network connection refused/timeout does not prove isolation')
    else:raise ValueError('network available')
    try:subprocess.run(['/usr/bin/true'],check=False,close_fds=True,capture_output=True)
    except PermissionError:results.append({'kind':'external-exec-denied','ok':True})
    except OSError:raise ValueError('external executable missing/error is not proof of denial')
    else:raise ValueError('external executable available')
    return root,results


def run(config,fixture_only=False):
    root,results=deny_probes(config)
    if fixture_only:
        import hashlib
        if config.get('fixture_only') is not True:raise ValueError('fixture mode requires explicit nonsecret fixture config')
        payload=root/'nonsecret-fixture.txt'
        if hashlib.sha256(payload.read_bytes()).hexdigest()!=config['fixture_payload_sha256']:raise ValueError('fixture payload bytes mismatch')
        return {'status':'ISOLATION-FIXTURE-DENY-PROBES-VERIFIED','deny_probes':results,'fixture_payload_bytes':payload.stat().st_size,'formal_package_verified':False}
    path=root/'tools/p26_archive_verifier.py';spec=importlib.util.spec_from_file_location('verifier',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    verdict,problems,report=module.verify(str(root),config['canonical_root_label'])
    if verdict!='VERIFIED-NOT-SEALED':raise ValueError('package verifier NOTREADY: '+str(problems))
    return {'status':'ISOLATED-INDEPENDENT-COPY-VERIFIED-NOT-SEALED','deny_probes':results,'manifest':report['manifest'],'source_sha256':report['source_go_sql_sha256']}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--config',required=True);parser.add_argument('--fixture-backend-test',action='store_true');args=parser.parse_args()
    try:print(json.dumps(run(json.load(open(args.config)),args.fixture_backend_test),indent=2))
    except (ValueError,OSError,KeyError,TypeError) as e:print(json.dumps({'status':'NOTREADY','problem':str(e)}));raise SystemExit(1)
