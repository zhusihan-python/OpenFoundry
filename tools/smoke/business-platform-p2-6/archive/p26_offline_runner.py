"""Root-released actual independent-copy runner. Pure tests never launch sandbox."""
import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
from pathlib import Path


def need(ok,msg):
    if not ok:raise ValueError(msg)


def load(name):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).parent/(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def preflight(config,release):
    need(release.get('approved') is True and release.get('phase')=='p26-independent-offline','Root independent offline phase required')
    need(release.get('config_sha256')==hashlib.sha256(json.dumps(config,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'Root exact offline config binding')
    keys=('source_package','package_copy','capture_dir','profile_path')
    paths={key:Path(config[key]).resolve() for key in keys}
    for key,path in paths.items():
        need(Path(config[key]).is_absolute() and Path(config[key]).absolute()==path,'noncanonical/symlink alias path: '+key)
    values=list(paths.values())
    for i,left in enumerate(values):
        for right in values[i+1:]:need(left!=right and left not in right.parents and right not in left.parents,'source/copy/capture/profile tree overlap')
    source,copy,output=paths['source_package'],paths['package_copy'],paths['capture_dir']
    need(not copy.exists() and not output.exists() and not paths['profile_path'].exists(),'independent copy/capture/profile must be new')
    for root in config['forbidden_roots']:
        need(Path(root).is_absolute() and Path(root).absolute()==Path(root).resolve(),'forbidden root alias')
    need(Path(config['probe_config']).resolve().is_relative_to(copy),'probe config must be inside resolved copy')
    need(release.get('manifest_sha256')==config['manifest_sha256'],'Root source manifest binding')
    need(bool(release.get('private_forbidden_roots')),'explicit private forbidden roots cannot be empty')
    need(set(release.get('private_forbidden_roots',[]))<=set(config['forbidden_roots']) and release.get('original_workspace') in config['forbidden_roots'],'actual original/private deny roots required')
    load('p26_offline_isolation_plan').plan(config)
    return source,copy,output


def execute(config,release):
    source,copy,output=preflight(config,release);manifest=load('p26_archive_manifest');before=manifest.verify_manifest(str(source),str(source/'manifest.json'))
    need(before['manifest_sha256']==config['manifest_sha256'],'source package manifest changed')
    # Assert nonsecret markers existed before entering sandbox; never open actual credentials.
    probe=json.load(open(source/Path(config['probe_config']).relative_to(copy)))
    observed=[]
    for entry in probe['deny_sentinels']:
        p=Path(entry['path']);need(p.is_absolute() and p.absolute()==p.resolve(),'sentinel path alias')
        need(p.name=='.p26-offline-read-deny-sentinel' and p.is_file() and not p.is_symlink(),'prepared nonsecret deny sentinel required')
        need(entry.get('existed_before_isolation') is True,'sentinel precondition missing');observed.append({'path':str(p),'exists_before':True,'size':p.stat().st_size})
    need({release['original_workspace'],*release['private_forbidden_roots']}<=set(config['forbidden_roots']),'incomplete original denies')
    need(all(any(Path(e['path']).resolve().is_relative_to(Path(root).resolve()) for e in probe['deny_sentinels']) for root in config['forbidden_roots']),'each forbidden root needs actual prepared sentinel')
    copy.mkdir();output.mkdir()
    for rel in manifest.walk_package(str(source))+['manifest.json']:
        src=source/rel;dest=copy/rel;dest.parent.mkdir(parents=True,exist_ok=True)
        with open(src,'rb') as f,open(dest,'xb') as out:shutil.copyfileobj(f,out)
    after=manifest.verify_manifest(str(copy),str(copy/'manifest.json'));need(after==before,'independent copy mismatch')
    isolation=load('p26_offline_isolation_plan').plan(config);profile=Path(config['profile_path']);need(not profile.exists() and not str(profile).startswith(str(copy)+'/'),'profile must be new outside package')
    with open(profile,'x') as f:f.write(isolation['profile'])
    argv=isolation['argv'];env={'PATH':str(Path(config['python_realpath']).parent),'PYTHONDONTWRITEBYTECODE':'1','PYTHONNOUSERSITE':'1'}
    (output/'command.json').write_text(json.dumps({'argv':argv,'cwd':str(copy),'env_keys':sorted(env),'close_fds':True,'timeout_s':300,'sentinels':observed,'profile_sha256':hashlib.sha256(profile.read_bytes()).hexdigest(),'manifest':before},indent=2))
    try:
        child=subprocess.run(argv,cwd=copy,env=env,close_fds=True,capture_output=True,timeout=300)
        (output/'stdout').write_bytes(child.stdout);(output/'stderr').write_bytes(child.stderr);(output/'rc').write_text(str(child.returncode)+'\n')
        need(child.returncode==0,'isolated original process nonzero')
        result=json.loads(child.stdout);need(result.get('status')=='ISOLATED-INDEPENDENT-COPY-VERIFIED-NOT-SEALED','isolated probe/verifier did not finish')
        need(result['manifest']['manifest_sha256']==before['manifest_sha256'],'isolated manifest mismatch')
    except subprocess.TimeoutExpired as e:
        (output/'stdout').write_bytes(e.stdout or b'');(output/'stderr').write_bytes(e.stderr or b'');(output/'timeout.json').write_text('{"timeout_s":300,"original_rc":"unavailable-killed-by-timeout"}\n');raise ValueError('isolated timed out; no original rc fabricated')
    return {'status':'INDEPENDENT-COPY-OFFLINE-VERIFIED-NOT-SEALED','manifest':before,'capture':str(output),'original_rc':0}


def selftest():
    try:preflight({}, {})
    except ValueError:pass
    else:raise AssertionError('unreleased offline execution accepted')
    print('PASS unreleased actual offline refused before file/copy/sandbox I/O controls=1');return 0

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('command',choices=['selftest','execute']);ap.add_argument('--config');ap.add_argument('--root-phase');args=ap.parse_args()
    if args.command=='selftest':raise SystemExit(selftest())
    try:print(json.dumps(execute(json.load(open(args.config)),json.load(open(args.root_phase))),indent=2))
    except (ValueError,OSError,KeyError,TypeError) as e:print(json.dumps({'status':'NOTREADY','problem':str(e)}));raise SystemExit(1)
