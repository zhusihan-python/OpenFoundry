"""Final Root-released secret provider. Values stay in memory/anonymous stdin pipe.

No real inputs are loaded by importing this module. The only CLI path requires
an exclusive signed Root release naming exact paths/keys/projections/roles,
package manifest and tool hashes. Failures never print exceptions/child bytes.
"""
import argparse,datetime,hashlib,json,os,re,stat,subprocess,sys
from pathlib import Path
from urllib.parse import urlsplit,unquote

SCHEMA='p26-secret-provider-release-v1'
PHASE='p26-final-secret-values-memory'
TOOLS=('p26_secret_values_memory.py','p26_secret_scan.py')


def need(ok):
    if not ok:raise ValueError('provider refused')


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(obj):return json.dumps(obj,sort_keys=True,separators=(',',':')).encode()


def no_symlink(path):
    p=Path(path);need(p.is_absolute())
    for item in [p,*p.parents]:need(not item.is_symlink())
    return p


def validate(release, output_created=False):
    need(release['schema']==SCHEMA and release['root_accepted'] is True and release['phase']==PHASE)
    need(datetime.datetime.now(datetime.timezone.utc)<datetime.datetime.fromisoformat(release['expires_utc'].replace('Z','+00:00')))
    package=no_symlink(release['package_root']).resolve();manifest=no_symlink(release['package_manifest']).resolve();output=no_symlink(release['output_dir']).resolve()
    need(package.is_dir() and manifest.is_file() and digest(manifest)==release['package_manifest_sha256'])
    need((output.is_dir() if output_created else not output.exists()) and not output.is_relative_to(package) and not package.is_relative_to(output))
    need(not manifest.is_relative_to(package));document=json.loads(manifest.read_text());need(document['schema']=='p26-secret-scan-members-v1' and document['members'])
    need(set(release['tool_shas'])==set(TOOLS))
    for name,d in release['tool_shas'].items():need(digest(Path(__file__).with_name(name))==d)
    entries=release['registered_inputs'];need(re.fullmatch('[0-9a-f]{64}',release['source_sha256']) and document['source_sha256']==release['source_sha256'])
    need(entries and hashlib.sha256(canonical(entries)).hexdigest()==release['registered_inputs_sha256'])
    need(len({e['id'] for e in entries})==len(entries))
    for e in entries:
        need(set(e)=={'id','path','key','role','projection'} and all(isinstance(e[k],str) and e[k] for k in e))
        need(e['projection'] in ('exact','bearer-token','url-password'))
        p=no_symlink(e['path']);need(not p.is_relative_to(package) and not p.is_relative_to(output))
    return package,output


def load_values(entries):
    values=[];metadata=[];loaded={}
    for e in entries:
        path=no_symlink(e['path'])
        if str(path) not in loaded:
            # No private input content hash is computed or persisted.
            descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
            try:
                before=os.fstat(descriptor);need(stat.S_ISREG(before.st_mode) and stat.S_IMODE(before.st_mode)==0o600)
                with os.fdopen(descriptor,'r',closefd=False) as stream:text=stream.read()
                after=os.fstat(descriptor);need((before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns))
            finally:os.close(descriptor)
            parsed={}
            for line in text.splitlines():
                if not line or line.startswith('#'):continue
                need('=' in line);key,value=line.split('=',1);need(key and key not in parsed)
                if len(value)>=2 and value[0]==value[-1] and value[0] in ('"',"'"):value=value[1:-1]
                parsed[key]=value
            loaded[str(path)]=parsed
        value=loaded[str(path)][e['key']]
        if e['projection']=='bearer-token':need(value.startswith('Bearer '));value=value[7:]
        elif e['projection']=='url-password':
            parsed=urlsplit(value);need(parsed.scheme in ('postgres','postgresql') and parsed.password is not None);value=unquote(parsed.password)
        need(value and '\n' not in value and '\r' not in value and '\x00' not in value)
        values.append(value);metadata.append(dict(e,value_count=1))
    return values,metadata


def run(release_file):
    release_path=no_symlink(release_file);release_bytes=release_path.read_bytes();release=json.loads(release_bytes);package,output=validate(release)
    output.mkdir(parents=True,exist_ok=False)
    command=[sys.executable,'-I','-B',str(Path(__file__).with_name('p26_secret_scan.py')),'scan',str(package),'--values-stdin','--audit-json']
    (output/'command.json').write_text(json.dumps(command,indent=2)+'\n')
    state={'schema':'p26-secret-input-registry-v1','release_id':release['release_id'],'phase':PHASE,'source_sha256':release['source_sha256'],'registered_inputs':release['registered_inputs'],'provider_original_rc':None,'scanner_original_rc':None,'values_persisted':False,'actual_value_count':0}
    try:
        # Recheck original phase/tools/manifest immediately before loading values.
        need(release_path.read_bytes()==release_bytes);validate(release,output_created=True)
        values,metadata=load_values(release['registered_inputs']);state['registered_inputs']=metadata;state['actual_value_count']=len(values)
        payload=('\n'.join(values)+'\n').encode()
        cp=subprocess.run(command,input=payload,capture_output=True,env={'PATH':os.defpath,'PYTHONDONTWRITEBYTECODE':'1','PYTHONNOUSERSITE':'1'},timeout=300)
        state['scanner_original_rc']=cp.returncode
        # Never persist raw child stderr or unexpected output, even on failures.
        need(not any(value.encode() in cp.stdout or value.encode() in cp.stderr for value in values))
        audit=json.loads(cp.stdout);need(audit['schema']=='p26-secret-allbyte-audit-v1' and audit['actual_values_stdin'] is True and audit['actual_value_count']==len(values))
        need(audit['scanned_members']==json.loads(Path(release['package_manifest']).read_text())['members'])
        need(audit['values_persisted'] is False and audit['actual_hit_count']==0 and audit['suspect']==0 and cp.returncode==0)
        need(release_path.read_bytes()==release_bytes);validate(release,output_created=True)
        (output/'scan-audit.json').write_bytes(cp.stdout)
        state['provider_original_rc']=0
        del values,payload
        result=0
    except BaseException:
        # Only typed state/counts survive. No exception message/value/value hash.
        state['provider_original_rc']=1;state['failure_kind']='provider-or-scanner-refused';result=1
    (output/'scanner.rc').write_text(str(state['scanner_original_rc']) if state['scanner_original_rc'] is not None else 'unavailable')
    (output/'registry.json').write_text(json.dumps(state,indent=2)+'\n');(output/'provider.rc').write_text(str(result)+'\n')
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);args=p.parse_args()
    try:return run(args.release_file)
    except BaseException:
        print('secret provider refused; no values reported',file=sys.stderr);return 1
if __name__=='__main__':sys.exit(main())
