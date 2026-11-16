"""Narrow Root-authorized main3 same-CID restart receipt chain validator.

Only StartedAt can derive from the initial Root deployment record. Immutable
per-service files and actual original receipts are read once and hashed. There
is no name adoption, executable command, service change or Root release rewrite.
"""
import copy,hashlib,json,re
from pathlib import Path

IDENTITY='{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}'
MOUNTS='{{json .Mounts}}'
PORTS='{{json .NetworkSettings.Ports}}\n{{json .HostConfig.PortBindings}}'
ENVNAMES='{{range .Config.Env}}{{println (index (split . "=") 0)}}{{end}}'

def canonical_mounts(mounts):
    if not isinstance(mounts,list) or any(not isinstance(m,dict) or not isinstance(m.get('Destination'),str) or not m['Destination'] for m in mounts):raise ValueError('malformed complete mount list')
    result={m['Destination']:m for m in mounts}
    if len(result)!=len(mounts):raise ValueError('duplicate mount destination')
    return result

def env_names(raw):
    names=raw.rstrip('\n').split('\n')
    if not names or any(not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',name) for name in names) or len(names)!=len(set(names)):
        raise ValueError('restart env names must be nonempty legal unique names')
    return set(names)

def derive(release,record,W):
    authority=release['main_boot_transition']
    digest=lambda data:hashlib.sha256(data).hexdigest()
    if digest(Path(__file__).read_bytes())!=authority['guard_extension_sha256']:raise ValueError('Root boot extension pin drift')
    writer=Path(authority['executor_tool']).resolve()
    if not writer.is_relative_to(W.G.ROOT_P26.resolve()) or digest(writer.read_bytes())!=authority['executor_tool_sha256']:raise ValueError('Root authorized restart writer pin drift')
    if authority['allowed_service_order']!=['dataset','pipeline','connector']:raise ValueError('Root restart order must be exact main3')
    root=Path(authority['ledger_dir']).resolve();output=Path(release['main_restart_output']).resolve()
    if not output.is_relative_to((W.G.ROOT_P26/'results').resolve()) or not root.is_relative_to(output):raise ValueError('Root restart ledger must stay under exact NEW output')
    files=sorted(root.iterdir());allowed={'00-anchor.json','01-dataset.json','02-pipeline.json','03-connector.json'}
    if any(not p.is_file() or p.name not in allowed for p in files):raise ValueError('unknown restart ledger entry')
    evidence={}
    def read(path):
        data=path.read_bytes();evidence[str(path)]={'sha256':digest(data),'bytes':len(data)};return data
    anchor=json.loads(read(root/'00-anchor.json'))
    expected={'schema':'p26-main-boot-transitions-v1','release_id':release['release_id'],'executor_tool_sha256':authority['executor_tool_sha256'],'base_deployment_record_sha256':release['deployment_record_sha256'],'allowed_service_order':authority['allowed_service_order']}
    if anchor!=expected:raise ValueError('restart anchor differs from explicit Root authority')
    def receipt(prefix,argv):
        p=Path(prefix).resolve()
        if not p.is_relative_to(output):raise ValueError('restart receipt outside authorized NEW output')
        raw={suffix:read(Path(str(p)+suffix)) for suffix in ('.cmd.json','.raw','.rc','.meta.json')}
        cmd=json.loads(raw['.cmd.json']);cmd=cmd.get('argv') if isinstance(cmd,dict) and set(cmd)=={'argv'} else cmd
        meta=json.loads(raw['.meta.json'])
        if cmd!=argv or raw['.rc'].decode().strip()!='0' or meta.get('original_rc',meta.get('original_child_rc'))!=0:raise ValueError('restart original command/rc evidence mismatch')
        return raw['.raw'].decode()
    new=copy.deepcopy(record);steps=[]
    for i,svc in enumerate(authority['allowed_service_order'],1):
        path=root/(str(i).zfill(2)+'-'+svc+'.json')
        if not path.exists():
            if any((root/(str(j).zfill(2)+'-'+v+'.json')).exists() for j,v in enumerate(authority['allowed_service_order'],1) if j>i):raise ValueError('restart chain has a missing predecessor')
            break
        step=json.loads(read(path));name=getattr(W,'Q3_'+svc.upper());pin=new['containers'][name]
        old={k:pin[k] for k in ('cid','image','started_at')};current=step['current']
        if step['schema']!='p26-main-boot-transition-v1' or step['release_id']!=release['release_id'] or step['service']!=svc or step['previous']!=old:raise ValueError('restart previous exact Root service anchor mismatch')
        if set(current)!=set(old) or current['cid']!=old['cid'] or current['image']!=old['image'] or current['started_at']==old['started_at'] or not current['started_at'].endswith('Z'):raise ValueError('only same CID/image with new UTC boot can derive')
        if not re.fullmatch('[a-f0-9]{64}',old['cid']) or not re.fullmatch('sha256:[a-f0-9]{64}',old['image']):raise ValueError('restart full identity pin required')
        cmd=authority['restart_commands'][svc]
        if cmd not in (['docker','restart',old['cid']],['docker','restart','--time','120',old['cid']],['docker','restart','--timeout','120',old['cid']]):raise ValueError('Root restart argv must target one exact main CID')
        snapshots={}
        for side,identity in [('before',old),('after',current)]:
            snapshot={}
            for key,fmt in [('identity',IDENTITY),('mounts',MOUNTS),('ports',PORTS),('env_names',ENVNAMES)]:
                value=receipt(step[side][key],['docker','inspect',old['cid'],'--format',fmt]);snapshot[key]=value
            if snapshot['identity'].strip().splitlines()!=[identity['cid'],identity['image'],identity['started_at'],'true']:raise ValueError('restart original actual '+side+' identity projection mismatch')
            snapshots[side]=snapshot
        receipt(step['restart_step'],cmd)
        if canonical_mounts(json.loads(snapshots['before']['mounts']))!=canonical_mounts(json.loads(snapshots['after']['mounts'])):raise ValueError('restart actual complete mounts drift')
        if snapshots['before']['ports'].strip()!=snapshots['after']['ports'].strip():raise ValueError('restart actual ports drift')
        before_names=env_names(snapshots['before']['env_names']);after_names=env_names(snapshots['after']['env_names'])
        if before_names!=after_names:raise ValueError('restart actual env name set drift')
        if svc=='connector' and after_names&set(W.LOOP_ENV_NAMES):raise ValueError('main Connector loops not OFF')
        new['containers'][name]['started_at']=current['started_at'];steps.append({'service':svc,'previous':old,'current':current,'proof':str(path)})
    return new,{'schema':'p26-verified-main-boot-chain-v1','steps':steps,'original_artifact_sha256':evidence,'derived_only_fields':['containers.main3.started_at']}
