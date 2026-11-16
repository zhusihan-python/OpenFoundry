"""Root-delegated ONE image replacement, original release retained unchanged.

Only cid/image/started_at of one permitted target derive in memory. All command
receipts are original bytes and current full identity/env are independently read
again by W10. No service command or Root approval file is produced here.
"""
import copy,hashlib,json,re
from pathlib import Path
STAGE='runtime-image-upgrade'
SUITE='p26-runtime'
SERVICES=('p26connectora',)
IDENTITY_JSON='{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"config_image":{{json .Config.Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"restart_count":{{json .RestartCount}},"labels":{{json .Config.Labels}},"networks":{{json .NetworkSettings.Networks}},"ports":{{json .NetworkSettings.Ports}},"mounts":{{json .Mounts}}}'
ENVNAMES='{{range .Config.Env}}{{println (index (split . "=") 0)}}{{end}}'
AUX_CONFIG={'HOST':'0.0.0.0','PORT':'50088','ALLOWED_EGRESS_HOSTS':'fixture,p26fixture,p25fixture','ALLOW_PRIVATE_NETWORK_EGRESS':'true','OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS':'0','OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS':'0','OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED':'false','CONNECTOR_SYNC_ASYNC_WORKERS':'1','CONNECTOR_SYNC_ASYNC_QUEUE':'64','CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS':'300','CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS':'300'}
AUX_CONFIG_KEYS=tuple(AUX_CONFIG)+('DATASET_SERVICE_URL',)
AUX_CONFIG_FORMAT='{{range .Config.Env}}{{$p := split . "="}}{{if or '+ ' '.join('(eq (index $p 0) "'+k+'")' for k in AUX_CONFIG_KEYS)+'}}{{println .}}{{end}}{{end}}'

def canonical_mounts(mounts):
    assert isinstance(mounts, list)
    assert all(isinstance(m, dict) and isinstance(m.get('Destination'), str) and m['Destination'] for m in mounts)
    result = {m['Destination']: m for m in mounts}
    assert len(result) == len(mounts)
    return result


def canonical_facts(fact):
    return dict(fact, mounts=canonical_mounts(fact['mounts']))


def target_name(service,W):
    if service not in SERVICES:raise ValueError('unknown/disallowed image transition service')
    return getattr(W,'Q3_'+service.upper()) if service in ('dataset','connector') else service

def config_for(service):
    return {**AUX_CONFIG,'DATASET_SERVICE_URL':'http://p26p22proxy:8091' if service=='p26connectorb' else 'http://dataset:50078'}

def configuration_pair(action):
    base=config_for('p26connectora')
    scheduler=dict(base,OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS='1')
    pairs={'p24-enable':(base,scheduler),'p24-restore':(scheduler,base)}
    if action not in pairs:raise ValueError('unknown A config action')
    return pairs[action]

def parse_config(raw):
    result={}
    for line in raw.strip().splitlines():
        if '=' not in line:raise ValueError('safe aux config malformed')
        key,value=line.split('=',1)
        if key in result or key not in AUX_CONFIG_KEYS:raise ValueError('extra/duplicate safe aux config key')
        result[key]=value
    return result

def env_names(raw):
    names=raw.rstrip('\n').splitlines()
    if not names or any(not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',name) for name in names):raise ValueError('illegal environment name')
    if len(names)!=len(set(names)):raise ValueError('duplicate environment name')
    return names

def derive(release,record,projection_path,release_sha,W):
    authority=release['image_transition'];service=authority['service']
    kind=authority.get('transition_kind','image')
    if kind != 'config':raise ValueError('A bridge permits config only')
    config_pair=None
    if kind=='config':
        if service!='p26connectora' or release['candidate_source']!='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e':raise ValueError('config transition only frozen S2 A')
        config_pair=configuration_pair(authority['config_action'])
        if authority['before_safe_config']!=config_pair[0] or authority['after_safe_config']!=config_pair[1]:raise ValueError('Root config pair not exact enumerated action')
    if release['stage']!=('runtime-config-transition' if kind=='config' else STAGE) or release['suite']!=SUITE:raise ValueError('image transition Root phase/suite scope mismatch')
    name=target_name(service,W)
    if authority['target_name']!=name or name not in record['containers']:raise ValueError('single target not exact original Root container')
    digest=lambda data:hashlib.sha256(data).hexdigest()
    root=W.G.ROOT_P26.resolve();output=Path(authority['output_dir']).resolve();path=Path(projection_path if projection_path is not None else authority['proof_file']).resolve()
    if not output.is_relative_to((root/'results').resolve()) or str(path)!=str(Path(authority['proof_file']).resolve()) or not path.is_relative_to(output):raise ValueError('image projection not exact authorized NEW output')
    original={}
    def read(p):
        p=Path(p)
        if p.is_symlink() or not p.is_file():raise ValueError('nonregular transition evidence')
        data=p.read_bytes();original[str(p.resolve())]={'sha256':digest(data),'bytes':len(data)};return data
    if digest(read(__file__))!=authority['guard_extension_sha256']:raise ValueError('Root image guard extension SHA drift')
    writer=Path(authority['executor_tool']).resolve()
    if not writer.is_relative_to(root) or digest(read(writer))!=authority['executor_tool_sha256']:raise ValueError('Root image executor SHA drift')
    build=authority['candidate_build'];accepted=json.loads(read(build['path']))
    if original[str(Path(build['path']).resolve())]['sha256']!=build['sha256'] or accepted['source']!=release['candidate_source']:raise ValueError('Root accepted candidate build/source bytes mismatch')
    candidate=next((c for c in accepted['candidates'] if c['service']==('dataset' if service=='dataset' else 'connector')),None)
    image=authority['candidate_image']
    if not candidate or candidate['image']!=image or not re.fullmatch('sha256:[a-f0-9]{64}',image):raise ValueError('candidate full image not same-source Root accepted build')
    old={k:record['containers'][name][k] for k in ('cid','image','started_at')}
    if kind=='image' and old['image']==image:raise ValueError('image transition must actually change candidate image')
    if kind=='config' and not (image==old['image']=='sha256:ea8244f03a3b07e72b77bd7539e13ae1d39cf91bf90a26d01675f008c64685d6'):raise ValueError('config transition must retain frozen S2 EA full image')
    argv=authority['approved_argv']
    files=[argv[i+1] for i,p in enumerate(argv[:-1]) if p in ('-f','--env-file')]
    if len([p for p in argv if p=='-f'])!=2 or set(files)!=set(authority['config_shas']):raise ValueError('every actual compose/env/override file must be Root SHA bound')
    override=[argv[i+1] for i,p in enumerate(argv[:-1]) if p=='-f'][1]
    if not Path(override).resolve().is_relative_to(root/'deployment'):raise ValueError('override outside NEW deployment')
    if kind=='config':
        exact=root/'deployment/legacy-stage'/('complete-A-'+authority['config_action']+'.json')
        expected={'services':{'p26connectora':{'image':image,'environment':config_pair[1]}}}
        if Path(override).resolve()!=exact or json.loads(read(exact))!=expected:raise ValueError('config override not exact complete A-only enumerated content')
    if service in ('dataset','connector'):
        prefix=['docker','compose','--env-file',str(W.Q3_ENV_FILE),'-f',str(W.COMPOSE_FILE),'-f',override,'--project-name',W.G.Q3_PROJECT]
    else:prefix=['docker','compose','-f',str(root/'deployment/legacy-stage/compose.yaml'),'-f',override,'--project-name','openfoundry-p26-legacy']
    expected_argv=prefix+['up','-d','--no-deps','--no-build','--pull=never']+(['--force-recreate'] if kind=='config' else [])+[service]
    if argv!=expected_argv:raise ValueError('Root image argv may replace exactly one approved service only')
    for file,sha in authority['config_shas'].items():
        if digest(read(file))!=sha:raise ValueError('actual compose/env/override bytes changed')
    if service.startswith('p26'):
        if digest(read('/private/tmp/openfoundry-p26-aux.env'))!=authority['aux_env_sha256']:raise ValueError('actual private aux env bytes changed')
    if projection_path is None:
        return record,{'schema':'p26-verified-image-authority-v1','applied':False,'service':service,'original_artifact_sha256':original,'actual_current_read_requests':[],'authority':'original Root single-target authority prechecked; no pin derived'}
    proof=json.loads(read(path))
    if kind=='config' and (proof.get('transition_kind')!='config' or proof.get('config_action')!=authority['config_action']):raise ValueError('proof config action scope mismatch')
    if any(proof.get(k)!=v for k,v in {'schema':'p26-image-transition-proof-v1','release_id':release['release_id'],'root_release_sha256':release_sha,'deployment_record_sha256':release['deployment_record_sha256'],'executor_tool_sha256':authority['executor_tool_sha256'],'service':service,'previous':old}.items()):raise ValueError('image transition original Root/record/writer/target anchor mismatch')
    current=proof['current']
    if set(current)!=set(old) or not re.fullmatch('[a-f0-9]{64}',current['cid']) or current['cid']==old['cid'] or current['image']!=image or current['started_at']==old['started_at'] or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z',current['started_at']):raise ValueError('image replacement needs actual NEW full CID/boot and exact candidate image')
    def receipt(key,cmd):
        p=Path(proof['receipts'][key]).resolve()
        if not p.is_relative_to(output):raise ValueError('image receipt outside authorized output')
        raw={suffix:read(str(p)+suffix) for suffix in ('.cmd.json','.raw','.rc','.meta.json')};actual=json.loads(raw['.cmd.json']);meta=json.loads(raw['.meta.json'])
        actual=actual.get('argv') if isinstance(actual,dict) and set(actual)=={'argv'} else actual
        if actual!=cmd or raw['.rc'].decode().strip()!='0' or meta.get('original_rc',meta.get('original_child_rc'))!=0:raise ValueError('original image command/rc/meta mismatch '+key)
        return raw['.raw'].decode()
    facts={};envs={};aux={}
    for side,pin in [('before',old),('after',current)]:
        fact=json.loads(receipt(side+'_identity',['docker','inspect',pin['cid'],'--format',IDENTITY_JSON]));facts[side]=fact
        if any(fact[k]!=v for k,v in {**pin,'config_image':pin['image'],'name':'/'+name,'running':True,'restart_count':0}.items()):raise ValueError('actual '+side+' full CID/image/configImage/boot/state mismatch')
        if set(fact)!=set(('cid','name','image','config_image','started_at','running','restart_count','labels','networks','ports','mounts')):raise ValueError('unsafe/partial identity JSON schema')
        project=W.G.Q3_PROJECT if service in ('dataset','connector') else 'openfoundry-p26-legacy'
        expected_labels={'com.docker.compose.project':project,'com.docker.compose.service':service}
        if service.startswith('p26'):expected_labels.update({'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_'+service[-1]})
        if any(fact['labels'].get(k)!=v for k,v in expected_labels.items()):raise ValueError('actual Compose/NEW ownership binding drift')
        envs[side]=env_names(receipt(side+'_env_names',['docker','inspect',pin['cid'],'--format',ENVNAMES]))
        if service.startswith('p26'):
            aux[side]=parse_config(receipt(side+'_aux_config',['docker','inspect',pin['cid'],'--format',AUX_CONFIG_FORMAT]))
            if aux[side]!=(config_pair[0 if side=='before' else 1] if kind=='config' else config_for(service)):raise ValueError('actual safe aux12 config/loops not exact approved baseline')
            permitted={'OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS','OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS','OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED'}
            if set(envs[side])&(set(W.LOOP_ENV_NAMES)-permitted):raise ValueError('unknown aux loop flag present')
        elif service=='connector' and set(envs[side])&set(W.LOOP_ENV_NAMES):raise ValueError('main Connector loops not OFF')
    receipt('command',argv)
    cid=receipt('discovery',prefix+['ps','-q',service]).strip()
    if cid!=current['cid']:raise ValueError('exact approved Compose discovery did not return this full NEW CID')
    if canonical_mounts(facts['before']['mounts'])!=canonical_mounts(facts['after']['mounts']) or facts['before']['ports']!=facts['after']['ports'] or set(envs['before'])!=set(envs['after']):raise ValueError('actual files mount/ports/env names changed across image-only replacement')
    if set(facts['before']['networks'])!=set(facts['after']['networks']) or set(facts['after']['networks'])!={W.G.Q3_PROJECT+'_p1net'}:raise ValueError('actual image network set mismatch')
    for network,pin in facts['before']['networks'].items():
        if facts['after']['networks'][network]['NetworkID']!=pin['NetworkID']:raise ValueError('actual network identity changed')
    result=copy.deepcopy(record);result['containers'][name].update(current)
    reads=[{'argv':['docker','inspect',current['cid'],'--format',IDENTITY_JSON],'format':'json','expected':facts['after']},{'argv':['docker','inspect',current['cid'],'--format',ENVNAMES],'format':'names','expected':envs['after']}]
    if service.startswith('p26'):reads.append({'argv':['docker','inspect',current['cid'],'--format',AUX_CONFIG_FORMAT],'format':'config','expected':aux['after']})
    return result,{'schema':'p26-verified-image-transition-v1','applied':True,'service':service,'previous':old,'current':current,'original_artifact_sha256':original,'derived_only_fields':['containers.'+name+'.cid','containers.'+name+'.image','containers.'+name+'.started_at'],'actual_current_read_requests':reads,'authority':'original Root release delegated one exact image transition; no new Root approval file'}


def verify_current(proof,run):
    for request in proof['actual_current_read_requests']:
        raw=run(request['argv'])
        value=json.loads(raw) if request['format']=='json' else parse_config(raw) if request['format']=='config' else env_names(raw)
        equal=set(value)==set(request['expected']) if request['format']=='names' else canonical_facts(value)==canonical_facts(request['expected']) if request['format']=='json' else value==request['expected']
        if not equal:raise ValueError('current actual image identity/mounts/ports/network/loops differ from original after receipt')
