#!/usr/bin/env python3
"""Root-released P25 copied fixture/proxy only; no Connector reconfigure."""
import argparse,json,re,subprocess,sys,time
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(P26));sys.path.insert(0,str(P26/'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
from final_old_artifact_readonly import evidenced_http,terminal_error
PHASE='legacy-p25-sidecars-create'
NAMES={'fixture':'p26p25fixture','proxy':'p26p25proxy'}
PORTS={'fixture':17103,'proxy':17104}
NET='openfoundry-platform-p1-q3v9_p1net'
PYTHON_IMAGE='sha256:c5ba3deef00c7758561a0f03c26f44f10589263d40e732b8748e0d1f38f6dd89'
FILES={key:P26/'regressions/vendor/p25_fixtures'/name for key,name in {'fixture':'rest_source_p25.py','proxy':'dataset_fault_proxy_p25.py'}.items()}
FMT='{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"config_image":{{json .Config.Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}},"networks":{{json .NetworkSettings.Networks}},"ports":{{json .NetworkSettings.Ports}},"mounts":{{json .Mounts}},"cmd":{{json .Config.Cmd}}}'
TOOLS={'deployment/create_legacy_p25_sidecars.py','deployment/final_old_artifact_readonly.py','common/p26_gates.py','preflight/p26_current_preflight.py','regressions/adapters/w10_guard_shim.py',*[str(f.relative_to(P26)) for f in FILES.values()]}


def canonical_mounts(mounts):
    assert isinstance(mounts, list)
    assert all(isinstance(m, dict) and isinstance(m.get('Destination'), str) and m['Destination'] for m in mounts)
    result = {m['Destination']: m for m in mounts}
    assert len(result) == len(mounts)
    return result


def canonical_facts(fact):
    return dict(fact, mounts=canonical_mounts(fact['mounts']))


def payload_for(instance):
    result=['python3','-I','/srv/'+FILES[instance].name,'0.0.0.0','8090' if instance=='fixture' else '8091']
    if instance=='proxy':result+=['http://dataset:50078']
    return result


def argv_for(instance,image):
    assert instance in NAMES and image==PYTHON_IMAGE
    target='/srv/'+FILES[instance].name
    return ['docker','run','-d','--pull=never','--name',NAMES[instance],'--network',NET,
        '--label','of-p26-owner=p26-http','--label','of-p26-stage=p2-6','--label','of-p26-role=legacy_p25_'+instance,
        '--mount','type=bind,src='+str(FILES[instance])+',dst='+target+',readonly',
        '-p','127.0.0.1:'+str(PORTS[instance])+(':'+('8090' if instance=='fixture' else '8091')),image]+payload_for(instance)


def validate(instance,pin,image):
    assert re.fullmatch('[a-f0-9]{64}',pin['cid']) and pin['name']=='/'+NAMES[instance]
    assert pin['image']==pin['config_image']==image==PYTHON_IMAGE and pin['running'] is True
    for key,value in {'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'legacy_p25_'+instance}.items():assert pin['labels'].get(key)==value
    assert set(pin['networks'])=={NET}
    port='8090/tcp' if instance=='fixture' else '8091/tcp'
    assert pin['ports'][port]==[{'HostIp':'127.0.0.1','HostPort':str(PORTS[instance])}]
    mounts=canonical_mounts(pin['mounts']);assert len(mounts)==1
    mount=mounts['/srv/'+FILES[instance].name];assert mount['Type']=='bind' and mount['RW'] is False
    assert mount['Source'] in [str(FILES[instance]),'/host_mnt'+str(FILES[instance])]
    assert pin['cmd']==payload_for(instance)


def run(release_file):
    r=G.load_release(release_file);assert r and PHASE in r['released_for']
    source=r.get('frozen_source_sha256',r.get('candidate_source'))
    assert re.fullmatch('[a-f0-9]{64}',source) and G.candidate_hash(G.OPENFOUNDRY)==source
    if 'candidate_source' in r:assert r['candidate_source']==source
    assert set(r['p25_sidecars_tool_shas'])==TOOLS
    for rel,sha in r['p25_sidecars_tool_shas'].items():assert G.sha256_file(P26/rel)==sha
    assert r['p25_sidecars_tool_shas']['deployment/create_legacy_p25_sidecars.py']==G.sha256_file(__file__)
    image=r['p25_sidecars_python_image'];assert image==PYTHON_IMAGE
    assert r['p25_sidecars_names']==NAMES and r['p25_sidecars_ports']==PORTS
    att=W.Attempt(P26/'results/w6/deployment','legacy-P25-sidecars');created={};attempted=[]
    def gate():
        assert G.load_release(release_file)==r and G.candidate_hash(G.OPENFOUNDRY)==source
        for rel,sha in r['p25_sidecars_tool_shas'].items():assert G.sha256_file(P26/rel)==sha
    def command(sid,argv,allowed=(0,)):
        gate();receipt=att.step_begin(sid,argv)
        try:cp=subprocess.run(argv,capture_output=True,text=True,timeout=120)
        except Exception as exc:
            att.step_finish(sid,receipt,type(exc).__name__,'unavailable',{'original_child_rc':None});raise RuntimeError(sid+' spawn/timeout') from None
        att.step_finish(sid,receipt,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_child_rc':cp.returncode});assert cp.returncode in allowed;return cp.stdout.strip()
    def fact(tag,cid):return json.loads(command(tag,['docker','inspect',cid,'--format',FMT]))
    def guard(side):
        binding=r['p25_sidecars_preservation_guard'];assert G.sha256_file(binding['tool'])==binding['tool_sha256']
        assert G.sha256_file(binding['release_file'])==binding['release_sha256']
        assert binding['tool']==str(P26/'regressions/adapters/w10_guard_shim.py')
        assert binding['argv']==['python3','-I','-B',binding['tool'],binding['release_file']]
        result=json.loads(command('whole-old-plus-typed-'+side,binding['argv']).splitlines()[-1]);assert result['ok'];return result
    try:
        guard_before=guard('before')
        protected={}
        assert {'p26fixture','p26proxy','p26connectora','p26connectorb','p26connectorc','p26connectora2','p26connectorb2',*G.Q3_CONTAINERS}.issubset(r['p25_sidecars_preserve_containers'])
        for name,pin in r['p25_sidecars_preserve_containers'].items():
            current=fact('preserve-before-'+name,pin['cid'])
            assert current['name']=='/'+name and all(current[k]==pin[k] for k in ['cid','image','started_at','running'])
            protected[name]=current
        names=set(command('collision-allnames',['docker','ps','-a','--format','{{.Names}}']).splitlines());assert not names.intersection(NAMES.values())
        assert not command('collision-ports',['lsof','-nP','-iTCP:17103-17104','-sTCP:LISTEN'],allowed=(0,1))
        assert command('cached-candidate-image',['docker','image','inspect',image,'--format','{{.Id}}'])==image
        for instance in ['fixture','proxy']:
            attempted.append(instance)
            cid=command('create-'+NAMES[instance],argv_for(instance,image));assert re.fullmatch('[a-f0-9]{64}',cid)
            created[instance]={'cid':cid,'verification':'pending'}
            (att.dir/'created-ledger.json').write_text(json.dumps(created,indent=2)+'\n')
            actual=fact('new-'+NAMES[instance],cid);validate(instance,actual,image);created[instance]=actual
            (att.dir/'created-ledger.json').write_text(json.dumps(created,indent=2)+'\n')
            for attempt in range(1,11):
                gate()
                try:
                    response,_=evidenced_http(att,'http://127.0.0.1:'+str(PORTS[instance])+('/healthz' if instance=='fixture' else '/admin/stats'))
                    if response.status==200:break
                except Exception:
                    if attempt==10:raise
                if attempt==10:raise RuntimeError('new cohort health not200')
                time.sleep(1)
        guard_after=guard('after')
        for name,pin in protected.items():assert canonical_facts(fact('preserve-after-'+name,pin['cid']))==canonical_facts(pin)
        (att.dir/'p25-sidecars.json').write_text(json.dumps(created,indent=2)+'\n')
        att.finish({'ok':True,'source_S2':source,'created':created,'preserved_exact':list(protected),'guard_before':guard_before,'guard_after':guard_after,'connector_or_existing_resource_reconfigured':False});print(att.dir);return 0
    except Exception as exc:
        candidates={}
        for inst in attempted:
            try:candidates[inst]=fact('failure-untrusted-'+inst,NAMES[inst])
            except Exception:candidates[inst]={'identity':'unavailable'}
        (att.dir/'partial-candidates.json').write_text(json.dumps({'created':created,'untrusted':candidates,'cleanup_executed':False},indent=2)+'\n')
        return terminal_error(att,exc,[])


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);a=p.parse_args()
    try:rc=run(a.release_file)
    except Exception as exc:print('P25 sidecars preflight refused ('+type(exc).__name__+')',file=sys.stderr);rc=1
    sys.exit(rc)
