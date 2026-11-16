#!/usr/bin/env python3
"""Root-released independent HTTP a2/b2. No legacy ABC/fixture reconfigure."""
import argparse,json,re,subprocess,sys,time
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(P26));sys.path.insert(0,str(P26/'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
from final_old_artifact_readonly import evidenced_http,terminal_error
PHASE='http-s2-aux-start'
NAMES={'a':'p26connectora2','b':'p26connectorb2'}
PORTS={'a':17101,'b':17102}
NET='openfoundry-platform-p1-q3v9_p1net'
AUX_ENV=Path('/private/tmp/openfoundry-p26-aux.env')
FMT='{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}},"networks":{{json .NetworkSettings.Networks}},"ports":{{json .NetworkSettings.Ports}},"mounts":{{json .Mounts}}}'
CONFIG={'HOST':'0.0.0.0','PORT':'50088','DATASET_SERVICE_URL':'http://p26proxy:8091','ALLOWED_EGRESS_HOSTS':'fixture,p26fixture','ALLOW_PRIVATE_NETWORK_EGRESS':'true','OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS':'0','OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS':'0','OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED':'false','CONNECTOR_SYNC_ASYNC_WORKERS':'1','CONNECTOR_SYNC_ASYNC_QUEUE':'64','CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS':'300','CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS':'300'}
TOOLS={'deployment/create_http_s2_cohort.py','deployment/final_old_artifact_readonly.py','common/p26_gates.py','preflight/p26_current_preflight.py','regressions/adapters/w10_guard_shim.py'}


def canonical_mounts(mounts):
    assert isinstance(mounts, list)
    assert all(isinstance(m, dict) and isinstance(m.get('Destination'), str) and m['Destination'] for m in mounts)
    result = {m['Destination']: m for m in mounts}
    assert len(result) == len(mounts)
    return result


def canonical_facts(fact):
    return dict(fact, mounts=canonical_mounts(fact['mounts']))


def argv_for(instance,image):
    assert instance in NAMES and re.fullmatch('sha256:[a-f0-9]{64}',image)
    argv=['docker','run','-d','--pull=never','--name',NAMES[instance],'--network',NET,
          '--label','of-p26-owner=p26-http','--label','of-p26-stage=p2-6','--label','of-p26-role=connector_'+instance,
          '--env-file',str(AUX_ENV),'-p','127.0.0.1:'+str(PORTS[instance])+':50088']
    for key,value in CONFIG.items():argv+=['-e',key+'='+value]
    return argv+[image]


def validate(instance,pin,image):
    assert re.fullmatch('[a-f0-9]{64}',pin['cid']) and pin['name']=='/'+NAMES[instance]
    assert pin['image']==image and pin['running'] is True
    for key,value in {'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_'+instance}.items():assert pin['labels'].get(key)==value
    assert set(pin['networks'])=={NET}
    assert pin['ports']['50088/tcp']==[{'HostIp':'127.0.0.1','HostPort':str(PORTS[instance])}]


def run(release_file):
    r=G.load_release(release_file);assert r and PHASE in r['released_for']
    source=r.get('frozen_source_sha256',r.get('candidate_source'))
    assert re.fullmatch('[a-f0-9]{64}',source) and G.candidate_hash(G.OPENFOUNDRY)==source
    if 'candidate_source' in r:assert r['candidate_source']==source
    assert set(r['http_s2_deployment_tool_shas'])==TOOLS
    for rel,sha in r['http_s2_deployment_tool_shas'].items():assert G.sha256_file(P26/rel)==sha
    assert r['http_s2_deployment_tool_shas']['deployment/create_http_s2_cohort.py']==G.sha256_file(__file__)
    image=r['expected_candidate_images']['connector'];assert re.fullmatch('sha256:[a-f0-9]{64}',image)
    assert r['http_s2_cohort_names']==NAMES and r['http_s2_cohort_ports']==PORTS
    assert AUX_ENV.is_file() and not AUX_ENV.is_symlink() and AUX_ENV.stat().st_mode&0o777==0o600
    att=W.Attempt(P26/'results/w6/deployment','http-S2-cohort');created={};attempted=[]
    for line in AUX_ENV.read_text().splitlines():
        if '=' in line:att.add_secret(line.split('=',1)[1])
    def gate():
        assert G.load_release(release_file)==r and G.candidate_hash(G.OPENFOUNDRY)==source
        for rel,sha in r['http_s2_deployment_tool_shas'].items():assert G.sha256_file(P26/rel)==sha
    def command(sid,argv,allowed=(0,)):
        gate();receipt=att.step_begin(sid,argv)
        try:cp=subprocess.run(argv,capture_output=True,text=True,timeout=120)
        except Exception as exc:
            att.step_finish(sid,receipt,type(exc).__name__,'unavailable',{'original_child_rc':None});raise RuntimeError(sid+' spawn/timeout') from None
        att.step_finish(sid,receipt,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_child_rc':cp.returncode});assert cp.returncode in allowed;return cp.stdout.strip()
    def fact(tag,cid):return json.loads(command(tag,['docker','inspect',cid,'--format',FMT]))
    def guard(side):
        binding=r['http_s2_preservation_guard'];assert G.sha256_file(binding['tool'])==binding['tool_sha256']
        assert G.sha256_file(binding['release_file'])==binding['release_sha256']
        assert binding['tool']==str(P26/'regressions/adapters/w10_guard_shim.py')
        assert binding['argv']==['python3','-I','-B',binding['tool'],binding['release_file']]
        result=json.loads(command('whole-old-plus-typed-'+side,binding['argv']).splitlines()[-1]);assert result['ok'];return result
    try:
        guard_before=guard('before')
        protected={}
        assert {'p26fixture','p26proxy','p26connectora','p26connectorb','p26connectorc',*G.Q3_CONTAINERS}.issubset(r['http_s2_preserve_containers'])
        for name,pin in r['http_s2_preserve_containers'].items():
            current=fact('preserve-before-'+name,pin['cid'])
            assert current['name']=='/'+name and all(current[k]==pin[k] for k in ['cid','image','started_at','running'])
            protected[name]=current
        names=set(command('collision-allnames',['docker','ps','-a','--format','{{.Names}}']).splitlines());assert not names.intersection(NAMES.values())
        assert not command('collision-ports',['lsof','-nP','-iTCP:17101-17102','-sTCP:LISTEN'],allowed=(0,1))
        assert command('cached-candidate-image',['docker','image','inspect',image,'--format','{{.Id}}'])==image
        for instance in ['a','b']:
            attempted.append(instance)
            cid=command('create-'+NAMES[instance],argv_for(instance,image));assert re.fullmatch('[a-f0-9]{64}',cid)
            created[instance]={'cid':cid,'verification':'pending'}
            (att.dir/'created-ledger.json').write_text(json.dumps(created,indent=2)+'\n')
            actual=fact('new-'+NAMES[instance],cid);validate(instance,actual,image);created[instance]=actual
            (att.dir/'created-ledger.json').write_text(json.dumps(created,indent=2)+'\n')
            clauses=' '.join('(eq (index $p 0) "'+k+'")' for k in CONFIG)
            fmt='{{range .Config.Env}}{{$p := split . "="}}{{if or '+clauses+'}}{{println .}}{{end}}{{end}}'
            pairs=[line.split('=',1) for line in command('actual-safe-config-'+instance,['docker','inspect',cid,'--format',fmt]).splitlines()]
            assert len(pairs)==len(CONFIG) and len(dict(pairs))==len(CONFIG) and dict(pairs)==CONFIG
            for attempt in range(1,11):
                gate()
                try:
                    response,_=evidenced_http(att,'http://127.0.0.1:'+str(PORTS[instance])+'/healthz')
                    if response.status==200:break
                except Exception:
                    if attempt==10:raise
                if attempt==10:raise RuntimeError('new cohort health not200')
                time.sleep(1)
        guard_after=guard('after')
        for name,pin in protected.items():assert canonical_facts(fact('preserve-after-'+name,pin['cid']))==canonical_facts(pin)
        w7_aux={inst:{'name':NAMES[inst],'cid':p['cid'],'image':p['image'],'labels':{'owner':'p26-http','stage':'p2-6','role':'connector_'+inst}} for inst,p in created.items()}
        (att.dir/'w7_aux.json').write_text(json.dumps(w7_aux,indent=2)+'\n')
        att.finish({'ok':True,'source_S2':source,'w7_aux':w7_aux,'created':created,'preserved_exact':list(protected),'guard_before':guard_before,'guard_after':guard_after,'legacyABC_fixture_proxy_reconfigured':False});print(att.dir);return 0
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
    except Exception as exc:print('HTTP S2 cohort preflight refused ('+type(exc).__name__+')',file=sys.stderr);rc=1
    sys.exit(rc)
