#!/usr/bin/env python3
"""Three explicit Root phases only. No J/P24/P25/restart/unknown cleanup."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib import request

sys.dont_write_bytecode = True
P26=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(P26))
from common import p26_gates as G
from preflight import p26_current_preflight as W
COMPOSE=P26/'deployment/legacy-stage/compose.yaml'
PROJECT='openfoundry-p26-legacy'
S='b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c'
CN='sha256:f23f69161c6f86f7402a18dbd14e24fdcad581ecbd9968e7b964ef1baf11df34'
PY='sha256:c5ba3deef00c7758561a0f03c26f44f10589263d40e732b8748e0d1f38f6dd89'
ORIGINAL_CIDS={'p26connectora':'ff40554413fd3708a54f4e469ad604bcfe812084b94bc117c035d17a7a9b303f','p26connectorb':'316b0282f5864486438e81a5c760b2a1dfccc3065dcbc2b06a35e757b4669355'}
ROLES={'p26connectora':'connector_a','p26connectorb':'connector_b','p26connectorc':'connector_c','p26p22proxy':'legacy_p22_proxy'}
PORTS={'p26connectora':17097,'p26connectorb':17098,'p26connectorc':17100,'p26p22proxy':17099}
LEGACY={'B_lease_lost_unpublished':'6d6dc56f-1498-4b13-871e-bab489e8b631','C_committed_unconfirmed':'4ff175b3-65d7-47b6-9a83-d46e03bbfe23'}
FMT='{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}},"networks":{{json .NetworkSettings.Networks}},"ports":{{json .NetworkSettings.Ports}}}'
ENV4='{{range .Config.Env}}{{$p := split . "="}}{{if or (eq (index $p 0) "CONNECTOR_SYNC_ASYNC_WORKERS") (eq (index $p 0) "CONNECTOR_SYNC_ASYNC_QUEUE") (eq (index $p 0) "CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS") (eq (index $p 0) "CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS")}}{{println .}}{{end}}{{end}}'


def validate_original(name, pin, actual, running=True):
    assert pin['cid']==ORIGINAL_CIDS[name] and pin['name']==name and pin['image']==CN
    assert all(actual[k]==pin[k] for k in ['cid','image','started_at'])
    assert actual['name']=='/'+name and actual['running'] is running
    for k,v in {'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':ROLES[name]}.items():
        assert actual['labels'].get(k)==v


def validate_new(name, actual):
    assert re.fullmatch('[a-f0-9]{64}',actual['cid'])
    assert actual['name']=='/'+name and actual['running'] is True
    assert actual['image']==(PY if name=='p26p22proxy' else CN)
    labels={'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':ROLES[name],
            'com.docker.compose.project':PROJECT,'com.docker.compose.service':name}
    assert all(actual['labels'].get(k)==v for k,v in labels.items())
    assert set(actual['networks'])=={W.G.Q3_PROJECT+'_p1net'}
    port='8091/tcp' if name=='p26p22proxy' else '50088/tcp'
    assert actual['ports'][port]==[{'HostIp':'127.0.0.1','HostPort':str(PORTS[name])}]


def validate_no_active(raw):
    assert json.loads(raw)==[], 'unexpected active NEW/unknown rows; preserve all'


def validate_legacy(sealed, rows, parents, all_active):
    """Not an ID exemption: sealed13+parents+NULL protocol, then exact whole set."""
    steps={s['step']:s for s in sealed['steps']}
    projection=steps['p21_bc_projection']['detail'];chain=steps['p21_bc_chain']['detail']
    assert steps['p21_bc_projection']['ok'] and steps['p21_bc_chain']['ok']
    assert projection['variant']=='p22-full-13col' and chain['target_column_candidates']==['output_dataset_id']
    def index(values,key):
        assert isinstance(values,list) and len(values)==2
        out={v[key]:v for v in values};assert len(out)==2 and set(out)==set(LEGACY.values());return out
    by_id=index(rows,'id');by_parent=index(parents,'run_id')
    boolean={'has_holder':'has_holder','has_token_col':'has_token','cancel_requested':'cancel_requested','gate_entered':'gate_entered','has_content_hash':'has_hash','has_version':'has_version'}
    fields={'status':'status','heartbeat':'heartbeat_at','expires':'lease_expires_at','control_mode':'control_mode','attempt':'attempt','generation':'generation','finished_at':'finished_at'}
    for label,rid in LEGACY.items():
        row,p=by_id[rid],by_parent[rid]
        W.C._check_row_schema(row,'historical-legacy',0)
        view={k:row[v] for k,v in fields.items()}
        for k,v in boolean.items():
            assert row[v] in ('true','false');view[k]='t' if row[v]=='true' else 'f'
        assert view==projection[label], 'historical legacy13 drift'
        old=chain['rows'][label]
        current={'run_org':p['run_org'],'run_owner':p['run_owner'],'sync_def':p['def_id'],'source_owner':p['source_owner'],'source_org':p['source_org'],'target_via_output_dataset_id':p['target_via_output_dataset_id']}
        assert current==old and p['recovery_protocol'] is None, 'historical parent/protocol drift'
        assert (p['def_id'],p['source_id'],p['target_via_output_dataset_id'],p['run_org'],p['run_owner'],p['source_org'],p['source_owner'])==(row['sync_def_id'],row['source_id'],row['output_dataset_id'],row['run_org_id'],row['run_owner_id'],row['conn_org_id'],row['owner_id'])
    assert index(all_active,'id')==by_id, 'extra/missing/drift active rows outside verified old9+legacy2'


def run(phase, release_file):
    r=G.load_release(release_file)
    assert r and phase in r['released_for'] and 'q3-readonly-preflight' in r['released_for']
    for rel in ['deployment/legacy_foundation_executor.py','deployment/legacy-stage/compose.yaml','common/p26_gates.py','preflight/p26_current_preflight.py','regressions/vendor/tree/business-platform-p2-2/dataset_proxy.py']:
        assert G.sha256_file(P26/rel)==r['legacy_foundation_tool_shas'][rel]
    assert G.candidate_hash(G.OPENFOUNDRY)==S==r['frozen_source_sha256']
    pinproof=r['legacy_original_aux_proof']
    assert G.sha256_file(pinproof['path'])==pinproof['sha256']
    proof=json.loads(Path(pinproof['path']).read_text())
    assert proof['original_aux']==r['legacy_original_aux']
    att=W.Attempt(P26/'results/w6/deployment','legacy-foundation-'+phase)
    created={};attempted=[]
    def gate():
        assert G.load_release(release_file)==r and G.candidate_hash(G.OPENFOUNDRY)==S
    def command(sid,argv,allowed=(0,)):
        gate();before=att.step_begin(sid,argv)
        try: cp=subprocess.run(argv,capture_output=True,text=True,timeout=120)
        except Exception as exc:
            att.step_finish(sid,before,type(exc).__name__,'unavailable',{'original_child_rc':None},kind='argv')
            raise RuntimeError(sid+' spawn/timeout') from None
        att.step_finish(sid,before,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_child_rc':cp.returncode},kind='argv')
        assert cp.returncode in allowed,sid+' original rc='+str(cp.returncode)
        return cp.stdout.strip()
    def inspect(sid,target):return json.loads(command(sid,['docker','inspect',target,'--format',FMT]))
    def oldcore(tag):
        argv=['python3','-I','-B',str(P26/'preflight/p26_current_preflight.py'),'run','--execute','--release-file',release_file,'--expect-dataset-image','sha256:4217c40275b9fb7b28f8ef872a6b64870876ce6e6c599ff4714531dd919ef75b','--expect-connector-image',CN]
        text=command('oldcore-'+tag,argv)
        result=Path(re.search(r'PASS -> (.*RESULT.json)',text).group(1))
        verdict=json.loads(result.read_text());assert verdict['ok']
        rcs=list(result.parent.glob('*.rc'));assert len(rcs)==131 and len(list(result.parent.glob('*.gate.rc')))==59
        assert all(f.read_text().strip()=='0' for f in rcs)
        return str(result)
    def zero_active():
        origin_start=att.n
        _,_,old=W.load_baseline(G.P25_FINAL_DIR)
        ids=','.join("'"+e['run_id']+"'::uuid" for e in old['protected9'])
        ref=r['legacy_sealed12'];sealed_path=Path(ref['path'])
        assert sealed_path.resolve()==(G.P25_FINAL_DIR/'12-reread-after-manifest.json').resolve()
        member=json.loads((G.P25_FINAL_DIR/'hash-manifest.json').read_text())['files']['12-reread-after-manifest.json']
        assert G.sha256_file(sealed_path)==ref['sha256']==member
        sealed=json.loads(sealed_path.read_text())
        pg=r['w7_pg_observer'];assert re.fullmatch('[a-f0-9]{64}',pg['cid']) and pg['user']=='openfoundry' and pg['database']=='connectors'
        assert pg['cid']==r['legacy_main_pins']['postgres']['cid']
        def select(sid,sql):
            raw=command(sid,['docker','exec',pg['cid'],'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','openfoundry','-d','connectors','-c',"BEGIN READ ONLY; SET TimeZone='UTC'; SET DateStyle='ISO, MDY';",'-c',sql,'-c','COMMIT;'])
            return json.loads(raw)
        legacy_ids=','.join("'"+rid+"'::uuid" for rid in LEGACY.values())
        original=W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'][-1]
        needle="r.control_mode = 'controlled' AND r.status IN ('pending','running')"
        assert needle in original
        rows=select('legacy-BC-fullsafe27',original.replace(needle,'r.id IN ('+legacy_ids+')'))
        parents=select('legacy-BC-full-parent-protocol',"SELECT json_agg(row_to_json(p)) FROM (SELECT r.id::text AS run_id,d.id::text AS def_id,d.source_id::text,r.org_id::text AS run_org,r.owner_id::text AS run_owner,c.owner_id::text AS source_owner,c.org_id::text AS source_org,d.output_dataset_id::text AS target_via_output_dataset_id,r.recovery_protocol FROM sync_runs r JOIN batch_sync_defs d ON d.id=r.sync_def_id JOIN connections c ON c.id=d.source_id WHERE r.id IN ("+legacy_ids+")) p;")
        all_active=select('whole-active-outside-old9-fullsafe27',original.replace('JOIN batch_sync_defs','LEFT JOIN batch_sync_defs').replace('JOIN connections','LEFT JOIN connections').replace(needle,"r.status IN ('pending','running') AND r.id NOT IN ("+ids+")"))
        validate_legacy(sealed,rows,parents,all_active)
        full_parents=select('legacy-BC-W8-full-parents',W.build_parents_argv([x['sync_def_id'] for x in rows])[-1])
        assert isinstance(full_parents,list) and len(full_parents)==2
        parent_by_id={x['def_id']:x for x in full_parents};assert len(parent_by_id)==2
        for row in rows:
            p=parent_by_id[row['sync_def_id']];assert set(p)==W.PARENTS_REQUIRED_KEYS
            assert (p['org_id'],p['source_id'],p['output_dataset_id'],p['conn_id'],p['conn_org_id'],p['conn_owner_id'],p['source_name'])==(row['def_org_id'],row['source_id'],row['output_dataset_id'],row['source_id'],row['conn_org_id'],row['owner_id'],row['source_name'])
        ordered=sorted(rows,key=lambda x:x['id'])
        evidence={'schema':'p26-w10-legacy-baseline-v1','sealed12':ref,'runs':ordered,'safe27':ordered,'parents':sorted(full_parents,key=lambda x:x['def_id']),'protocols':sorted([{'id':x['run_id'],'recovery_protocol':x['recovery_protocol']} for x in parents],key=lambda x:x['id']),'legacy_chain_actual':parents,'crosscheck13_parent_protocolNULL':True,'whole_active_outside_old9_exact_verified_legacy2':True,'origins':{'actual_attempt':str(att.dir),'commands':'legacy-BC-fullsafe27/full-parent-protocol/whole-active-outside-old9-fullsafe27/W8-full-parents cmd/raw/rc recorded alongside this file; every command rc0'}}
        evidence['origins']['artifacts']=[{'path':str(f),'sha256':G.sha256_file(f),'original_rc':0} for f in sorted(att.dir.iterdir()) if f.name[:2].isdigit() and int(f.name.split('-',1)[0])>origin_start]
        if phase!='legacy-private-env' or 'legacy_safe27_baseline' in r:
            baseline=r['legacy_safe27_baseline'];assert G.sha256_file(baseline['path'])==baseline['sha256']
            accepted=json.loads(Path(baseline['path']).read_text())
            assert evidence['safe27']==accepted['safe27'] and evidence['parents']==accepted['parents']
            assert evidence['protocols']==accepted['protocols']
        n=len(list(att.dir.glob('legacy-BC-safe27-*.json')))+1
        (att.dir/('legacy-BC-safe27-'+str(n)+'.json')).write_text(json.dumps(evidence,indent=2)+'\n')
    def compose(tail):return ['docker','compose','-f',str(COMPOSE),'--project-name',PROJECT]+tail
    def health(name):
        url='http://127.0.0.1:'+str(PORTS[name])+('/p22proxy/healthz' if name=='p26p22proxy' else '/healthz')
        for attempt in range(1,11):
            gate();sid='health-'+name+'-'+str(attempt);before=att.step_begin(sid,{'method':'GET','url':url})
            try:
                with request.urlopen(url,timeout=5) as response: status,body=response.status,response.read(1048576)
                att.step_finish(sid,before,body,0,{'status':status,'transport_outcome':0},kind='http')
                assert status==200;return
            except Exception as exc:
                att.step_finish(sid,before,type(exc).__name__,1,{'status':None,'transport_outcome':1},kind='http')
                if attempt==10:raise RuntimeError('health not ready') from None
                time.sleep(1)
    def create(name):
        attempted.append(name)
        command('create-'+name,compose(['up','-d','--no-deps','--no-build','--pull=never',name]))
        cid=command('new-cid-'+name,compose(['ps','-q',name]))
        assert re.fullmatch('[a-f0-9]{64}',cid)
        created[name]={'cid':cid,'verification':'pending'}
        (att.dir/'created-ledger.json').write_text(json.dumps(created,indent=2)+'\n')
        facts=inspect('new-identity-'+name,cid);validate_new(name,facts)
        created[name]=facts
        (att.dir/'created-ledger.json').write_text(json.dumps(created,indent=2)+'\n')
        if name!='p26p22proxy':
            raw=command('safe-config4-'+name,['docker','inspect',cid,'--format',ENV4])
            pairs=[ln.split('=',1) for ln in raw.splitlines()]
            assert len(pairs)==4 and len(dict(pairs))==4
            assert dict(pairs)=={'CONNECTOR_SYNC_ASYNC_WORKERS':'1','CONNECTOR_SYNC_ASYNC_QUEUE':'64','CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS':'300','CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS':'300'}
        health(name)
    try:
        auxenv=Path('/private/tmp/openfoundry-p26-aux.env')
        assert auxenv.is_file() and not auxenv.is_symlink() and auxenv.stat().st_mode&0o777==0o600
        for ln in auxenv.read_text().splitlines():
            if '=' in ln:att.add_secret(ln.split('=',1)[1])
        for ln in W.Q3_ENV_FILE.read_text().splitlines():
            if ln.startswith('OF_P1_PG_PASSWORD='):
                att.add_secret(ln.split('=',1)[1].strip().strip('"').strip("'"))
        before_core=oldcore('before')
        before_main={s:inspect('before-'+s,W.G.Q3_PROJECT+'-'+s+'-1') for s in ['dataset','pipeline','connector','postgres','fixture']}
        for s,actual in before_main.items():
            assert all(actual[k]==r['legacy_main_pins'][s][k] for k in ['cid','image','started_at'])
        zero_active()
        original_before={}
        for name in ORIGINAL_CIDS:
            pin=r['legacy_original_aux'][name]
            original_before[name]=inspect('original-initial-'+name,pin['cid'])
            validate_original(name,pin,original_before[name])
        project_names=set(command('existing-legacy-project',['docker','ps','-a','--filter','label=com.docker.compose.project='+PROJECT,'--format','{{.Names}}']).splitlines())
        assert project_names==({'p26p22proxy','p26connectorc'} if phase=='legacy-AB-transition' else set()),'unknown/extra legacy project containers; preserve'
        for image in [CN,PY]:
            assert command('cached-image-'+image[7:19],['docker','image','inspect',image,'--format','{{.Id}}'])==image
        command('compose-quiet-validation',compose(['config','--quiet']))
        if phase=='legacy-private-env':
            q3={k:v.strip().strip('"').strip("'") for k,v in (ln.split('=',1) for ln in W.Q3_ENV_FILE.read_text().splitlines() if ln and not ln.startswith('#') and '=' in ln)}
            smoke=Path('/private/tmp/openfoundry-p26-http-smoke.env');assert not smoke.is_symlink() and smoke.stat().st_mode&0o777==0o600
            vals=dict(ln.split('=',1) for ln in smoke.read_text().splitlines() if ln and '=' in ln)
            jwt=vals['P26_W7_JWT_WRITER'];assert jwt.startswith('Bearer ')
            for value in [q3['OF_P1_PG_PASSWORD'],q3['OF_P1_JWT_SECRET'],jwt,jwt[7:]]:att.add_secret(value)
            # Only new private paths; secrets never enter evidence or stdout.
            metadata=[]
            for directory,base in [('/private/tmp/openfoundry-p26-legacy-env','17100')]:
                dest=Path(directory);assert not dest.exists() and not dest.is_symlink()
                gate()
                dest.mkdir(mode=0o700)
                env={'OF_P1_PG_PASSWORD':q3['OF_P1_PG_PASSWORD'],'OF_P1_JWT_SECRET':q3['OF_P1_JWT_SECRET'],'OF_P1_SERVICE_JWT':jwt[7:],'OF_P1_CONNECTOR_BASE':'http://127.0.0.1:'+base,'OF_P1_DATASET_BASE':W.ORIGINS['dataset'],'OF_P1_PIPELINE_BASE':W.ORIGINS['pipeline'],'OF_P1_FIXTURE_BASE':W.ORIGINS['fixture']}
                assert all('\n' not in v and '\r' not in v for v in env.values())
                with os.fdopen(os.open(dest/'.env',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as stream:stream.write(''.join(k+'='+v+'\n' for k,v in env.items()))
                config={'services':{'postgres':{'container_name':W.Q3_POSTGRES,'image':before_main['postgres']['image']}}}
                (dest/'compose.yaml').write_text(json.dumps(config,indent=2)+'\n')
                metadata.append({'directory':str(dest),'env_mode':'0600','key_names':sorted(env),'compose_usage':'NEW existing-q3-postgres exec locator only; never up/start/restart'})
            (att.dir/'private-env-metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
        elif phase=='legacy-extra-create':
            names=set(command('all-container-names',['docker','ps','-a','--format','{{.Names}}']).splitlines())
            assert not names.intersection({'p26connectorc','p26p22proxy'}),'NEW name collision'
            ports=command('port-collision',['lsof','-nP','-iTCP:17099-17100','-sTCP:LISTEN'],allowed=(0,1));assert not ports
            create('p26p22proxy');create('p26connectorc')
        else:
            assert phase=='legacy-AB-transition'
            extra=r['legacy_extra_ledger'];assert G.sha256_file(extra['path'])==extra['sha256']
            prior=json.loads(Path(extra['path']).read_text())
            for name in ['p26p22proxy','p26connectorc']:
                actual=inspect('accepted-extra-'+name,prior[name]['cid'])
                assert actual==prior[name];validate_new(name,actual)
            for name in ORIGINAL_CIDS:
                pin=r['legacy_original_aux'][name]
                actual=inspect('original-'+name,pin['cid']);validate_original(name,pin,actual)
            for name in ORIGINAL_CIDS:
                pin=r['legacy_original_aux'][name];zero_active()
                validate_original(name,pin,inspect('pre-stop-'+name,pin['cid']))
                command('stop-original-'+name,['docker','stop','--time','30',pin['cid']])
                validate_original(name,pin,inspect('stopped-'+name,pin['cid']),False)
                command('remove-original-'+name,['docker','rm',pin['cid']])
            create('p26connectora');create('p26connectorb')
        zero_active()
        after_core=oldcore('after')
        for s,pin in before_main.items():assert inspect('after-'+s,W.G.Q3_PROJECT+'-'+s+'-1')==pin
        if phase!='legacy-AB-transition':
            for name,pin in original_before.items():assert inspect('original-after-'+name,pin['cid'])==pin
        assert G.candidate_hash(G.OPENFOUNDRY)==S
        att.finish({'ok':True,'phase':phase,'source_S':S,'oldcore_before':before_core,'oldcore_after':after_core,'oldmain5_exactunchanged':True,'created':created,'cleanup_executed':False})
        print(att.dir);return 0
    except Exception as exc:
        # A failing up may have created a container before returning nonzero.
        # Capture only untrusted candidate facts, never adopt or act on them.
        candidates={}
        for name in attempted:
            try:candidates[name]=inspect('failure-untrusted-candidate-'+name,name)
            except Exception:candidates[name]={'inspection':'unavailable'}
        att.finish({'ok':False,'phase':phase,'error':type(exc).__name__+': '+str(exc),'created':created,'untrusted_failure_candidates':candidates,'cleanup_executed':False})
        print('foundation failed ('+type(exc).__name__+'); evidence='+str(att.dir),file=sys.stderr);return 1


if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('phase',choices=['legacy-private-env','legacy-extra-create','legacy-AB-transition']);ap.add_argument('--release-file',required=True);a=ap.parse_args()
    try:rc=run(a.phase,a.release_file)
    except Exception as exc:print('foundation preflight refused ('+type(exc).__name__+')',file=sys.stderr);rc=1
    sys.exit(rc)
