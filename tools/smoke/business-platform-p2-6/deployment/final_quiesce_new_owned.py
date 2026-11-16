#!/usr/bin/env python3
"""Disable only Root-proven NEW schedules, revoke grants, then stop the exact five owned Connectors.

No cancel/recover/row cleanup exists. A stop-induced held-row change fails the
post whole typed guard and is retained as failure, never adopted as a baseline.
"""
import argparse,json,re,subprocess,sys,uuid
from pathlib import Path
from urllib import request,error
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(P26));sys.path.insert(0,str(P26/'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
from final_old_artifact_readonly import terminal_error,evidenced_http
CONNECTORS={'p26connectora':17097,'p26connectorb':17098,'p26connectorc':17100,'p26connectora2':17101,'p26connectorb2':17102}
ROLES={'p26connectora':'connector_a','p26connectorb':'connector_b','p26connectorc':'connector_c','p26connectora2':'connector_a','p26connectorb2':'connector_b'}
HTTP_CIDS={'p26connectora2':'ffe58ff5c30caf7e7afa8a808b2a489049e0a650fe6f7217d81057dfd23e2640','p26connectorb2':'7766b279b07f6766861a20c89453a5bd277817eaaae8fc69c221b2c78d664fea'}
TOOLS={'deployment/final_quiesce_new_owned.py','deployment/final_old_artifact_readonly.py','common/p26_gates.py','preflight/p26_current_preflight.py','regressions/adapters/w10_guard_shim.py'}
FMT='{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}},"ports":{{json .HostConfig.PortBindings}}}'


def proven_document(ref):
    assert Path(ref['path']).resolve().is_relative_to((P26/'results').resolve())
    assert G.sha256_file(ref['path'])==ref['sha256']
    text=Path(ref['path']).read_text()
    document=json.loads(text[text.index('{'):])
    for part in ref.get('json_pointer','').strip('/').split('/'):
        if part:document=document[int(part)] if isinstance(document,list) else document[part.replace('~1','/').replace('~0','~')]
    return document


def validate_definition(entry,protected):
    for key in ['def_id','source_id','output_dataset_id','actor_id']:
        assert str(uuid.UUID(entry[key]))==entry[key]
    if entry['org_id'] is not None:assert str(uuid.UUID(entry['org_id']))==entry['org_id']
    assert entry['def_id'] not in protected
    definition=proven_document(entry['definition_creation'])
    source=proven_document(entry['source_creation'])
    assert (definition['id'],definition['source_id'],definition['output_dataset_id'],definition.get('org_id'))==(entry['def_id'],entry['source_id'],entry['output_dataset_id'],entry['org_id'])
    assert (source['id'],source['owner_id'],source.get('org_id'))==(entry['source_id'],entry['actor_id'],entry['org_id'])
    assert entry['instance'] in CONNECTORS


def validate_connector_ledger(pins,main_cids):
    assert set(pins)==set(CONNECTORS)
    assert len({pin['cid'] for pin in pins.values()})==len(pins)
    for name,pin in pins.items():
        assert re.fullmatch('[a-f0-9]{64}',pin['cid']) and pin['cid'] not in main_cids
        assert pin['running'] is True
        assert pin['ownership']=={'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':ROLES[name]}
        if name in HTTP_CIDS:assert pin['cid']==HTTP_CIDS[name]


def schedule_actions(row,armed):
    actions=[]
    if armed:actions.append(('DELETE','/schedule:arm',None))
    if row['schedule_enabled'] is not False or row['schedule_next_fire_at'] is not None:
        actions.append(('PATCH','',{'schedule_enabled':False}))
    return actions


def validate_coverage(defs,coverage,protected,source):
    assert coverage['schema']=='p26-final-owned-def-coverage-v1' and coverage['source_sha256']==source
    required=coverage['definition_ids']
    assert isinstance(required,list) and required and len(required)==len(set(required))
    assert all(str(uuid.UUID(did))==did for did in required)
    assert len({entry['def_id'] for entry in defs})==len(defs)
    assert {entry['def_id'] for entry in defs}==set(required) and not set(required)&protected
    creators=coverage['creators'];assert len(creators)==len(required) and {x['def_id'] for x in creators}==set(required)
    by_id={x['def_id']:x for x in creators}
    for entry in defs:
        assert by_id[entry['def_id']]['definition_creation']==entry['definition_creation']
        assert by_id[entry['def_id']]['source_creation']==entry['source_creation']


def run(release_file):
    r=G.load_release(release_file);assert r and r['root_accepted'] and 'final-new-owned-quiesce' in r['released_for']
    source=r['candidate_source'];assert re.fullmatch('[a-f0-9]{64}',source) and G.candidate_hash(G.OPENFOUNDRY)==source
    assert set(r['quiesce_tools_sha256'])==TOOLS
    for rel,sha in r['quiesce_tools_sha256'].items():assert G.sha256_file(P26/rel)==sha
    assert r['quiesce_tools_sha256']['deployment/final_quiesce_new_owned.py']==G.sha256_file(__file__)
    for name in ['typed_roster','owned_definitions','owned_definition_coverage','quiesce_aux_ledger','deployment_record']:
        assert G.sha256_file(r[name])==r[name+'_sha256']
    defs=json.loads(Path(r['owned_definitions']).read_text())['definitions']
    assert len({x['def_id'] for x in defs})==len(defs)
    _,_,old=W.load_baseline(G.P25_FINAL_DIR)
    protected={x['def_id'] for x in old['protected9']}|{x['def_id'] for x in old['cp8']}
    protected|={'e9b80125-63a8-4d4b-b1de-e3bb5763c0b1','b6df951d-6136-4172-8a79-e9b5db0fcda6'}
    typed=json.loads(Path(r['typed_roster']).read_text())
    for row in typed.get('runs',[]):
        if row.get('protocol')=='legacy-null' and row.get('control_mode')=='legacy':protected.add(row['def_id'])
    coverage=json.loads(Path(r['owned_definition_coverage']).read_text())
    validate_coverage(defs,coverage,protected,source)
    for entry in defs:validate_definition(entry,protected)
    pins=json.loads(Path(r['quiesce_aux_ledger']).read_text())['containers']
    record=json.loads(Path(r['deployment_record']).read_text())
    assert set(record['containers'])==set(W.G.Q3_CONTAINERS)
    validate_connector_ledger(pins,{v['cid'] for v in record['containers'].values()})
    att=W.Attempt(P26/'results/w6/deployment','final-new-owned-quiesce');stopped={}
    secret=W.read_jwt_secret();att.add_secret(secret)
    def gate():
        assert G.load_release(release_file)==r and G.candidate_hash(G.OPENFOUNDRY)==source
        for rel,sha in r['quiesce_tools_sha256'].items():assert G.sha256_file(P26/rel)==sha
    def command(sid,argv):
        gate();prefix=att.step_begin(sid,argv)
        try:cp=subprocess.run(argv,capture_output=True,text=True,timeout=180)
        except Exception as exc:att.step_finish(sid,prefix,type(exc).__name__,'unavailable',{'original_child_rc':None});raise RuntimeError(sid+' spawn/timeout') from None
        att.step_finish(sid,prefix,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_child_rc':cp.returncode});assert cp.returncode==0;return cp.stdout
    def guard(side):
        binding=r['quiesce_guard_'+side]
        assert binding['tool']==str(P26/'regressions/adapters/w10_guard_shim.py') and G.sha256_file(binding['tool'])==binding['tool_sha256']
        assert G.sha256_file(binding['release_file'])==binding['release_sha256']
        assert binding['argv']==['python3','-I','-B',binding['tool'],binding['release_file']]
        result=json.loads(command('whole-old-plus-typed-'+side,binding['argv']).strip().splitlines()[-1]);assert result['ok'];return result
    def fact(name,pin,expected_running):
        actual=json.loads(command('exact-owned-connector-'+name,['docker','inspect',pin['cid'],'--format',FMT]))
        assert actual['name']=='/'+name and actual['running'] is expected_running
        assert all(actual[k]==pin[k] for k in ['cid','image','started_at'])
        assert all(actual['labels'].get(k)==v for k,v in pin['ownership'].items())
        assert actual['ports']['50088/tcp']==[{'HostIp':'127.0.0.1','HostPort':str(CONNECTORS[name])}];return actual
    def mutate(entry,method,suffix,body=None):
        gate();token=W.sign_testing_jwt(secret,entry['actor_id'],entry['org_id']);att.add_secret(token)
        url='http://127.0.0.1:'+str(CONNECTORS[entry['instance']])+'/api/v1/data-connection/syncs/'+entry['def_id']+suffix
        raw=json.dumps(body).encode() if body is not None else None
        prefix=att.step_begin('owned-definition-'+method,{'method':method,'url':url,'body':body,'authorization_recorded':False})
        req=request.Request(url,data=raw,method=method,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        try:
            try:
                with request.build_opener(request.ProxyHandler({}),W._NoRedirectHandler).open(req,timeout=15) as response:status,data=response.status,response.read(W.HTTP_MAX_BODY+1)
            except error.HTTPError as exc:status,data=exc.code,exc.read(W.HTTP_MAX_BODY+1)
            att.step_finish('owned-definition-'+method,prefix,data,0,{'http_status':status,'transport_outcome':0,'original_child_rc':None},kind='http')
        except Exception as exc:
            att.step_finish('owned-definition-'+method,prefix,type(exc).__name__,1,{'transport_outcome':1,'original_child_rc':None},kind='http');raise RuntimeError('owned definition transport failure') from None
        assert status==200 and len(data)<=W.HTTP_MAX_BODY
    def schedule_readback(require_quiet=True):
        pg=r['w7_pg_observer'];assert re.fullmatch('[a-f0-9]{64}',pg['cid'])
        record=json.loads(Path(r['deployment_record']).read_text());assert pg['cid']==record['containers'][W.Q3_POSTGRES]['cid']
        prefix=['docker','exec',pg['cid'],'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','openfoundry','-d','connectors','-c','BEGIN READ ONLY;','-c']
        grants=json.loads(command('whole-grants-zero',prefix+["SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT sync_def_id::text FROM sync_schedule_grants) t;",'-c','COMMIT;']));assert {x['sync_def_id'] for x in grants}.issubset({x['def_id'] for x in defs})
        if require_quiet:assert grants==[]
        ids=','.join("'"+x['def_id']+"'::uuid" for x in defs)
        sql="SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT id::text,schedule_enabled,schedule_next_fire_at FROM batch_sync_defs WHERE id IN ("+(ids or 'NULL')+")) t;"
        rows=json.loads(command('owned-def-schedules-disabled',prefix+[sql,'-c','COMMIT;']));assert {x['id'] for x in rows}=={x['def_id'] for x in defs}
        if require_quiet:assert all(x['schedule_enabled'] is False and x['schedule_next_fire_at'] is None for x in rows)
        return rows,grants
    def old_counters():
        _,catalog,_=W.load_baseline(G.P25_FINAL_DIR);actual={}
        for name,want in catalog['counter_bodies_sha256'].items():
            gate();response,_=evidenced_http(att,W.ORIGINS['fixture']+'/admin/src/'+name)
            assert response.status==200 and G.sha256_bytes(response.body)==want
            actual[name]=want
        return actual
    try:
        before=guard('before');counters_before=old_counters()
        for name,pin in pins.items():fact(name,pin,True)
        rows,grants=schedule_readback(False)
        state={row['id']:row for row in rows};armed={row['sync_def_id'] for row in grants}
        for entry in defs:
            for method,suffix,body in schedule_actions(state[entry['def_id']],entry['def_id'] in armed):
                mutate(entry,method,suffix,body)
        schedules=schedule_readback()
        # This proves zero actual worker processes after stop, not an inference
        # from a database lease count. Never call cancel/recover/clear APIs.
        for name,pin in pins.items():
            fact(name,pin,True)
            command('stop-only-exact-owned-'+name,['docker','stop','--time','120',pin['cid']])
            fact(name,pin,False);stopped[name]=dict(pin,running=False)
            (att.dir/'quiet-owned-connectors-ledger.json').write_text(json.dumps({'schema':'p26-final-owned-aux-v1','source_sha256':source,'containers':stopped},indent=2)+'\n')
        after=guard('after');schedule_readback();assert old_counters()==counters_before
        origins=[{'path':str(f),'sha256':G.sha256_file(f)} for f in sorted(att.dir.iterdir()) if f.is_file()]
        result={'schema':'p26-final-quiesce-proof-v1','ok':True,'source_sha256':source,'whole_schedule_grants_count':0,'owned_def_next_fire_count':0,'worker_processes_stopped':True,'typed_held_preserved':True,'typed_roster_sha256':r['typed_roster_sha256'],'owned_definitions':defs,'owned_definition_coverage':{'path':r['owned_definition_coverage'],'sha256':r['owned_definition_coverage_sha256']},'stopped_connectors':stopped,'origins':origins,'guard_before':before,'guard_after':after,'unknown_or_old_rows_cancelled_adopted_cleared':False}
        att.finish(result);print(att.dir);return 0
    except Exception as exc:
        (att.dir/'partial-stopped-owned-connectors.json').write_text(json.dumps(stopped,indent=2)+'\n')
        return terminal_error(att,exc,[])


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);a=p.parse_args()
    try:rc=run(a.release_file)
    except Exception as exc:print('new-owned quiesce preflight refused ('+type(exc).__name__+')',file=sys.stderr);rc=1
    sys.exit(rc)
