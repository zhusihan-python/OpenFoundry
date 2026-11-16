#!/usr/bin/env python3
"""Exact Root-owned aux CIDs only, after ABC last-write and final read-only proof.
No row, volume, network, image, old fixture, or main-service deletion exists.
"""
import argparse,json,re,subprocess,sys
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(P26));sys.path.insert(0,str(P26/'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
from final_old_artifact_readonly import terminal_error
FMT='{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}},"ports":{{json .HostConfig.PortBindings}}}'


CONNECTORS={'p26connectora','p26connectorb','p26connectorc','p26connectora2','p26connectorb2'}
OWNED_ROLES={'p26connectora':'connector_a','p26connectorb':'connector_b','p26connectorc':'connector_c','p26connectora2':'connector_a','p26connectorb2':'connector_b','p26fixture':'fixture','p26proxy':'proxy','p26p22proxy':'legacy_p22_proxy','p26p25fixture':'legacy_p25_fixture','p26p25proxy':'legacy_p25_proxy'}
OWNED_PORTS={'p26connectora':(17097,50088),'p26connectorb':(17098,50088),'p26connectorc':(17100,50088),'p26connectora2':(17101,50088),'p26connectorb2':(17102,50088),'p26fixture':(17095,8090),'p26proxy':(17096,8091),'p26p22proxy':(17099,8091),'p26p25fixture':(17103,8090),'p26p25proxy':(17104,8091)}
HTTP_CIDS={'p26connectora2':'ffe58ff5c30caf7e7afa8a808b2a489049e0a650fe6f7217d81057dfd23e2640','p26connectorb2':'7766b279b07f6766861a20c89453a5bd277817eaaae8fc69c221b2c78d664fea'}


def validate_aux_ledger(pins,main_cids):
    assert set(pins)==set(OWNED_ROLES) and len({pin['cid'] for pin in pins.values()})==len(pins)
    for name,pin in pins.items():
        assert re.fullmatch('[a-f0-9]{64}',pin['cid']) and pin['cid'] not in main_cids
        assert pin['ownership']=={'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':OWNED_ROLES[name]}
        if name in CONNECTORS:assert pin['running'] is False
        else:assert type(pin['running']) is bool
        if name in HTTP_CIDS:assert pin['cid']==HTTP_CIDS[name]


def validate_target(name,pin,actual,main_cids):
    assert name in OWNED_ROLES and name not in set(W.G.Q3_CONTAINERS)
    assert re.fullmatch('[a-f0-9]{64}',pin['cid']) and pin['cid'] not in main_cids
    assert actual['name']=='/'+name
    assert all(actual[k]==pin[k] for k in ['cid','image','started_at','running'])
    assert pin['ownership']['of-p26-owner']=='p26-http' and pin['ownership']['of-p26-stage']=='p2-6'
    assert pin['ownership']['of-p26-role']==OWNED_ROLES[name]
    host,inner=OWNED_PORTS[name];assert actual['ports'][str(inner)+'/tcp']==[{'HostIp':'127.0.0.1','HostPort':str(host)}]
    assert all(actual['labels'].get(k)==v for k,v in pin['ownership'].items())
    if name in CONNECTORS:assert pin['running'] is False


def run(release_file):
    r=G.load_release(release_file)
    assert r and r['root_accepted'] and 'final-owned-aux-cleanup' in r['released_for']
    source=r['candidate_source'];assert re.fullmatch('[a-f0-9]{64}',source)
    assert G.candidate_hash(G.OPENFOUNDRY)==source
    for rel,sha in r['cleanup_tools_sha256'].items():assert G.sha256_file(P26/rel)==sha
    assert r['cleanup_tools_sha256']['deployment/final_owned_aux_cleanup.py']==G.sha256_file(__file__)
    for key in ['deployment_record','typed_roster','final_aux_ledger']:
        assert G.sha256_file(r[key])==r[key+'_sha256']
    # Actual final proofs are supplied later by Root, never fabricated from
    # today's P22/Foundation pins or simple schema/count assertions.
    for key in ['ABC_last_write_accepted','final_readonly_accepted','final_quiesce_proof']:
        ref=r[key];assert G.sha256_file(ref['path'])==ref['sha256']
        assert json.loads(Path(ref['path']).read_text())['ok'] is True
    quiet=json.loads(Path(r['final_quiesce_proof']['path']).read_text())
    assert quiet['worker_processes_stopped'] and quiet['whole_schedule_grants_count']==0 and quiet['owned_def_next_fire_count']==0 and quiet['typed_held_preserved']
    record=json.loads(Path(r['deployment_record']).read_text());aux=json.loads(Path(r['final_aux_ledger']).read_text())
    assert aux['schema']=='p26-final-owned-aux-v1' and aux['source_sha256']==source
    pins=aux['containers'];assert set(r['cleanup_order'])==set(pins) and len(r['cleanup_order'])==len(pins)
    assert set(record['containers'])==set(W.G.Q3_CONTAINERS)
    validate_aux_ledger(pins,{v['cid'] for v in record['containers'].values()})
    assert set(quiet['stopped_connectors'])==CONNECTORS
    assert all(quiet['stopped_connectors'][name]==pins[name] for name in CONNECTORS)
    att=W.Attempt(P26/'results/w6/deployment','final-owned-aux-cleanup');removed=[]
    def command(sid,argv):
        assert G.load_release(release_file)==r and G.candidate_hash(G.OPENFOUNDRY)==source
        prefix=att.step_begin(sid,argv)
        try:cp=subprocess.run(argv,capture_output=True,text=True,timeout=180)
        except Exception as exc:
            att.step_finish(sid,prefix,type(exc).__name__,'unavailable',{'original_child_rc':None},kind='argv');raise RuntimeError(sid+' spawn/timeout') from None
        att.step_finish(sid,prefix,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_child_rc':cp.returncode},kind='argv')
        assert cp.returncode==0,sid+' original rc='+str(cp.returncode)
        return cp.stdout
    def guard(tag):
        g=r['cleanup_typed_guard'];assert g['tool']==str(P26/'regressions/adapters/w10_guard_shim.py') and G.sha256_file(g['tool'])==g['tool_sha256']
        assert g['argv']==['python3','-I','-B',g['tool'],release_file]
        result=json.loads(command('whole-old-plus-typed-'+tag,g['argv']).strip().splitlines()[-1]);assert result['ok'];return result
    def fact(tag,cid):return json.loads(command(tag,['docker','inspect',cid,'--format',FMT]))
    try:
        # Main pin set is main5 only; stopped aux have a separate Root ledger.
        assert set(record['containers'])==set(W.G.Q3_CONTAINERS)
        main_cids={v['cid'] for v in record['containers'].values()}
        mains={n:fact('main-before-'+n,v['cid']) for n,v in record['containers'].items()}
        for n,v in record['containers'].items():
            assert all(mains[n][k]==v[k] for k in ['cid','image','started_at']) and mains[n]['running']
        before=guard('before')
        for n,pin in pins.items():validate_target(n,pin,fact('owned-before-'+n,pin['cid']),main_cids)
        for n in r['cleanup_order']:
            pin=pins[n];actual=fact('owned-immediate-'+n,pin['cid']);validate_target(n,pin,actual,main_cids)
            if actual['running']:
                command('stop-exact-owned-'+n,['docker','stop','--time','120',pin['cid']])
                stopped=fact('owned-stopped-'+n,pin['cid']);expected=dict(pin,running=False);validate_target(n,expected,stopped,main_cids)
            command('remove-exact-owned-'+n,['docker','rm',pin['cid']])
            removed.append({'name':n,'cid':pin['cid']})
            (att.dir/'removed-ledger.json').write_text(json.dumps(removed,indent=2)+'\n')
        inventory=command('inventory-after-cleanup',['docker','ps','-a','--no-trunc','--format','{{.ID}}']).splitlines()
        assert not set(x['cid'] for x in removed).intersection(inventory)
        after=guard('after')
        for n,pin in record['containers'].items():assert fact('main-after-'+n,pin['cid'])==mains[n]
        att.finish({'ok':True,'source_S':source,'removed':removed,'guard_before':before,'guard_after':after,'oldmain_fixture_PG_unchanged':True,'rows_volumes_networks_images_deleted':False});print(att.dir);return 0
    except Exception as exc:
        (att.dir/'partial-removed-ledger.json').write_text(json.dumps(removed,indent=2)+'\n')
        return terminal_error(att,exc,[])


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);a=p.parse_args()
    try:rc=run(a.release_file)
    except Exception as exc:print('owned cleanup preflight refused ('+type(exc).__name__+')',file=sys.stderr);rc=1
    sys.exit(rc)
