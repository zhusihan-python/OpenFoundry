#!/usr/bin/env python3
"""Root-released main3 same-CID restart. No old-only preservation comparator.

Requires a Root-accepted quiesce proof and the W10 boot-transition extension.
Never writes a Root release or automatically rolls back a partial restart.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(P26));sys.path.insert(0,str(P26/'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
from final_old_artifact_readonly import evidenced_http,terminal_error
BOOT=P26/'regressions/adapters/w10_boot_transition.py'
ORDER=['dataset','pipeline','connector']
TOOLS={'deployment/final_main_restart.py','deployment/final_old_artifact_readonly.py','common/p26_gates.py','preflight/p26_current_preflight.py','regressions/adapters/w10_boot_transition.py','regressions/adapters/w10_guard_shim.py'}


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


def canonical_mounts(mounts):
    assert isinstance(mounts, list)
    assert all(isinstance(m, dict) and isinstance(m.get('Destination'), str) and m['Destination'] for m in mounts)
    result = {m['Destination']: m for m in mounts}
    assert len(result) == len(mounts)
    return result


def exclusive_json(path,value):
    with path.open('x') as stream:
        stream.write(json.dumps(value,indent=2)+'\n');stream.flush();os.fsync(stream.fileno())


def validate_change(previous,current,before,after,service):
    assert set(current)==set(previous)=={'cid','image','started_at'}
    assert current['cid']==previous['cid'] and current['image']==previous['image']
    assert re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z',current['started_at'])
    assert current['started_at']!=previous['started_at']
    assert canonical_mounts(json.loads(before['mounts']))==canonical_mounts(json.loads(after['mounts']))
    assert before['ports'].strip()==after['ports'].strip()
    names=[]
    for raw in [before['env_names'],after['env_names']]:
        current_names=raw.rstrip('\n').splitlines()
        assert current_names and all(re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',name) for name in current_names)
        assert len(current_names)==len(set(current_names))
        names.append(set(current_names))
    assert names[0]==names[1]
    if service=='connector':assert not names[1].intersection(W.LOOP_ENV_NAMES)


def run(release_file):
    r=G.load_release(release_file)
    assert r and r['root_accepted'] and 'final-main3-restart' in r['released_for']
    source=r['candidate_source'];assert re.fullmatch('[a-f0-9]{64}',source)
    assert G.candidate_hash(G.OPENFOUNDRY)==source
    assert set(r['final_main_tools_sha256'])==TOOLS
    for rel,sha in r['final_main_tools_sha256'].items():assert G.sha256_file(P26/rel)==sha
    assert r['final_main_tools_sha256']['deployment/final_main_restart.py']==G.sha256_file(__file__)
    assert G.sha256_file(BOOT)==r['main_boot_transition']['guard_extension_sha256']
    spec=importlib.util.spec_from_file_location('Root_boot_contract',BOOT);B=importlib.util.module_from_spec(spec);spec.loader.exec_module(B)
    authority=r['main_boot_transition']
    assert Path(authority['executor_tool']).resolve()==Path(__file__).resolve()
    assert authority['executor_tool_sha256']==G.sha256_file(__file__) and authority['allowed_service_order']==ORDER
    assert G.sha256_file(r['deployment_record'])==r['deployment_record_sha256']
    base=json.loads(Path(r['deployment_record']).read_text())
    # Actual final inputs are supplied by Root after P22/P24/P25; no historical
    # foundation A/B/C CID/boot values are embedded here.
    q=r['final_quiesce_proof'];assert G.sha256_file(q['path'])==q['sha256']
    quiet=json.loads(Path(q['path']).read_text())
    assert quiet['ok'] and quiet['whole_schedule_grants_count']==0 and quiet['owned_def_next_fire_count']==0
    assert quiet['worker_processes_stopped'] and quiet['typed_held_preserved']
    assert quiet['typed_roster_sha256']==r['typed_roster_sha256']
    for ref in quiet['origins']:assert G.sha256_file(ref['path'])==ref['sha256']
    assert G.sha256_file(r['final_aux_ledger'])==r['final_aux_ledger_sha256']
    aux=json.loads(Path(r['final_aux_ledger']).read_text())['containers']
    assert set(base['containers'])==set(W.G.Q3_CONTAINERS)
    validate_aux_ledger(aux,{pin['cid'] for pin in base['containers'].values()})
    assert set(quiet['stopped_connectors'])==CONNECTORS
    assert all(quiet['stopped_connectors'][name]==aux[name] for name in CONNECTORS)
    output=Path(r['main_restart_output']).resolve()
    assert output.is_relative_to((P26/'results/w6/deployment').resolve())
    output.mkdir(parents=True,exist_ok=False)
    ledger=Path(authority['ledger_dir']).resolve();assert ledger.is_relative_to(output)
    assert ledger!=output
    ledger.mkdir(parents=True,exist_ok=False)
    att=W.Attempt(output,'commands')
    def gate():
        assert G.load_release(release_file)==r and G.candidate_hash(G.OPENFOUNDRY)==source
        for rel,sha in r['final_main_tools_sha256'].items():assert G.sha256_file(P26/rel)==sha
    def command(sid,argv,timeout=180):
        gate();prefix=att.step_begin(sid,argv)
        try:cp=subprocess.run(argv,capture_output=True,text=True,timeout=timeout)
        except Exception as exc:
            att.step_finish(sid,prefix,type(exc).__name__,'unavailable',{'original_child_rc':None},kind='argv')
            raise RuntimeError(sid+' spawn/timeout; original rc unavailable') from None
        att.step_finish(sid,prefix,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_child_rc':cp.returncode},kind='argv')
        assert cp.returncode==0,sid+' original rc='+str(cp.returncode)
        return cp.stdout,prefix
    def inspect(service,tag):
        name=getattr(W,'Q3_'+service.upper());pin=base['containers'][name];raw={};receipts={}
        for key,fmt in [('identity',B.IDENTITY),('mounts',B.MOUNTS),('ports',B.PORTS),('env_names',B.ENVNAMES)]:
            raw[key],receipts[key]=command(tag+'-'+key,['docker','inspect',pin['cid'],'--format',fmt])
        lines=raw['identity'].strip().splitlines();assert len(lines)==4 and lines[3]=='true'
        return dict(zip(['cid','image','started_at'],lines[:3])),raw,receipts
    def guard(tag):
        binding=r['final_typed_guard']
        assert binding['tool']==str(P26/'regressions/adapters/w10_guard_shim.py')
        assert binding['supports_main_boot_transition'] is True
        assert G.sha256_file(binding['tool'])==binding['tool_sha256']
        assert binding['argv']==['python3','-I','-B',binding['tool'],release_file]
        text,prefix=command('whole-old-plus-typed-'+tag,binding['argv'])
        result=json.loads(text.strip().splitlines()[-1]);assert result['ok']
        return {'result':result,'receipt_prefix':prefix}
    def quiet_aux():
        for name,pin in aux.items():
            assert pin['ownership']['of-p26-owner']=='p26-http' and pin['ownership']['of-p26-stage']=='p2-6'
            fmt='{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}\n{{json .Config.Labels}}\n{{json .HostConfig.PortBindings}}'
            raw,_=command('quiet-owned-'+name,['docker','inspect',pin['cid'],'--format',fmt]);lines=raw.strip().splitlines()
            assert lines[:4]==[pin['cid'],pin['image'],pin['started_at'],str(pin['running']).lower()]
            labels=json.loads(lines[4]);assert all(labels.get(k)==v for k,v in pin['ownership'].items())
            host,inner=OWNED_PORTS[name];assert json.loads(lines[5])[str(inner)+'/tcp']==[{'HostIp':'127.0.0.1','HostPort':str(host)}]
    def health(service):
        for i in range(1,11):
            gate()
            try:
                resp,_=evidenced_http(att,W.ORIGINS[service]+'/healthz')
                if resp.status==200:return
            except Exception:
                # Original failed read evidence was already completed. Only
                # GET readiness is retried; restart commands never are.
                if i==10:raise RuntimeError('main health transport not ready') from None
            if i==10:raise RuntimeError('main health not200')
            time.sleep(1)
    def counters():
        _,catalog,_=W.load_baseline(G.P25_FINAL_DIR)
        result={}
        for name,want in catalog['counter_bodies_sha256'].items():
            gate();resp,_=evidenced_http(att,W.ORIGINS['fixture']+'/admin/src/'+name)
            assert resp.status==200 and G.sha256_bytes(resp.body)==want
            result[name]=want
        return result
    completed=[]
    try:
        anchor={'schema':'p26-main-boot-transitions-v1','release_id':r['release_id'],'executor_tool_sha256':authority['executor_tool_sha256'],'base_deployment_record_sha256':r['deployment_record_sha256'],'allowed_service_order':ORDER}
        exclusive_json(ledger/'00-anchor.json',anchor)
        guard_before=guard('before');quiet_aux();before_counters=counters()
        fixed={}
        for service in ['postgres','fixture']:
            fixed[service]=inspect(service,'fixed-before-'+service)
            pin=base['containers'][getattr(W,'Q3_'+service.upper())]
            assert fixed[service][0]=={k:pin[k] for k in ['cid','image','started_at']}
        for i,service in enumerate(ORDER,1):
            old,before,before_refs=inspect(service,service+'-before')
            root_pin=base['containers'][getattr(W,'Q3_'+service.upper())]
            assert old=={k:root_pin[k] for k in ['cid','image','started_at']}
            cmd=authority['restart_commands'][service]
            assert cmd in [['docker','restart',old['cid']],['docker','restart','--time','120',old['cid']],['docker','restart','--timeout','120',old['cid']]]
            # Persist exact intent before mutation; partial failures never auto-retry.
            exclusive_json(output/(service+'-restart-intent.json'),{'service':service,'previous':old,'argv':cmd,'before':before_refs})
            _,restart_ref=command('restart-only-'+service,cmd)
            current,after,after_refs=inspect(service,service+'-after')
            validate_change(old,current,before,after,service)
            step={'schema':'p26-main-boot-transition-v1','release_id':r['release_id'],'service':service,'previous':old,'current':current,'before':before_refs,'restart_step':restart_ref,'after':after_refs}
            exclusive_json(ledger/(str(i).zfill(2)+'-'+service+'.json'),step)
            completed.append(service)
            health(service)
            assert counters()==before_counters
            for fixed_service,before_fixed in fixed.items():
                now=inspect(fixed_service,'fixed-after-'+service+'-'+fixed_service)
                assert now[0]==before_fixed[0] and all(canonical_mounts(json.loads(now[1][k]))==canonical_mounts(json.loads(before_fixed[1][k])) if k=='mounts' else now[1][k].strip()==before_fixed[1][k].strip() for k in before_fixed[1])
        # Extension validates every original receipt and only derives boot fields.
        derived,transition_proof=B.derive(r,base,W);assert len(transition_proof['steps'])==3
        quiet_aux();guard_after=guard('after')
        exclusive_json(output/'derived-deployment-record.json',derived)
        exclusive_json(output/'restart-transition-proof.json',transition_proof)
        att.finish({'ok':True,'source_S':source,'completed':completed,'guard_before':guard_before,'guard_after':guard_after,'PG_oldfixture_unchanged':True,'fixed_oldfixture_counters_unchanged':True,'wholetyped_preserved':True,'rollback_executed':False})
        print(att.dir);return 0
    except Exception as exc:
        exclusive_json(output/'PARTIAL.json',{'ok':False,'completed_restart_receipts':completed,'auto_retry_rollback':False,'Root_review_required':True})
        return terminal_error(att,exc,[])


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);a=p.parse_args()
    try:rc=run(a.release_file)
    except Exception as exc:print('final main restart preflight refused ('+type(exc).__name__+')',file=sys.stderr);rc=1
    sys.exit(rc)
