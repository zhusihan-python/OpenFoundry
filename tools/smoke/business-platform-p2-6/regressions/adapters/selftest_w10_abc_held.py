#!/usr/bin/env python3
import json,sys,tempfile,uuid
from pathlib import Path
from types import SimpleNamespace as NS
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_suite_adapter as A
import w10_abc_held as B
H=A.GUARD

def main():
    with tempfile.TemporaryDirectory(dir=H.P26/'results/w10') as td:
        p=Path(td); release=p/'release.json'; rel={'schema':'p26-w10-release-v1','root_accepted':True,'suite':'p21','stage':'p21abcfinal3','expires_utc':'2099-01-01T00:00:00Z'}; release.write_text(json.dumps(rel))
        source,actor,org,did,rid,target=[str(uuid.uuid4()) for _ in range(6)]
        row={k:'' for k in H.W.C.SCHEMA_27}
        row.update(id=rid,sync_def_id=did,source_id=source,output_dataset_id=target,requested_by=actor,owner_id=actor,run_owner_id=actor,org_id=org,conn_org_id=org,def_org_id=org,run_org_id=org,status='running',control_mode='controlled',source_name='p21-p21b-01234567',fixture_src='p21b-01234567',lease_expires_at='2026-11-15 00:00:00+00',started_at='2026-11-14 23:59:00+00',heartbeat_at='2026-11-14 23:59:10+00',attempt='1',generation='1',has_holder='true',has_token='true',gate_entered='false',cancel_requested='false',has_hash='false',has_version='false')
        mode={'value':'good'}; cmds=[]
        def runner(cmd,**kwargs):
            cmds.append(cmd)
            if 'json_build_object' in cmd[-1]: raw=json.dumps({'id':rid,'expired':mode['value']!='fresh','recovery_protocol':None})
            else: raw=json.dumps([{**row,'generation':'changed'}] if mode['value']=='changed' else [row])
            return NS(returncode=0,stdout=raw,stderr='')
        f=A.Facade(runner,rel,release); f.attempt=H.W.Attempt(p,'held-test')
        f.owned.sources[source]={'id':source,'name':row['source_name'],'actor':actor,'tenant':org}
        f.owned.defs[did]={'id':did,'source_id':source,'output_dataset_id':target,'actor':actor,'tenant':org}
        f.owned.runs[rid]={'id':rid,'def_id':did,'actor':actor,'tenant':org,'status':'running'}
        m=NS(scenario_b=True,SCENARIOS={'B':('B',lambda suffix,ev:ev.update(pages_after_wait=2,pages_before_inject=2,target_files=0,http_status=409))},M=NS(http=lambda *a,**kw:NS(status=200,body={'request_count':2})))
        B.install(m,f); m.SCENARIOS['B'][1]('01234567',{})
        assert set(f.frozen_b)=={rid}; B.recheck(f)
        for value in ('fresh','changed'):
            mode['value']=value
            try: B.recheck(f)
            except ValueError: pass
            else: assert False,value
        assert row['status']=='running' and all('UPDATE ' not in c[-1] for c in cmds)
        print(json.dumps({'ok':True,'actual_http_sql_docker_calls':0,'checks':['completed B exact created identity+quiet counters frozen','actual safe27+DB-clock expired read','fresh lease or changed safe27 rejects exclusion','B stays running; no mutation/adoption/cancel/clear']},indent=2))
if __name__=='__main__': main()
