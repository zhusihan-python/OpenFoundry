#!/usr/bin/env python3
import ast,json,sys,uuid
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_suite_adapter as A
from w10_owned_sql import Registry

def main():
    r=Registry({'owned_sql_templates':['p1-new-target-check','p21-new-run-expiry','p21-new-def-terminal-trigger','p23-owned-source-nowait'],'serial_owned_window':True})
    actor={'sub':str(uuid.uuid4()),'org_id':str(uuid.uuid4())}
    target,sid,did,rid=[str(uuid.uuid4()) for _ in range(4)]
    r.observe('dataset','/v1/datasets','POST',{'name':'NEW'},201,{'id':target},actor)
    r.observe('connector','/api/v1/data-connection/sources','POST',{'name':'NEW'},201,{'id':sid},actor)
    payload={'source_id':sid,'output_dataset_id':target}
    r.observe('connector','/api/v1/data-connection/syncs','POST',payload,201,{'id':did},actor)
    r.observe('connector',f'/api/v1/data-connection/syncs/{did}/runs','GET',None,200,[{'id':rid,'sync_def_id':did,'status':'running','org_id':actor['org_id'],'requested_by':actor['sub']}],actor)
    assert did in r.defs and rid in r.runs
    foreign=str(uuid.uuid4()); r.observe('connector','/api/v1/data-connection/syncs','POST',payload,201,{'id':foreign},{'sub':str(uuid.uuid4()),'org_id':actor['org_id']})
    assert foreign not in r.defs
    p1=A.load(A.TREE/A.SUITES['p1']); captured=[]; p1.psql=lambda sql,**kw:captured.append(sql)
    p1.block_target_publish(target); p1.unblock_target_publish()
    assert r.authorize(captured[0],'datasets')=='p1-new-target-check'
    assert not r.pending
    r.mark_install_attempt('p1-new-target-check')
    assert r.authorize(captured[0].replace(target,str(uuid.uuid4())),'datasets') is None
    assert r.authorize(captured[1],'datasets')=='p1-check-cleanup'
    r.complete('p1-check-cleanup',1); assert 'p1-check' in r.pending
    r.complete('p1-check-cleanup',0); assert not r.pending
    p21=A.load(A.TREE/A.SUITES['p21'])
    tree=ast.parse((A.TREE/A.SUITES['p21']).read_text()); count=0
    for node in ast.walk(tree):
        if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id=='psql_connectors' and node.args:
            expr=node.args[0]
            if not isinstance(expr,(ast.JoinedStr,ast.Constant)): continue
            try: value=eval(compile(ast.Expression(expr),'NEW copied SQL expression','eval'),{'sync_def_id':did,'run_id':rid,**vars(p21)})
            except (NameError,TypeError): continue
            if value.startswith(('UPDATE sync_runs SET lease_expires_at','CREATE OR REPLACE FUNCTION','CREATE TRIGGER','DROP TRIGGER','DROP FUNCTION')):
                purpose=r.authorize(value,'connectors')
                assert purpose is not None,repr(value)
                if purpose in ('p21-new-def-terminal-function','p21-new-def-terminal-trigger'): r.mark_install_attempt(purpose)
                r.complete(purpose,0); count+=1
    assert count==5,count
    assert r.authorize(f"SELECT id FROM connections WHERE id = '{sid}'::uuid FOR UPDATE NOWAIT;",'connectors')=='p23-owned-source-nowait'
    assert r.authorize(f"SELECT id FROM connections WHERE id = '{foreign}'::uuid FOR UPDATE NOWAIT;",'connectors') is None
    assert not r.pending
    from p22_legacy_probe_setup import insert_sql
    fixture=insert_sql(r,str(uuid.uuid4()),did)
    assert fixture.startswith('INSERT INTO sync_runs') and "'legacy',NULL,NULL,NULL,NULL,NULL,NULL,NULL" in fixture and 'RETURNING id::text' in fixture
    try: insert_sql(r,str(uuid.uuid4()),foreign)
    except KeyError: pass
    else: assert False
    print(json.dumps({'ok':True,'actual_http_sql_docker_calls':0,'checks':['actual successful creation identities bind parent/tenant','foreign actor/unknown UUID rejected','exact original NEW-copy P1 CHECK SQL accepted','cleanup failure stays pending','five exact copied ABC SQL expressions accepted','NOWAIT only actual-created Source','NEW legacy INSERT only actual-created parent chain and explicit stage/protocol NULL']},indent=2))
if __name__=='__main__': main()
