#!/usr/bin/env python3
"""Pure producer tests: actual Runner/checker, injected HTTP+SQL, network tripwires."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import types
import urllib.request
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
EV=HERE/'results/2026-11-14-p25-final'


def load(name,file):
    spec=importlib.util.spec_from_file_location(name,HERE/file)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def main():
    producer=load('owned_test','prepare_owned_quiesce_p25.py')
    roster=load('owned_roster_test','p25_tail_roster.py');engine=roster.http_pure()
    q=load('owned_q_test','check_owned_quiesced_p25.py')
    owners=json.loads((EV/'tail-owner-roster.json').read_text())
    selected=json.loads((EV/'tail-aggregate.json').read_text())
    ids,parents,inputs=producer.owned_inputs(EV,engine)
    assert {o['def_id'] for o in owners}<=set(ids)
    h1trial=json.loads((EV/'h1-trial/attempt1/p25-healthy-20261114T162306/handles.json').read_text())
    assert all(h['ref'] in ids for h in h1trial['handles'] if h['kind']=='def')
    print('actual EV union:',len(ids),'P25 defs; includes trial/failure/retries/h10/tail')
    uid=lambda i:'00000000-0000-4000-8000-%012d'%i
    clean=uid(900);test_ids=sorted([o['def_id'] for o in owners]+[clean]);test_parents={o['def_id']:o['source_id'] for o in owners};test_parents[clean]=uid(901)
    selected_by={o['def_id']:o for o in selected['final_owner_roster']}
    tests=[]
    with tempfile.TemporaryDirectory() as td:
        root=Path(td)
        producer.RELEASED_FOR_EXECUTION=False
        gate=['--ev',str(EV),'--tail-summary',str(EV/'tail-aggregate.json'),'--tail-bindings',str(EV/'tail-selected-bindings.json'),
              '--tail-declaration',str(EV/'tail-declaration.json'),'--evidence-root',str(root/'must-not-exist'),'--released']
        with patch.dict(os.environ,{'P25_CONFIRM':'yes'}),patch.object(producer.LiveIO,'api',side_effect=AssertionError('gate constructed live IO')):
            assert producer.main(gate)==3 and not (root/'must-not-exist').exists()
        faults = ['good','extra-nonowner-new-run','old-same-run-new','missing-first-run','duplicate-first-run','malformed-new-run'] if '--new-run-only' in sys.argv else ['good','wrong-parent','missing-owner-run','admitted','wrong-busy-owner','changed-CP','extra-SQL-owner','extra-new-run','grants-left','malformed-SQL-bool']
        for fault in faults:
            class FakeAPI:
                def __init__(self):
                    self.calls=[];self.mutated=False;self.cps={};self.runs={}
                    for did in test_ids:
                        owner=next((o for o in owners if o['def_id']==did),None)
                        if owner:
                            status=selected_by[did]['run_status']
                            self.cps[did]={'mode':'cursor','confirmed_epoch':0,'checkpoint_len':0,'checkpoint_sha256':None,
                                'owner':{'run_id':owner['run_id'],'run_status':status,'blocked_reason':'requires_reconciliation'}}
                            self.runs[did]=[{'id':owner['run_id'],'sync_def_id':did,'status':status,'generation':1,'attempt':1,
                                'control_mode':'controlled','publish_entered_at':None if owner['run_ref']=='same' else '2026-11-14T17:00:00Z',
                                'started_at':'2026-11-14T17:00:00Z'}]
                        else:self.cps[did]=None;self.runs[did]=[]
                    if fault=='missing-owner-run':self.runs[owners[0]['def_id']]=[]
                def call(self,base,method,path,body=None,**kwargs):
                    assert base=='connector';self.calls.append((method,path,body));status=200;value=None
                    if path.startswith('/api/v1/connections/'):
                        assert method=='GET';value={'id':path.rsplit('/',1)[1]}
                    else:
                        did=path.split('/syncs/')[1].split('/')[0];assert did in test_ids
                        if method=='GET' and path.endswith('/checkpoint'):
                            value=copy.deepcopy(self.cps[did])
                            if value is None:status=404;value={'error':'incremental checkpoint not initialized for this sync definition'}
                            elif fault=='changed-CP' and self.mutated:value['confirmed_epoch']=99
                        elif method=='GET' and path.endswith('/runs'):value=copy.deepcopy(self.runs[did])
                        elif method=='GET':
                            value={'id':did,'source_id':test_parents[did],'output_dataset_id':uid(902)}
                            if fault=='wrong-parent':value['source_id']=uid(999)
                        elif method=='DELETE':assert path.endswith('/schedule:arm');self.mutated=True;value={'revoked':True}
                        elif method=='PATCH':assert body=={'schedule_enabled':False};self.mutated=True;value={'id':did}
                        elif method=='POST':
                            assert path.endswith('/run?async=true');own=next(o for o in owners if o['def_id']==did)
                            status=409;value={'code':'sync_incremental_busy','active_run_id':own['run_id']}
                            if fault=='admitted':status=202;value={'id':uid(950),'sync_def_id':did,'status':'pending'}
                            if fault=='wrong-busy-owner':value['active_run_id']=uid(950)
                        else:raise AssertionError('unapproved method')
                    raw=json.dumps(value).encode()
                    return {'status':status,'json':value,'raw':raw,'error':None}
            api=FakeAPI()
            class FakeIO:
                def api(self,m,rec):return api
                def command(self,argv):
                    assert argv[:3]==['docker','compose','-f'];sql=argv[-1]
                    if 'FROM batch_sync_defs d WHERE' in sql:
                        rows=[{'id':did,'schedule_enabled':'false','cursor_null':'true'} for did in test_ids]
                        if fault=='malformed-SQL-bool':rows[0]['schedule_enabled']=False
                    elif 'FROM sync_def_checkpoints c' in sql:
                        rows=[{'def_id':o['def_id'],'source_id':o['source_id'],'def_source_id':o['source_id'],
                               'run_id':o['run_id'],'run_def_id':o['def_id'],'run_status':selected_by[o['def_id']]['run_status'],
                               'blocked_reason':'requires_reconciliation'} for o in owners]
                        if fault=='extra-SQL-owner':rows.append(dict(rows[0],def_id=uid(999),run_def_id=uid(999)))
                    elif 'FROM sync_schedule_grants g' in sql:rows=[] if fault!='grants-left' else [{'sync_def_id':clean}]
                    elif 'started_at >' in sql:
                        rows=[{'id':o['run_id'],'sync_def_id':o['def_id']} for o in owners if o.get('origin')=='intentional-tail']
                        if fault=='extra-new-run':rows.append({'id':uid(998),'sync_def_id':owners[0]['def_id']})
                        if fault=='extra-nonowner-new-run':rows.append({'id':uid(998),'sync_def_id':clean})
                        if fault=='old-same-run-new':
                            old=next(o for o in owners if o.get('origin')!='intentional-tail');rows.append({'id':old['run_id'],'sync_def_id':old['def_id']})
                        if fault=='missing-first-run':rows.pop()
                        if fault=='duplicate-first-run':rows.append(dict(rows[0]))
                        if fault=='malformed-new-run':rows.append({'id':'not-UUID','sync_def_id':clean})
                    elif 'FROM sync_runs r WHERE' in sql:rows=[r for rs in api.runs.values() for r in rs]
                    else:raise AssertionError('unexpected actual SQL: '+sql)
                    return 0,'SET\nSET\n'+json.dumps(rows)+'\n',''
            args=types.SimpleNamespace(evidence_root=str(root/fault),env_dir='/private/tmp/openfoundry-platform-p1-q3v9',tail_summary=str(EV/'tail-aggregate.json'))
            runner=producer.Runner(args,engine,q,FakeIO(),test_ids,test_parents,inputs,owners,json.loads((EV/'tail-declaration.json').read_text()))
            with patch.object(urllib.request,'urlopen',side_effect=AssertionError('network forbidden')),\
                 patch.object(socket,'socket',side_effect=AssertionError('socket forbidden')),\
                 patch.object(subprocess,'run',side_effect=AssertionError('subprocess forbidden')):
                rc=runner.run()
            assert (rc==0)==(fault=='good'),(fault,runner.report)
            mutations=[c for c in api.calls if c[0]!='GET'];posts=[c for c in mutations if c[0]=='POST']
            if fault=='good':
                assert len(posts)==len(owners) and len({p[1] for p in posts})==len(owners)
                assert sum(c[0]=='PATCH' for c in mutations)==len(test_ids) and sum(c[0]=='DELETE' for c in mutations)==len(test_ids)
                assert producer.sha(root/fault/'blocked-proof.json')
            if fault in ['wrong-parent','missing-owner-run']:assert mutations==[]
            if fault in ['admitted','wrong-busy-owner']:assert len(posts)==1
            if fault=='admitted':assert any(h.get('origin')=='unexpected-block-probe-admission' for h in json.loads(Path(runner.rec.handles_path).read_text())['handles'])
            tests.append({'case':fault,'rc':rc,'POSTs':len(posts)})
            print(fault,rc,len(posts))
    print(json.dumps({'ok':True,'tests':tests,'root_release_gate_rc':3,'no_live_IO':True}))
    return 0


if __name__=='__main__':raise SystemExit(main())
