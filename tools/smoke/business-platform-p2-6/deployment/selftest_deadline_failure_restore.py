#!/usr/bin/env python3
"""Pure failure-window/Root-authority tests. No Docker, SQL, or HTTP."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
from unittest.mock import patch

sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('deadline_failure_pure',HERE/'legacy_deadline_driver.py');D=importlib.util.module_from_spec(s);s.loader.exec_module(D)


def run(out):
    out.mkdir(parents=True,exist_ok=False);results=[]
    anchor={'name':D.NAME,'cid':'a'*64,'image':D.IMAGE,'started_at':'2026-11-15T01:00:00Z'}
    for case,mutate,ok in [
        ('up-returncode-failure-owned-new-proof',None,True),
        ('post-config-failure-explicit-Root-correction',lambda r,f,p:p['actual_config'].update(CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS=8),True),
        ('post-health-failure-stopped-owned-new-proof',lambda r,f,p:p['actual_inspect'].update(running=False),True),
        ('up-failed-old-container-intact-proof',lambda r,f,p:p['actual_inspect'].update(cid=anchor['cid']),True),
        ('no-separate-Root-phase',lambda r,f,p:r.update(released_for=[]),False),
        ('unknown-CID-not-observed',lambda r,f,p:p['actual_inspect'].update(cid='c'*64),False),
        ('wrong-image',lambda r,f,p:p['actual_inspect'].update(image='sha256:'+'0'*64),False),
        ('unknown-owner',lambda r,f,p:p['actual_inspect']['labels'].update({'of-p26-owner':'other'}),False),
        ('wrong-compose-project',lambda r,f,p:p['actual_inspect']['labels'].update({'com.docker.compose.project':'q3'}),False),
        ('wrong-origin-release',lambda r,f,p:r['driver_failure_restore'].update(origin_release_id='other'),False),
        ('missing-config-field',lambda r,f,p:p['actual_config'].pop(D.KEYS[0]),False),
        ('mismatched-intent-binding',lambda r,f,p:p.update(transition_intent_sha256='0'*64),False),
    ]:
        root=out/case/'mock-p26';folder=root/'results/w6/deployment/failure-attempt';folder.mkdir(parents=True)
        intent={'schema':'p26-w10-driver-transition-intent-v1','release_id':'original-case','action':'up-deadline:7','root_anchor':anchor,'previous':dict(anchor,started_at='2026-11-15T02:00:00Z'),'source_S':'pure-source','recreate_argv':['docker','compose','-f',str(D.HERE/'legacy-stage/compose.yaml'),'-f',str(D.HERE/'legacy-stage/deadline7.yaml'),'--project-name','openfoundry-p26-legacy','up','-d','--no-deps','--no-build','--pull=never','--force-recreate',D.NAME]}
        ref=D.persist_transition_intent(folder/'transition-intent.json',intent)
        # Fault occurs AFTER intent durability, BEFORE success chain creation.
        assert json.loads(Path(ref['path']).read_text())==intent
        assert D.sha(ref['path'])==ref['sha256']
        assert not (folder/'success-chain.json').exists()
        failure={'schema':'p26-w10-driver-failure-v1','release_id':'original-case','transition_intent':ref,'observed_candidate_cids':['b'*64],'cleanup_executed':False}
        proof={'transition_intent_sha256':ref['sha256'],'actual_inspect':{'name':'/'+D.NAME,'cid':'b'*64,'image':D.IMAGE,'started_at':'2026-11-15T03:00:00Z','running':True,'labels':dict(D.LABELS)},'actual_config':dict(zip(D.KEYS,[1,64,7,300]))}
        r={'released_for':['legacy-deadline-failure-restore'],'candidate_source':'pure-source','driver_failure_restore':{'origin_release_id':'original-case'}}
        if mutate:mutate(r,failure,proof)
        for key,filename,data in [('manifest','FAILURE.json',failure),('target_proof','target-proof.json',proof)]:
            f=folder/filename;f.write_text(json.dumps(data));r['driver_failure_restore'][key]={'path':str(f),'sha256':D.sha(f)}
        accepted=False
        with patch.object(D,'P26',root),patch.object(D.subprocess,'run',side_effect=AssertionError('forbidden process')) as proc:
            try:D.failure_restore_authority(r,anchor);accepted=True
            except (AssertionError,ValueError,KeyError):pass
            assert proc.call_count==0
        assert accepted==ok,case
        results.append({'case':case,'pass':True,'Root_authority_accepted':accepted,'auto_restore_calls':0})
    # Existing intent bytes must never be overwritten on rerun.
    root=out/'exclusive-intent';root.mkdir();f=root/'intent.json';D.persist_transition_intent(f,{'first':True})
    try:D.persist_transition_intent(f,{'second':True})
    except FileExistsError:pass
    else:raise AssertionError('intent overwrite allowed')
    assert json.loads(f.read_text())=={'first':True}
    results.append({'case':'exclusive-durable-intent-no-overwrite','pass':True,'auto_restore_calls':0})
    (out/'RESULT.json').write_text(json.dumps({'pure_only':True,'passed':len(results),'cases':results,'actual_mutation_SQL_HTTP_calls':0,'test_scope':'durable intent + explicit Root failure authority parser; no claim of live recovery','driver_sha256':D.sha(HERE/'legacy_deadline_driver.py')},indent=2)+'\n');print(out);print(str(len(results))+' failure-window contract cases passed; actual calls=0')


if __name__=='__main__':run(Path(sys.argv[1]).resolve())
