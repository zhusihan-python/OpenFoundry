#!/usr/bin/env python3
"""Pure NEW ownership/provenance/phase guards; no actual process or HTTP."""
import importlib.util,json,copy,sys
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
def module(name):
    s=importlib.util.spec_from_file_location(name,HERE/(name+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
Q=module('final_quiesce_new_owned');C=module('final_owned_aux_cleanup')


def run(out):
    out.mkdir(parents=True,exist_ok=False);results=[]
    entry={'def_id':'11111111-1111-4111-8111-111111111111','source_id':'22222222-2222-4222-8222-222222222222','output_dataset_id':'33333333-3333-4333-8333-333333333333','actor_id':'44444444-4444-4444-8444-444444444444','org_id':'55555555-5555-4555-8555-555555555555','instance':'p26connectorc'}
    for key,data in [('definition_creation',{'id':entry['def_id'],'source_id':entry['source_id'],'output_dataset_id':entry['output_dataset_id'],'org_id':entry['org_id']}),('source_creation',{'id':entry['source_id'],'owner_id':entry['actor_id'],'org_id':entry['org_id']})]:
        f=out/(key+'.json');f.write_text(json.dumps(data));entry[key]={'path':str(f),'sha256':Q.G.sha256_file(f)}
    for name,mutate,protected,ok in [('new-owned-provenance',None,set(),True),('protected-old-def',None,{entry['def_id']},False),('wrong-source',lambda e:e.update(source_id='66666666-6666-4666-8666-666666666666'),set(),False),('wrong-actor',lambda e:e.update(actor_id='66666666-6666-4666-8666-666666666666'),set(),False),('main-instance-refused',lambda e:e.update(instance='main'),set(),False),('tampered-originhash',lambda e:e['source_creation'].update(sha256='0'*64),set(),False)]:
        value=copy.deepcopy(entry)
        if mutate:mutate(value)
        accepted=False
        try:Q.validate_definition(value,protected);accepted=True
        except (AssertionError,ValueError,KeyError):pass
        assert accepted==ok;results.append({'case':name,'pass':True,'accepted':accepted})
    pin={'cid':'a'*64,'image':'sha256:'+'a'*64,'started_at':'2026-11-15T01:00:00Z','running':False,'ownership':{'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_a'}}
    original={'cid':pin['cid'],'name':'/p26connectora','image':pin['image'],'started_at':pin['started_at'],'running':False,'labels':dict(pin['ownership']),'ports':{'50088/tcp':[{'HostIp':'127.0.0.1','HostPort':'17097'}]}}
    for name,change,main,ok in [('exact-stopped-owned-cleanup',None,set(),True),('unknown-CID',lambda f:f.update(cid='b'*64),set(),False),('unknown-role',lambda f:f['labels'].update({'of-p26-role':'other'}),set(),False),('mainCID-cannot-clean',None,{pin['cid']},False),('active-worker-cannot-clean',lambda f:f.update(running=True),set(),False)]:
        f=copy.deepcopy(original)
        if change:change(f)
        accepted=False
        try:C.validate_target('p26connectora',pin,f,main);accepted=True
        except AssertionError:pass
        assert accepted==ok;results.append({'case':name,'pass':True,'accepted':accepted})
    for mod in [Q,C]:
        with patch.object(mod.G,'load_release',return_value=None),patch.object(mod.subprocess,'run',side_effect=AssertionError('actualcallforbidden')) as process:
            try:mod.run('pure-no-release')
            except AssertionError:pass
            else:raise AssertionError('unreleased mutation allowed')
            assert process.call_count==0
        results.append({'case':mod.__name__+'-unreleased-zero-calls','pass':True})
    (out/'RESULT.json').write_text(json.dumps({'pure_only':True,'passed':len(results),'actual_resource_calls':0,'cases':results,'tool_shas':{f:Q.G.sha256_file(HERE/f) for f in ['final_quiesce_new_owned.py','final_owned_aux_cleanup.py']}},indent=2)+'\n');print(out);print(str(len(results))+' final guard cases passed; actual calls=0')


if __name__=='__main__':run(Path(sys.argv[1]).resolve())
