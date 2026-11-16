#!/usr/bin/env python3
"""Pure foundation guard checks. No resource process is started."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
from unittest.mock import patch

sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('foundation_pure',HERE/'legacy_foundation_executor.py');M=importlib.util.module_from_spec(s);s.loader.exec_module(M)


def run(out):
    out.mkdir(parents=True,exist_ok=False);results=[]
    real=json.loads((M.P26/'results/w6/deployment/legacy-original-aux-readonly-20261115T081450Z/RESULT.json').read_text())['original_aux']
    def original(name,change=None,ok=False):
        pin=real['p26connectora'];facts={'cid':pin['cid'],'image':pin['image'],'started_at':pin['started_at'],'name':'/p26connectora','running':True,'labels':{'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_a'}}
        if change:change(facts)
        accepted=False
        try:M.validate_original('p26connectora',pin,facts);accepted=True
        except AssertionError:pass
        assert accepted==ok;results.append({'case':name,'pass':True,'accepted':accepted})
    original('actualRootcandidateAaccepted',ok=True)
    original('stale06-52AbootRejected',lambda f:f.update(started_at='2026-11-15T06:52:42.100000Z'))
    original('wrongCIDRejected',lambda f:f.update(cid='a'*64))
    original('wrongImageRejected',lambda f:f.update(image='sha256:'+'0'*64))
    original('wrongNameRejected',lambda f:f.update(name='/old-main-connector'))
    original('wrongOwnerRejected',lambda f:f['labels'].update({'of-p26-owner':'unknown'}))
    original('wrongRoleRejected',lambda f:f['labels'].update({'of-p26-role':'connector_c'}))
    original('stoppedOriginalRejected',lambda f:f.update(running=False))
    n='p26connectorc'
    valid={'cid':'c'*64,'image':M.CN,'name':'/'+n,'running':True,'networks':{'openfoundry-platform-p1-q3v9_p1net':{}},'ports':{'50088/tcp':[{'HostIp':'127.0.0.1','HostPort':'17100'}]},'labels':{'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_c','com.docker.compose.project':M.PROJECT,'com.docker.compose.service':n}}
    for name,change,ok in [('newCaccepted',None,True),('unknownComposeProject',lambda f:f['labels'].update({'com.docker.compose.project':'q3'}),False),('wrongHostPort',lambda f:f['ports']['50088/tcp'][0].update(HostPort='17088'),False),('publicBindRejected',lambda f:f['ports']['50088/tcp'][0].update(HostIp='0.0.0.0'),False),('wrongNetwork',lambda f:f.update(networks={'other':{}}),False)]:
        f=copy.deepcopy(valid)
        if change:change(f)
        accepted=False
        try:M.validate_new(n,f);accepted=True
        except AssertionError:pass
        assert accepted==ok;results.append({'case':name,'pass':True,'accepted':accepted})
    for name,raw,ok in [('newActiveZero','[]',True),('unknownPendingRejected','[{"run_id":"unknown","status":"pending"}]',False),('noncontrolledRunningRejected','[{"run_id":"unknown","status":"running","control_mode":null}]',False),('partialCountRejected','null',False)]:
        accepted=False
        try:M.validate_no_active(raw);accepted=True
        except AssertionError:pass
        assert accepted==ok;results.append({'case':name,'pass':True,'accepted':accepted})
    with patch.object(M.G,'load_release',return_value=None),patch.object(M.subprocess,'run',side_effect=AssertionError('resourcecall forbidden')) as proc:
        try:M.run('legacy-AB-transition','pure-no-release')
        except AssertionError:pass
        else:raise AssertionError('unreleased mutation allowed')
        assert proc.call_count==0
    results.append({'case':'absentRootphaseZeroCalls','pass':True})
    (out/'RESULT.json').write_text(json.dumps({'pure_only':True,'actual_resource_calls':0,'passed':len(results),'cases':results,'helper_sha256':M.G.sha256_file(HERE/'legacy_foundation_executor.py')},indent=2)+'\n');print(out);print(str(len(results))+' guard cases passed; actual resource calls=0')


if __name__=='__main__':run(Path(sys.argv[1]).resolve())
