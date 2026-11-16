#!/usr/bin/env python3
"""Replay captured actual bad/good J projection through real Facade boundaries."""
import copy,json,sys,tempfile,subprocess
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_suite_adapter as A
H=A.GUARD

def main():
    base=H.P26/'results/w10/Nsetup-P22-P23-ready-attempt1/actual-p22'
    actual=json.loads((base/'adapter.json').read_text());result0=json.loads((base/'driver-chain.json').read_text());original=Path(result0['evidence']['path']);originsha=H.sha(original);originalproof=json.loads(original.read_text())
    deployment=H.P26/'results/w6/deployment/legacy-AB-transition-outer-20261115T083635Z/deployment-record.json'
    with tempfile.TemporaryDirectory(dir=H.P26/'results/w10') as td,patch('socket.socket',side_effect=AssertionError('actual network forbidden')):
        td=Path(td);binding={'tool':str(Path(__file__).resolve()),'tool_sha256':H.sha(__file__),'argv':['FAKE-captured-actual-driver']}
        release={'schema':'p26-w10-release-v1','root_accepted':True,'suite':'p22','stage':'pure-captured-P22','expires_utc':'2035-01-01T00:00:00Z','release_id':result0['release_id'],'candidate_source':'b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c','service_map':{'connector2':'p26connectora'},'deployment_record':str(deployment),'deployment_record_sha256':H.sha(deployment),'aux_project':'openfoundry-p26-legacy','aux_compose':str(td/'NEW-aux-compose.yaml'),'driver_commands':{'up-deadline:7':binding}}
        rp=td/'release.json';rp.write_text(json.dumps(release));old=['sh',str(A.TREE/'business-platform-p2-2/p22_connector2.sh'),'up-deadline','7'];outcome={'fixed':False};calls=[]
        fixedproof=copy.deepcopy(originalproof);fixedproof['current']['name']='p26connectora';fp=td/'fixed-projection-proof.json';fp.write_text(json.dumps(fixedproof));fixed=copy.deepcopy(result0);fixed['current']['name']='p26connectora';fixed['evidence']={'path':str(fp),'sha256':H.sha(fp)}
        def runner(cmd,**kw):
            calls.append(cmd)
            if cmd==binding['argv']:return NS(returncode=0,stdout=json.dumps(fixed if outcome['fixed'] else result0),stderr='')
            assert cmd[:3]==['docker','inspect','p26connectora'] and 'Config.Labels' in cmd[-1]
            labels={'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_a','com.docker.compose.project':'openfoundry-p26-legacy','com.docker.compose.service':'p26connectora'}
            assert all(originalproof['actual_inspect']['labels'][k]==v for k,v in labels.items())
            cur=originalproof['current'];return NS(returncode=0,stdout='\n'.join([cur['cid'],cur['image'],cur['started_at'],'true']+list(labels.values())),stderr='')
        def facade():
            f=A.Facade(runner,release,rp);f.dynamic_pins=copy.deepcopy(actual['dynamic_new_identity_chain']);f.attempt=H.W.Attempt(td,'captured');return f
        bad=facade()
        try:bad.run(old)
        except A.StageAbort as exc:assert 'exact NEW A identity chain' in exc.reason
        else:assert False
        assert bad.records[-1]['rc']==0 and 'routing refusal' in bad.abort_reason
        outcome['fixed']=True;good=facade();assert good.run(old).returncode==0
        assert good.dynamic_pins['p26connectora']['cid']==result0['current']['cid'] and good.deadline_transition_seen
        # Execute the actual copied main/Exception catcher, changing only the
        # scenario functions to fake IO boundary and a detectable next action.
        mod=A.load(A.TREE/A.SUITES['p22']);mod.load_p1=lambda env:setattr(mod,'M',NS(wait_health=lambda:None,expect=lambda ok,msg,*args:None if ok else (_ for _ in ()).throw(AssertionError(msg)),scrub=str));mod.contract_probe=lambda ev:None;later=[];outcome['fixed']=False
        mod.SCENARIOS['J']=(mod.SCENARIOS['J'][0],lambda suffix,ev:facade().run(old));mod.SCENARIOS['K']=(mod.SCENARIOS['K'][0],lambda suffix,ev:later.append('next-business-case'))
        argv=sys.argv
        try:
            sys.argv=['NEW-copied-p22','--env-dir',str(td),'--only','J,K']
            try:mod.main()
            except A.StageAbort:pass
            else:assert False
        finally:sys.argv=argv
        assert not later and not (td/'result_p22.json').exists()
        compose=A.Facade(lambda *args,**kwargs:NS(returncode=23,stdout='original compose failure',stderr='original stderr'),release,rp)
        mod.SCENARIOS['J']=(mod.SCENARIOS['J'][0],lambda suffix,ev:compose.run(['docker','compose','-f','NEW-compose','ps','-q','connector2']))
        try:
            sys.argv=['NEW-copied-p22','--env-dir',str(td),'--only','J,K']
            try:mod.main()
            except A.StageAbort:pass
            else:assert False
        finally:sys.argv=argv
        assert not later and compose.records[-1]['rc']==23 and 'infrastructure original rc=23' in compose.abort_reason
        # Normal child SQL nonzero is returned to the unchanged business
        # assertion; infrastructure exceptions alone abort the stage.
        sqlf=A.Facade(lambda *args,**kw:NS(returncode=42,stdout='',stderr='expected SQL negative'),release,rp)
        cp=sqlf.run(['docker','compose','-f','NEWlocator','exec','-T','postgres','psql','-c','SELECT 1;'])
        assert cp.returncode==42 and not hasattr(sqlf,'abort_reason')
        p1=A.load(A.TREE/A.SUITES['p1']);p1.http=lambda *args,**kwargs:NS(status=409,body={'code':'expected_business_negative'})
        sqlf.owned.before_http=sqlf.require_phase;sqlf.owned.instrument(p1)
        assert p1.http('connector','/expected-business-negative',method='POST').status==409
    assert H.sha(original)==originsha
    print(json.dumps({'ok':True,'actual_HTTP_SQL_Docker_calls':0,'actual_original_proof':str(original),'actual_original_proof_sha256':originsha,'checks':['captured actual previous I boot and bad slash-name result refused by real Facade.run; original child rc0 retained','only corrected logical projection accepts same captured actual CID/image/labels/config and updates dynamic A','real copied P22 main Exception catch cannot catch StageAbort; next business case never executes','compose nonzero originalrc23 escapes real copied main catcher with no later case','normal SQL child rc42 and HTTP409 business negative remain unchanged; no false infrastructure abort','original captured failure proof unchanged']},indent=2))
if __name__=='__main__':main()
