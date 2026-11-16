#!/usr/bin/env python3
"""Run the NEW h10 facade over the actual copied runner and original engine fakes."""
import json,os,sys,tempfile
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_p25_runtime as R
from selftest_p26_p25_closure import healthy_bundle_test

def main():
    with tempfile.TemporaryDirectory(dir=R.P26/'results/w10') as td,patch('socket.socket',side_effect=AssertionError('live forbidden')),patch('subprocess.run',side_effect=AssertionError('live child forbidden')),patch('urllib.request.urlopen',side_effect=AssertionError('HTTP forbidden')),patch.dict(os.environ,P25_CONFIRM='yes',OPENFOUNDRY_SMOKE_JWT='eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMTExMTExMS0xMTExLTQxMTEtODExMS0xMTExMTExMTExMTEiLCJvcmdfaWQiOiIyMjIyMjIyMi0yMjIyLTQyMjItODIyMi0yMjIyMjIyMjIyMjIifQ.PURE'):
        root=Path(td);bundleRoot=root/'bundle';bundleRoot.mkdir();bundle=healthy_bundle_test(bundleRoot)
        fixture=R.load('selftest_h10_crash_runner_p25.py');m=fixture.m;r=fixture.r;fio=fixture.FakeIO(root)
        document=json.loads(bundle.read_text());handles=json.loads(Path(document['attempts']['handles_file']).read_text())['handles']
        for h in handles:
            if h.get('kind')=='source' and h.get('ok'):fio.connector.create_source(h['ref'],h['config'])
        source=document['final_owner_roster'][0]['source_id']
        for h in handles:
            if h.get('kind')=='def' and h.get('ok'):
                target=fio.proxy.create_dataset('existing')['id'];fio.connector.create_def(h['ref'],source,target)
        for owner in document['final_owner_roster']:
            rid,did=owner['run_id'],owner['def_id'];fio.connector.runs[rid]={'id':rid,'sync_def_id':did,'status':'failed','generation':1,'attempt':1,'control_mode':'controlled','publish_entered_at':'2026-11-14T00:00:01Z','started_at':'2026-11-14T00:00:00Z'};fio.connector.defs[did]['owner']=rid
        quiet=next(d for d,v in fio.connector.defs.items() if not v['owner']);fio.connector.trigger(quiet)
        output=root/'runtime';output.mkdir();selected=root/'selected.json';selected.write_text(json.dumps({'schema':'p25-selected-build-v1','ok':True,'all_ok':True,'source_sha256':R.SOURCE,'images':{'p26connectorc':fixture.IMAGE}}));env=root/'env';env.mkdir();(env/'compose.yaml').write_text('# fake')
        deployment=root/'deployment.json';deployment.write_text('{}');typed=root/'typed.json';typed.write_text('{}')
        release=root/'release.json';rel={'schema':'p26-w10-release-v1','root_accepted':True,'suite':'p25','stage':'p25-h10','expires_utc':'2099-01-01T00:00:00Z','release_id':'pure','candidate_source':R.SOURCE,'p25_runtime_sha256':R.H.sha(R.__file__),'p25_closure_ledger_sha256':R.H.sha(R.COPY.parent/'p25-closure-ledger.json'),'p25_tool_shas':{n:R.H.sha(R.HERE/n) for n in ['p26_p25_transport.py','p26_p25_typed_guard.py','p26_p25_export.py','p26_p25_supplements.py','p26_healthy_bundle.py','p26_h13_ledger_read.py']},'deployment_record':str(deployment),'deployment_record_sha256':R.H.sha(deployment),'typed_roster':str(typed),'typed_roster_sha256':R.H.sha(typed),'p25_inputs':{str(selected):R.H.sha(selected)}};release.write_text(json.dumps(rel))
        ctx=object.__new__(R.Context);ctx.args=None;ctx.W=R.H.W;ctx.output=output;ctx.attempt=R.H.W.Attempt(output,'fake-runtime');ctx.release=rel;ctx.release_path=release;ctx.mode='h10';ctx.created={};ctx.counts={'kill':0,'start':0};ctx.runner=None;ctx.pin={'cid':fixture.CID,'image':fixture.IMAGE,'started_at':fio.started};ctx.target='p26connectorc';ctx.bases={'connector':'http://127.0.0.1:17100','dataset':'http://127.0.0.1:17078','fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'}
        class API:
            def __init__(self,bases,jwt,rec,scenario):self.inner=m.FakeApi(fio.connector,fio.fixture,fio.proxy,rec,scenario);self.jwt=jwt
            def call(self,base,method,path,body=None,**kwargs):
                result=self.inner.call(base,method,path,body,**kwargs);ctx.observe(base,method,path,body,result,'fake-original-http-receipt',token=self.jwt);return result
        m.HttpApi=API;ctx.engine=lambda:m
        child=[];guards=[];ctx.guard_held=lambda stage,export=False:guards.append((stage,export)) or {'ok':True}
        def run(argv,**kwargs):
            if '--mode' in argv and argv[argv.index('--mode')+1]=='engine-child':
                child.append(argv);context=Path(argv[argv.index('--child-context')+1]);assert R.H.sha(context)==argv[argv.index('--child-context-sha256')+1]
                tail=argv[argv.index('--')+1:];return fio.command([sys.executable,'-I',str(R.COPY/'smoke_p25.py')]+tail,kwargs['timeout'])
            return fio.command(argv,kwargs['timeout'])
        ctx.real_run=run
        oldload=R.load
        with patch.object(R,'load',side_effect=lambda name:r if name=='h10_crash_runner_p25.py' else oldload(name)),patch.object(r.LiveIO,'now',side_effect=fio.now),patch.object(r.LiveIO,'timestamp',side_effect=fio.timestamp),patch.object(r.LiveIO,'sleep',side_effect=fio.sleep):
            args=['--env-dir',str(env),'--selected-build',str(selected),'--healthy-summary',str(bundle),'--evidence-root',str(output/'h10'),'--connector-base',ctx.bases['connector'],'--dataset-base',ctx.bases['dataset'],'--fixture-base',ctx.bases['fixture'],'--proxy-base',ctx.bases['proxy_admin'],'--fixture-src-url','http://p26p25fixture:8090','--released']
            code=ctx.invoke_h10(args)
        assert code==0,ctx.runner.report
        assert ctx.runner.engine.cfg['fixture_src_url']==R.FIXTURE_SOURCE_URL and fio.connector.sources[ctx.runner.held['source_id']]['config']['base_url']==R.FIXTURE_SOURCE_URL
        assert all(argv[argv.index('--fixture-src-url')+1]==R.FIXTURE_SOURCE_URL for argv in child)
        assert ctx.counts=={'kill':1,'start':1} and len(child)==2 and guards==[('immediately-before-kill',False),('after-post',True)]
        assert ctx.created[ctx.runner.held['run_id']]['kind']=='run' and ctx.runner.report['ok']
        assert any(c[:4]==['docker','kill','-s','KILL'] and c[-1]==fixture.CID for c in fio.calls)
        bad=list(args);bad[bad.index('--fixture-src-url')+1]='http://p25fixture:8090'
        try:ctx.invoke_h10(bad)
        except ValueError:pass
        else:raise AssertionError('old fixture host accepted')
    print(json.dumps({'ok':True,'actual_http_sql_docker_calls':0,'checks':['NEW facade runs actual copied h10 runner and engine pre/post with fake IO','exact Root CID kill once + same CID start once + real pre/post child argv/context SHA routing','actual create/admission observation binds held tuple','preservation guard hooks execute before kill and after post; unknown is never cleared']},indent=2))
if __name__=='__main__':main()
