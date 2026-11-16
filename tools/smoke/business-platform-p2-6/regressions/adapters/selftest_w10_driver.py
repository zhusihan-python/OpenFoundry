#!/usr/bin/env python3
import json,sys,tempfile
from pathlib import Path
from types import SimpleNamespace as NS
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_suite_adapter as A
H=A.GUARD

def main():
    with tempfile.TemporaryDirectory(dir=H.P26/'results/w10') as td:
        t=Path(td); rec=t/'record.json'; release=t/'release.json'; proofpath=t/'proof.json'
        pin={'cid':'a'*64,'image':'sha256:'+'b'*64,'started_at':'2026-11-15T00:00:00Z'}
        current={'name':'p26connectora',**pin,'cid':'c'*64,'started_at':'2026-11-15T00:00:01Z'}
        rec.write_text(json.dumps({'containers':{'p26connectora':pin}}))
        keys=('CONNECTOR_SYNC_ASYNC_WORKERS','CONNECTOR_SYNC_ASYNC_QUEUE','CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS','CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS')
        cfg=dict(zip(keys,(1,64,7,300))); labels={'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_a','com.docker.compose.project':'openfoundry-p26-legacy','com.docker.compose.service':'p26connectora'}
        proof={'release_id':'pure-test','action':'up-deadline:7','previous':{'name':'p26connectora',**pin},'current':current,'current_config':cfg,'safe_labels':labels}
        proofpath.write_text(json.dumps(proof)); result={k:proof[k] for k in ('release_id','action','previous','current','current_config')}; result.update(schema='p26-w10-driver-result-v1',evidence={'path':str(proofpath),'sha256':H.sha(proofpath)})
        binding={'tool':str(Path(__file__)),'tool_sha256':H.sha(__file__),'argv':['fake-NEW-driver']}
        rel={'schema':'p26-w10-release-v1','root_accepted':True,'suite':'p22','stage':'p22full11','expires_utc':'2099-01-01T00:00:00Z','release_id':'pure-test','service_map':{'connector2':'p26connectora'},'deployment_record':str(rec),'deployment_record_sha256':H.sha(rec),'aux_project':'openfoundry-p26-legacy','driver_commands':{'up-deadline:7':binding}}
        release.write_text(json.dumps(rel)); calls=[]
        def run(cmd,**kw):
            calls.append(cmd)
            if cmd==binding['argv']: out=json.dumps(result)
            elif 'Config.Labels' in cmd[-1]: out='\n'.join([current['cid'],current['image'],current['started_at'],'true']+list(labels.values()))
            elif 'Config.Env' in cmd[-1]: out='\n'.join(k+'='+str(v) for k,v in cfg.items())
            else: out=current['started_at']
            return NS(returncode=0,stdout=out,stderr='')
        f=A.Facade(run,rel,release)
        old=['sh',str(A.TREE/'business-platform-p2-2/p22_connector2.sh'),'up-deadline','7']
        assert f.run(old).returncode==0 and f.dynamic_pins['p26connectora']['cid']==current['cid']
        assert f.run(['docker','inspect','--format','{{.State.StartedAt}}',current['cid']]).returncode==0
        cp=f.run(['docker','compose','-f','old-compose','config'])
        assert cp.returncode==0 and 'connector2:' in cp.stdout and 'TASK_TTL_SECS: "7"' in cp.stdout
        before=len(calls)
        try: f.run(['docker','inspect','--format','{{json .Config.Env}}',current['cid']])
        except (ValueError,A.StageAbort): pass
        else: assert False
        assert len(calls)==before
        # Copied main catches Exception per case. Infrastructure refusal must
        # escape that catcher so no subsequent case can write after a bad J.
        bad= A.Facade(run,rel,release); result['current']={**current,'name':'/p26connectora'}
        continued=[]
        try:
            try: bad.run(old)
            except Exception: continued.append('caught-as-business-failure')
            continued.append('later-case-would-write')
        except A.StageAbort as exc: assert exc.code==2
        else: assert False
        assert not continued and bad.records[-1]['rc']==0
        result['current']=current
        failed=A.Facade(lambda cmd,**kw:NS(returncode=19,stdout='original failure',stderr='original stderr'),rel,release)
        try:failed.run(old)
        except A.StageAbort:pass
        else:assert False
        assert failed.records[-1]['rc']==19
        rel['expires_utc']='2000-01-01T00:00:00Z';release.write_text(json.dumps(rel));before=len(calls)
        try:A.Facade(run,rel,release).run(old)
        except A.StageAbort:pass
        else:assert False
        assert len(calls)==before
        print(json.dumps({'ok':True,'actual_http_sql_docker_calls':0,'checks':['Root-pinned NEW driver argv and actual identity chain','new CID only admitted after proof+actual label/CID/image/boot verification','raw four-key config readback projected to legacy logical connector2','Config.Env dump denied before runner','slash-name malformed driver proof aborts stage outside original Exception catcher; driver original rc0 retained','actual nonzero driver rc19 retained and stage aborted','expired P22 phase aborts before runner']},indent=2))
if __name__=='__main__': main()
