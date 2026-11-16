#!/usr/bin/env python3
"""Executable closure/preservation adapters under explicit pure I/O fakes."""
import contextlib,io,json,sys,tempfile,subprocess,uuid
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_p25_runtime as R
import p26_p25_transport as T
from p26_healthy_bundle import build,verify_bundle


def transport_test(root):
    m=R.load('smoke_p25.py');attempt=R.H.W.Attempt(root,'transport');attempt.add_secret('PRIVATE-JWT')
    ctx=NS(gate=lambda:None,W=R.H.W,attempt=attempt,bases={'dataset':'http://127.0.0.1:17078'},observe=lambda *args,**kw:None)
    T.install(m,ctx);records=[];rec=NS(record=lambda *args:records.append(args[-1]));requests=[];mode={'bad':False}
    class Response:
        status=200
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def read(self,*args):return b'{"id":"0001"}\n'
    class Opener:
        def open(self,req,**kw):
            requests.append(req)
            if len(requests)%2:
                from urllib.error import HTTPError
                target=('http://evil.invalid' if mode['bad'] else 'http://127.0.0.1:17078')+'/v1/_internal/local-fs/file?sig=PRIVATE-HMAC'
                raise HTTPError(req.full_url,302,'redirect',{'Location':target},io.BytesIO())
            return Response()
    with patch.object(T.request,'build_opener',return_value=Opener()):
        api=m.HttpApi(ctx.bases,'PRIVATE-JWT',rec,'pure-download')
        result=api.call('dataset','GET','/v1/datasets/d/files/f/download',raw=True)
        assert result['status']==200 and result['raw']==b'{"id":"0001"}\n'
        assert requests[0].has_header('Authorization') and not requests[1].has_header('Authorization')
        mode['bad']=True;requests.clear();result=api.call('dataset','GET','/v1/datasets/d/files/f/download',raw=True)
        assert result['status']==302 and result['error'] and len(requests)==1
    for path in attempt.dir.rglob('*'):
        if path.is_file(): assert b'PRIVATE-HMAC' not in path.read_bytes() and b'PRIVATE-JWT' not in path.read_bytes()


def healthy_bundle_test(root):
    fixture=R.load('selftest_aggregate_healthy_p25.py');args=fixture.fixture(root)
    sources=[{'summary':args['original_summary'],'step':args['original_step'],'original_rc':1}]+[{'summary':r['summary'],'step':r['step'],'case':r['case'],'original_rc':0} for r in args['replacements']]
    # The new contract requires selected h9 to be an independent fresh rc0.
    original=json.loads(Path(args['original_summary']).read_text());row=next(x for x in original['scenarios'] if x['id']=='h9');ev=root/'fresh-h9-evidence';ev.mkdir();hp=ev/'handles.json';hp.write_text('{"handles":[],"count":0}');(ev/'h9-attempt-0001.json').write_text(json.dumps(row));summary=root/'fresh-h9.json';summary.write_text(json.dumps({'phase':'healthy','released':True,'failed':[],'scenarios':[row],'quiet':{'ok':True},'attempts':{'evidence_dir':str(ev),'handles_file':str(hp)},'final_owner_roster':[],'final_run_roster':[]}));step=root/'fresh-h9-step';step.mkdir();(step/'cmd.json').write_text(json.dumps(['python3','-I',str(R.COPY/'smoke_p25.py'),'--phase','healthy','--only','h9','--released','--result-file',str(summary)]));(step/'rc').write_text('0\n');(step/'stdout.log').write_text(json.dumps({'result_file':str(summary),'failed':[]}));(step/'stderr.log').write_text('');sources.append({'summary':str(summary),'step':str(step),'case':'h9','original_rc':0})
    for index,source in enumerate(sources):
        step=Path(source['step']);cmd=step/('cmd.json' if (step/'cmd.json').exists() else 'cmd.txt')
        import shlex
        old=json.loads(cmd.read_text()) if cmd.suffix=='.json' else shlex.split(cmd.read_text().removeprefix('CMD: '))
        tail=old[2:];release=root/('phase'+str(index)+'.json');runtime=root/('runtime'+str(index)+'.json')
        release.write_text(json.dumps({'schema':'p26-w10-release-v1','root_accepted':True,'candidate_source':R.SOURCE,'p25_mode':'engine','p25_argv':tail,'p25_runtime_sha256':R.H.sha(R.__file__)}))
        argv=['python3','-I',str(Path(R.__file__)),'--mode','engine','--release-file',str(release),'--output','PURE','--']+tail
        cmd.write_text(json.dumps(argv) if cmd.suffix=='.json' else shlex.join(argv))
        runtime.write_text(json.dumps({'source_S':R.SOURCE,'mode':'engine','wrapper_exception':None,'original_engine_or_runner_rc':source['original_rc'],'runtime_sha256':R.H.sha(R.__file__),'initial_guard':{'ok':True}}));source['runtime_result']=str(runtime)
    m=R.load('smoke_p25.py');selection={s['id']:0 for s in m.SCENARIOS if s['phase']=='healthy' and s['id']!='h10'}
    for index,source in enumerate(sources[1:],1):selection[source['case']]=index
    q=R.load('collect_http_quiet_p25.py');hps=[json.loads(Path(v['summary']).read_text())['attempts']['handles_file'] for v in sources];proof,proofpath=q.collect(hps,root/'quiet-with-fresh-h9',api_factory=lambda e,rec:fixture.API(rec));qstep=root/'quiet-with-fresh-h9-step';qstep.mkdir();qargv=['python3','-I',str(R.COPY/'collect_http_quiet_p25.py'),'--released']+sum((['--handles',p] for p in hps),[]);(qstep/'cmd.json').write_text(json.dumps(qargv));(qstep/'rc').write_text('0\n');(qstep/'stdout.log').write_text(json.dumps({'ok':True,'proof_file':str(proofpath)}));(qstep/'stderr.log').write_text('');args['final_quiet_proof']=str(proofpath);args['final_quiet_step']=str(qstep)
    manifest={'sources':sources,'selection':selection,'final_source_index':3,'quiet_proof':args['final_quiet_proof'],'quiet_step':args['final_quiet_step'],'h13_ledger_step':args['h13_ledger_step'],'handles_output':str(root/'new-union.json')}
    path=root/'manifest.json';path.write_text(json.dumps(manifest));bundle,handles=build(path)
    Path(manifest['handles_output']).write_bytes(handles);output=root/'new-bundle.json';output.write_text(json.dumps(bundle))
    assert verify_bundle(output)['selected']==12
    # A current failed-case choice cannot be called green by a manifest count.
    saved=selection['h13'];selection['h13']=0;path.write_text(json.dumps(manifest))
    try: build(path)
    except ValueError:pass
    else:assert False
    selection['h13']=saved;path.write_text(json.dumps(manifest))
    # A global wrapper/preservation failure is never a selectable case repair.
    receipt=Path(sources[1]['runtime_result']);original=receipt.read_text();data=json.loads(original);data['wrapper_exception']='preservation shadow';receipt.write_text(json.dumps(data))
    try:build(path)
    except ValueError:pass
    else:assert False
    receipt.write_text(original)
    return output


def main():
    with tempfile.TemporaryDirectory(dir=R.P26/'results/w10') as td:
        root=Path(td);R.check_closure()
        with patch('socket.socket',side_effect=AssertionError('actual socket forbidden')),patch('subprocess.run',side_effect=AssertionError('actual child forbidden')),patch('urllib.request.urlopen',side_effect=AssertionError('actual HTTP forbidden')):
            try:R.main(['--release-file',str(root/'missing.json'),'--mode','engine','--output',str(root/'out'),'--','--phase','healthy'])
            except OSError:pass
            else:assert False
            transport_test(root)
            data=root/'bundle';data.mkdir();healthy_bundle_test(data)
            sqlenv=root/'sql-env';sqlenv.mkdir();(sqlenv/'compose.yaml').write_text('services: {}\n')
            sqlengine=R.load('smoke_p25.py');sqlrec=sqlengine.AttemptRecorder(str(root/'sql-failure'),'sql-failure');did,sid=[str(uuid.uuid4()) for _ in range(2)];sqlrec.persist_handle('def',did);sqlrec.persist_handle('source',sid);sqlengine._SQL_RELEASE_ARMED['armed']=True
            with patch('subprocess.run',side_effect=OSError('pure spawn failure')):unavailable=sqlengine.PsqlFactReader(str(sqlenv),sqlrec).snapshot('spawn',did,sid)
            assert unavailable['ok'] is False and unavailable['rc'] is None
            sqlrow=json.loads(next(Path(sqlrec.dir).glob('sql-facts-attempt-*.json')).read_text());assert sqlrow['rc'] is None and sqlrow['original_rc'] is None
            # Actual accepted S-PG files are consumed pure; PG is never rerun.
            engine=R.load('smoke_p25.py');rec=engine.AttemptRecorder(str(root/'h12'),'h12');instance=engine.P25Engine(None,{},rec);result=engine.ScenarioResult('h12','healthy')
            oldproof=str(R.P26/'results/w6/pg/h12-rollback-compat-proof/proof.json');instance.h12(None,result,oldproof);assert not result.failed
            try:R.check_pg_proof({'candidate_source':R.SOURCE,'p25_inputs':{oldproof:R.H.sha(oldproof)}},['--pg-proof-file',oldproof])
            except ValueError:pass
            else:assert False,'oldS h12 proof accepted as current S2'
            fresh=str(R.P26/('results/w6/S2/'+R.SOURCE+'/h12-rollback-compat-proof/proof.json'))
            R.check_pg_proof({'candidate_source':R.SOURCE,'p25_inputs':{fresh:R.H.sha(fresh)}},['--pg-proof-file',fresh])
            freshrec=engine.AttemptRecorder(str(root/'h12-S2'),'h12-S2');freshresult=engine.ScenarioResult('h12','healthy');engine.P25Engine(None,{},freshrec).h12(None,freshresult,fresh);assert not freshresult.failed
    print(json.dumps({'ok':True,'actual_http_sql_docker_calls':0,'checks':['NEW patched copies load with exact origin/copy/patch SHA ledger','h13 real copied SQLreader spawn failure records unavailable/null original child rc, never fabricated -1','missing Root release zero actual IO','download redirect sends no JWT and persists no HMAC; wrong origin refused','actual-source healthy12 bundle recomputation + failed-case/global-shadow negatives','historical oldS h12 pure-replay preserved but S2 runtime refuses it; fresh actual S2 normal/race/unified rollback proof passes Root source pin and unchanged copied h12 reader']},indent=2))
if __name__=='__main__': main()
