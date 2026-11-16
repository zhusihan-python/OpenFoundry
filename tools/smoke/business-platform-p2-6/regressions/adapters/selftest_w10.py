#!/usr/bin/env python3
"""Pure injectable executable-path tests. ZERO actual HTTP/SQL/Docker calls."""
import importlib.util,json,sys,tempfile
from pathlib import Path
from types import SimpleNamespace as NS
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_suite_adapter as A
import w10_guard_shim as H


def main():
    checks=[]
    def test(name,fn): fn(); checks.append(name)
    with tempfile.TemporaryDirectory(dir=H.P26/'results/w10') as td:
        tmp=Path(td)
        def imports():
            # Original full suite code is actually imported from NEW copies.
            for p in A.SUITES.values(): A.load(A.TREE/p)
            for p in ['business-platform-p2-3/admission_snapshots.py','business-platform-p2-4/p24_live_guard.py']:
                A.load(A.TREE/p)
        test('real NEW closure imports P1/P21/P22/P23/P24/P25/snapshots/pins',imports)
        def nested_helpers():
            import urllib.request
            (tmp/'.env').write_text('OF_P1_PG_PASSWORD=fake\nOF_P1_JWT_SECRET=fake\nOF_P1_SERVICE_JWT=fake\n')
            original=urllib.request.urlopen
            class Reply:
                status=200
                def __enter__(self): return self
                def __exit__(self,*args): pass
            urllib.request.urlopen=lambda *args,**kw: Reply()
            try:
                A.load(A.TREE/A.SUITES['p21']).load_p1(tmp)
                args=NS(env_dir=str(tmp),connector2_base='http://127.0.0.1:17097',connector3_base='http://127.0.0.1:17098',dataset_proxy_base='http://127.0.0.1:17099')
                p22=A.load(A.TREE/A.SUITES['p22']); p22.ARGS=args; p22.load_p1(tmp)
                p23=A.load(A.TREE/A.SUITES['p23']); p23.ARGS=args; p23.load_helpers(tmp); p23.snap_module()
                p24=A.load(A.TREE/A.SUITES['p24']); p24.load_helpers(tmp); p24._load_p23_smoke()
                assert p23.M.BASES['connector2']==args.connector2_base
            finally: urllib.request.urlopen=original
        test('actual nested P21/P22/P23/P24 helper loading via NEW relative tree with fake health',nested_helpers)
        source=tmp/'S.json'; source.write_text(json.dumps({'matches_S':True,'offline_recipe_sha':H.G.candidate_hash(H.G.OPENFOUNDRY)}))
        release=tmp/'release.json'
        rel={'schema':'p26-w10-release-v1','root_accepted':True,'suite':'p23','stage':'p23full8','expires_utc':'2099-01-01T00:00:00Z','candidate_source':A.candidate(source),
            'suite_argv':['--env-dir',td,'--allow-restart'],'expect_guard_calls':1,
            'aux_compose':td+'/new-aux.yaml','aux_project':'p26-owned',
            'service_map':{'connector':'p26C','connector2':'p26A','connector3':'p26B'}}
        recfile=tmp/'deployment.json'; pin={'cid':'a'*64,'image':'sha256:'+'b'*64,'started_at':'2026-11-15T00:00:00Z'}; recfile.write_text(json.dumps({'containers':{'p26C':pin}})); rel.update(deployment_record=str(recfile),deployment_record_sha256=H.sha(recfile))
        release.write_text(json.dumps(rel)); calls=[]; initial=[]
        def runner(cmd,*args,**kw):
            calls.append(cmd)
            if cmd[:2]==['docker','inspect']: return NS(returncode=0,stdout='\n'.join([pin['cid'],pin['image'],pin['started_at'],'true']),stderr='')
            if cmd[:2]==['docker','compose'] and 'restart' in cmd: pin['started_at']='2026-11-15T00:00:01Z'
            return NS(returncode=0,stdout='real fake output',stderr='fake error')
        def fake_guard(path): initial.append(path); return {'ok':True,'injected':True}
        def loader(path):
            actual=A.load(path)
            assert actual.P1_SMOKE.is_relative_to(A.TREE)
            def main():
                assert sys.argv[1:]==rel['suite_argv']
                import subprocess
                cp=subprocess.run(['python3','-I',str(A.TREE/'business-platform-p2-3/p23_live_guard.py'),td,'connector'],capture_output=True,text=True)
                assert cp.returncode==0
                cp=subprocess.run(['docker','compose','-f',td+'/compose.yaml','--project-name','old','restart','connector'])
                assert cp.returncode==0
                return 17
            actual.main=main
            return actual
        opts=NS(stage='p23full8',release_file=str(release),suite='p23',suite_tail=['--']+rel['suite_argv'],candidate_source_file=str(source),expect_guard_calls=1,env_dir=td,record=str(tmp/'result.json'))
        def execution():
            oldargv=sys.argv; oldrun=A.subprocess.run
            assert A.execute(opts,runner,fake_guard,loader)==17
            assert sys.argv is oldargv and A.subprocess.run is oldrun
            rec=json.loads(Path(opts.record).read_text()); assert rec['guard_calls']==1 and rec['commands'][0]['rc']==0
            route=[c for c in calls if c[:2]==['docker','compose']][-1]
            assert route[-1]=='p26C' and route[3]==td+'/new-aux.yaml'
        test('real module/scoped argv/original rc/guard count/owned mutation route/restoration',execution)
        def mismatch():
            opts.record=str(tmp/'wrongphase.json')
            rel['suite']='p22'; release.write_text(json.dumps(rel)); calls.clear(); initial.clear()
            try: A.execute(opts,runner,fake_guard,loader)
            except (ValueError,A.StageAbort): pass
            else: assert False
            assert not calls and not initial
        test('unreleased/wrong phase zero actual runners',mismatch)
        def exact():
            H.exact([{'id':'a','v':1}],[{'id':'a','v':1}],'id')
            for rows in ([{'id':'a','v':2}],[{'id':'a','v':1},{'id':'shadow','v':1}],[{'id':'a','v':1}]*2,[]):
                try: H.exact([{'id':'a','v':1}],rows,'id')
                except (ValueError,A.StageAbort): pass
                else: assert False
        test('whole-set value/shadow/duplicate/missing negatives',exact)
        def routes():
            f=A.Facade(runner,rel,release)
            for cmd in [['docker','compose','-f','x','stop','fixture'],['python3','-I',str(A.TREE/'business-platform-p2-3/p23_live_guard.py'),'d','wrong']]:
                before=len(calls)
                try: f.run(cmd)
                except (ValueError,A.StageAbort): pass
                else: assert False
                assert len(calls)==before
        test('old fixture mutation and guard near-miss denied before runner',routes)
        def expiry_sql():
            rel['suite']='p23'; release.write_text(json.dumps(rel)); f=A.Facade(runner,rel,release)
            f.run(['docker','compose','-f',td+'/compose.yaml','--project-name','q3','exec','-T','postgres','psql','-c','SELECT 1;'])
            assert 'BEGIN READ ONLY;' in calls[-1] and calls[-1][-1]=='COMMIT;'
            rel['expires_utc']='2000-01-01T00:00:00Z'; release.write_text(json.dumps(rel)); before=len(calls)
            try: f.run(['docker','compose','-f','x','restart','connector'])
            except (ValueError,A.StageAbort): pass
            else: assert False
            assert len(calls)==before
        test('Postgres read-only transaction enforced and expired release rejects mutation',expiry_sql)
    print(json.dumps({'ok':True,'pure':True,'actual_http_sql_docker_calls':0,'checks':checks},indent=2))
    return 0
if __name__=='__main__': sys.exit(main())
