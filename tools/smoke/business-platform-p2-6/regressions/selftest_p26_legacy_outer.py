#!/usr/bin/env python3
"""Real seven-stage launcher with fake children/secret reader; zero live IO."""
import importlib.util,json,subprocess,sys,tempfile
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('outer',Path(__file__).with_name('p26_legacy_outer.py'));O=importlib.util.module_from_spec(spec);spec.loader.exec_module(O)

def main():
    rows=[]
    with tempfile.TemporaryDirectory(dir=O.P26/'results/w10') as td:
        root=Path(td);candidate=root/'source.json';candidate.write_text('{"pure":true}')
        cases=[('p1','p1pre9'),('p22','p22-full'),('p23','p23-full'),('p24','p24-full'),('p25','p25-healthy'),('p1','p1post9'),('p21','p21-ABC-final')]
        for suite,stage in cases:
            base=root/stage;base.mkdir();env=base/'env';env.mkdir();out=base/'outer';rp=base/'release.json'
            r={'schema':'p26-w10-release-v1','root_accepted':True,'expires_utc':'2035-01-01T00:00:00Z','release_id':'pure-'+stage,'suite':suite,'stage':stage,'candidate_source':'f'*64,'legacy_candidate_source_file_sha256':O.sha(candidate),'legacy_outer_output':str(out),'legacy_env_dir':str(env),'evidence_root':str(base/'adapter'),'expect_guard_calls':0,'suite_argv':['--env-dir',str(env)],'legacy_expected_names':['real_named_case'],'p1_outer_output':str(base/'p1'),'p25_output':str(base/'p25'),'p25_mode':'engine','p25_argv':['--phase','healthy','--only','h12'],'p25_identity':{'actor':'primary','tenant':'org'},'p25_org2_identity':{'actor':'foreign','tenant':'foreign-org'}}
            r['legacy_active_tools_sha256']={p:O.sha(O.P26/p) for p in O.active_paths(r)};r['legacy_child_argv']=O.child_argv(r,rp,candidate);rp.write_text(json.dumps(r));calls=[]
            def child(cmd,**kw):
                calls.append(cmd)
                if '--repo' in cmd:assert cmd[-1]=='f'*64;return subprocess.CompletedProcess(cmd,0,b'actual fake source',b'')
                assert cmd==r['legacy_child_argv']
                stdout=b'original stdout\n';stderr=b'FAKE_SECRET must scrub\n'
                if suite=='p1':
                    p=Path(r['p1_outer_output']);p.mkdir();(p/'RESULT.json').write_text('{"ok":true}')
                elif suite=='p25':
                    assert kw['env']['OPENFOUNDRY_SMOKE_ARM_JWT']==kw['env']['OPENFOUNDRY_SMOKE_JWT'] and kw['env']['P25_CONFIRM']=='yes'
                    p=Path(r['p25_output'])/'runtime-pure';p.mkdir(parents=True);(p/'RESULT.json').write_text(json.dumps({'wrapper_exception':None,'original_engine_or_runner_rc':0}));stdout=b'{"result_file":"pure","failed":[]}'
                else:
                    p=env/('result_'+suite+('-new' if suite=='p24' else '')+'.json');p.write_text(json.dumps({'results':{'real_named_case':{'status':'pass'}},'counts':{'pass':1,'fail':0},'official':True}));ad=Path(r['evidence_root']);ad.mkdir();(ad/'adapter.json').write_text(json.dumps({'cleanup_ok':True,'pending_sql_cleanup':[],'pending_holders':0,'count_ok':True,'abort_reason':None}))
                    if suite=='p24':stdout=('RESULT_FILE='+str(p)+'\n').encode()
                return subprocess.CompletedProcess(cmd,0,stdout,stderr)
            args=['--release-file',str(rp),'--candidate-source-file',str(candidate),'--output',str(out)]
            with patch.object(O.subprocess,'run',side_effect=child),patch.object(O.H.W,'read_jwt_secret',return_value='FAKE_SECRET'):
                assert O.main(args)==0
            result=json.loads((out/'RESULT.json').read_text());assert result['ok'] and result['child_original_rc']==0 and len(calls)==3 and (out/'child/rc').read_text()=='0\n'
            assert b'FAKE_SECRET' not in (out/'child/stderr.log').read_bytes();rows.append({'stage':stage,'pass':True})
        # Missing Root phase and bad exact child command cannot spawn anything.
        for label,mutate in [('unaccepted',lambda x:x.update(root_accepted=False)),('argv-extra',lambda x:x['legacy_child_argv'].append('--escape')),('tool-drift',lambda x:x['legacy_active_tools_sha256'].update({'regressions/p26_legacy_outer.py':'0'*64}))]:
            bad=dict(r);badout=root/('bad-'+label);bad['legacy_outer_output']=str(badout);bad['legacy_child_argv']=list(r['legacy_child_argv']);bad['legacy_active_tools_sha256']=dict(r['legacy_active_tools_sha256']);mutate(bad);rp.write_text(json.dumps(bad));badargs=['--release-file',str(rp),'--candidate-source-file',str(candidate),'--output',str(badout)]
            with patch.object(O.subprocess,'run',side_effect=AssertionError('actual child forbidden')):
                try:O.main(badargs)
                except ValueError:pass
                else:raise AssertionError(label+' accepted')
            rows.append({'case':label,'pass':True,'child_calls':0})
        for failure in ('child23','spawn'):
            bad=dict(r);badout=root/('bad-'+failure);bad['legacy_outer_output']=str(badout);rp.write_text(json.dumps(bad));calls=[]
            def failed(cmd,**kw):
                calls.append(cmd)
                if '--repo' in cmd:return subprocess.CompletedProcess(cmd,0,b'fake source',b'')
                if failure=='spawn':raise OSError('FAKE_SECRET')
                return subprocess.CompletedProcess(cmd,23,b'original failure',b'FAKE_SECRET')
            with patch.object(O.subprocess,'run',side_effect=failed),patch.object(O.H.W,'read_jwt_secret',return_value='FAKE_SECRET'):
                assert O.main(['--release-file',str(rp),'--candidate-source-file',str(candidate),'--output',str(badout)])==(23 if failure=='child23' else 2)
            receipt=json.loads((badout/'RESULT.json').read_text());assert not receipt['ok'] and receipt['child_original_rc']==(23 if failure=='child23' else None) and receipt['source_after_original_rc']==0 and len(calls)==3
            assert (badout/'child/rc').read_text()==('23\n' if failure=='child23' else 'unavailable\n')
            assert 'FAKE_SECRET' not in (badout/'RESULT.json').read_text();rows.append({'case':failure,'pass':True})
    print(json.dumps({'pure_only':True,'cases':rows,'actual_child_HTTP_SQL_Docker':0}))
if __name__=='__main__':main()
