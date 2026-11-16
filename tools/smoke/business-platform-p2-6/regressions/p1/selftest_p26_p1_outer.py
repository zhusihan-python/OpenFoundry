#!/usr/bin/env python3
"""Real outer argv/config/archive/rc routing with injected children only."""
import importlib.util,json,os,sys,tempfile,uuid,subprocess
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('outer',Path(__file__).with_name('p26_p1_outer.py'));O=importlib.util.module_from_spec(spec);spec.loader.exec_module(O)

def main():
    source='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e';checks=[]
    with tempfile.TemporaryDirectory(dir=O.P26/'results/w10') as td:
        td=Path(td);candidate=td/'S2.json';candidate.write_text(json.dumps({'matches_S':True,'offline_recipe_sha':source}));maps={p:O.sha(O.P26/p) for p in O.active_paths()}
        for stage in ('p1pre9','p1post9'):
            env=Path('/private/tmp')/('openfoundry-p26-PURE'+uuid.uuid4().hex+'-'+stage+'-env');env.mkdir(mode=0o700)
            try:
                (env/'.env').write_text('OF_P1_JWT_SECRET=pure-fake\n');(env/'compose.yaml').write_text('services: {}\n')
                for p in env.iterdir():p.chmod(0o600)
                out=td/stage;release=td/(stage+'.json');r={'schema':'p26-w10-release-v1','root_accepted':True,'expires_utc':'2035-01-01T00:00:00Z','stage':stage,'suite':'p1','candidate_source':source,'p1_env_dir':str(env),'p1_tools_sha256':maps,'p1_private_env_sha256':O.sha(env/'.env'),'p1_env_locator_sha256':O.sha(env/'compose.yaml'),'p1_outer_output':str(out),'expect_guard_calls':0,'suite_argv':['--env-dir',str(env)],'p1_candidate_source_file_sha256':O.sha(candidate),'evidence_root':str(td/(stage+'-commands'))};release.write_text(json.dumps(r));calls=[]
                def child(cmd,**kw):
                    calls.append(cmd)
                    if '--repo' in cmd:assert cmd[cmd.index('--expect')+1]==source
                    else:
                        assert cmd[cmd.index('--stage')+1]==stage and cmd[cmd.index('--env-dir')+1]==str(env)
                        target=Path(cmd[cmd.index('--record')+1]);target.parent.mkdir(parents=True);target.write_text(json.dumps({'cleanup_ok':True,'pending_sql_cleanup':[],'pending_holders':0,'count_ok':True,'guard_calls':0}));(env/'result.json').write_text(json.dumps({'results':{n:{'status':'pass'} for n in O.NAMES},'counts':{'pass':9,'fail':0,'uncovered':0},'uncovered':[]}));(env/'last-identities.json').write_text('{"pure":true}')
                    return subprocess.CompletedProcess(cmd,0,b'pure original child stdout',b'')
                argv=sys.argv
                try:
                    sys.argv=['outer','--release-file',str(release),'--candidate-source-file',str(candidate),'--output',str(out)]
                    with patch.object(O.subprocess,'run',side_effect=child):assert O.main()==0
                    result=json.loads((out/'RESULT.json').read_text());assert result['candidate_source']==source and result['p1_env_dir']==str(env) and result['stage']==stage and len(calls)==3 and (out/'p1-report.json').exists()
                    calls.clear();r['p1_env_dir']='/private/tmp/openfoundry-p26-p1pre9-env';release.write_text(json.dumps(r))
                    with patch.object(O.subprocess,'run',side_effect=child):
                        try:O.main()
                        except ValueError:pass
                        else:assert False
                    assert not calls
                finally:sys.argv=argv
                checks.append(stage+' dynamic S2/env argv + ninecase archive + oldS env rejection')
            finally:
                for p in env.iterdir():p.unlink()
                env.rmdir()
    print(json.dumps({'ok':True,'actual_HTTP_SQL_Docker_child_calls':0,'checks':checks,'active_P1_tool_count':len(maps)},indent=2))
if __name__=='__main__':main()
