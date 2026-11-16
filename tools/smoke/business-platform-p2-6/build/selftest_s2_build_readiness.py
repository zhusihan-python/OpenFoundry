#!/usr/bin/env python3
"""Pure future-source/runtime binding tests; never calls Docker/build/PG."""
import copy,hashlib,importlib.util,json,sys
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[1];sys.path.insert(0,str(P26))
from build import p26_build_executor as B
s=importlib.util.spec_from_file_location('s2_plan',P26/'deployment/prepare_s2_validation_plan.py');P=importlib.util.module_from_spec(s);s.loader.exec_module(P)


def run(out):
    out.mkdir(parents=True,exist_ok=False);results=[]
    pin={'cid':'a'*64,'image':'sha256:'+'b'*64,'started_at':'2026-11-15T06:45:25.373428845Z'}
    f=out/'synthetic-running-snapshot.json';f.write_text(json.dumps({'containers':{B.Q3_PROJECT+'-dataset-1':pin}}))
    release={'build_running_snapshot':{'path':str(f),'sha256':B.G.sha256_file(f)}}
    assert B.root_running_snapshot(release,'dataset')==pin
    assert B.root_running_snapshot(None,'dataset') is None
    results += [{'case':'Root-current-candidate-snapshot-accepted','pass':True},{'case':'legacy-no-snapshot-mode-preserved','pass':True}]
    raw='\n'.join([pin['cid'],pin['image'],pin['image'],pin['started_at'],'true'])+'\n'
    B.check_running_snapshot(raw,pin)
    for name,index,value in [('wrongCID',0,'c'*64),('wrongimage',1,'sha256:'+'c'*64),('tag-not-fullConfigImage',2,'some-tag'),('wrongboot',3,'2026-11-15T09:00:00Z'),('stoppedruntime',4,'false')]:
        lines=raw.splitlines();lines[index]=value
        try:B.check_running_snapshot('\n'.join(lines),pin)
        except B.Abort:pass
        else:raise AssertionError(name+' accepted')
        results.append({'case':name+'-reject','pass':True})
    invalid=copy.deepcopy(release);invalid['build_running_snapshot']['sha256']='0'*64
    try:B.root_running_snapshot(invalid,'dataset')
    except B.Abort:pass
    else:raise AssertionError('bad snapshotSHA accepted')
    results.append({'case':'wrong-Root-snapshotSHA-reject','pass':True})
    source='2'*64
    plan=P.plan(source,'/private/tmp/Root-future-S2-release.json',str(f),B.G.sha256_file(f))
    assert plan['counts']=={'newPG':27,'newPG_normal_race_runs':54,'h12_extra_normal_race':2,'unifiedPG':114,'unit_services':3,'candidate_builds':2,'commands_total':14}
    assert len(plan['steps'])==14 and all(x['argv'][0:3]==['python3','-I','-B'] for x in plan['steps'])
    for step in plan['steps']:
        assert step['argv'][step['argv'].index('--expect-source-sha')+1]==source
        assert '/results/w6/S2/'+source+'/' in step['outer_root']
        assert '--release-file' in step['argv'] and '--execute' in step['argv']
    for svc in ['dataset','connector']:
        candidate=B.plan_steps(svc,source)
        assert candidate['create'][-1]=='sha256:'+B.OLD_FULL_IDS[svc]
        assert source[:12] in candidate['commit'][-1]
        assert '--network' in candidate['compile'] and candidate['compile'][candidate['compile'].index('--network')+1]=='none'
    assert '.Config .\\"Cmd' not in (P26/'build/p26_build_executor.py').read_text()
    results += [{'case':'14-future-source-explicit-commands-safeEVdirs','pass':True},{'case':'oldbaseIDs-stillfixed-newunique-source-tags','pass':True},{'case':'offline-compiler-network-none','pass':True},{'case':'nullsafe-Cmd-template-typo-removed','pass':True}]
    (out/'RESULT.json').write_text(json.dumps({'pure_only':True,'passed':len(results),'cases':results,'actual_test_build_service_calls':0,'synthetic_source_NOT_actual_S2':source,'build_executor_sha256':B.G.sha256_file(P26/'build/p26_build_executor.py'),'plan_generator_sha256':B.G.sha256_file(P26/'deployment/prepare_s2_validation_plan.py')},indent=2)+'\n');print(out);print(str(len(results))+' S2 readiness cases passed; actual test/build/service calls=0')


if __name__=='__main__':run(Path(sys.argv[1]).resolve())
