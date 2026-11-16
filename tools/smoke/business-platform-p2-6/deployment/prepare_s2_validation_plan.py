#!/usr/bin/env python3
"""Emit only: fresh-source full validation/build argv. Never runs any step."""
import argparse,hashlib,json,re
from pathlib import Path
P26=Path(__file__).resolve().parents[1]


def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def names(p):return [s.strip() for s in p.read_text().splitlines() if s.strip() and not s.startswith('#')]


def plan(source,root_release,runtime_snapshot,runtime_sha):
    assert re.fullmatch('[a-f0-9]{64}',source)
    assert Path(runtime_snapshot).is_absolute() and re.fullmatch('[a-f0-9]{64}',runtime_sha)
    root=P26/'results/w6/S2'/source
    pg=P26/'pg/p26_pg_runner.py';build=P26/'build/p26_build_executor.py'
    h12=P26/'pg/compat/p25-h12-rollback-names.txt'
    normal={s:P26/('pg/p26-'+s+'-pg-names.txt') for s in ['dataset','connector']}
    unified={s:P26/('pg/unified/p26-unified-'+s+'-names.txt') for s in ['dataset','pipeline','connector']}
    assert sum(len(names(p)) for p in normal.values())==27
    assert sum(len(names(p)) for p in unified.values())==114
    assert names(h12)==['TestPGCheckpointAdvanceAllOrNothingRollback']
    steps=[]
    def pgstep(label,service,mode,suite,roster,phase):
        argv=['python3','-I','-B',str(pg),'run-tests','--execute','--service',service,'--suite',suite,'--test-mode',mode,'--names',str(roster),'--expect-source-sha',source,'--release-file',root_release]
        steps.append({'step':label,'required_phase':phase,'argv':argv,'environment_names_values_safe':{'P26_PG_EV_DIR':str(root/'pg')},'outer_root':str(root/'outer'/label),'required_names':len(names(roster)),'names_path':str(roster),'names_sha256':sha(roster),'require':'originalrc0/version/sourcebeforeafter==RootS2/names exact1RUN1PASS0SKIPFAIL/wrapper0; firstfailurestop; no later stage automatically'})
    for mode in ['normal','race']:
        for service in ['dataset','connector']:
            pgstep('new27-'+service+'-'+mode,service,mode,'new',normal[service],'test-'+service+'-'+mode)
    for mode in ['normal','race']:
        pgstep('h12-rollback-'+mode,'connector',mode,'new',h12,'test-connector-'+mode)
    for service in ['dataset','pipeline','connector']:
        pgstep('unified114-'+service,service,'normal','unified',unified[service],'test-unified-'+service+'-normal')
    for service in ['dataset','pipeline','connector']:
        steps.append({'step':'unit-'+service,'required_phase':'unit-'+service,'argv':['python3','-I','-B',str(pg),'run-unit','--execute','--service',service,'--expect-source-sha',source,'--release-file',root_release],'environment_names_values_safe':{'P26_PG_EV_DIR':str(root/'unit')},'outer_root':str(root/'outer'/('unit-'+service)),'require':'originalrc0/version/sourcebeforeafter==RootS2/wrapper0; fullservicemodule UNIT, offline/noPG'})
    for service in ['dataset','connector']:
        steps.append({'step':'build-'+service,'required_phase':'build-'+service,'argv':['python3','-I','-B',str(build),'run','--execute','--service',service,'--expect-source-sha',source,'--release-file',root_release],'environment_names_values_safe':{'P26_BUILD_EV_DIR':str(root/'build')},'outer_root':str(root/'outer'/('build-'+service)),'require':'alloriginalrc0, cachedGo/fullbaseID/sourceS2/binarySHA, newuniqueTag+fullImage/safeconfig, exactnewstoppedbuildCIDcleanup; currentq3 oldS mainCID/boot/image + oldtags unchanged pre/post'})
    return {'code_only':True,'steps_executed':0,'expected_source_S2':source,'new_evidence_root':str(root),'tool_shas':{str(x.relative_to(P26)):sha(x) for x in [pg,build,P26/'common/p26_gates.py']},'Root_release_addition':{'frozen_source_sha256':source,'build_running_snapshot':{'path':runtime_snapshot,'sha256':runtime_sha},'exact_phase_each_step':'Root grants only the intended concrete argv; test-connector phase also serves h12, names SHA+argv must be separately pinned'},'steps':steps,'counts':{'newPG':27,'newPG_normal_race_runs':54,'h12_extra_normal_race':2,'unifiedPG':114,'unit_services':3,'candidate_builds':2,'commands_total':len(steps)},'preservation':'All original S greens/failures/tags/images/evidence untouched. No q3SQL/service/reconfigure/up in this plan. No oldS resume shortcut. Dockeraccess through normal require_escalated workflow only. DSNs solely runner600temp/scrubbed, no values in outer env/cmd.','next_after_Root_acceptance':['twoS2candidate exact fullIDs + binarySHA + buildsource bindings','Roottyped-preservation currentbaseline (not old-only9/8 due legitimateN/held)','RootoneService mainDataset image-only switch+bothactualfullpins/health/mount/PGfixtureunchanged+typedguard','RootoneService mainConnector image-only switch+loopsOff+typedguard','RootexactownedA/B/C S2 reconfigure only after zeroauthorizedactive/typedheld preservation; never unknown adopt/cleanup','NEW HTTP10/P1pre9+legacy regressions perRoot phases; no old success relabeled toS2'],'blocked_before_actual':['Root freezes S2 after onlyLegacySolGo fix/tests','Root reviews runner/build/toolSHAs + exactnames/argv/release scopes','Root pins actualoldS-running main snapshot (CID/fullimage/configImagefullID/boot), no name auto-adopt','Current release does NOT authorize these tests/build/up']}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source-sha',required=True);p.add_argument('--release-file',required=True);p.add_argument('--runtime-snapshot',required=True);p.add_argument('--runtime-sha',required=True);p.add_argument('--output',required=True);a=p.parse_args();out=Path(a.output).resolve();assert out.is_relative_to((P26/'results/w6').resolve());out.parent.mkdir(parents=True,exist_ok=True)
    with out.open('x') as stream:json.dump(plan(a.source_sha,a.release_file,a.runtime_snapshot,a.runtime_sha),stream,indent=2);stream.write('\n')
    print(out)
