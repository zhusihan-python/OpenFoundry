"""Narrow review regression controls; no package assembly or sandbox execution."""
import copy
import hashlib
import importlib.util
import json
import tempfile
from pathlib import Path


def module(name):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def run(workspace):
    b=Path(__file__).parent;core=module('p26_formal_core');original=module('p26_original_proofs');assembler=module('p26_package_assembler');offline=module('p26_offline_runner');roster=json.loads((b/'p26_formal_roster.json').read_text());source=roster['source']['go_sql_sha256'];factory=lambda:original.Reader(workspace);pgid='pg-new-connector-normal';pg=dict(id=pgid,**roster['items'][0]['contracts'][pgid]);p1=json.loads((b/'p26_P1pre9_contract.draft.json').read_text())['proof'];p1step={'id':'p1','classification':'actual','rc_kind':'unix','rc':0,'artifact':p1['outer']+'/RESULT.json','rc_artifact':p1['outer']+'/p1-child.rc','proof':p1};results=[]
    def bad(name,fn):
        try:fn();passed=False
        except (ValueError,OSError,KeyError,TypeError):passed=True
        results.append({'name':name,'mutated':True,'rejected':passed})
    # Establish valid original baselines before trying to attach them to unrelated scopes.
    core.original(pg,source,factory);core.original(p1step,source,factory)
    for scope in ('final-db-preservation-abc','final-cleanup-new-owned-cids','secret-scan-final','legacy-regressions-P1-P2.25','final_restart_and_receipt'):
        for step in (pg,p1step):
            item={'id':scope,'status':'accepted','satisfiers':[step['id']],'contracts':{step['id']:{k:v for k,v in step.items() if k!='id'}}}
            bad(scope+'<-'+step['proof']['kind'],lambda item=item,step=step:core.validate_item(item,{step['id']:step},source,factory))
    for name,change in [('missing-artifact',lambda s:s.update(artifact='missing-artifact.raw')),('missing-rc',lambda s:s.update(rc_artifact='missing.rc')),('missing-original-files',lambda s:s['proof'].pop('original_files')),('HTTP-missing-outer-original',lambda s:s.update(rc_kind='http-status',rc=200,expected_status=200,outer_rc=0,outer_rc_artifact='missing-outer.rc'))]:
        step=copy.deepcopy(pg);change(step);bad(name,lambda step=step:core.original(step,source,factory))
    swapped=copy.deepcopy(pg['proof']);bad('PG-normal-swapped-to-race-scope',lambda:core.scope_kind('pg-new-27-normal-27-race','pg-new-connector-race',swapped))
    bad('HTTP10-scope-cannot-use-PG',lambda:core.scope_kind('http-live-full-suite','http10-exact-aggregate',pg['proof']))
    bad('duplicate-evidence-ID',lambda:core.index_steps({'steps':[pg,pg]}))
    bad('whole-history-bool-and-subset-not-proof',lambda:core.validate_failed_history({'reviewed':True,'paths':['only-green']}))
    # Every actual read must bind the exact destination, workspace origin/path, hash and size.
    rel=pg['rc_artifact'];raw=(Path(workspace)/rel).read_bytes();entry={'origin':'workspace','path':rel,'destination':rel,'sha256':hashlib.sha256(raw).hexdigest(),'size':len(raw)}
    assembler.bound_reader_factory({rel:entry},workspace)().read(rel)
    all_mapped={path:dict(origin='workspace',path=path,destination=path,**pin) for path,pin in pg['proof']['original_files'].items()}
    all_mapped.pop(pg['artifact'])
    bad('artifact-not-ledger-member',lambda:core.original(pg,source,assembler.bound_reader_factory(all_mapped,workspace)))
    for name,mutation in [('ledger-wrong-origin',{'origin':'root-review'}),('ledger-wrong-original-path',{'path':'another.rc'}),('ledger-wrong-hash',{'sha256':'0'*64}),('ledger-wrong-size',{'size':999})]:
        e=dict(entry,**mutation);bad(name,lambda e=e:assembler.bound_reader_factory({rel:e},workspace)().read(rel))
    bad('independent-rc-not-in-ledger',lambda:assembler.bound_reader_factory({},workspace)().rc(rel))
    # Build a structural 402-source ledger, then mutate in additional source.pem/binary.
    allowdoc=json.loads((b/'p26_source402_allowlist.json').read_text());allowed=allowdoc['paths'];members=[{'origin':'workspace','path':p,'destination':'source/'+p,'kind':'source',**allowdoc['files'][p]} for p in allowed];spec={'format':'p26-copy-ledger-v1','source_sha256':core.FROZEN_S,'history_roots_explicit':True,'members':members,'formal_required_paths':['x'],'history_required_paths':['x']}
    for extra in ('services/dataset-versioning-service/private.pem','services/dataset-versioning-service/blob.bin'):
        modified=copy.deepcopy(spec);modified['members'].append({'origin':'workspace','path':extra,'destination':'source/'+extra,'kind':'source','sha256':'0'*64,'size':0});bad('source-402-plus-'+extra,lambda modified=modified:assembler.validate_spec(modified))
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp).resolve();badlist=root/'bad-source-list.json';doc=json.loads((b/'p26_source402_allowlist.json').read_text());doc['paths'].append('services/dataset-versioning-service/private.pem');badlist.write_text(json.dumps(doc));bad('allowlist-anchor-tampered',lambda:core.source_allowlist(badlist))
        src=root/'source';src.mkdir();alias=root/'alias';alias.symlink_to(src,target_is_directory=True)
        base={'source_package':str(src),'package_copy':str(root/'copy'),'capture_dir':str(root/'capture'),'profile_path':str(root/'profile.sb'),'probe_config':str(root/'copy/probe.json'),'python_realpath':'/runtime/python','runtime_read_roots':['/runtime'],'forbidden_roots':[str(root/'workspace'),str(root/'private')],'manifest_sha256':'x'}
        baseline_release={'approved':True,'phase':'p26-independent-offline','manifest_sha256':'x','private_forbidden_roots':[str(root/'private')],'original_workspace':str(root/'workspace'),'config_sha256':hashlib.sha256(json.dumps(base,sort_keys=True,separators=(',',':')).encode()).hexdigest()}
        offline.preflight(base,baseline_release)
        for name,mut in [('capture-inside-source',{'capture_dir':str(src/'capture')}),('profile-inside-source',{'profile_path':str(src/'profile.sb')}),('copy-inside-source',{'package_copy':str(src/'copy')}),('capture-source-alias',{'capture_dir':str(alias/'capture')}),('capture-inside-copy',{'capture_dir':str(root/'copy/capture')}),('profile-capture-overlap',{'profile_path':str(root/'capture/profile.sb')})]:
            config=dict(base,**mut);release={'approved':True,'phase':'p26-independent-offline','manifest_sha256':'x','private_forbidden_roots':[str(root/'private')],'original_workspace':str(root/'workspace'),'config_sha256':hashlib.sha256(json.dumps(config,sort_keys=True,separators=(',',':')).encode()).hexdigest()};bad(name,lambda config=config,release=release:offline.preflight(config,release))
    return {'valid_original_baselines':['actual PG','actual P1pre9'],'controls':results,'failed':sum(not r['rejected'] for r in results),'actual_copy_or_sandbox':False}

if __name__=='__main__':
    import sys
    x=run(sys.argv[1]);print(json.dumps(x,indent=2));raise SystemExit(bool(x['failed']))
