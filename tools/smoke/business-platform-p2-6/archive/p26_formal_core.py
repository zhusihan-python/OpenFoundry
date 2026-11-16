"""Shared pre-copy / copied-package original evidence rules. No service execution."""
import hashlib
import json
import importlib.util
from pathlib import Path

KINDS={'pg-new-27-normal-27-race':'s2-pg','pg-unified-114':'s2-pg','unit-3services-full-untagged':'s2-unit','build-two-original':'s2-build-original','http-live-full-suite':'http10','q3-deploy-actual':'s2-runtime-aggregate','legacy-regressions-P1-P2.25':'final-legacy-regressions','secret-scan-final':'final-actual-secret','final-db-preservation-abc':'final-db-preservation-abc','final-cleanup-new-owned-cids':'final-cleanup-complete','final_restart_and_receipt':'final-restart-receipt','h12-targeted':'s2-pg'}
SETS={'legacy-regressions-P1-P2.25':{'legacy-S2-final-through-P1post9'},'secret-scan-final':{'final-actual-secret-scan'},'final-db-preservation-abc':{'final-ABC-last-and-readonly'},'final-cleanup-new-owned-cids':{'final-exact-owned10-cleanup'},'final_restart_and_receipt':{'final-main3-postP1-oldartifact-exactreceipt'},'h12-targeted':{'pg-h12-normal','pg-h12-race'},'pg-new-27-normal-27-race':{'pg-new-connector-normal','pg-new-connector-race','pg-new-dataset-normal','pg-new-dataset-race'},'pg-unified-114':{'pg-unified-connector-normal','pg-unified-dataset-normal','pg-unified-pipeline-normal'},'unit-3services-full-untagged':{'unit-dataset','unit-pipeline','unit-connector'},'build-two-original':{'build-original-dataset','build-original-connector'},'http-live-full-suite':{'http10-exact-aggregate'},'q3-deploy-actual':{'q3-S2-runtime-aggregate'}}


SOURCE_ALLOWLIST_SHA256='8fff35f6834dcda9fdacb60fdfe32331bf7262c329287e9f6a71a9cbaa725655'
# Root froze S2; old S remains historical, later final proof schemas remain unsupported.
ACTIVE_FINAL_SOURCE='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
FROZEN_S='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'


def source_allowlist(path):
    data=Path(path).read_bytes();need(hashlib.sha256(data).hexdigest()==SOURCE_ALLOWLIST_SHA256,'frozen source allowlist hash changed')
    doc=json.loads(data);need(doc['source_sha256']==FROZEN_S and len(doc['paths'])==len(set(doc['paths']))==404,'frozen source allowlist shape')
    need(set(doc.get('files',{}))==set(doc['paths']),'frozen404 file pins missing')
    return set(doc['paths'])


def source_pins(path):
    source_allowlist(path)
    return json.loads(Path(path).read_bytes())['files']


def need(ok,msg):
    if not ok:raise ValueError(msg)


def original_module():
    spec=importlib.util.spec_from_file_location('p26_original_proofs',Path(__file__).with_name('p26_original_proofs.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def index_steps(doc):
    result={}
    for s in doc.get('steps',[]):
        need(isinstance(s,dict) and s.get('id') and s['id'] not in result,'evidence step id missing/duplicated')
        result[s['id']]=s
    return result


def original(step,source,factory):
    need(step.get('classification')=='actual','history/fake classification cannot satisfy')
    r=factory();r.read(step.get('artifact',''));kind=step.get('rc_kind');rc=step.get('rc')
    need(type(rc)is int,'original rc must be integer')
    if kind=='unix':need(rc==0,'unix original rc nonzero')
    elif kind=='http-status':
        need(100<=rc<=599 and rc==step.get('expected_status') and type(step.get('outer_rc'))is int and step['outer_rc']==0,'HTTP status/outer Unix semantics')
        r.rc(step.get('outer_rc_artifact',''),0)
    else:raise ValueError('unknown top-level rc_kind; normalized_harness is validated only within HTTP proof')
    r.rc(step.get('rc_artifact',''),rc)
    proof=step.get('proof',{});need(bool(proof.get('original_files')),'original_files hash/size closure required')
    _,files=original_module().validate(None,proof,source,reader=factory())
    return dict(r.files,**files)


def scope_kind(iid,sid,proof):
    need(iid in KINDS,'unknown formal scope')
    required=KINDS[iid];need(required is not None,'unsupported final original schema: '+iid)
    need(proof.get('epoch')==ACTIVE_FINAL_SOURCE,'formal same active S2 proof epoch required')
    kind=proof.get('kind')
    need(kind==required,'formal scope-kind mismatch: '+iid)
    if iid in ('legacy-regressions-P1-P2.25','final-db-preservation-abc','final-cleanup-new-owned-cids','final_restart_and_receipt'):need(proof.get('contract_version')=='p26-final-actual-components-v1','mandatory final actual independent component composition')
    if iid=='http-live-full-suite':need(proof.get('epoch_schema')=='S2' and proof.get('closure_summary') and proof.get('current_summary'),'formal S2 HTTP requires current full original aggregate')
    if kind=='s2-build-original':
        service=sid.removeprefix('build-original-')
        need(Path(proof.get('inner','')).name.startswith('build-'+service+'_'),'formal build service/composite identity')
    if kind=='s2-unit':need(proof.get('service')==sid.removeprefix('unit-'),'formal unit service identity')
    if kind=='s2-pg':
        counts={'pg-h12-normal':1,'pg-h12-race':1,'pg-new-connector-normal':18,'pg-new-connector-race':18,'pg-new-dataset-normal':9,'pg-new-dataset-race':9,'pg-unified-connector-normal':82,'pg-unified-dataset-normal':15,'pg-unified-pipeline-normal':17}
        need(len(proof.get('required_names',[]))==counts[sid],'formal PG full required scope')
        expected=('h12-rollback-'+sid.removeprefix('pg-h12-') if sid.startswith('pg-h12-') else 'new27-'+sid.removeprefix('pg-new-') if sid.startswith('pg-new-') else 'unified114-'+sid.removeprefix('pg-unified-').removesuffix('-normal'))
        need(proof.get('step')==expected and Path(proof.get('outer','')).name==expected,'formal S2 PG accepted original chain identity')


def validate_item(item,steps,source,factory,scope=None):
    iid=scope or item.get('id');sids=item.get('satisfiers',[])
    need(sids and len(sids)==len(set(sids)),'nonempty unique satisfiers required')
    need(iid in KINDS and KINDS[iid] is not None,'unsupported final original schema: '+str(iid))
    need(set(sids)==SETS[iid],'formal exact full satisfier scope mismatch')
    need(item.get('status') in ('accepted','pending'),'unresolved formal status')
    seen={}
    for sid in sids:
        need(sid in steps,'required evidence step missing: '+sid);step=steps[sid]
        need(item.get('contracts',{}).get(sid)=={k:v for k,v in step.items() if k!='id'},'Root original contract missing/mismatch')
        scope_kind(iid,sid,step.get('proof',{}));seen.update(original(step,source,factory))
    return seen


def validate_all(roster,evidence,acceptance,source,factory):
    need(ACTIVE_FINAL_SOURCE is not None and source==ACTIVE_FINAL_SOURCE,'formal same frozen S2 required; old S is historical-only')
    required=set(KINDS)-{'final_restart_and_receipt','h12-targeted'};items=roster.get('items',[]);ids=[i.get('id') for i in items]
    need(len(ids)==len(set(ids)) and set(ids)==required,'formal required IDs missing/extra/duplicated')
    steps=index_steps(evidence);need(acceptance.get('roster_complete') is True,'Root complete acceptance required')
    accepted={}
    for x in acceptance.get('accepted',[]):
        need(isinstance(x,dict) and x.get('id') and x['id'] not in accepted,'Root acceptance missing/duplicate actual objects');accepted[x['id']]=x.get('satisfiers')
    seen={}
    for item in items:
        if item.get('status')=='pending':need(accepted.get(item['id'])==item.get('satisfiers'),'pending actual acceptance contract missing')
        seen.update(validate_item(item,steps,source,factory))
    h12=roster.get('pre_seal_requirements',{}).get('h12-targeted',{})
    seen.update(validate_item(h12,steps,source,factory,'h12-targeted'))
    restart=roster.get('pre_seal_requirements',{}).get('final_restart_and_receipt',{})
    seen.update(validate_item(restart,steps,source,factory,'final_restart_and_receipt'))
    seen.update(validate_failed_history(acceptance.get('failed_history_review'),source,factory))
    return seen


def validate_failed_history(review,source=FROZEN_S,factory=None):
    need(isinstance(review,dict) and review.get('kind')=='final-whole-failed-history' and review.get('epoch')==source and review.get('original_files') and factory is not None,'whole actual failed-history original closure absent; NOTREADY')
    _,files=original_module().validate(None,review,source,reader=factory())
    return files
