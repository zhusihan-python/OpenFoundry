#!/usr/bin/env python3
"""Current source/container + old/typed-NEW whole-set guard; no mutations.

Root release pins ledger and typed roster bytes. Schema alone is never proof:
all compared current rows come from W8's real SELECT argv / HTTP transport.
"""
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path
sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(P26))
from preflight import p26_current_preflight as W
from common import p26_gates as G


def exact(expected, actual, key):
    """Compare complete row dictionaries; extras and duplicate IDs fail."""
    def index(rows):
        out = {}
        for row in rows:
            rid = row[key]
            if rid in out:
                raise ValueError('duplicate '+key+': '+str(rid))
            out[rid] = row
        return out
    a, b = index(expected), index(actual)
    if a != b:
        raise ValueError('whole-set mismatch '+key+' extra='+str(sorted(set(b)-set(a)))+
                         ' missing='+str(sorted(set(a)-set(b)))+
                         ' changed='+str(sorted(k for k in a.keys() & b.keys() if a[k] != b[k])))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_release(path, stage=None, suite=None):
    release=json.loads(Path(path).read_text())
    if release.get('schema')!='p26-w10-release-v1' or release.get('root_accepted') is not True:
        raise ValueError('Root release required')
    expiry=datetime.fromisoformat(release['expires_utc'].replace('Z','+00:00'))
    if expiry.tzinfo is None or datetime.now(timezone.utc)>=expiry:
        raise ValueError('Root release expired')
    if not release.get('stage') or not release.get('suite'):
        raise ValueError('explicit stage/suite required')
    if stage is not None and release['stage']!=stage: raise ValueError('stage not released')
    if suite is not None and release['suite']!=suite: raise ValueError('suite not released')
    return release


def check(release_path, runner=None, transport=None, deployment_projection=None):
    release = validate_release(release_path)
    if release.get('schema') != 'p26-w10-release-v1' or release.get('root_accepted') is not True:
        raise ValueError('Root phase release required')
    for field in ('deployment_record','typed_roster'):
        if sha(release[field]) != release[field+'_sha256']:
            raise ValueError(field+' bytes not Root-pinned')
    rec = json.loads(Path(release['deployment_record']).read_text())
    typed = json.loads(Path(release['typed_roster']).read_text())
    image_projection=None; image_extension=None; original_release_sha=sha(release_path)
    if release.get('image_transition') is not None or deployment_projection is not None:
        if release.get('main_boot_transition') is not None:raise ValueError('one transition authority only; image and restart scopes cannot combine')
        helper=Path(__file__).resolve().parent/'w10_image_transition.py'
        if sha(helper)!=release['image_transition']['guard_extension_sha256']:raise ValueError('Root image helper pin drift')
        spec=importlib.util.spec_from_file_location('w10_image_transition',helper);image_extension=importlib.util.module_from_spec(spec);spec.loader.exec_module(image_extension)
        rec,image_projection=image_extension.derive(release,rec,deployment_projection,original_release_sha,W)
    boot_transition=None
    if release.get('main_boot_transition') is not None:
        helper=Path(__file__).resolve().parent/'w10_boot_transition.py'
        if sha(helper)!=release['main_boot_transition']['guard_extension_sha256']: raise ValueError('Root main boot helper pin drift')
        spec=importlib.util.spec_from_file_location('w10_boot_transition',helper);extension=importlib.util.module_from_spec(spec);spec.loader.exec_module(extension)
        rec,boot_transition=extension.derive(release,rec,W)
    if typed.get('schema') != 'p26-w10-typed-v1':
        raise ValueError('typed roster schema')
    if G.candidate_hash(G.OPENFOUNDRY) != release['candidate_source']:
        raise ValueError('actual source != candidate S')
    runner = runner or (lambda cmd: subprocess.run(cmd,capture_output=True,text=True,timeout=120))
    transport = transport or W.http_call
    records=[]
    if not Path(release['evidence_root']).resolve().is_relative_to((P26/'results/w10').resolve()):
        raise ValueError('guard evidence must stay inside results/w10')
    att=W.Attempt(Path(release['evidence_root']),'guard')
    att.add_secret(W.read_jwt_secret())
    if image_projection:att.record('verified-Root-image-authority-and-transition',{},json.dumps(image_projection),0,{'original_rc':None,'kind':'local-derived-proof'})
    if boot_transition: att.record('verified-main-boot-receipt-chain',{},json.dumps(boot_transition),0,{'original_rc':None,'kind':'local-derived-proof'})
    def run(cmd):
        validate_release(release_path,release['stage'],release['suite'])
        path=att.step_begin('command',cmd)
        try: cp=runner(cmd)
        except Exception as exc:
            att.step_finish('command',path,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None,'failure_kind':'spawn-or-timeout'})
            raise
        att.step_finish('command',path,cp.stdout,cp.returncode,{'stderr':cp.stderr})
        records.append({'argv':cmd,'rc':cp.returncode,'stdout':att.redact(cp.stdout),'stderr':att.redact(cp.stderr)})
        if cp.returncode:
            raise ValueError('guard command rc='+str(cp.returncode))
        return cp.stdout
    if image_projection:image_extension.verify_current(image_projection,run)
    required={W.Q3_FIXTURE,W.Q3_DATASET,W.Q3_PIPELINE,W.Q3_CONNECTOR}
    if not required.issubset(rec['containers']) or rec.get('source_sha256')!=release['candidate_source']:
        raise ValueError('base3/fixture/source deployment pins missing')
    for name,pin in rec['containers'].items():
        if not re.fullmatch('[0-9a-f]{64}',pin['cid']) or not re.fullmatch('sha256:[0-9a-f]{64}',pin['image']):
            raise ValueError('full CID/image required')
        if name==W.Q3_FIXTURE and pin['started_at']!=W.EXPECTED_FIXTURE_STARTED_AT:
            raise ValueError('old fixture boot pin changed')
        expected_running=pin.get('expected_running',True)
        if not isinstance(expected_running,bool): raise ValueError('typed expected_running must be boolean')
        if not expected_running:
            labels=pin.get('ownership',pin.get('labels',{}))
            if name in required|{W.Q3_POSTGRES} or labels.get('of-p26-owner')!='p26-http' or labels.get('of-p26-stage')!='p2-6':
                raise ValueError('only Root-pinned NEW owned aux may be expected stopped')
        raw=run(['docker','inspect',name,'--format','{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}'])
        if raw.strip().splitlines() != [pin['cid'],pin['image'],pin['started_at'],str(expected_running).lower()]:
            raise ValueError('actual CID/image/boot/running drift '+name)
    _,catalog,derived=W.load_baseline(G.P25_FINAL_DIR)
    catalog['public_views']=W.public_view_baselines(derived,catalog)
    oldruns=[x['public_projection_27'] for x in derived['protected9']]
    oldcp=[x['cp_owner_row_full'] for x in derived['cp8']]
    newruns=[]; newcp=[]; newparents=[]
    for entry in typed['runs']:
        for key in ('run_id','def_id','source_id','tenant','actor','protocol','purpose','expected_state','evidence'):
            if not entry.get(key): raise ValueError('missing typed field '+key)
        evidence=entry['evidence']
        if sha(evidence['path']) != evidence['sha256']: raise ValueError('typed origin bytes drift')
        row=entry['safe_projection_27']
        original=json.loads(Path(evidence['path']).read_text())
        for part in evidence.get('json_pointer','').strip('/').split('/'):
            if part: original=original[int(part)] if isinstance(original,list) else original[part.replace('~1','/').replace('~0','~')]
        if original!=row: raise ValueError('typed row differs from original evidence')
        W.C._check_row_schema(row,'typed',0)
        if row['id']!=entry['run_id'] or row['sync_def_id']!=entry['def_id']:
            raise ValueError('typed run binding drift')
        if row['control_mode']=='controlled' and row['status'] in ('pending','running'): newruns.append(row)
        if entry.get('cp_owner_row_full'):
            owner=entry['cp_owner_row_full']
            if any(owner[k]!=row[other] for k,other in (('run_id','id'),('sync_def_id','sync_def_id'),('source_id','source_id'),('output_dataset_id','output_dataset_id'))):
                raise ValueError('typed CP owner parent binding mismatch')
            newcp.append(owner)
        if not entry.get('parent'): raise ValueError('typed parent required')
        parent=entry['parent']
        if parent['source_name']!=row['source_name'] or parent['conn_id']!=row['source_id'] or parent['def_id']!=row['sync_def_id'] or parent['source_id']!=row['source_id'] or parent['output_dataset_id']!=row['output_dataset_id'] or parent['org_id']!=row['org_id'] or parent['conn_org_id']!=row['org_id'] or parent['conn_owner_id']!=row['requested_by']:
            raise ValueError('typed parent/actor binding mismatch')
        newparents.append(parent)
        if (row['source_id']!=entry['source_id'] or row['org_id']!=entry['tenant']
            or row['requested_by']!=entry['actor'] or row['control_mode']!=entry['control_mode']
            or row['status']!=entry['expected_state'] or row['gate_entered']!=entry['pubentered']):
            raise ValueError('typed actual field binding mismatch')
    newparents=list({p['def_id']:p for p in newparents}.values())
    if {p['def_id'] for p in newparents}!={v['def_id'] for v in typed.get('public_views',[])}:
        raise ValueError('typed whole Def public baseline missing/extra')
    protocol_cmd=list(W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'])
    # Independent read, no change to historical safe27 schema. Current whole
    # controlled live predicate includes every NEW shadow; equality below still
    # covers the full original query, never a filtered old UUID result.
    typed_ids=[e['run_id'] for e in typed['runs']]
    for rid in typed_ids:
        if not G.UUID_RE.fullmatch(rid): raise ValueError('unsafe typed Run UUID')
    if typed_ids:
        sql="SELECT json_agg(row_to_json(t)) FROM (SELECT id::text,recovery_protocol FROM sync_runs WHERE id IN ("+','.join("'"+r+"'::uuid" for r in typed_ids)+")) t;"
        # Keep exact q3 docker/psql prefix, replace both -c statements with one.
        protocol_cmd=protocol_cmd[:protocol_cmd.index('-c')]+['-c',sql]
        actual_protocol=W.C._parse_rows_stdout(run(protocol_cmd),'typed protocol')
        expected_protocol=[{'id':e['run_id'],'recovery_protocol':None if e['protocol']=='legacy-null' else e['protocol']} for e in typed['runs']]
        exact(expected_protocol,actual_protocol,'id')
    if typed_ids:
        typed_cmd=list(W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'])
        predicate="r.control_mode = 'controlled' AND r.status IN ('pending','running')"
        if typed_cmd[-1].count(predicate)!=1: raise ValueError('W8 safe27 query shape drift')
        typed_cmd[-1]=typed_cmd[-1].replace(predicate,'r.id IN ('+','.join("'"+r+"'::uuid" for r in typed_ids)+')')
        typed_actual=W.C._parse_rows_stdout(run(typed_cmd),'typed full safe27')
        for row in typed_actual: W.C._check_row_schema(row,'actual typed',0)
        exact([r['safe_projection_27'] for r in typed['runs']],typed_actual,'id')
        legacy_new=[r['safe_projection_27'] for r in typed['runs'] if r['control_mode']=='legacy']
        if legacy_new:
            # N requires legacy control_mode independently from protocol NULL.
            # Whole legacy population is pinned by Root before NEW fixture setup;
            # do not hide a legacy shadow by selecting only registered IDs.
            old_legacy=typed['legacy_whole_baseline_27']
            legacy_cmd=list(W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'])
            legacy_cmd[-1]=legacy_cmd[-1].replace(predicate,"r.control_mode = 'legacy' AND r.status IN ('pending','running')")
            legacy_actual=W.C._parse_rows_stdout(run(legacy_cmd),'whole legacy safe27')
            exact(old_legacy+legacy_new,legacy_actual,'id')
    expected=oldruns+newruns
    current=W.C._parse_rows_stdout(run(W.C.OLDPG_READONLY_ARGV['controlled_rows_27col']),'whole controlled')
    for row in current: W.C._check_row_schema(row,'actual',0)
    exact(expected,current,'id')
    currentcp=W.C._parse_rows_stdout(run(W.C.OLDPG_READONLY_ARGV['cp_owners']),'whole CP')
    exact(oldcp+newcp,currentcp,'run_id')
    # Parent scope is the full old+typed protected Def union. Whole-set equality
    # is established before applying W8 old-specific provenance assertions.
    olddefs=[x['def_id'] for x in catalog['public_views']]
    newdefs=[x['def_id'] for x in newparents]
    for did in olddefs+newdefs:
        if not G.UUID_RE.fullmatch(did): raise ValueError('unsafe def UUID')
    parents=W.C._parse_rows_stdout(run(W.build_parents_argv(set(olddefs+newdefs))),'parents')
    if len(parents)!=len(set(olddefs+newdefs)) or {x['def_id'] for x in parents}!=set(olddefs+newdefs):
        raise ValueError('parent whole set mismatch')
    exact(newparents,[x for x in parents if x['def_id'] not in olddefs],'def_id')
    specs=W.build_step_specs(catalog)
    pspec=next(s for s in specs if s['id']=='sql-cp-parents')
    gate=pspec['gate'][1](json.dumps([x for x in parents if x['def_id'] in olddefs]),
        {'catalog':catalog,'baseline_cp8':derived['cp8']})
    if not gate['ok']: raise ValueError(str(gate))
    secret=W.read_jwt_secret(); sub,org=W.jwt_identity_from_baseline(derived,catalog)
    token=W.sign_testing_jwt(secret,sub,org)
    for view in catalog['public_views']+typed.get('public_views',[]):
        did=view['def_id']; base=W.ORIGINS['connector']+'/api/v1/data-connection/syncs/'+did
        view_token=W.sign_testing_jwt(secret,view.get('actor',sub),view.get('tenant',org))
        validate_release(release_path,release['stage'],release['suite'])
        cp_path=att.step_begin('public-checkpoint',{'url':base+'/checkpoint','method':'GET'})
        cp=transport(base+'/checkpoint',auth=view_token)
        att.step_finish('public-checkpoint',cp_path,cp.body,0,{'status':cp.status})
        run_path=att.step_begin('public-runs',{'url':base+'/runs','method':'GET'})
        validate_release(release_path,release['stage'],release['suite'])
        runs=transport(base+'/runs',auth=view_token)
        att.step_finish('public-runs',run_path,runs.body,0,{'status':runs.status})
        # W8 transport is a dict; parse bodies with the original comparator.
        records.append({'def_id':did,'checkpoint_status':cp.status,'runs_status':runs.status,
            'checkpoint_body_sha256':hashlib.sha256(cp.body).hexdigest(),
            'runs_body_sha256':hashlib.sha256(runs.body).hexdigest()})
        expected_cp_status=view.get('checkpoint_status',200)
        if cp.status!=expected_cp_status: raise ValueError('checkpoint status drift')
        result=W.compare_public_view(did,view['baseline_cp'],view['baseline_runs'],
            200 if expected_cp_status==404 else cp.status,
            {} if expected_cp_status==404 else json.loads(cp.body),runs.status,json.loads(runs.body))
        if not result['ok']: raise ValueError(str(result))
    if image_projection:
        if sha(release_path)!=original_release_sha:raise ValueError('Root original release changed during image guard')
        for path,proof in image_projection['original_artifact_sha256'].items():
            if sha(path)!=proof['sha256']:raise ValueError('image authority/original receipt changed during guard')
    if boot_transition:
        for path,proof in boot_transition['original_artifact_sha256'].items():
            if sha(path)!=proof['sha256']: raise ValueError('main restart original receipt changed during guard')
        ledger=Path(release['main_boot_transition']['ledger_dir'])
        if {str(p) for p in ledger.iterdir()}!={p for p in boot_transition['original_artifact_sha256'] if Path(p).parent==ledger}: raise ValueError('main restart ledger changed during guard')
    att.finish({'ok':True,'records':records,'verified_main_boot_transition':boot_transition,'verified_image_projection':image_projection})
    return {'evidence_dir':str(att.dir),'ok':True,'controlled_count':len(current),'cp_count':len(currentcp),'records':records,'verified_main_boot_transition':boot_transition,'verified_image_projection':image_projection}


def main(argv):
    if len(argv)==1:result=check(argv[0])
    elif len(argv)==3 and argv[1]=='--deployment-projection':result=check(argv[0],deployment_projection=argv[2])
    else:raise ValueError('usage: w10_guard_shim.py ROOT_RELEASE.json [--deployment-projection EXACT_PROOF]')
    print(json.dumps(result,sort_keys=True));return 0

if __name__=='__main__':
    try: sys.exit(main(sys.argv[1:]))
    except (ValueError,KeyError,OSError,TypeError) as exc:
        print('w10 guard refused: '+str(exc),file=sys.stderr); sys.exit(2)
