#!/usr/bin/env python3
"""Root-released NEW legacy N fixture setup; never runs or cancels a sync.

Creates three fresh HTTP parents (disabled Def), then exactly one fresh INSERT.
Every old whole set is compared before/after; failed attempts retain all evidence
and partial owned resources, with no adoption, retry, cancellation or cleanup.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import uuid
from urllib import request,error
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import w10_suite_adapter as A
import w10_guard_shim as H
import p22_legacy_probe_setup as S

STAGE='p22-new-legacy-probe-setup'
SETUP_TOOLS={'regressions/adapters/'+n for n in ['p22_legacy_probe_entry.py','p22_legacy_probe_setup.py','w10_suite_adapter.py','w10_guard_shim.py','w10_owned_sql.py','w10_holder_seam.py','w10_abc_held.py']}
SETUP_TOOLS|={'common/p26_gates.py','common/p26_source_hash.py','cohort/p26_cohort_cli.py','preflight/p26_current_preflight.py','regressions/vendor/closure-ledger.json','regressions/vendor/tree/business-platform-p1/smoke/smoke.py'}
PRIVATE={'lease_holder','lease_token','lease_expires_at','heartbeat_at','lease_expires','heartbeat'}

def digest(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def public_safe(value):
    if isinstance(value,dict):
        for k,v in value.items():
            if k in PRIVATE: raise ValueError('private public-response key: '+k)
            public_safe(v)
    elif isinstance(value,list):
        for v in value: public_safe(v)


def select(facade,name,sql):
    prefix=list(H.W.C.OLDPG_READONLY_ARGV['cp_owners'])
    raw,step=facade.read_actual(name,prefix[:prefix.index('-c')]+['-c','BEGIN READ ONLY;','-c',sql,'-c','COMMIT;'])
    return H.W.C._parse_rows_stdout(raw,name),step


def legacy_state(facade,name,baseline,extra=None):
    """Whole legacy safe27 and exact sealed BC parent/protocol bindings."""
    cmd=list(H.W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'])
    needle="r.control_mode = 'controlled' AND r.status IN ('pending','running')"
    if cmd[-1].count(needle)!=1: raise ValueError('safe27 query shape drift')
    cmd[-1]=cmd[-1].replace(needle,"r.control_mode = 'legacy' AND r.status IN ('pending','running')")
    raw,step=facade.read_actual(name+'-whole-legacy',cmd)
    rows=H.W.C._parse_rows_stdout(raw,name)
    for row in rows: H.W.C._check_row_schema(row,'legacy',0)
    H.exact(baseline['runs']+([extra['safe_projection_27']] if extra else []),rows,'id')
    ids=','.join("'"+r['id']+"'::uuid" for r in baseline['runs'])
    protocols,pstep=select(facade,name+'-BC-protocol',"SELECT json_agg(row_to_json(t)) FROM (SELECT id::text,recovery_protocol FROM sync_runs WHERE id IN ("+ids+")) t;")
    H.exact(baseline['protocols'],protocols,'id')
    if any(p['recovery_protocol'] is not None for p in protocols): raise ValueError('historical protocol must remain NULL')
    full,pfull=facade.read_actual(name+'-full-parent-union',H.W.build_parents_argv([r['sync_def_id'] for r in rows]))
    full=H.W.C._parse_rows_stdout(full,name+' full parents')
    if len(full)!=len({r['sync_def_id'] for r in rows}): raise ValueError('historical/new full-parent set missing/extra')
    for row in rows:
        p=next((p for p in full if p['def_id']==row['sync_def_id']),None)
        if not p or any(p[k]!=row[v] for k,v in [('source_id','source_id'),('conn_id','source_id'),('output_dataset_id','output_dataset_id'),('org_id','org_id'),('conn_org_id','org_id'),('conn_owner_id','owner_id'),('source_name','source_name')]):
            raise ValueError('legacy parent tenant/actor/source binding drift')
    H.exact(baseline['parents'],[p for p in full if p['def_id'] in {r['sync_def_id'] for r in baseline['runs']}],'def_id')
    return {'rows':rows,'parents':full,'whole_step':step,'sealed_parent_step':pstep,'full_parent_step':pfull}


class Setup:
    def __init__(self,r,rpath,out,runner=None,http=None):
        self.r=r; self.rpath=rpath; self.out=Path(out); self.http_impl=http
        self.facade=A.Facade(runner or subprocess.run,r,rpath)
        self.facade.attempt=H.W.Attempt(self.out,'N-setup')
        self.facade.owned.attempt=self.facade.attempt
        secret=H.W.read_jwt_secret(); self.facade.attempt.add_secret(secret)
        self.actor=str(uuid.uuid4());self.tenant=str(uuid.uuid4())
        self.token=H.W.sign_testing_jwt(secret,self.actor,self.tenant)
        self.facade.attempt.add_secret(self.token)
        self.created={}
        self.phase_only=self.facade.require_phase
        self.facade.require_phase=self.gate
    def gate(self):
        self.phase_only()
        if H.G.candidate_hash(H.G.OPENFOUNDRY)!=self.r['candidate_source']: raise ValueError('actual source changed')
        for p,sha in self.r['setup_tools_sha256'].items():
            if digest(H.P26/p)!=sha: raise ValueError('setup active tool drift '+p)
        for key in ('deployment_record','typed_roster','legacy_baseline'):
            if digest(self.r[key])!=self.r[key+'_sha256']: raise ValueError('setup input drift '+key)
    def call(self,service,path,payload=None,method=None):
        self.gate(); method=method or ('POST' if payload is not None else 'GET')
        base={'dataset':'http://127.0.0.1:17078','connector':'http://127.0.0.1:17100'}[service]
        if not path.startswith('/') or '?' in path or '#' in path: raise ValueError('unbounded setup path')
        raw=json.dumps(payload).encode() if payload is not None else None
        step=self.facade.attempt.step_begin('http-'+method,{'method':method,'url':base+path,'request_body':payload})
        status=None;content=b''
        try:
            if self.http_impl: status,content=self.http_impl(base+path,method,raw,self.token)
            else:
                opener=request.build_opener(H.W._NoRedirectHandler())
                req=request.Request(base+path,data=raw,headers={'Authorization':'Bearer '+self.token,'Content-Type':'application/json'},method=method)
                try:
                    with opener.open(req,timeout=30) as resp: status=resp.status;content=resp.read(H.W.HTTP_MAX_BODY+1)
                except error.HTTPError as exc: status=exc.code;content=exc.read(H.W.HTTP_MAX_BODY+1)
            if len(content)>H.W.HTTP_MAX_BODY: raise ValueError('oversized setup response')
            body=json.loads(content); public_safe(body)
        except Exception as exc:
            self.facade.attempt.step_finish('http-'+method,step,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None,'http_status':status,'body_sha256':hashlib.sha256(content).hexdigest()})
            raise
        self.facade.attempt.step_finish('http-'+method,step,content,'unavailable',{'original_rc':None,'http_status':status})
        if status not in (200,201,404): raise ValueError('setup HTTP status '+str(status))
        self.facade.owned.observe(service,path,method,payload,status,body,{'sub':self.actor,'org_id':self.tenant})
        return status,body,step
    def disabled(self,did,name):
        rows,step=select(self.facade,name,"SELECT json_agg(row_to_json(t)) FROM (SELECT id::text,schedule_enabled,(schedule_next_fire_at IS NULL) AS next_fire_null FROM batch_sync_defs WHERE id='"+did+"'::uuid) t;")
        if rows!=[{'id':did,'schedule_enabled':False,'next_fire_null':True}]: raise ValueError('N Def must remain disabled with NULL next fire')
        return step
    def run(self):
        self.gate(); before=H.check(self.rpath)
        baseline=json.loads(Path(self.r['legacy_baseline']).read_text())
        if baseline.get('schema')!='p26-w10-legacy-baseline-v1': raise ValueError('actual legacy baseline schema')
        if baseline.get('crosscheck13_parent_protocolNULL') is not True or baseline.get('whole_active_outside_old9_exact_verified_legacy2') is not True: raise ValueError('Root actual sealed BC acceptance missing')
        if len(baseline['runs'])!=2 or len({r['id'] for r in baseline['runs']})!=2: raise ValueError('expected exact historical legacy BC2 baseline')
        legacy_before=legacy_state(self.facade,'before',baseline)
        p1=A.load(A.TREE/A.SUITES['p1'])
        suffix='p26-N-'+uuid.uuid4().hex
        source_payload=p1.source_payload(suffix,suffix)
        cfg=source_payload['config'];cfg['domain']='p26fixture:8090';cfg['base_url']='http://p26fixture:8090';cfg['permissions']['allowed_egress_hosts']=['p26fixture:8090']
        _,source,_=self.call('connector','/api/v1/data-connection/sources',source_payload)
        sid=source['id'];uuid.UUID(sid); self.created['source_id']=sid
        _,target,_=self.call('dataset','/v1/datasets',{'name':suffix+'-out','format':'json'})
        tid=target['id'];uuid.UUID(tid); self.created['output_dataset_id']=tid
        _,definition,_=self.call('connector','/api/v1/data-connection/syncs',{'source_id':sid,'output_dataset_id':tid,'schedule_enabled':False})
        did=definition['id'];uuid.UUID(did); self.created['def_id']=did
        if did not in self.facade.owned.defs: raise ValueError('actual HTTP Registry missing new parent chain')
        self.disabled(did,'new-Def-disabled-before')
        probe=S.insert_new_fixture(self.facade,did)
        self.created['run_id']=probe['run_id']
        H.exact(baseline['runs'],probe['legacy_whole_baseline_27'],'id')
        legacy_after=legacy_state(self.facade,'after',baseline,probe['typed_entry'])
        H.exact(legacy_before['parents'],[p for p in legacy_after['parents'] if p['def_id']!=did],'def_id')
        self.disabled(did,'new-Def-disabled-after')
        views=[]
        for connector in ('http://127.0.0.1:17100',):
            cpstatus,cp,cpstep=self.call('connector','/api/v1/data-connection/syncs/'+did+'/checkpoint')
            rs,runs,rstep=self.call('connector','/api/v1/data-connection/syncs/'+did+'/runs')
            if cpstatus!=404 or rs!=200: raise ValueError('new legacy probe must have no CP owner and readable Run')
            items=runs if isinstance(runs,list) else runs.get('items',[]) if isinstance(runs,dict) else []
            if len(items)!=1 or items[0].get('id')!=probe['run_id'] or items[0].get('status')!='running' or items[0].get('control_mode')!='legacy': raise ValueError('new public Run set/state mismatch')
            public_row=items[0]
            if any(public_row.get(k)!=v for k,v in {'sync_def_id':did,'org_id':self.tenant,'requested_by':self.actor}.items()): raise ValueError('new public Run parent/identity mismatch')
            views.append({'def_id':did,'actor':self.actor,'tenant':self.tenant,'checkpoint_status':cpstatus,'baseline_cp':{},'baseline_runs':runs,'checkpoint_step':cpstep,'runs_step':rstep})
        after=H.check(self.rpath)
        typed=json.loads(Path(self.r['typed_roster']).read_text())
        if any(e['run_id']==probe['run_id'] for e in typed['runs']): raise ValueError('fresh Run accidentally pre-registered')
        typed.update(schema='p26-w10-typed-v1',runs=typed['runs']+[probe['typed_entry']],public_views=typed.get('public_views',[])+views,legacy_whole_baseline_27=baseline['runs'])
        candidate=self.out/'typed-new-N-candidate.json';candidate.write_text(json.dumps(typed,indent=2)+'\n')
        return {'ok':True,'status':'actual collected; Root must accept typed candidate before P22','created':self.created,'identity':{'actor':self.actor,'tenant':self.tenant},'probe':probe,'typed_candidate':str(candidate),'typed_candidate_sha256':digest(candidate),'before_guard':before,'after_guard':after,'legacy_before':legacy_before,'legacy_after':legacy_after,'actual_owned_receipts':self.facade.owned.receipts}


def execute(release_path,output,runner=None,http=None):
    r=H.validate_release(release_path,STAGE,'p22')
    out=Path(output).resolve()
    if not out.is_relative_to((H.P26/'results/w10').resolve()) or str(out)!=r['setup_output']: raise ValueError('exclusive setup output not Root-bound')
    if r.get('allow_new_legacy_fixture_insert') is not True or r.get('setup_entry_sha256')!=digest(__file__): raise ValueError('exact setup authority/tool pin absent')
    if set(r['setup_tools_sha256'])!=SETUP_TOOLS: raise ValueError('exact active setup closure map required')
    for p,sha in r['setup_tools_sha256'].items():
        if digest(H.P26/p)!=sha: raise ValueError('setup closure drift '+p)
    out.mkdir(parents=True,exist_ok=False)
    obj=Setup(r,release_path,out,runner,http);result={'ok':False}
    try: result=obj.run();returncode=0
    except (Exception,A.StageAbort) as exc:
        result={'ok':False,'error':type(exc).__name__+': '+str(exc),'created':obj.created,'insert_attempt':getattr(obj.facade,'new_legacy_insert_attempt',None),'actual_owned_receipts':obj.facade.owned.receipts,'policy':'preserve partial owned objects and original failure; no retry/adoption/cancel'};returncode=2
    finally:
        result['insert_attempt']=getattr(obj.facade,'new_legacy_insert_attempt',None)
        result['source_after']=H.G.candidate_hash(H.G.OPENFOUNDRY)
        result['tools_after']={p:digest(H.P26/p) for p in r['setup_tools_sha256']}
        result['tools_unchanged']=result['tools_after']==r['setup_tools_sha256']
        result['source_unchanged']=result['source_after']==r['candidate_source']
        if not result['tools_unchanged'] or not result['source_unchanged']: result['ok']=False;returncode=2
        obj.facade.attempt.finish(result)
        (out/'RESULT.json').write_text(obj.facade.attempt.redact(json.dumps(result,indent=2))+'\n')
    return returncode


def main(argv=None):
    ap=argparse.ArgumentParser();ap.add_argument('--release-file',required=True);ap.add_argument('--output',required=True);a=ap.parse_args(argv)
    return execute(a.release_file,a.output)
if __name__=='__main__':
    try:sys.exit(main())
    except Exception as exc: print('N setup refused: '+type(exc).__name__+': '+str(exc),file=sys.stderr);sys.exit(2)
