"""Freeze exact completed B unknown without adopting/cancelling/clearing it."""
import json
from urllib.parse import quote

def install(module,facade):
    if not hasattr(module,'scenario_b'): return
    original=module.SCENARIOS['B'][1]
    def completed_b(suffix,ev):
        result=original(suffix,ev)  # original join/409/no-publish/27s counter checks
        candidates=[r for r in facade.owned.runs.values() if facade.owned.sources[facade.owned.defs[r['def_id']]['source_id']]['name']=='p21-p21b-'+suffix]
        if len(candidates)!=1: raise ValueError('B exact actual-created Run missing/duplicate')
        run=candidates[0]; rid=run['id']; src='p21b-'+suffix
        current=module.M.http('fixture','/admin/src/'+quote(src))
        if current.status!=200 or current.body.get('request_count')!=ev['pages_after_wait'] or ev['pages_after_wait']!=ev['pages_before_inject'] or ev['target_files']!=0 or ev['http_status']!=409:
            raise ValueError('B original joined worker/source quiet facts drift')
        row,proof=read(facade,rid)
        if row['sync_def_id']!=run['def_id'] or row['org_id']!=run['tenant'] or row['requested_by']!=run['actor'] or row['status']!='running':
            raise ValueError('B unknown identity/tenant/state mismatch')
        facade.frozen_b[rid]={'safe_projection_27':row,'original_evidence':proof,'source_counter':current.body['request_count'],'purpose':'completed original B; expired lease; unknown preserved'}
        facade.attempt.record('typed-frozen-new-B',{'run_id':rid},json.dumps(facade.frozen_b[rid]),0,{'original_rc':None,'derived_from_actual_reads':True})
        return result
    module.SCENARIOS=dict(module.SCENARIOS,B=(module.SCENARIOS['B'][0],completed_b))

def read(facade,rid):
    from w10_guard_shim import W
    facade.require_phase()
    cmd=list(W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'])
    predicate="r.control_mode = 'controlled' AND r.status IN ('pending','running')"
    cmd[-1]=cmd[-1].replace(predicate,"r.id='"+rid+"'::uuid")
    raw,path=facade.read_actual('B-safe27',cmd)
    rows=W.C._parse_rows_stdout(raw,'B snapshot')
    if len(rows)!=1: raise ValueError('B safe27 row missing/duplicate')
    W.C._check_row_schema(rows[0],'B actual',0)
    cmd=cmd[:cmd.index('-c')]+['-c',"SELECT json_build_object('id',r.id::text,'expired',r.lease_expires_at IS NOT NULL AND r.lease_expires_at <= clock_timestamp(),'recovery_protocol',r.recovery_protocol) FROM sync_runs r WHERE r.id='"+rid+"'::uuid;"]
    raw,leasepath=facade.read_actual('B-DBclock-lease',cmd)
    lease=json.loads(raw[raw.index('{'):raw.rindex('}')+1])
    if lease!={'id':rid,'expired':True,'recovery_protocol':None}: raise ValueError('B lease not DB-clock expired or protocol drift')
    return rows[0],{'safe27_step':path,'lease_step':leasepath}

def recheck(facade):
    from w10_guard_shim import exact
    for rid,snapshot in facade.frozen_b.items():
        row,_=read(facade,rid)
        exact([snapshot['safe_projection_27']],[row],'id')
