"""Actual post-scenario typed unknown export with whole-set equality.

Expected new identities come only from this process's successful HTTP creates
and the unchanged engine's actual scenario/roster recorder output.
"""
import json
from pathlib import Path


def export_summary(ctx,summary_path):
    from p26_p25_runtime import H
    W=H.W; ctx.gate(); summary=json.loads(Path(summary_path).read_text())
    root=json.loads(Path(ctx.release['typed_roster']).read_text())
    if H.sha(ctx.release['typed_roster'])!=ctx.release['typed_roster_sha256']: raise ValueError('typed Root pin changed')
    _,catalog,derived=W.load_baseline(H.G.P25_FINAL_DIR)
    catalog['public_views']=W.public_view_baselines(derived,catalog)
    oldrun=[r['public_projection_27'] for r in derived['protected9']]; oldcp=[r['cp_owner_row_full'] for r in derived['cp8']]
    roots={r['run_id']:r for r in root['runs']}
    known={o['run_id']:o for o in summary.get('owner_roster',[])}
    # Full finally rosters include imported known held owners; no ID is inferred
    # merely from an extra database row.
    dynamic=[]
    for owner in summary['final_owner_roster']:
        if owner['run_id'] in roots: continue
        if owner['run_id'] not in known: raise ValueError('unproduced owner not eligible for dynamic export')
        did=owner['def_id']; parent=ctx.created.get(did)
        if not parent or parent['kind']!='def' or parent['source_id']!=owner['source_id']:
            raise ValueError('new owner Def/Source not actual-created this invocation')
        dynamic.append(owner)
    def rows(tag,cmd): return W.C._parse_rows_stdout(ctx.sql(tag,cmd),tag)
    controlled=rows('export-whole-controlled',W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'])
    cprows=rows('export-whole-cp',W.C.OLDPG_READONLY_ARGV['cp_owners'])
    expected_root=[r['safe_projection_27'] for r in roots.values() if r['control_mode']=='controlled' and r['expected_state'] in ('running','pending')]
    H.exact(oldrun+expected_root,controlled,'id')
    dynamic_ids=[r['run_id'] for r in dynamic]
    newrows=[]
    if dynamic_ids:
        for rid in dynamic_ids:
            if not H.G.UUID_RE.fullmatch(rid): raise ValueError('bad dynamic UUID')
        cmd=list(W.C.OLDPG_READONLY_ARGV['controlled_rows_27col']); cmd[-1]=cmd[-1].replace("r.control_mode = 'controlled' AND r.status IN ('pending','running')",'r.id IN ('+','.join("'"+r+"'::uuid" for r in dynamic_ids)+')')
        newrows=rows('export-actual-new-safe27',cmd)
    if {r['id'] for r in newrows}!=set(dynamic_ids) or len(newrows)!=len(dynamic_ids): raise ValueError('dynamic actual Run set mismatch')
    actualcp=[r for r in cprows if r['run_id'] in dynamic_ids]
    if {r['run_id'] for r in actualcp}!=set(dynamic_ids): raise ValueError('new CP owner absent')
    H.exact(oldcp+[r['cp_owner_row_full'] for r in roots.values() if r.get('cp_owner_row_full')]+actualcp,cprows,'run_id')
    rootparents={r['parent']['def_id']:r['parent'] for r in roots.values()}; olddefs={r['def_id'] for r in catalog['public_views']}
    dynamic_parents={}
    for owner in dynamic:
        created=ctx.created[owner['def_id']]; src=ctx.created[owner['source_id']]
        if src['kind']!='source' or src['actor']!=created['actor'] or src['tenant']!=created['tenant']: raise ValueError('created source identity drift')
        dynamic_parents[owner['def_id']]={'def_id':owner['def_id'],'org_id':created['tenant'],'source_id':owner['source_id'],'output_dataset_id':created['output_dataset_id'],'conn_id':owner['source_id'],'conn_org_id':created['tenant'],'conn_owner_id':created['actor'],'source_name':src['name'],'connector_type':'rest_api'}
    parents=rows('export-parents',W.build_parents_argv(olddefs|set(rootparents)|set(dynamic_parents)))
    H.exact(list(rootparents.values())+list(dynamic_parents.values()),[r for r in parents if r['def_id'] not in olddefs],'def_id')
    spec=next(s for s in W.build_step_specs(catalog) if s['id']=='sql-cp-parents')
    oldcheck=spec['gate'][1](json.dumps([p for p in parents if p['def_id'] in olddefs]),{'catalog':catalog,'baseline_cp8':derived['cp8']})
    if not oldcheck['ok']: raise ValueError('old full parents changed')
    if newrows:
        cmd=list(W.C.OLDPG_READONLY_ARGV['cp_owners']); cmd=cmd[:cmd.index('-c')]+['-c',"SELECT json_agg(row_to_json(t)) FROM (SELECT id::text,recovery_protocol FROM sync_runs WHERE id IN ("+','.join("'"+r+"'::uuid" for r in dynamic_ids)+")) t;"]
        H.exact([{'id':r,'recovery_protocol':None} for r in dynamic_ids],rows('export-protocol',cmd),'id')
    engine=ctx.engine(); rec=engine.AttemptRecorder(str(ctx.output/'export-public'),'export-public'); api=engine.HttpApi(ctx.bases,__import__('os').environ['OPENFOUNDRY_SMOKE_JWT'],rec,'export-public')
    entries=[]; views=[]
    for row in newrows:
        W.C._check_row_schema(row,'new owner actual',0); did=row['sync_def_id']; p=dynamic_parents[did]
        if row['source_id']!=p['source_id'] or row['output_dataset_id']!=p['output_dataset_id'] or row['requested_by']!=p['conn_owner_id'] or row['org_id']!=p['org_id'] or row['status'] not in ('failed','aborted') or row['gate_entered']!='true': raise ValueError('new tail unknown actual parent/stage mismatch')
        cp=api.call('connector','GET','/api/v1/data-connection/syncs/'+did+'/checkpoint'); runs=api.call('connector','GET','/api/v1/data-connection/syncs/'+did+'/runs')
        if cp['status']!=200 or runs['status']!=200 or (cp['json'].get('owner') or {}).get('run_id')!=row['id']: raise ValueError('new public owner proof failed')
        origin=ctx.output/('actual-safe27-'+row['id']+'.json'); origin.write_text(json.dumps(row,indent=2)+'\n')
        entries.append({'run_id':row['id'],'def_id':did,'source_id':p['source_id'],'tenant':p['org_id'],'actor':p['conn_owner_id'],'control_mode':'controlled','protocol':'legacy-null','purpose':'actual '+known[row['id']].get('source','P25 tail')+' retained unknown; never adopted/cleared','expected_state':row['status'],'pubentered':row['gate_entered'],'safe_projection_27':row,'cp_owner_row_full':next(c for c in actualcp if c['run_id']==row['id']),'parent':p,'evidence':{'path':str(origin),'sha256':H.sha(origin),'json_pointer':''},'actual_creation':ctx.created[did]})
        views.append({'def_id':did,'actor':p['conn_owner_id'],'tenant':p['org_id'],'checkpoint_status':200,'baseline_cp':cp['json'],'baseline_runs':runs['json']})
    output=ctx.output/'typed-new-owners.json'
    output.write_text(json.dumps({'schema':'p26-p25-typed-export-v1','summary':{'path':str(Path(summary_path).resolve()),'sha256':H.sha(summary_path)},'entries':entries,'public_views':views,'whole_controlled_count':len(controlled),'whole_cp_count':len(cprows),'requires_Root_acceptance':True},indent=2)+'\n')
    return output
