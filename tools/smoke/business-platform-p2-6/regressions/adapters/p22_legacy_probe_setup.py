"""NEW N-only legacy fixture INSERT used by p22_legacy_probe_entry.py.

Root must release the setup stage and actual HTTP parent creation. Never converts a controlled
Run, never touches a pre-existing Run, and never reuses an old C sample.
"""
import uuid
import json
import hashlib
from pathlib import Path


def insert_sql(owned,run_id,def_id):
    run_id=str(uuid.UUID(run_id)); def_id=str(uuid.UUID(def_id))
    if run_id in owned.runs: raise ValueError('probe Run must be a fresh UUID')
    parent=owned.defs[def_id]
    source=owned.sources[parent['source_id']]; target=owned.datasets[parent['output_dataset_id']]
    for field in ('actor','tenant'):
        if not parent[field] or source[field]!=parent[field] or target[field]!=parent[field]:
            raise ValueError('actual-created parent tenant/actor mismatch')
    actor,org,src,out=[str(uuid.UUID(parent[k])) for k in ('actor','tenant','source_id','output_dataset_id')]
    return ("INSERT INTO sync_runs (id,sync_def_id,status,org_id,owner_id,requested_by,control_mode,recovery_protocol,"
            "cancel_requested_at,publish_entered_at,lease_holder,lease_token,heartbeat_at,lease_expires_at) "
            f"SELECT '{run_id}'::uuid,d.id,'running',d.org_id,c.owner_id,'{actor}'::uuid,'legacy',NULL,NULL,NULL,NULL,NULL,NULL,NULL "
            "FROM batch_sync_defs d JOIN connections c ON c.id=d.source_id "
            f"WHERE d.id='{def_id}'::uuid AND d.source_id='{src}'::uuid AND d.output_dataset_id='{out}'::uuid "
            f"AND d.org_id='{org}'::uuid AND c.org_id='{org}'::uuid AND c.owner_id='{actor}'::uuid "
            "AND d.schedule_enabled=false AND d.schedule_next_fire_at IS NULL RETURNING id::text;")


def insert_new_fixture(facade,def_id):
    """Concrete exact argv builder/executor for a later Root-released setup.

    Input facade Registry must already contain actual HTTP-created Source,
    target and Def. The Root setup collector must subsequently freeze before/
    after whole-legacy safe27, this exact row/protocol/parent/public evidence.
    This function cannot issue an UPDATE/DELETE or modify an existing Run.
    """
    facade.require_phase()
    if (facade.release.get('stage')!='p22-new-legacy-probe-setup'
        or facade.release.get('allow_new_legacy_fixture_insert') is not True):
        raise ValueError('NEW N fixture INSERT stage not released')
    rid=str(uuid.uuid4()); sql=insert_sql(facade.owned,rid,def_id)
    from w10_guard_shim import W, exact
    parent=facade.owned.defs[def_id]
    def query(name,cmd):
        raw,receipt=facade.read_actual(name,cmd)
        begin=raw.find('['); end=raw.rfind(']')
        if begin<0 or end<begin: raise ValueError('expected actual JSON array')
        rows=json.loads(raw[begin:end+1])
        if not isinstance(rows,list): raise ValueError('actual SQL rows not a list')
        return rows,receipt
    def safe27(name,predicate):
        cmd=list(W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'])
        old="r.control_mode = 'controlled' AND r.status IN ('pending','running')"
        if cmd[-1].count(old)!=1: raise ValueError('safe27 helper shape drift')
        cmd[-1]=cmd[-1].replace(old,predicate).replace('SELECT json_agg(row_to_json(t))','SELECT COALESCE(json_agg(row_to_json(t)),\'[]\'::json)',1)
        return query(name,cmd)
    legacy_predicate="r.control_mode = 'legacy' AND r.status IN ('pending','running')"
    before,before_path=safe27('legacy-whole-before',legacy_predicate)
    absent,_=safe27('new-legacy-UUID-absent',"r.id='"+rid+"'::uuid")
    if absent: raise ValueError('new probe UUID already exists; no adoption')
    parents_before,parent_path=query('new-probe-parent-before',W.build_parents_argv([def_id]))
    if len(parents_before)!=1: raise ValueError('actual parent before missing/duplicate')
    actual_parent=parents_before[0]
    for key,expected in {'def_id':def_id,'source_id':parent['source_id'],'conn_id':parent['source_id'],'output_dataset_id':parent['output_dataset_id'],'org_id':parent['tenant'],'conn_org_id':parent['tenant'],'conn_owner_id':parent['actor']}.items():
        if actual_parent[key]!=expected: raise ValueError('actual-created parent drift '+key)
    prefix=list(W.C.OLDPG_READONLY_ARGV['cp_owners'])
    cmd=prefix[:prefix.index('-c')]+['-q','-c',sql]
    facade.new_legacy_insert_attempt={'run_id':rid,'def_id':def_id,'argv':cmd,'acknowledged':False}
    raw,path=facade.read_actual('new-legacy-fixture-insert',cmd)
    facade.new_legacy_insert_attempt.update(evidence_step=path,acknowledged=raw.strip()==rid)
    if raw.strip()!=rid: raise ValueError('NEW legacy fixture INSERT did not return its exact new UUID')
    rows,rowpath=safe27('new-legacy-safe27-after',"r.id='"+rid+"'::uuid")
    if len(rows)!=1: raise ValueError('new fixture row missing/duplicate')
    row=rows[0]; W.C._check_row_schema(row,'new N fixture',0)
    if row['control_mode']!='legacy' or row['status']!='running' or row['sync_def_id']!=def_id:
        raise ValueError('new fixture actual control mode/state/parent drift')
    for key in ('gate_entered','cancel_requested','has_holder','has_token'):
        if row[key]!='false': raise ValueError('new legacy fixture stage/lease fields not NULL')
    protocol_sql="SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT id::text,recovery_protocol FROM sync_runs WHERE id='"+rid+"'::uuid) t;"
    protocol,_=query('new-legacy-protocol-after',prefix[:prefix.index('-c')]+['-c',protocol_sql])
    exact([{'id':rid,'recovery_protocol':None}],protocol,'id')
    parents_after,_=query('new-probe-parent-after',W.build_parents_argv([def_id])); exact(parents_before,parents_after,'def_id')
    after,_=safe27('legacy-whole-after',legacy_predicate); exact(before+[row],after,'id')
    origin=Path(facade.attempt.dir)/'new-legacy-safe27-origin.json'
    origin.write_text(json.dumps(row,indent=2)+'\n')
    evidence={'path':str(origin),'sha256':hashlib.sha256(origin.read_bytes()).hexdigest(),'json_pointer':''}
    entry={'run_id':rid,'def_id':def_id,'source_id':parent['source_id'],'tenant':parent['tenant'],'actor':parent['actor'],'control_mode':'legacy','protocol':'legacy-null','purpose':'N unsupported cancel of NEW legacy fixture','expected_state':'running','pubentered':row['gate_entered'],'safe_projection_27':row,'parent':actual_parent,'evidence':evidence}
    return {'run_id':rid,'def_id':def_id,'source_id':parent['source_id'],'tenant':parent['tenant'],'actor':parent['actor'],
        'control_mode':'legacy','protocol':'legacy-null','insert_evidence_step':path,'typed_entry':entry,
        'legacy_whole_baseline_27':before,'before_step':before_path,'parent_before_step':parent_path,'safe27_after_step':rowpath,
        'status':'actual SQL/parent/protocol whole-set collected; requires public checkpoint/Run baseline and Root acceptance before N'}
