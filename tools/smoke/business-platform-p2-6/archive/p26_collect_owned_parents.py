"""Root-released exact-ID CURRENT safe parent SELECT. No credential/env reads.

Creates only archive evidence. Source config, auth, lease/token/body columns are
absent from the fixed SELECT. It never discovers IDs by owner/name or mutates DB.
"""
import argparse,datetime,hashlib,importlib.util,json,re,subprocess,sys
from pathlib import Path
UUID=re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
PHASE='p26-current-owned-parent-readonly'
FIELDS={'def_id','org_id','source_id','output_dataset_id','conn_id','conn_org_id','conn_owner_id','source_name','connector_type'}

def need(ok,why='current-parent collector refused'):
    if not ok:raise ValueError(why)

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def validate(release):
    need(release['schema']=='p26-owned-parent-current-release-v1' and release['phase']==PHASE and release['root_accepted'] is True)
    need(datetime.datetime.now(datetime.timezone.utc)<datetime.datetime.fromisoformat(release['expires_utc'].replace('Z','+00:00')))
    need(sha(__file__)==release['collector_sha256'] and sha(release['plan_path'])==release['plan_sha256'])
    plan=json.loads(Path(release['plan_path']).read_text());need(plan['schema']=='p26-owned-creation-and-current-parent-v1' and plan['source_sha256']==release['source_sha256'])
    entries=plan['entries'];ids={x['def_id'] for x in entries};need(entries and len(ids)==len(entries) and not ids.intersection(plan['protected_exclusions']))
    for row in entries:need(all(UUID.fullmatch(row[k]) for k in ('def_id','source_id','output_dataset_id','expected_actor')) and (row['expected_tenant'] is None or UUID.fullmatch(row['expected_tenant'])))
    need(re.fullmatch('[0-9a-f]{64}',release['pg_pin']['cid']) and release['pg_pin']['image'].startswith('sha256:'))
    return plan

def sql_argv(release,plan):
    ids=','.join("'"+x['def_id']+"'::uuid" for x in sorted(plan['entries'],key=lambda x:x['def_id']))
    sql="SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT d.id::text AS def_id,d.org_id::text,d.source_id::text,d.output_dataset_id::text,c.id::text AS conn_id,c.org_id::text AS conn_org_id,c.owner_id::text AS conn_owner_id,c.name AS source_name,c.connector_type FROM batch_sync_defs d JOIN connections c ON c.id=d.source_id WHERE d.id IN ("+ids+") ORDER BY d.id) t;"
    return ['docker','exec',release['pg_pin']['cid'],'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','openfoundry','-d','connectors','-c','BEGIN READ ONLY;','-c',sql,'-c','COMMIT;']

def compare(plan,rows):
    by={x['def_id']:x for x in rows};need(len(by)==len(rows)==len(plan['entries']) and set(by)=={x['def_id'] for x in plan['entries']},'CURRENT exact parent set missing/extra/duplicate')
    for expected in plan['entries']:
        row=by[expected['def_id']];need(set(row)==FIELDS,'unexpected unsafe/missing parent column')
        need(row['source_id']==row['conn_id']==expected['source_id'] and row['output_dataset_id']==expected['output_dataset_id'],'CURRENT parent mismatch')
        need(row['conn_owner_id']==expected['expected_actor'] and row['org_id']==row['conn_org_id']==expected['expected_tenant'],'CURRENT exact approved owner/tenant mismatch')
    return by

def run(path):
    release_bytes=Path(path).read_bytes();release=json.loads(release_bytes);plan=validate(release)
    output=Path(release['output_dir']);need(output.is_absolute() and not output.exists());output.mkdir(parents=True,exist_ok=False)
    p26=Path(__file__).resolve().parents[1];spec=importlib.util.spec_from_file_location('sourcegate',p26/'common/p26_gates.py');G=importlib.util.module_from_spec(spec);spec.loader.exec_module(G)
    need(sha(p26/'common/p26_gates.py')==release['source_gate_sha256'])
    def gate():need(Path(path).read_bytes()==release_bytes and G.candidate_hash(p26.parents[2])==release['source_sha256']);validate(release)
    def command(name,argv):
        gate();(output/(name+'.cmd.json')).write_text(json.dumps(argv,indent=2)+'\n')
        try:cp=subprocess.run(argv,capture_output=True,timeout=120)
        except BaseException:
            (output/(name+'.rc')).write_text('unavailable\n');(output/(name+'.meta.json')).write_text(json.dumps({'original_child_rc':None,'failure_kind':'spawn-or-timeout'})+'\n');raise
        (output/(name+'.meta.json')).write_text(json.dumps({'original_child_rc':cp.returncode,'kind':'Unix-child'})+'\n')
        (output/(name+'.rc')).write_text(str(cp.returncode)+'\n');(output/(name+'.stdout')).write_bytes(cp.stdout);(output/(name+'.stderr')).write_bytes(cp.stderr)
        need(cp.returncode==0,'original safe read command nonzero');return cp.stdout.decode()
    try:
        gate();(output/'source-before.json').write_text(json.dumps({'source':release['source_sha256']}))
        fmt='{"cid":{{json .Id}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}}}'
        before=json.loads(command('PG-before',['docker','inspect',release['pg_pin']['cid'],'--format',fmt]));need(before==dict(release['pg_pin'],running=True),'PG actual identity changed')
        raw=command('CURRENT-exact-parent-SELECT',sql_argv(release,plan));values=[json.loads(x) for x in raw.splitlines() if x.startswith('[')];need(len(values)==1);by=compare(plan,values[0])
        after=json.loads(command('PG-after',['docker','inspect',release['pg_pin']['cid'],'--format',fmt]));need(after==before)
        normalized={'scope':'DERIVED CURRENT safe-parent fragments, not historical POST response bodies','definitions':{did:{'definition':{'id':did,'source_id':r['source_id'],'output_dataset_id':r['output_dataset_id'],'org_id':r['org_id']},'source':{'id':r['conn_id'],'owner_id':r['conn_owner_id'],'org_id':r['conn_org_id']}} for did,r in by.items()}}
        (output/'normalized-current-parent-fragments.json').write_text(json.dumps(normalized,indent=2)+'\n')
        origins={'schema':'p26-current-parent-normalization-ledger-v1','scope':'DERIVED CURRENT fragments plus separately preserved historical creation basis','plan':{'path':release['plan_path'],'sha256':release['plan_sha256']},'original_SELECT':{'stdout':str(output/'CURRENT-exact-parent-SELECT.stdout'),'stdout_sha256':sha(output/'CURRENT-exact-parent-SELECT.stdout'),'original_rc_file':str(output/'CURRENT-exact-parent-SELECT.rc'),'original_rc':0},'derived_file':{'path':str(output/'normalized-current-parent-fragments.json'),'sha256':sha(output/'normalized-current-parent-fragments.json')},'definitions':{e['def_id']:{'source_id':e['source_id'],'output_dataset_id':e['output_dataset_id'],'historical_basis':e,'definition_pointer':'/definitions/'+e['def_id']+'/definition','source_pointer':'/definitions/'+e['def_id']+'/source','derivation':'safe CURRENT parent SQL aliases def_id→id; conn_id→source.id; conn_owner_id→owner_id; conn_org_id→org_id; no historical POST body reconstruction'} for e in plan['entries']}}
        (output/'origin-ledger.json').write_text(json.dumps(origins,indent=2)+'\n')
        gate();(output/'source-after.json').write_text(json.dumps({'source':release['source_sha256']}));(output/'RESULT.json').write_text(json.dumps({'schema':'p26-current-owned-parent-proof-v1','scope':'CURRENT exact ID/parent verification only; historical creation basis remains separate','source_sha256':release['source_sha256'],'plan_sha256':release['plan_sha256'],'definition_ids':sorted(by),'normalized_fragments_sha256':sha(output/'normalized-current-parent-fragments.json'),'SELECT_original_rc':0,'PG_unchanged':True,'historical_POST_bodies_reconstructed':False,'cutoff':plan['cutoff']},indent=2)+'\n');return 0
    except BaseException:
        (output/'FAILED.json').write_text(json.dumps({'ok':False,'kind':'bounded-safe-parent-read-failed','auto_retry':False})+'\n');return 1
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);a=p.parse_args()
    try:rc=run(a.release_file)
    except BaseException:print('current parent collection refused; no private data reported',file=sys.stderr);rc=1
    sys.exit(rc)
