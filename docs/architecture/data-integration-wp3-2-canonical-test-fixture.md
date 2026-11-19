# Canonical runner minimal fixture — source-backed execution recipe

This recipe was corrected after runtime executed the real seed and first scheduled cycle on 2026-11-17. Seed HTTP creation/publish succeeded (201); output rows read returned 200 with A/2 and B/5. Formal V1–V8 two-cycle acceptance is still pending. This documentation task started from read-only source inspection; it did not start a stack, compile Go, copy old results, or change production data. The exact existing recipe is `tools/smoke/business-platform-p1/smoke/smoke.py:1517` (`build_chain`), with helper definitions at lines 501, 510, 727, 738. The P1 source asserts real HTTP statuses, committed branch/schema, and published spec RID. P1 assertions alone do not prove canonical execution. The operative canonical seed is now `tools/smoke/data-integration-wp3-2/canonical_seed.py`; its published identity spec was exercised by runtime. `tenant_build_lifecycle_integration_test.go` exercises real PBS PG but uses an httptest dataset service: its reduced dataset DDL is **not** a real DVS seed.

## Topology and identity

P1 compose uses PBS `DATABASE_URL=.../pipelines`, DVS `DATABASE_URL=.../datasets` on the same PG server; they need not share a database. All scheduling CRUD, scheduler tick/outbox, admission and canonical runner **must use the same PBS database**. All input/output dataset CRUD, branches, uploads, resolution and outputs:commit must target the **same DVS instance/database and storage root**. Set PBS `DATASET_SERVICE_URL` to that DVS HTTP base; do not point it at the PBS endpoint. Both services use the same `JWT_SECRET`. P1 also supplies `DATASET_SERVICE_BEARER`; canonical lifecycle HTTP requests propagate the caller grant JWT (main.go:119), so an unrelated service identity cannot substitute for its tenant.

Mint a fresh HS256 token with `tools/smoke/business-platform-p1/mint_token.py --secret "$TEST_JWT_SECRET" --sub "$SUBJECT_UUID" --org "$ORG_UUID" --roles admin --hours 2`. Its default permissions are `dataset.write,dataset:write`; claims include sub, jti, iat, exp. For personal omit `--org`; use the same subject throughout. Admin is convenient for setup but does not widen personal ownership. Token must remain unexpired through scheduled execution; schedule grant token must equal the caller domain and subject. Do not read a production .env or reuse expired archived JWTs.

## Execute minimal HTTP seed

Set `DVS`, `PBS`, `TOKEN` to the dedicated test services and freshly minted token. The following Python uses only standard-library HTTP, creates two real datasets, main branches, CSV rows, input commit, pipeline and published spec. IDs are response IDs; do not synthesize dataset RIDs from UUIDs. The identity JobSpec below matches the runtime-executed seed.

```python
import json, os, urllib.request, urllib.error, uuid
DVS, PBS, TOKEN = (os.environ[k].rstrip('/') for k in ('DVS','PBS','TOKEN'))
def call(base,path,body=None,raw=None,ctype='application/json',allowed=(200,201)):
    data=raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req=urllib.request.Request(base+path,data=data,headers={'Authorization':'Bearer '+TOKEN,'Content-Type':ctype},method='POST' if data is not None else 'GET')
    try:
        with urllib.request.urlopen(req) as r: status,buf=r.status,r.read()
    except urllib.error.HTTPError as e: status,buf=e.code,e.read()
    assert status in allowed,(path,status,buf)
    return json.loads(buf) if buf else {}
suffix=uuid.uuid4().hex[:8]
inp=call(DVS,'/v1/datasets',{'name':'sched-in-'+suffix,'format':'csv'})
out=call(DVS,'/v1/datasets',{'name':'sched-out-'+suffix,'format':'json'})
for ds in (inp,out):
    call(DVS,f"/v1/datasets/{ds['id']}/branches",{'name':'main'},allowed=(200,201,409))
csv=b'\xef\xbb\xbfsku,qty\nA,2\nB,5\n'
boundary='Fixture'+suffix
raw=(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="inventory.csv"\r\nContent-Type: text/csv\r\n\r\n'.encode()+csv+f'\r\n--{boundary}--\r\n'.encode())
up=call(DVS,f"/v1/datasets/{inp['id']}/upload",raw=raw,ctype='multipart/form-data; boundary='+boundary)
physical=up.get('physical_uri','')
assert physical,up
storage=physical[len('local:///'):] if physical.startswith('local:///') else physical
commit=call(DVS,f"/v1/datasets/{inp['id']}/outputs:commit",{
 'branch':'main','transaction_type':'SNAPSHOT','summary':'canonical fixture',
 'schema':{'fields':[{'name':'sku','type':'string'},{'name':'qty','type':'integer'}],'file_format':'TEXT'},
 'files':[{'logical_path':up.get('path') or 'inventory.csv','size_bytes':up.get('size_bytes') or len(csv),'storage_path':storage}]})
assert commit.get('branch')=='main',commit
call(DVS,f"/v1/datasets/{inp['id']}/schema?branch=main")
call(DVS,f"/v1/datasets/{inp['id']}/files/rows?branch=main&mode=preview&limit=10")
nodes=[{'id':'input','label':'Dataset','transform_type':'dataset_input','input_dataset_ids':[inp['id']],'config':{'source_kind':'dataset','dataset_id':inp['id']}},
 {'id':'filter','label':'Quantity over 3','transform_type':'filter','depends_on':['input'],'config':{'predicate':'qty > 3'}},
 {'id':'output','label':'Filtered inventory','transform_type':'output_dataset','depends_on':['filter'],'output_dataset_id':out['id'],'config':{}}]
pipeline=call(PBS,'/api/v1/pipelines',{'name':'sched-pipe-'+suffix,'pipeline_type':'FASTER','nodes':nodes})
spec=call(PBS,'/v1/job-specs/TRANSFORM',{'pipeline_rid':pipeline['id'],'branch_name':'main','inputs':[{'dataset_rid':inp['id']}],'output_dataset_rids':[out['id']],'logic_payload':{'transform_type':'dataset_input','dataset_rid':inp['id']}},allowed=(201,))
assert spec.get('rid'),spec
print(json.dumps({'input':inp,'output':out,'pipeline':pipeline,'spec':spec,'branch':'main'}))
```

No output file upload or output transaction should be pre-created: canonical reservation owns output transactions. Creating only the pipeline is insufficient; publish TRANSFORM separately. Do not invoke legacy `/runs` as evidence that the scheduled canonical runner ran.

## Schedule and evidence

Use the returned pipeline `id` as `pipeline_rid`, branch `main`. POST `/api/v1/scheduling/grants` with `{"schedule_key":"fixture-<unique>","pipeline_rid":"<id>","token":"<same JWT>"}`; capture id/generation. POST `/api/v1/scheduling/definitions` with that key/pipeline, `build_branch:"main"`, `cron:"* * * * *"`, `grant_id` and exact `grant_generation`. These scheduling APIs have real HTTP/JWT/PG coverage in D evidence `10-api-sol-test.txt`, exit 0; runtime has now executed this identity fixture through the first scheduled cycle. Raw evidence: `/private/tmp/ofwp32-runtime-sol-evidence/first-scheduled-dispatch.json` and `/private/tmp/ofwp32-runtime-sol-evidence/first-scheduled-product-http.json`. The product HTTP evidence shows status 200 and exactly rows `[{qty:2,sku:A},{qty:5,sku:B}]`. This does not claim formal two-cycle V1–V8 completion.

Observe the producer tick → CDC/Kafka → dispatch → canonical runner. Main wires Store/JobSpecs/Versioning/Locks/Builds/Plans to the production ports and NodeRunner/MetadataCommitter/MetadataTransactions plus DVSBaseURL. Require nonempty actual output rows (`A,2` and `B,5` for this identity fixture), committed DVS output transaction, scheduling dispatch terminal success and receipt/outcome evidence; an enabled schedule or a created build alone does not prove execution. Preserve raw HTTP statuses/bodies with JWTs redacted and exact process exit codes. No raw SQL fixture was executed in this task; API seed avoids bypassing dataset storage/branch/versioning invariants.

## Canonical payload correction and execution boundary

Canonical dispatch uses `logic_payload.transform_type`; the original P1 `kind` payload covered resolution/build smoke and is not the canonical node protocol. A single published filter spec has no predecessor rows merely because the editor pipeline JSON contains input/filter/output nodes. The identity spec above intentionally reads the dataset directly; the editor DAG does not turn it into a filter. To test filtering, publish two JobSpecs with an intermediate dataset dependency: a `dataset_input` spec writes the intermediate dataset, then a filter spec consumes that intermediate dataset and writes the final output. That two-spec shape has not been executed by this recipe and must be verified separately before claiming filter coverage.

Scheduling uses its own reservation handoff. Verify the manual path with a separate fixture so the scheduled cycle has its expected starting head. The WP3.4 smoke runs ordinary `POST /api/v1/execute` with only the persisted Build ID, checks the pinned rows and matches the committed dataset transaction to the Build's job output. The earlier F15 reproduction returned HTTP 200 with `BUILD_FAILED` after dataset versioning returned a 409.

Manual execution over the dataset HTTP adapter requires the Build creator's dataset-write permission and the output dataset's active branch. Build creation records a build-bound reservation. The keyed output commit verifies its dataset, branch, creator and Build binding, then exchanges it for the published snapshot within the same database transaction. Existing ordinary Builds with unmarked reservations can be aborted and recreated under the new protocol. If publication has landed before the build service can record the acknowledgement, repeating execute looks up the durable receipts. A Build with unresolved reservations can be aborted through the Build API, which reconciles receipts before finalizing any outstanding reservation.
