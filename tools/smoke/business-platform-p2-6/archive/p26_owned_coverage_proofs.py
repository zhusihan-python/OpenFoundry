"""Narrow combined creation provenance + exact current SAFE-parent reader.

No network. This never fabricates a missing historical POST body or adopts by
owner/name. Future Root-approved collector must supply exact GET originals.
"""
import hashlib,json,re
UUID=re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')

def need(ok,why):
    if not ok:raise ValueError(why)

def pointer(obj,path):
    for part in path.strip('/').split('/'):
        if part:obj=obj[int(part)] if isinstance(obj,list) else obj[part.replace('~1','/').replace('~0','~')]
    return obj

def validate(r,p,source):
    plan=r.doc(p['approved_plan']);need(plan['schema']=='p26-owned-creation-and-current-parent-v1' and plan['source_sha256']==source,'Root exact combined coverage plan/source')
    entries=plan['entries'];ids={x['def_id'] for x in entries};need(entries and len(ids)==len(entries) and not ids.intersection(plan['protected_exclusions']),'duplicate/protected/new Def plan')
    for e in entries:need(all(UUID.fullmatch(e[key]) for key in ('def_id','source_id','output_dataset_id','expected_actor')) and (e['expected_tenant'] is None or UUID.fullmatch(e['expected_tenant'])),'missing exact Root actor/tenant/parent metadata')
    r.rc(p['outer_original_rc']);need(hashlib.sha256(r.read(p['collector_source'])).hexdigest()==p['collector_sha256'],'actual SAFE collector historical source')
    need(set(p['observations'])==ids,'current parent GET scope must equal exact approved IDs')
    def readref(ref):
        raw=r.read(ref['relative_path']);need(hashlib.sha256(raw).hexdigest()==ref['sha256'],'creation original/derived reference changed');return pointer(json.loads(raw),ref.get('json_pointer',''))
    for e in entries:
        did,sid,target=e['def_id'],e['source_id'],e['output_dataset_id'];basis=e['basis']
        if basis['kind']=='full-safe-POST':
            d=readref(basis['definition']);s=readref(basis['source']);need((d['id'],d['source_id'],d['output_dataset_id'])==(did,sid,target) and s['id']==sid and s['owner_id']==e['expected_actor'] and s.get('org_id')==d.get('org_id')==e['expected_tenant'],'original full POST tuple/explicit actor tenant')
        elif basis['kind']=='authenticated-creation-projection':
            d=readref(basis['definition']);s=readref(basis['source']);need((d['id'],d['source_id'],d['output_dataset_id'],d['actor'],d['tenant'])==(did,sid,target,e['expected_actor'],e['expected_tenant']) and (s['id'],s['actor'],s['tenant'])==(sid,e['expected_actor'],e['expected_tenant']),'actual authenticated creation projection')
        else:
            need(basis['kind']=='case-roster-plus-request-status201','unknown creation proof basis')
            case=readref(basis['case']);row=pointer(case,basis['owned_def_pointer']);src=pointer(case,basis['owned_source_pointer'])
            need(row['kind']=='sync_def' and row['id']==did and row['source']==sid and src['kind']=='source' and src['id']==sid,'original case actual owned tuple')
            request=readref(basis['def_request']);need(request['source_id']==sid and request['output_dataset_id']==target,'original Def POST request parent tuple')
            for ref in basis['status_markers']:
                raw=r.read(ref['relative_path']);need(hashlib.sha256(raw).hexdigest()==ref['sha256'] and re.fullmatch(rb'status:201 bytes:[1-9][0-9]*\n',raw),'original POST201 marker; body remains unavailable')
        for kind,rid in [('definition',did),('source',sid)]:
            observation=r.doc(p['observations'][did][kind]);need(observation['schema']=='p26-safe-parent-GET-original-v1' and observation['method']=='GET' and observation['http_status']==200 and observation['transport_outcome']==0 and observation['original_child_rc'] is None,'actual SAFE parent GET status/transport semantics')
            route='/api/v1/data-connection/'+('syncs/' if kind=='definition' else 'sources/')+rid
            need(observation['path']==route and observation['auth_identity']=={'actor':e['expected_actor'],'tenant':e['expected_tenant']},'exact GET route and explicit actor/tenant; no discovery by owner')
            body=observation['safe_body'];need(body['id']==rid and hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest()==observation['safe_body_sha256'],'original SAFE projection changed')
            if kind=='definition':need(body['source_id']==sid and body['output_dataset_id']==target and body.get('org_id')==e['expected_tenant'],'current exact Def parents')
            else:need(body['owner_id']==e['expected_actor'] and body.get('org_id')==e['expected_tenant'] and 'config' not in body,'current Source owner/tenant exact; configuration omitted')
    return {'schema':'p26-verified-combined-parent-coverage-v1','definitions':len(ids),'scope':'current safe parents + explicit actual creation provenance; historical response-body gaps retained','complete_final_epoch':False}
