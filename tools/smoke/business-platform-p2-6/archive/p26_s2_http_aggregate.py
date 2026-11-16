"""Additional actual S2 HTTP10 closure, separate CURRENT and boot evidence."""
import hashlib,importlib.util,json,os,shlex
from pathlib import Path


def need(ok,why):
    if not ok:raise ValueError(why)


def command_privacy(r, p, source):
    import importlib.util
    path = os.path.join(os.path.dirname(__file__), 'p26_final_history_review.py')
    spec = importlib.util.spec_from_file_location('private_command_reader', path)
    module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module, module.private_command_evidence(r, p, source)


def complete(r,p,source):
    privacy,private=command_privacy(r,p,source);private_seen=set()
    label=p['canonical_root_label']+'/'
    def local(path):
        need(path.startswith(label),'non-workspace HTTP original reference');return path[len(label):]
    def original(ref):
        path=local(ref['path'])
        if privacy.private_command_shape(r,p,source,path,private,ref) is not None:
            private_seen.add(path);return None  # Original private bytes were not read; only their new exact evidence was verified.
        raw=r.read(path);need(hashlib.sha256(raw).hexdigest()==ref['sha256'] and len(raw)==ref.get('size',ref.get('bytes')),'HTTP original member changed');return raw
    summary=r.doc(p['closure_summary']);need(summary['source_S']==source and len(summary['cases'])==10,'S2 HTTP summary full source/scope')
    cases={x['case']:x for x in p['cases']};members=set();ops=0
    for c in summary['cases']:
        proof=cases[c['case']];need(proof['inner']==local(c['inner']) and proof['outer']==local(c['outer']),'actual HTTP original case tuple')
        for ref in c['original_files']:original(ref);members.add(ref['path'])
        original(c['result']);result=r.doc(proof['inner']+'/RESULT.json');need(result['scenario']==c['case'] and result['outcome']=='pass' and result['cleanup_verified'] is True,'HTTP own original terminal evidence')
        count=proof.get('owned_rc_count',0);need(count==c['owned_actual_unix_rc0_count'],'case original owned process count');ops+=count
    case_private={local(path) for path in members} & private_seen
    need(len(members)==summary['original_member_count']==4906 and ops==434,'S2 exact original4906 and owned434')
    pg=summary['postguard'];r.rc(local(pg['outer'])+'/outer.rc')
    for ref in pg['files']:original(ref)
    before=r.doc(local(pg['outer'])+'/source-before.json');after=r.doc(local(pg['outer'])+'/source-after.json')
    need(source in before.values() and source in after.values(),'postHTTP actual source before/after')
    spec=importlib.util.spec_from_file_location('rt',Path(__file__).with_name('p26_runtime_aggregate.py'));rt=importlib.util.module_from_spec(spec);spec.loader.exec_module(rt)
    baseline=rt.business(r,p['preservation_baseline']);now=rt.business(r,local(pg['inner']));need(now==baseline,'HTTP10 postguard old+typed whole changed')
    # Boot chain observations stay attached to their own time and source step.
    chain=json.loads(original(summary['aux_boot_chain']));need(len(chain)==40,'HTTP exact40 boot observations')
    created={};latest={};actions={};owned_refs=0
    for item in chain:
        kind=item['kind'];facts=item.get('facts')
        if kind=='creation_original':
            need(json.loads(original(item['original']))==facts,'aux2 original creation observation');created[facts['name']]=facts
        elif kind=='actual_owned_original':
            for key in ('meta','cmd','stdout','rc'):original(item[key])
            need(int(original(item['rc']))==0 and json.loads(original(item['meta']))['original_rc']==0,'aux2 original owned process rc')
            owned_refs+=1
            if facts is None:
                cmd=shlex.split(original(item['cmd']).decode().splitlines()[0]);cid=original(item['stdout']).decode().strip()
                need(item['method'] in ('kill','start') and cid in {x['cid'] for x in created.values()} and cmd[0]=='docker' and cmd[1]==item['method'] and cmd[-1]==cid,'aux2 exact original owned lifecycle command')
                actions[cid]=item['method']
                continue
            need(json.loads(original(item['stdout']))==facts,'aux2 original owned identity observation')
            name=facts['name'];need(name in created and facts['cid']==created[name]['cid'] and facts['image']==created[name]['image'] and facts['owner']=='p26-http' and facts['stage']=='p2-6','aux2 original boot parent');latest[name]=facts
        else:
            need(kind=='CURRENT_exact_CID_postHTTP10_not_historical' and json.loads(original(item['original']))==facts,'unknown/fake current boot observation')
            for role,observation in facts['observed'].items():
                name='/p26connector'+role+'2';need(observation['cid']==created[name]['cid'] and observation['image']==created[name]['image'] and observation['running'] is True,'CURRENT same original exact CID/image')
                if name in latest:
                    previous=latest[name]
                    if previous['running'] is False:
                        need(actions.get(observation['cid'])=='start' and observation['started_at']!=previous['started_at'],'CURRENT running state requires original exact-CID start; do not backfill historical boot')
                    else:need(observation==previous,'CURRENT unchanged boot disagrees original observation')
    need(len(created)==2 and owned_refs==37,'aux2 creation/owned boot observation scope')
    for ref in summary['current_aux2_files']:original(ref)
    current_summary=r.doc(p['current_summary']);need(current_summary['scope'].startswith('CURRENT supplemental'),'CURRENT must remain separate from historical')
    for ref in current_summary['originals']:original(ref)
    out=local(current_summary['outer']);d=local(current_summary['inner']);r.rc(out+'/outer.rc');cb=r.doc(out+'/before.json');ca=r.doc(out+'/after.json')
    need(cb==ca['facts'] and cb['source']==source and cb['helper_sha256']==hashlib.sha256(r.read(out+'/helper-source.py')).hexdigest(),'CURRENT actual source/tool/helper drift')
    need(cb['helper_sha256']=='82c3bccbceab2051b787ed98b5220765e8291f61346fdd80214ed784cb6a19db','CURRENT actual fixed helper version')
    expected_tools={x['source']:x['sha256'] for x in r.doc(p['cases'][0]['tools_checkpoint'])['tools']};need(cb['tools']==expected_tools,'CURRENT historical HTTP dependency pins')
    result=r.doc(d+'/RESULT.json');original_case=r.doc(cases['crossscope_permission_legacy_refusals']['inner']+'/RESULT.json')
    need(result['original_result_sha256']==hashlib.sha256(r.read(cases['crossscope_permission_legacy_refusals']['inner']+'/RESULT.json')).hexdigest(),'CURRENT original historical linkage')
    selection=result['selection'];need((selection['dataset'],selection['definition'],selection['run'])==(original_case['dataset_id'],original_case['def_id'],original_case['run_id']),'CURRENT exact historical tuple')
    need(any(x.get('kind')=='source' and x['id']==selection['source'] for x in original_case['owned_roster']),'CURRENT source original creation')
    metas={}
    for f in os.listdir(r.path(d)):
        if not f.endswith('.meta'):continue
        base=d+'/'+f[:-5];m=r.doc(base+'.meta')
        shape=privacy.private_command_shape(r,p,source,base+'.cmd',private)
        if shape is None:cmd_is_get=r.text(base+'.cmd').startswith('GET ')
        else:cmd_is_get=shape['method']=='GET';private_seen.add(base+'.cmd')
        raw=r.text(base+'.raw');metas[f]=m
        if m.get('rc_kind')=='normalized_harness':
            status=m['original_http_status'];r.rc(base+'.rc',0 if 200<=status<300 else status);need(m['transport_exception'] is None and cmd_is_get,'CURRENT actual GET-only transport')
            if 'safe_response' in m:need(json.loads(raw.split('\n',1)[1])==m['safe_response'],'CURRENT raw SAFE projection')
        else:r.rc(base+'.rc')
    def one(tag):
        rows=[m for f,m in metas.items() if tag in f];need(len(rows)==1,'CURRENT missing/duplicate '+tag);return rows[0]
    src=one('fullsafe-source')['safe_response'];definition=one('fullsafe-def')['safe_response'];run=one('fullsafe-run')['safe_response'];rec=one('receipt-proof-receipt-get')['safe_response'];txn=rec['transaction'];md=txn['metadata'];head=one('CURRENT-head')['safe_response']
    need(src['id']==definition['source_id']==selection['source'] and definition['id']==run['sync_def_id']==selection['definition'] and definition['output_dataset_id']==txn['dataset_id']==selection['dataset'],'CURRENT full Source/Def/Run/Dataset parents')
    need(src['owner_id']==run['owner_id']==run['requested_by'] and src['org_id']==definition['org_id']==run['org_id'],'CURRENT full tenant/actor')
    need(run['id']==selection['run'] and run['status']=='succeeded' and run['generation']==3 and run['recovery']['protocol']=='p26-v1' and run['recovery']['state']=='finalized','CURRENT finalized tuple')
    need(rec['idempotency_key']=='of-p26-pub-v1:'+run['id'] and md['lineage']=={'source_id':selection['source'],'sync_def_id':selection['definition'],'sync_run_id':selection['run']},'CURRENT keyed receipt/lineage')
    need(txn['status']=='COMMITTED' and txn['id']==run['dataset_version_id']==run['recovery']['transaction_id']==head['head_transaction_id'],'CURRENT receipt=head=Run')
    versions=one('versions-p1')['safe_response']['data'];need(len(versions)==1 and versions[0]['transaction_id']==txn['id'],'CURRENT exact one version')
    need(one('plain-snapshot-CP404')['original_http_status']==404,'CURRENT ordinary snapshot CP404')
    download=one('file-bytes');data=r.read(d+'/'+download['download_artifact']);digest=hashlib.sha256(data).hexdigest()
    need(digest==download['response_sha256']==md['content_hash']==run['content_hash']==result['file_download_sha256'] and len(data)==md['bytes']==run['bytes_written']==827 and len(data.splitlines())==md['rows']==run['rows_written']==32,'CURRENT full download/receipt/Run bytes/hash/rows')
    for line in data.splitlines():json.loads(line)
    for ref in p['history_originals']:original(ref)
    return {'original_case_members':len(members),'owned_actual_Unix0':ops,'CURRENT_separate':True,'postguard_originals':len(pg['files']),'boot_chain_observations':len(chain),'original_case_body_bytes_read':len(members)-len(case_private),'private_command_bodies_not_read':len(private_seen),'private_command_body_bytes_verified_from_package':not bool(private_seen)}
