"""Explicit readers for accepted first3/h12/deployment slices; never full-suite acceptance."""
import hashlib
import json
import os
import re


def need(ok, reason):
    if not ok: raise ValueError(reason)


def members(r, directory):
    return sorted(os.listdir(r.path(directory)))


def command_privacy(r, p, source):
    import importlib.util
    path = os.path.join(os.path.dirname(__file__), 'p26_final_history_review.py')
    spec = importlib.util.spec_from_file_location('private_command_reader', path)
    module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module, module.private_command_evidence(r, p, source)


def http_first3(r, p, source):
    privacy, private=command_privacy(r,p,source);private_seen=set()
    d,o=p['inner'],p['outer']
    r.rc(o+'/case.rc');r.read(o+'/case.cmd');r.read(o+'/case.raw');r.read(o+'/case.stdout.raw');r.read(o+'/case.stderr.raw')
    need(r.doc(o+'/case.meta.json')['original_unix_rc']==0,'HTTP original outer rc')
    pins=r.doc(o+'/pins.json');after=r.doc(o+'/case.meta.json') if p.get('epoch_schema')=='S2' else r.doc(o+'/candidate-after.json')
    if p.get('epoch_schema')=='S2':
        need(pins['source_S']==source and after['source_unchanged'] is True and after['tools_unchanged'] is True,'S2 first3 source/tools drift')
        need(pins['frozen_dependencies_sha256']==after['tool_shas_after'] and len(after['inner_attempts'])==1 and after['inner_attempts'][0].endswith('/'+d),'S2 first3 tool/inner linkage')
    need(pins['actual_candidate_hash_before']==after['actual_candidate_hash_after']==source,'HTTP actual source drift')
    need(pins==p['pins'],'HTTP identity pins differ')
    result=r.doc(d+'/RESULT.json');need(result['scenario']==p['case'],'HTTP wrong case')
    metas={}
    for f in members(r,d):
        if not f.endswith('.meta'):continue
        base=d+'/'+f[:-5];m=r.doc(base+'.meta');metas[f]=m
        if privacy.private_command_shape(r,p,source,base+'.cmd',private) is None:r.read(base+'.cmd')
        else:private_seen.add(base+'.cmd')
        raw=r.text(base+'.raw')
        if m.get('rc_kind')=='normalized_harness':
            status=m['original_http_status'];need(type(status)is int and 100<=status<=599 and m['transport_exception'] is None,'HTTP status/transport')
            r.rc(base+'.rc',0 if 200<=status<300 else status)
            need(raw.startswith('status:'+str(status)+' bytes:'+str(m['response_bytes'])), 'HTTP original raw status/bytes mismatch')
            if 'safe_response' in m:
                need(json.loads(raw.split('\n',1)[1])==m['safe_response'], 'HTTP safe raw/meta projection mismatch')
        else:r.rc(base+'.rc')
    def select(fragment):
        return [m for f,m in metas.items() if fragment in f]
    def safe(fragment):
        xs=select(fragment);need(len(xs)==1,'HTTP missing/duplicate input '+fragment);return xs[0]['safe_response']
    need(safe('preflight-fixture-boot')['boot_id']==p['fixture_boot'] and safe('preflight-proxy-boot')['boot_id']==p['proxy_boot'],'HTTP boot changed')
    original_receipt=safe('receipt-proof-receipt-get');txn=original_receipt['transaction'];txmeta=txn['metadata']
    receipt=dict(original_receipt, content_hash=txmeta['content_hash'], txn_status=txn['status'], txn_id=txn['id'], lineage_run_id=txmeta['lineage']['sync_run_id'])
    data=r.read(d+'/'+p['download']);sha=hashlib.sha256(data).hexdigest();rows=len(data.splitlines())
    need(all(json.loads(line) is not None for line in data.splitlines()),'HTTP NDJSON parse')
    need(sha==p['sha256'] and rows==p['rows'] and len(data)==p['bytes'],'HTTP original download hash/rows/size')
    need(receipt['content_hash']==sha and receipt['txn_status']=='COMMITTED','HTTP receipt/content')
    run=receipt['lineage_run_id'];need(receipt['idempotency_key']=='of-p26-pub-v1:'+run,'HTTP receipt key')
    versions=safe('versions-p1')['data'];need(len(versions)==p['versions'] and len({v['transaction_id'] for v in versions})==p['versions'],'HTTP version count')
    need(receipt['txn_id'] in {v['transaction_id'] for v in versions},'HTTP receipt version binding')
    runs=[m['safe_response'] for m in select('run-poll') if m.get('safe_response',{}).get('id')==run]
    need(any(x.get('dataset_version_id')==receipt['txn_id'] and x.get('content_hash')==sha for x in runs),'HTTP run receipt linkage')
    listed=safe('file-list')['files'];matching=[x for x in listed if x['logical_path']=='sync-'+run+'.ndjson'];need(len(listed)==p['versions'] and len(matching)==1 and receipt['txn_id'] in matching[0]['physical_uri'], 'HTTP listed file/Run/transaction binding')
    need(len(original_receipt['files'])==1 and original_receipt['files'][0]['size_bytes']==len(data) and original_receipt['files'][0]['path']=='sync-'+run+'.ndjson', 'HTTP receipt original file binding')
    downloads=select('file-bytes');need(len(downloads)==1 and downloads[0]['response_sha256']==sha and downloads[0]['response_bytes']==len(data),'HTTP download meta binding')
    if p['case']=='healthy_incremental_epoch_once':
        cps=select('cp-poll');epochs=[m['safe_response'].get('confirmed_epoch',0) for m in cps]
        observed=[epoch for i,epoch in enumerate(epochs) if i==0 or epoch!=epochs[i-1]]
        need(cps[0]['original_http_status']==404 and observed==[0,1,2],'HTTP CP initial/epoch once')
        last=cps[-1]['safe_response']['last_confirmed'];need(last['run_id']==run and last['txn_id']==receipt['txn_id'] and last['content_hash']==sha,'HTTP CP receipt binding')
    if p['case']=='dropack_auto_retry_exact_replay':
        logs=select('proxy-log-poll')[-1]['safe_response'];commits=[x for x in logs if x.get('ack_key')==receipt['idempotency_key']]
        need(len(commits)==2 and [x['upstream_status'] for x in commits]==[201,200],'HTTP exact replay statuses')
        need([x['dropped'] for x in commits]==[True,False],'HTTP lost ACK replay')
        for x in commits:
            need(x['request_body_sha256']==x['ack_fingerprint']==receipt['request_fingerprint'] and x['ack_txn_id']==receipt['txn_id'] and x['ack_content_hash']==sha,'HTTP replay fingerprint/receipt')
        need(len({x['ack_sha256'] for x in commits})==1 and commits[0]['ack_sha256']==select('receipt-proof-receipt-get')[0]['response_sha256'],'HTTP same ACK hash')
        counts=[safe(tag+'-fixture-src')['request_count'] for tag in ('04','07','09')];need(counts==(p['source_counts'] if p.get('epoch_schema')=='S2' else [9,12,12]) and counts[1]-counts[0]==3 and counts[2]==counts[1],'HTTP source reread')
        need(safe('11-proxy-stats')['drops']-safe('preflight-proxy-stats')['drops']==1,'HTTP drop delta')
    if p['case']=='healthy_snapshot_and_legacy':
        legacy=select('legacy-no-keyed-receipt');need(len(legacy)==1 and legacy[0]['original_http_status']==404,'HTTP legacy keyed receipt must absent')
    need(safe('cleanup-fixture-quiet')['quiet'] is True,'HTTP fixture not quiet')
    need(safe('cleanup-stats')['commit_inflight']==0 and safe('cleanup-barrier-state')['barrier'] is None,'HTTP cleanup inflight/barrier')
    if p.get('epoch_schema')=='S2':
        cp=r.doc(p['tools_checkpoint']);tool_rows=cp['tools'];pins_tools={x['snapshot']:x['sha256'] for x in tool_rows}
        need(len(tool_rows)==len(pins_tools)==11 and pins_tools==p['historical_tools'],'S2 first3 ten dependencies plus wrapper')
        toolmap={x['source']:x['sha256'] for x in tool_rows if x['source']!='http/run_p26_s2_actual.py'}
        wrapper=[x['sha256'] for x in tool_rows if x['source']=='http/run_p26_s2_actual.py']
        need(toolmap==pins['frozen_dependencies_sha256'] and wrapper==[pins['wrapper_sha256']],'S2 first3 historical tool linkage')
        for name,expected in pins_tools.items():need(hashlib.sha256(r.read(name)).hexdigest()==expected,'S2 historical tool hash')
    else:
        snap=p['tools_snapshot'];manifest=r.doc(snap+'/SNAPSHOT.json');pins_tools=dict(manifest['formal_seven_sha256']);pins_tools[p['wrapper_file']]=manifest['wrapper_sha256']
        need(manifest['formal_seven_sha256']==r.doc(snap+'/CHECKPOINT-first3-evidence-tools.json')['whole_file_sha256'], 'HTTP snapshot/checkpoint seven tool pins differ')
        need(pins_tools==p['historical_tool_pins'], 'HTTP Root historical tool contract changed')
        for name,expected in pins_tools.items():need(hashlib.sha256(r.read(snap+'/'+name)).hexdigest()==expected,'HTTP historical tool hash')
        need(len(pins_tools)==8,'HTTP eight tools required')
    return {'kind':'http-first3','case':p['case'],'outer_unix_rc':0,'steps_rc_kind':'normalized_harness','download_sha256':sha,'rows':rows,'tools':len(pins_tools),'full_HTTP_complete':False,'private_command_bodies_not_read':len(private_seen),'private_command_body_bytes_verified_from_package':not bool(private_seen)}


def h12(r,p,source):
    chains=r.doc(p['compat']+'/original-chains.json');r.doc(p['compat']+'/proof.json')
    for chain,binding in zip(chains,p['chains']):
        if binding.get('outer'):
            outer=binding['outer'];r.rc(outer+'/outer.rc');r.read(outer+'/outer.cmd-before.json');r.read(outer+'/outer.stderr');r.read(outer+'/outer.raw')
            need(binding['inner'].split('/')[-1] in r.text(outer+'/outer.stdout'), 'h12 outer original binding')
        d=binding['inner'];meta=r.doc(d+'/01-go-test.meta');raw=r.text(d+'/01-go-test.raw');name=p['name']
        for suffix in ('.rc','.wrapper.rc','.source-gate.rc','.version-gate.rc','.names-gate.rc'):r.rc(d+'/01-go-test'+suffix)
        need(meta['source_before']==meta['source_after']==source,'h12 original source')
        counts=[len(re.findall(pattern+re.escape(name)+r'(?:\s|$)',raw,re.M)) for pattern in (r'^=== RUN\s+',r'^--- PASS:\s+',r'^--- SKIP:\s+',r'^--- FAIL:\s+')];need(counts==[1,1,0,0],'h12 exact name')
        need(raw.splitlines()[0]==binding['go'],'h12 Go')
        for fname,sha in chain['original_artifact_shas'].items():need(hashlib.sha256(r.read(d+'/'+fname)).hexdigest()==sha,'h12 original hash')
        for suffix in ('.raw','.rc'):need(r.read(p['compat']+'/'+chain['stage']+suffix)==r.read(d+'/01-go-test'+suffix),'h12 copied original bytes')
        kv=dict(line.split('=',1) for line in r.text(p['compat']+'/'+chain['stage']+'.meta').splitlines() if '=' in line);need(kv['source_content_sha256_before']==kv['source_content_sha256_after']==source and kv['original_meta_sha256']==hashlib.sha256(r.read(d+'/01-go-test.meta')).hexdigest() and kv['adapter']=='JSON-original-source-fields-to-legacy-KV-only','h12 derived KV original binding')
    need(len(p['chains'])==3 and len(chains)==3,'h12 exact 3 stage closure')
    return {'kind':'h12','new_normal_race':2,'existing_unified_reused':True,'unified_count_unchanged':114}


def w8(r,p,source):
    d,baseline=p['directory'],p['baseline'];names=members(r,d);rcs=[f for f in names if f.endswith('.rc')]
    need(len(rcs)==131,'W8 exact original rc count')
    for f in rcs:r.rc(d+'/'+f)
    gates=[f for f in names if f.endswith('.gate.json')];need(len(gates)==59,'W8 gates set')
    for f in gates:need(r.doc(d+'/'+f).get('ok') is True,'W8 original gate false')
    # Full raw SQL/public observations and their request/meta inputs, no gate-summary substitute.
    for f in names:
        if f.endswith(('.raw','.cmd.json','.meta.json')):r.read(d+'/'+f)
    compared=[]
    for f in names:
        if f.endswith('.raw') and (any(x in f for x in ('sql-controlled','sql-cp','view-','fixture-counter'))):
            need(r.read(d+'/'+f)==r.read(baseline+'/'+f),'W8 full old safe/parent bytes differ: '+f);compared.append(f)
    need(any('sql-controlled' in x for x in compared) and any('sql-cp' in x for x in compared) and any('view-' in x for x in compared),'W8 old9/8/parent closure absent')
    return {'kind':'w8-preservation-slice','original_rcs':131,'gates':59,'full_old_observations_equal':len(compared),'final_typed_preservation':False}


def deployment(r,p,source):
    d=p['directory'];names=members(r,d)
    need(r.doc(d+'/RESULT.json')['source']==source,'deployment frozen source mismatch')
    for f in names:
        if f.endswith('.rc'):
            expect=1 if f=='collision-ports.rc' else 0;r.rc(d+'/'+f,expect)
            if expect:need(r.read(d+'/collision-ports.stdout')==b'' and r.read(d+'/collision-ports.stderr')==b'','new4 expected lsof empty')
        elif f.endswith(('.stdout','.stderr','.raw','.cmd-before.json','.meta.json')):r.read(d+'/'+f)
    if p['mode']=='switch':
        if p['service']=='dataset':
            need(r.doc(d+'/outer-observed.json')['original_tool_exit_code']==0,'deployment observed outer original rc')
        else:
            outer=p['outer'];r.rc(outer+'/outer.rc');r.read(outer+'/outer.stdout');r.read(outer+'/outer.stderr')
        svc=p['service']
        for other in ('dataset','connector','pipeline','fixture','postgres'):
            before=r.doc(d+'/before-'+other+'.stdout');after=r.doc(d+'/after-'+other+'.stdout')
            if other!=svc:need(before==after,'switch changed other identity')
            else:need(after['image']==p['image'] and after['id']==p['cid'] and before['mounts']==after['mounts'] and before['ports']==after['ports'] and after['running'],'switch image/cid/mounts/ports')
    else:
        for outer in p['outer_dirs']:
            r.rc(outer+'/outer.rc');r.read(outer+'/outer.stdout');r.read(outer+'/outer.stderr')
        for name,expected in p['identities'].items():
            actual=r.doc(d+'/inspect-'+name+'.stdout');need(actual==expected,'new4 safe identity changed')
        need(len(p['identities'])==4,'new4 exact four identities')
    guard=w8(r,p['guard'],source)
    return {'kind':'deployment-slice','mode':p['mode'],'guard':guard,'final_preservation_complete':False}


def oldpre(r,p,source):
    """Accepted oldpre snapshot only; future typed-final preservation remains separate."""
    d=p['directory'];names=members(r,d)
    r.rc(p['outer']+'/outer.rc')
    for f in members(r,p['outer']):
        if f.endswith(('.cmd','.raw','.stdout','.stderr','.json')):r.read(p['outer']+'/'+f)
    for f in names:
        if f.endswith('.rc'):r.rc(d+'/'+f)
        elif f.endswith(('.raw','.cmd.json','.meta.json')):r.read(d+'/'+f)
        elif f.endswith('.gate.json'):need(r.doc(d+'/'+f).get('ok') is True,'oldpre gate false')
    need(sum(f.endswith('.rc') for f in names)==131 and sum(f.endswith('.gate.json') for f in names)==59,'oldpre exact original scope')
    for f,expected in p['whole_sql_snapshots'].items():
        lines=r.text(d+'/'+f).splitlines();payload=json.loads(next(x for x in lines if x.startswith('[')))
        need(payload==expected,'oldpre whole safe snapshot mismatch')
    need(len(p['whole_sql_snapshots']['73-sql-controlled-9.raw'])==9 and len(p['whole_sql_snapshots']['75-sql-cp-owners.raw'])==8,'oldpre exact old9/cp8')
    return {'kind':'w8-oldpre','original_rcs':131,'gates':59,'old_controlled':9,'old_checkpoint_owners':8,'final_typed_preservation':False}
