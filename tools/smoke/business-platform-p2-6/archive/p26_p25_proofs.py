"""Prepared dedicated P25 original readers. Never import/execute P25 tooling.

Current producers are p26 healthy bundle, h10 runner, tail declaration and
supplements. Full formal legacy remains closed until real inputs bind them.
"""
import ast,hashlib,json,re


def need(ok,why):
    if not ok:raise ValueError(why)


def specs(data):
    tree=ast.parse(data.decode());nodes=[n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='SCENARIOS' for t in n.targets)];need(len(nodes)==1,'P25 original SCENARIOS registry')
    result={}
    for node in nodes[0].elts:
        fields={ast.literal_eval(k):v for k,v in zip(node.keys,node.values)};sid=ast.literal_eval(fields['id']);need(sid not in result,'P25 duplicate scenario source')
        result[sid]={'phase':ast.literal_eval(fields['phase']),'gates':set(ast.literal_eval(fields['gates']))}
    need(set(result)=={'h'+str(i) for i in range(1,16)},'P25 full15 original source registry')
    # These are the actual emitted gate labels accepted by the current NEW
    # healthy producer, which preserves the complete original business details.
    for sid,gates in {'h2':{'G_SNAPSHOT_ONLY'},'h3':{'G_EMPTY_BATCH','G_LEDGER'},'h13':{'G_CAS_ATOMIC','G_OWNER_DURABLE'},'h11':{'G_ACK_STRICT'}}.items():result[sid]['gates']=gates
    need(result['h5']['gates']==set(),'frozen h5 attribution-only registry must remain empty')
    result['h5']['assertion_coverage']=False
    return result


def ref(r,p,entry):
    prefix=p['canonical_root_label']+'/'
    path=entry['path'];need(path.startswith(prefix),'P25 nonworkspace original reference');raw=r.read(path[len(prefix):]);need(hashlib.sha256(raw).hexdigest()==entry['sha256'],'P25 original referenced SHA')
    if 'bytes' in entry or 'size' in entry:need(len(raw)==entry.get('bytes',entry.get('size')),'P25 referenced original size')
    return raw


def good_case(row,spec):
    need(row['failed'] is False and row['phase']==spec['phase'],'P25 original case phase/failure')
    gates=row['gates']
    if row.get('id')=='h5':
        need(gates==[] and spec.get('assertion_coverage') is False and row['owner_roster']==row['run_roster']==[],'h5 attribution only: zero scenario gates/API-derived rosters')
        return
    need(gates,'asserted P25 scenario cannot use an empty gate list')
    names=[g['gate'] for g in gates]
    need(set(names)==spec['gates'] and all(g['ok'] is True for g in gates),'P25 actual mandatory emitted gate set')
    need(all(g.get('detail') for g in gates),'P25 full assertion details absent')


HEALTHY_GATE_COUNTS={'h1':27,'h2':14,'h3':20,'h4':12,'h5':0,'h6':14,'h7':4,'h8':17,'h9':8,'h12':1,'h13':42,'h15':7}

def healthy_case(row,registry):
    sid=row.get('id');need(sid in HEALTHY_GATE_COUNTS and sid in registry,'mandatory healthy case identity')
    good_case(row,registry[sid])
    need(len(row['gates'])==HEALTHY_GATE_COUNTS[sid],'mandatory healthy full gate count')
    if 'assertion_coverage' in row:need(row['assertion_coverage'] is (sid!='h5'),'mandatory healthy assertion coverage flag')


def chain(r,p,c,source,expected_rc):
    r.rc(c['original_rc_file'],expected_rc);runtime=r.doc(c['runtime_result'])
    need(runtime['source_S']==source and runtime['wrapper_exception'] is None and runtime['original_engine_or_runner_rc']==expected_rc,'P25 runtime original source/exception/rc')
    before=r.text(c['source_before']);after=r.text(c['source_after']);need(source in before and source in after,'P25 actual source before/after')
    need(c['original_members'] and c['historical_tools'],'P25 actual command/tool closure required')
    for path,pin in c['original_members'].items():
        raw=r.read(path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['size'],'P25 full command/meta/raw/rc original changed')
    for path,digest in c['historical_tools'].items():need(hashlib.sha256(r.read(path)).hexdigest()==digest,'P25 actually imported historical tool changed')
    need(runtime['runtime_sha256'] in c['historical_tools'].values(),'P25 runtime source must be an actual historical tool blob')
    return runtime


def healthy(r,p,source,registry):
    if p.get('kind')=='p25-healthy-global':
        import importlib.util
        from pathlib import Path
        spec=importlib.util.spec_from_file_location('healthyglobal',Path(__file__).with_name('p26_healthy_global_reader.py'));hg=importlib.util.module_from_spec(spec);spec.loader.exec_module(hg);result=hg.compile_bundle(r,p,source);bundle=json.loads(hg.ref(r,p,p['bundle']));ids={x['id'] for x in bundle['scenarios']};need(result['full_healthy12'] is True and ids=={sid for sid,s in registry.items() if s['phase']=='healthy' and sid!='h10'},'healthy global exact12 source registry')
        for row in bundle['scenarios']:healthy_case(row,registry)
        return ids,bundle
    bundle=r.doc(p['bundle']);need(bundle['schema']=='p26-p25-healthy-bundle-v1' and bundle['failed']==[],'P25 NEW healthy bundle required')
    expected={sid for sid,s in registry.items() if s['phase']=='healthy' and sid!='h10'};rows={x['id']:x for x in bundle['scenarios']};need(set(rows)==expected and len(rows)==len(bundle['scenarios'])==12,'P25 exact healthy12')
    manifest=json.loads(ref(r,p,bundle['source_manifest']));need(set(manifest['selection'])==expected and len(manifest['sources'])==len(bundle['sources']),'P25 healthy actual source selection scope')
    loaded=[]
    for index,descriptor in enumerate(bundle['sources']):
        original=r.doc(p['canonical_to_local'][descriptor['summary_path']]);need(hashlib.sha256(r.read(p['canonical_to_local'][descriptor['summary_path']])).hexdigest()==descriptor['summary_sha256'],'P25 source summary byte linkage')
        rc=manifest['sources'][index]['original_rc'];need(rc in (0,1),'P25 source rc kind');chain(r,p,p['source_chains'][index],source,rc)
        need((original['failed']==[])==(rc==0) and set(original['failed']).issubset(expected),'P25 failed original must remain failed')
        records={}
        for entry in descriptor['evidence_records']:records[entry['path']]=json.loads(ref(r,p,entry))
        for sid,row in ((x['id'],x) for x in original['scenarios']):
            record=descriptor['scenario_records'][sid];need(json.loads(ref(r,p,record))==row,'P25 original case recorder/summary differs')
        handles=r.doc(p['canonical_to_local'][descriptor['handles_file']]);need(handles['count']==len(handles['handles']),'P25 source handles whole count')
        loaded.append(original)
    for sid,row in rows.items():
        good_case(row,registry[sid]);chosen=manifest['selection'][sid];actual=[x for x in loaded[chosen]['scenarios'] if x['id']==sid];need(actual==[row],'P25 selected case is not its own real original')
    failures=[x for x in loaded if x['failed']];need(len(bundle['original_failures_preserved'])==len(failures),'P25 failed source history omitted')
    need(bundle['final_run_roster']==[] and bundle['quiet']['ok'] is True,'P25 healthy final whole run/quiet')
    need('h5' in rows and rows['h5']['gates']==[],'healthy12 includes h5 attribution record without invented assertions')
    return set(rows),bundle


def h10(r,p,source,registry):
    if p.get('kind')=='p25-h10-actual-complete':
        import importlib.util
        from pathlib import Path
        spec=importlib.util.spec_from_file_location('h10freeze',Path(__file__).with_name('p26_h10_freeze_reader.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        result=m.h10_complete(r,p,source)
        return {'h10'},result['binding']

    if p.get('kind')=='p25-h10-actual-crash-component':
        import importlib.util
        from pathlib import Path
        spec=importlib.util.spec_from_file_location('h10actual',Path(__file__).with_name('p26_h10_actual_reader.py'));actual=importlib.util.module_from_spec(spec);spec.loader.exec_module(actual);actual.actual(r,p,source)
        raise ValueError('actual h10 crash verified; independent natural frozen-held component still required; NOTREADY')
    chain(r,p,p['chain'],source,0);driver=r.doc(p['driver_result']);need(driver['ok'] is True,'P25 actual h10 driver failed')
    crash=r.doc(p['crash']);need(crash['schema']=='p25-crash-record-v1' and crash['method']=='sigkill+start-same-image','h10 real crash record')
    need(crash['container_id']==crash['container']==p['cid'] and crash['image_before']==crash['image_after']==crash['image']==p['image'] and crash['started_at_before']!=crash['started_at_after'],'h10 exact owned CID/image/new boot')
    for role in ('kill','start','health'):
        action=crash[role];need(action['rc']==0,'h10 actual original child action rc')
        for member in p['action_originals'][role]:ref(r,p,member)
    prior=r.doc(p['prior']);need(hashlib.sha256(r.read(p['prior'])).hexdigest()==crash['pre_snapshot_sha256'],'h10 actual pre snapshot linkage')
    for phase in ('pre','post'):
        summary=r.doc(p[phase+'_summary']);rows=summary['scenarios'];need(len(rows)==1 and rows[0]['id']=='h10' and summary['failed']==[],'h10 actual pre/post exact one case')
        good_case(rows[0],registry['h10']);need(r.doc(p[phase+'_scenario_record'])==rows[0],'h10 original recorder exact')
    binding=r.doc(p['binding']);need(binding['known_bindings']['h10-held']==crash['held'],'h10 actual binding/held identity')
    frozen=r.doc(p['frozen_held']);need(frozen['frozen_for_next_phase'] is True and frozen['lease_expired_by_DB_clock'] is True and frozen['cp_owner_row_full']['blocked_reason']=='requires_reconciliation','h10 natural expiry/retained owner proof')
    need(p['natural_clock_originals'],'h10 actual natural DB clock rows absent')
    clocks=[r.doc(path) for path in p['natural_clock_originals']];need(any(x['lease_expired'] is False for x in clocks) and clocks[-1]['lease_expired'] is True,'h10 natural DB lease false-to-true')
    return {'h10'},binding


def tail(r,p,source,registry,binding):
    chain(r,p,p['chain'],source,0);summary=r.doc(p['summary']);declaration=r.doc(p['declaration']);bound=r.doc(p['bindings'])
    expected={sid for sid,spec in registry.items() if spec['phase']=='tail'};rows={x['id']:x for x in summary['scenarios']}
    need(summary['phase']=='tail' and summary['failed']==[] and set(rows)==expected and len(rows)==len(summary['scenarios']),'P25 actual exact tail case scope')
    for sid,row in rows.items():good_case(row,registry[sid]);need(r.doc(p['scenario_records'][sid])==row,'P25 original tail scenario recorder')
    digest=lambda x:hashlib.sha256(json.dumps(x,sort_keys=True).encode()).hexdigest()
    need(digest(declaration['spec'])==declaration['declaration_sha256']==bound['declaration_sha256'] and declaration['known_bindings']==binding['known_bindings'],'tail same actual h10 declaration linkage')
    need(bound['summary_sha256']==hashlib.sha256(r.read(p['summary'])).hexdigest() and bound['problems']==[] and bound['bindings']==summary['tail_bindings'] and digest(bound['bindings'])==bound['bindings_sha256'],'tail exact original summary/binding hashes')
    # Whole dual-roster contracts are Root-reviewed complete original arrays,
    # not an ID allowlist; unknown extras and duplicate identities are refused.
    def exact(actual,expected,key):
        need(len({x[key] for x in actual})==len(actual) and sorted(actual,key=lambda x:x[key])==sorted(expected,key=lambda x:x[key]),'tail whole dual roster changed/missing/extra')
    exact(summary['final_owner_roster'],p['owner_rows'],'run_id');exact(summary['final_run_roster'],p['run_rows'],'run_id')
    need(p['whole_roster_originals'],'tail original full owner+run observations absent')
    for path,pin in p['whole_roster_originals'].items():need(hashlib.sha256(r.read(path)).hexdigest()==pin['sha256'],'tail full roster original changed')
    return set(rows)


def full(r,p,source):
    if p.get('contract_version')=='p26-actual-P25-full15-v1':
        import importlib.util
        from pathlib import Path
        spec=importlib.util.spec_from_file_location('fullactual',Path(__file__).with_name('p26_p25_full_reader.py'));actual=importlib.util.module_from_spec(spec);spec.loader.exec_module(actual);return actual.full(r,p,source)
    need(p['epoch']==source and p['sidecar_bases']=={'fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'},'P25 separate current sidecar contract')
    code=r.read(p['engine_source']);need(hashlib.sha256(code).hexdigest()==p['engine_sha256'],'P25 original engine source');registry=specs(code)
    hs,bundle=healthy(r,p['healthy'],source,registry);cs,binding=h10(r,p['h10'],source,registry);ts=tail(r,p['tail'],source,registry,binding)
    need(hs|cs|ts==set(registry) and not hs&cs and not hs&ts and not cs&ts,'P25 exact15 independent scenario coverage')
    import importlib.util
    from pathlib import Path
    h5_legacy=p['h5_legacy_compatibility'];need(h5_legacy['kind']=='legacy-actual-stage' and h5_legacy['suite']=='p22' and h5_legacy['epoch']==source,'h5 attribution needs same-S2 full P22 compatibility proof')
    spec=importlib.util.spec_from_file_location('legacyh5',Path(__file__).with_name('p26_legacy_proofs.py'));legacy=importlib.util.module_from_spec(spec);spec.loader.exec_module(legacy);legacy.actual_stage(r,h5_legacy,source)
    need(set(p['supplements'])=={'h12','h13','h14','quiet','typed_export'},'P25 mandatory supplements exact scope')
    # Dedicated supplement readers are bound to actual raw producer originals
    # next, once they exist; a summary/ID cannot open this barrier.
    raise ValueError('P25 supplement original contracts await real h12/h13/h14/quiet/typed-export inputs; NOTREADY')


def healthy_one(r,p,source):
    """Actual standalone h1..h9 original component; never healthy12/full15."""
    audit=r.doc(p['audit']);sid=p['case'];need(sid in ('h1','h2','h3','h4','h5','h6','h7','h8','h9','h12','h13','h15') and audit['source']==source,'known actual single healthy source')
    need(audit['original_launch_rc']==audit['original_child_rc']==audit['runtime_engine_rc' if sid=='h9' else 'runtime_original_engine_rc']==0,'component audit/actual original rc consistency')
    expected_members={'h1':251,'h2':211,'h3':316,'h4':611,'h5':246,'h6':436,'h7':446,'h8':541,'h9':648,'h12':366,'h13':1242,'h15':541};need(len(audit['files'])==expected_members[sid],'structured original-audit exact member map')
    prefix='tools/smoke/business-platform-p2-6/'
    for path,pin in audit['files'].items():
        raw=r.read(prefix+path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'P25 actual component original SHA/bytes')
    outer=p['outer'];r.rc(p['launch_rc']);r.rc(outer+'/child/rc')
    for side in ('source-before','source-after'):
        r.rc(outer+'/'+side+'/rc');need(source in r.text(outer+'/'+side+'/stdout.log'),'component actual source before/after')
    tools=r.doc(outer+'/tools-before.json');need(tools==r.doc(outer+'/tools-after.json'),'component actual imported source map drift')
    for label,digest in tools.items():need(hashlib.sha256(r.read(outer+'/tools-before/'+label)).hexdigest()==digest,'component historical used tool source')
    argv=r.doc(outer+'/child/cmd.json');need('--only' in argv and argv[argv.index('--only')+1]==sid and '--phase' in argv and argv[argv.index('--phase')+1]=='healthy','single original exact phase/case argv')
    runtime=json.loads(ref(r,p,audit['runtime_terminal']));need(runtime['source_S']==source and runtime['mode']=='engine' and runtime['wrapper_exception'] is None and runtime['original_engine_or_runner_rc']==0 and runtime['runtime_sha256']==tools['regressions/p25/p26_p25_runtime.py'],'component original engine/runtime status/source')
    summary=json.loads(ref(r,p,audit['summary']));need(summary['phase']=='healthy' and summary['failed']==[] and len(summary['scenarios'])==1 and summary['scenarios'][0]['id']==sid,'component exactly one successful original case')
    registry=specs(r.read(outer+'/tools-before/regressions/vendor/p25-runtime/smoke_p25.py'));case=summary['scenarios'][0];healthy_case(case,registry)
    records=[];case_records=[]
    for path in audit['files']:
        if '/engine-evidence/' not in path or not path.endswith('.json') or path.endswith('/handles.json'):continue
        value=r.doc(prefix+path)
        if value==case:case_records.append(path)
        if isinstance(value,dict) and value.get('method'):
            if 'resp_body' in value:
                body=value['resp_body'].encode();need(len(body)==value['resp_bytes'] and hashlib.sha256(body).hexdigest()==value['resp_sha256'],'actual observed HTTP body bytes/hash')
            records.append(value)
    need(len(case_records)==1,'component original scenario-result recorder')
    handles=json.loads(ref(r,p,audit['handles']));need(handles['count']==len(handles['handles'])==({'h1':6,'h2':7,'h3':15,'h4':46,'h5':44,'h6':95,'h7':184,'h8':374,'h9':1459,'h12':2910,'h13':5826,'h15':42}[sid]),'component actual complete owned handle roster')
    created_defs={h['ref'] for h in handles['handles'] if h['kind']=='def' and h['ok'] and h.get('scenario')==sid and not h.get('imported_from')};created_sources={h['ref'] for h in handles['handles'] if h['kind']=='source' and h['ok'] and h.get('scenario')==sid and not h.get('imported_from')};created_runs={h['ref'] for h in handles['handles'] if h['kind']=='run' and h['ok'] and h.get('scenario')==sid and not h.get('imported_from')}
    creations=[json.loads(v['resp_body']) for v in records if v.get('method')=='POST' and v.get('status')==201 and 'resp_body' in v]
    defparents={d['id']:d for d in creations if isinstance(d,dict) and d.get('id') in created_defs}
    sourceparents={d['id']:d for d in creations if isinstance(d,dict) and d.get('id') in created_sources}
    need(set(defparents)==created_defs and set(sourceparents)==created_sources,'actual POST Source/Def parent creation set')
    need(runtime['h10_command_counts']=={'kill':0,'start':0},'healthy component has no owned lifecycle actions')
    bodies=[(v,json.loads(v['resp_body'])) for v in records if v.get('status')==200 and 'resp_body' in v]
    if sid=='h13':
        scheduled={tick['run_id'] for obs,body in bodies if '/schedule/ticks' in obs['path'] and isinstance(body,list) for tick in body if tick.get('sync_def_id') in created_defs and tick.get('outcome')=='enqueued'}
        need(len(scheduled)==3,'h13 exact3 actual enqueued scheduler Run identities');created_runs|=scheduled
    runs={}
    for observation,body in bodies:
        if isinstance(body,dict) and body.get('id') in created_runs and body.get('status')=='succeeded':runs[body['id']]=body
    expected_success={'h1':3,'h2':2,'h3':3,'h4':6,'h5':0,'h6':2,'h7':1,'h8':2,'h9':1,'h12':0,'h13':4,'h15':1}[sid]
    need(len(runs)==expected_success and (set(runs)==created_runs if sid not in ('h4','h8','h9') else set(runs).issubset(created_runs)),'component original successful Run set')
    if sid=='h5':
        need(runtime['created_safe']=={} and not created_defs and not created_sources and not created_runs and not any(v.get('method')=='POST' and v.get('status')==201 for v in records),'h5 attribution only must not invent business calls/creations')
        import importlib.util
        from pathlib import Path
        compat=p['h5_legacy_compatibility'];need(compat['kind']=='legacy-actual-stage' and compat['suite']=='p22' and compat['epoch']==source,'h5 same-S2 full P22 compatibility proof')
        sp=importlib.util.spec_from_file_location('h5legacy',Path(__file__).with_name('p26_legacy_proofs.py'));legacy=importlib.util.module_from_spec(sp);sp.loader.exec_module(legacy);legacy.actual_stage(r,compat,source)
    if sid=='h4':
        failed={body['id']:body for _,body in bodies if isinstance(body,dict) and body.get('id') in created_runs and body.get('status')=='failed'}
        need(len(failed)==6 and set(failed)|set(runs)==created_runs and len(created_defs)==6,'h4 exact6 successful baselines and6 rejected candidates')
        for did in created_defs:
            success=[x for x in runs.values() if x['sync_def_id']==did];bad=[x for x in failed.values() if x['sync_def_id']==did]
            need(len(success)==len(bad)==1 and success[0]['rows_written']==24,'h4 each actual Def baseline/rejection pair')
            fail=bad[0];need(fail['bytes_written']==fail['rows_written']==fail['files_written']==0 and fail['dataset_version_id'] is None and fail['content_hash'] is None and fail['error'] in ('rest sync: completed checkpoint candidate is missing or invalid','rest sync: records returned but completed checkpoint did not advance'),'h4 rejected candidate no output and sanitized error')
            cp=[body for obs,body in bodies if '/syncs/'+did+'/checkpoint' in obs['path'] and isinstance(body,dict) and body.get('confirmed_epoch')==1]
            need(cp and all('owner' not in x and x['last_confirmed']['run_id']==success[0]['id'] for x in cp),'h4 original CP1 preserved/released after rejection')
    ordered=sorted(runs.values(),key=lambda x:x['started_at']);need([run['rows_written'] for run in ordered]==({'h1':[24,20,30],'h2':[24,4],'h3':[24,0,0],'h4':[24]*6,'h5':[],'h6':[12,12],'h7':[24],'h8':[24,24],'h9':[12],'h12':[],'h13':[12,8,12,8],'h15':[12]}[sid]),'component fixture expected row counts')
    observations=[]
    for index,run in enumerate(ordered):
        parent=defparents[run['sync_def_id']];original_source=sourceparents[parent['source_id']]
        need(original_source['owner_id']==run['owner_id']==run['requested_by'] and original_source.get('org_id')==parent.get('org_id')==run.get('org_id'),'successful original Run exact creation parent/actor/tenant')
        if sid in ('h4','h7'):continue  # original h4 records Run/CP; no fabricated Txn/download originals
        txn=next((body for _,body in bodies if isinstance(body,dict) and body.get('id')==run['dataset_version_id'] and 'tx_type' in body),None);need(txn and txn['status']=='COMMITTED' and txn['tx_type']=='SNAPSHOT' and txn['branch_name']=='main','component original flat committed SNAPSHOT/main')
        parent=defparents[run['sync_def_id']];original_source=sourceparents[parent['source_id']];need(original_source['owner_id']==run['owner_id']==run['requested_by'] and original_source.get('org_id')==parent.get('org_id')==run.get('org_id') and txn['dataset_id']==parent['output_dataset_id'],'actual Run/Source/Def/Dataset full creation parent binding')
        meta=txn['metadata'];need(meta['lineage']['source_id']==parent['source_id'],'actual transaction source lineage')
        need(meta['rows']==run['rows_written'] and meta['bytes']==run['bytes_written'] and meta['content_hash']==run['content_hash'] and meta['lineage']['sync_run_id']==run['id'] and meta['lineage']['sync_def_id']==run['sync_def_id'],'component Run/flat transaction full facts')
        files=[f for _,body in bodies if isinstance(body,dict) and isinstance(body.get('files'),list) for f in body['files'] if f.get('transaction_id')==txn['id']]
        ids={f['id'] for f in files};need(len(ids)==1,'component original transaction-bound fileID')
        download=[record for record in records if record.get('status')==200 and any('/files/'+fid+'/download' in record.get('path','') for fid in ids)]
        need(download and all(v['resp_bytes']==run['bytes_written'] and (v['resp_sha256']==run['content_hash'] or (sid=='h3' and v['resp_bytes']==0 and v['resp_sha256'] is None and run['content_hash']==hashlib.sha256(b'').hexdigest())) for v in download),'actual observed download SHA/len versus Run/Txn')
        if sid=='h1':need(run['incremental']['base_epoch']==index,'h1 base epoch0/1/2')
        observations.append({'run':run['id'],'transaction':txn['id'],'rows':run['rows_written'],'observed_download_bytes':run['bytes_written'],'observed_download_sha256':download[0]['resp_sha256'],'run_transaction_sha256':run['content_hash'],'historical_download_body_saved':False})
    if sid=='h1':
        cps=[body for _,body in bodies if isinstance(body,dict) and body.get('confirmed_epoch') in (1,2,3)];need({x['confirmed_epoch'] for x in cps}=={1,2,3} and all('owner' not in x for x in cps),'h1 CP1/2/3 released')
        ledgers=[body for observation,body in bodies if '/admin/log/p25h1' in observation['path']];need(ledgers,'h1 original fixture ledger')
        ledger=max(ledgers,key=len);need(len(ledger)==6,'h1 exact6 page requests')
        need([sum(x['window']==i for x in ledger) for i in (0,1,2)]==[2,3,1],'h1 original page counts2/3/1')
        for i in (0,1,2):
            rows=[x for x in ledger if x['window']==i];want=None if i==0 else 'p25h1-tok-'+str(i-1);need(all(x['token']==want for x in rows) and rows[0]['cursor']=='' and all(x['cursor'] for x in rows[1:]) and len({x['cursor'] for x in rows})==len(rows),'h1 fixed input token/page cursor origin')
    if sid=='h3':
        for did in created_defs:
            rs=sorted([x for x in ordered if x['sync_def_id']==did],key=lambda x:x['started_at'])
            cp=[body for obs,body in bodies if '/syncs/'+did+'/checkpoint' in obs['path'] and isinstance(body,dict) and body.get('confirmed_epoch')==len(rs)]
            need(cp and all('owner' not in x and x['last_confirmed']['run_id']==rs[-1]['id'] for x in cp),'h3 each actual zero-batch CP advanced and released')
        need(sorted(len([x for x in ordered if x['sync_def_id']==d]) for d in created_defs)==[1,2],'h3 existing and first empty batch exact Def scope')
    if sid=='h6':
        need(len(created_defs)==2 and len(created_sources)==1 and len({d['output_dataset_id'] for d in defparents.values()})==2,'h6 shared Source independent two Def/output parents')
        owners={body['owner']['run_id'] for _,body in bodies if isinstance(body,dict) and isinstance(body.get('owner'),dict) and body.get('confirmed_epoch')==0}
        need(owners==created_runs,'h6 both original concurrent owned CP identities')
        busy=[json.loads(x['resp_body']) for x in records if x.get('status')==409 and 'resp_body' in x]
        need(len(busy)==1 and busy[0]['code']=='sync_incremental_busy' and busy[0]['active_run_id'] in created_runs,'h6 exact original busy409/active run')
    if sid=='h7':
        need(len(created_defs)==len(created_sources)==len(created_runs)==1,'h7 exact original created parent scope')
        denied=[x for x in records if x.get('status')==409]
        fences=[x for x in denied if x['method'] in ('PATCH','POST') and json.loads(x['resp_body']).get('error')]
        need(len(fences)==4 and sorted(x['method'] for x in fences)==['PATCH','PATCH','PATCH','POST'],'h7 four original mutation fences409')
        hidden=[x for x in records if x.get('status')==404 and '/syncs/'+next(iter(created_defs)) in x['path']]
        need(len(hidden)==2 and all(x['method']=='GET' for x in hidden),'h7 original tenant checkpoint/run404')
        stale=[json.loads(x['resp_body']) for x in denied if 'resp_body' in x and json.loads(x['resp_body']).get('code')=='sync_incremental_binding_stale']
        need(len(stale)==1,'h7 real binding stale refusal')
    if sid=='h8':
        failed={body['id']:body for _,body in bodies if isinstance(body,dict) and body.get('id') in created_runs and body.get('status')=='failed'}
        need(len(created_defs)==3 and len(created_sources)==3 and len(failed)==3 and set(failed)|set(runs)==created_runs,'h8 original failed3/retried2 exact whole run scope')
        need(sorted(x['error'] for x in failed.values())==sorted(['rest sync: upstream returned status 500']*2+['rest sync: exceeded max_pages limit (1) with another cursor pending — refusing to truncate snapshot']),'h8 actual mid/terminal/maxpages refusal')
        for failure in failed.values():
            need(failure['rows_written']==failure['bytes_written']==failure['files_written']==0 and failure['dataset_version_id'] is None,'h8 failure before publish')
            cp=[body for obs,body in bodies if '/syncs/'+failure['sync_def_id']+'/checkpoint' in obs['path'] and isinstance(body,dict) and body.get('confirmed_epoch')==0 and 'owner' not in body]
            need(cp,'h8 failed batch epoch0 released original checkpoint')
        ledgers=sorted([body for obs,body in bodies if '/admin/log/p25h1' in obs['path']],key=len)
        need(ledgers and len(ledgers[-1])-len(ledgers[0])==8,'h8 original fault/retry four page-pair ledger growth')
        delta=ledgers[-1][len(ledgers[0]):];need(all(x['window']==0 and x['token'] is None for x in delta) and [bool(x['cursor']) for x in delta]==[False,True]*4,'h8 failed/retry full windowA fixed null-token page origins')
    if sid=='h9':
        aborted={body['id']:body for _,body in bodies if isinstance(body,dict) and body.get('id') in created_runs and body.get('status')=='aborted'}
        need(len(created_defs)==len(created_sources)==len(aborted)==1 and set(aborted)|set(runs)==created_runs,'h9 exact fresh cancel/retry Run scope excluding imported history')
        bad=next(iter(aborted.values()));need(bad['rows_written']==bad['bytes_written']==bad['files_written']==0 and bad['dataset_version_id'] is None and bad['content_hash'] is None,'h9 cancellation before publish')
        did=next(iter(created_defs));cp0=[body for obs,body in bodies if '/syncs/'+did+'/checkpoint' in obs['path'] and isinstance(body,dict) and body.get('confirmed_epoch')==0 and 'owner' not in body];need(cp0,'h9 canceled original CP0 released')
        import importlib.util
        from pathlib import Path
        sp=importlib.util.spec_from_file_location('h9drain',Path(__file__).with_name('p26_h9_drain_reader.py'));drain=importlib.util.module_from_spec(sp);sp.loader.exec_module(drain);drain.verify(r,p,audit,runtime,summary,handles)
    if sid in ('h12','h13','h15'):
        import importlib.util
        from pathlib import Path
        sp=importlib.util.spec_from_file_location('latehealthy',Path(__file__).with_name('p26_late_healthy_readers.py'));late=importlib.util.module_from_spec(sp);sp.loader.exec_module(late);late.verify(r,p,source,audit,case,records,bodies,created_defs,created_sources,created_runs,runs)
    need(summary['quiet']['ok'] is True and summary['quiet']['problems']==[] and summary['final_owner_roster']==summary['final_run_roster']==[],'component actual finally whole rosters/quiet')
    export=json.loads(ref(r,p,audit['typed_export']));need(export['entries']==[] and export['public_views']==[],'component no fabricated new held owners')
    return {'assertion_coverage':sid!='h5','same_S2_P22_compatibility_linked':sid=='h5','kind':'p25-actual-healthy-component','case':sid,'original_engine_rc':0,'gate_assertions':len(case['gates']),'download_observations':observations,'historical_download_body_saved':False,'full_healthy12':False,'full_P25_15':False}
