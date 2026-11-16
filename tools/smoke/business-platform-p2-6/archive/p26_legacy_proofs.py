"""Original NEW-copy legacy slice reader; never execute/import legacy source blobs."""
import hashlib
import ast
import json
import os
import re

P1_NAMES={'dataset_isolation','pipeline_isolation','connector_isolation','legacy_regression','sync_real_rest','hash_change','sync_failures','build_positive','build_isolation'}

def need(ok,msg):
    if not ok:raise ValueError(msg)


def p1_slice(r,p,source):
    need(p['slice'] in ('P1pre9','P1post9'),'unknown P1 slice')
    o,d=p['launch'],p['outer'];r.rc(o+'/rc')
    for f in ('cmd.json','stdout.log','stderr.log'):r.read(o+'/'+f)
    for stem in ('source-before','p1-child','source-after'):
        r.rc(d+'/'+stem+'.rc')
        for suffix in ('.cmd.json','.stdout','.stderr'):r.read(d+'/'+stem+suffix)
    for stem in ('source-before','source-after'):
        raw=r.text(d+'/'+stem+'.stdout');need('CANDIDATE_SOURCE_SHA256='+source in raw and 'CANDIDATE_VERIFY=OK' in raw,'P1 actual original S')
    stdout=r.text(d+'/p1-child.stdout');report=r.doc(d+'/p1-report.json')
    names=re.findall(r'^\[smoke\] PASS ([A-Za-z0-9_]+) \(',stdout,re.M)
    need(set(names)==P1_NAMES and len(names)==9 and not re.search(r'^\[smoke\] (?:FAIL|UNCOVERED)',stdout,re.M),'P1 exact named original passes')
    need('[smoke] pass=9 fail=0 uncovered=0' in stdout,'P1 original aggregate')
    need(set(report['results'])==P1_NAMES and all(x['status']=='pass' for x in report['results'].values()) and report['counts']=={'pass':9,'fail':0,'uncovered':0} and report['uncovered']==[],'P1 report exact scope')
    need(report['evidence']==p['business_assertion_inputs'],'P1 original business assertions differ')
    before=r.doc(d+'/tools-before.json');need(before==r.doc(d+'/tools-after.json') and len(before)==28,'P1 exact before/after28 tools')
    snapshot=r.doc(p['tool_blob_ledger']);need(len(snapshot['rows'])==28,'P1 snapshot ledger exact28')
    prefix=p['canonical_root_label']+'/tools/smoke/business-platform-p2-6/'
    need({row['origin'].removeprefix(prefix):row['sha256'] for row in snapshot['rows']}==before,'P1 snapshot historical origin hash map')
    need(set(p['tool_blobs'])==set(before),'P1 exact28 source blob closure missing')
    for tool,path in p['tool_blobs'].items():need(hashlib.sha256(r.read(path)).hexdigest()==before[tool],'P1 actual source blob differs historical SHA')
    adapter=r.doc(p['actual']+'/adapter.json');guard=p['guard'];suite=p['suite_commands']
    # Compare every original command/raw/rc/meta against explicit Root-reviewed original projections.
    for directory in (guard,suite):
        for f in sorted(os.listdir(r.path(directory))):
            if f.endswith('.rc'):r.rc(directory+'/'+f)
            elif f.endswith(('.raw','.json')):r.read(directory+'/'+f)
    for rel,expected in p['original_command_inputs'].items():need(r.text(rel)==expected,'P1 original guard/owned SQL input changed')
    guard_doc=r.doc(guard+'/RESULT.json');need(guard_doc==p['whole_initial_guard'],'P1 whole old safe guard changed')
    need(len(guard_doc['records'])==15,'P1 exact guard records')
    for i,record in enumerate(guard_doc['records'][:7],1):
        base=guard+'/'+str(i).zfill(2)+'-command';need(r.doc(base+'.cmd.json')==record['argv'] and r.text(base+'.raw')==record['stdout']+'\n'+record['stderr'] and record['rc']==0,'P1 guard original cmd/raw/rc binding')
    for i,count in ((5,9),(6,8),(7,8)):
        raw=r.text(guard+'/'+str(i).zfill(2)+'-command.raw');rows=json.loads(next(line for line in raw.splitlines() if line.startswith('[')));need(len(rows)==count,'P1 full old SQL set size')
    for j,record in enumerate(guard_doc['records'][7:]):
        for offset,kind in ((0,'checkpoint'),(1,'runs')):
            base=guard+'/'+str(8+2*j+offset).zfill(2)+'-public-'+kind
            need(r.doc(base+'.meta.json')['status']==record[kind+'_status']==200 and hashlib.sha256(r.read(base+'.raw')[:-1]).hexdigest()==record[kind+'_body_sha256'],'P1 old public raw status/body SHA')
    need(guard_doc['ok'] is True and adapter['initial_guard']['controlled_count']==9 and adapter['initial_guard']['cp_count']==8,'P1 old9CP8 initial guard')
    need(adapter['suite_rc']==0 and adapter['raised'] is None and adapter['cleanup_ok'] is True and adapter['pending_sql_cleanup']==[] and adapter['pending_holders']==0,'P1 owned cleanup incomplete')
    commands=[]
    for f in os.listdir(r.path(suite)):
        if f.endswith('.cmd.json'):
            cmd=r.doc(suite+'/'+f);commands.append(cmd)
    encoded=json.dumps(commands)
    need('ADD CONSTRAINT p1_smoke_block_publish' in encoded and 'DROP CONSTRAINT IF EXISTS p1_smoke_block_publish' in encoded and 'pg_constraint' in encoded,'P1 owned CHECK install/catalog/cleanup originals missing')
    install=[x for x in commands if isinstance(x,dict) and 'ADD CONSTRAINT p1_smoke_block_publish' in json.dumps(x)]
    need(len(install)==1,'P1 CHECK install exact single command')
    need(adapter['suite_sha256']==before['regressions/vendor/tree/business-platform-p1/smoke/smoke.py'],'P1 actual child source blob binding')
    return {'kind':'legacy-p1-slice','slice':p['slice'],'original_unix_rc':0,'named':9,'fail':0,'uncovered':0,'historical_source_blobs':28,'final_legacy_complete':False}


STAGE_NAMES={
    'p1':P1_NAMES,
    'p21':{'heartbeat_progress','lease_loss_fencing','terminal_commit_fault'},
    'p22':{'async_admission_and_completion','running_cancel_cross_process','queue_pending_cancel_and_queuefull','gate_wins_too_late_via_proxy','tenant_and_binding_denials','sigterm_shutdown_no_redispatch','deadline_bounds','sync_disconnect_compat','jwt_expiry_no_fallback','restart_state_preserved','legacy_cancel_unsupported'},
    'p23':{'admission_capacity_shared','admission_release_matrix','admission_min_interval','denial_precede_throttle','admission_config_validation','contention_real_db_lock','public_private_field_sweep','concurrent_inflight_invariant'},
    'p24':{'g1','g2','g3','g4','g5','g6','g7'}}


def actual_stage(r,p,source):
    """Exact current p26_legacy_outer + source/tool/child/raw chain, one suite.

    This is not fulllegacy or ABC last-write acceptance. P25 has its separate
    engine, crash and tail contracts and cannot be passed through this reader.
    """
    suite=p['suite'];need(suite in STAGE_NAMES,'stage reader exact known suite')
    d=p['outer'];r.rc(p['launch_rc']);result=r.doc(d+'/RESULT.json')
    need(result['schema']=='p26-legacy-actual-outer-v1' and result['suite']==suite and result['stage']==p['stage'] and result['candidate_source']==source,'legacy outer suite/source')
    for name in ('source-before','child','source-after'):
        base=d+'/'+name;r.rc(base+'/rc');cmd=r.doc(base+'/cmd.json');raw=r.read(base+'/stdout.log');err=r.read(base+'/stderr.log');meta=r.doc(base+'/meta.json')
        need(meta['original_rc']==0 and meta['stdout_sha256']==hashlib.sha256(raw).hexdigest() and meta['stderr_sha256']==hashlib.sha256(err).hexdigest(),'legacy original child/stdout/stderr linkage')
        if name!='child':need('--expect' in cmd and source in cmd and b'CANDIDATE_VERIFY=OK' in raw and source.encode() in raw,'legacy actual source before/after')
        else:need(cmd==p['actual_child_argv'],'legacy exact original child argv')
    before=r.doc(d+'/tools-before.json');need(before==r.doc(d+'/tools-after.json') and before,'legacy actual tool map changed/empty')
    for tool,digest in before.items():need(hashlib.sha256(r.read(d+'/tools-before/'+tool)).hexdigest()==digest,'legacy historical imported tool blob')
    report=r.doc(p['report']);rows=report['results'];names=STAGE_NAMES[suite]
    need(set(rows)==names and all(x['status']=='pass' for x in rows.values()),'legacy full exact named suite required')
    counts={'pass':len(names),'fail':0}
    if suite=='p1':counts['uncovered']=0
    need(report['counts']==counts and (suite=='p1' or (report['official'] is True and report['partial'] is False)),'legacy original full counts/official')
    evidence=report['expectations' if suite=='p24' else 'evidence'];need(evidence and evidence==p['business_assertion_inputs'],'legacy original full business inputs changed/empty')
    adapter=r.doc(p['adapter']);need(adapter['schema']=='p26-w10-execution-v1' and adapter['suite']==suite and adapter['suite_rc']==0 and adapter['raised'] is None and adapter['abort_reason'] is None,'legacy original adapter process')
    need(adapter['cleanup_ok'] is True and adapter['pending_sql_cleanup']==[] and adapter['pending_holders']==0 and adapter['guard_calls']==adapter['expected_guard_calls'] and adapter['count_ok'] is True,'legacy owned cleanup/guard count')
    need(adapter['suite_sha256']==before[p['suite_source']],'legacy child imported original source SHA')
    # Per-command expected nonzero statuses (locks, negative SQL, HTTP) remain
    # their original integers; they are not converted to generic success.
    need(p['original_commands'],'legacy original business commands absent')
    actual={n[:-3] for n in os.listdir(r.path(p['commands_dir'])) if n.endswith('.rc')}
    need(actual==set(p['original_commands']),'legacy exact command scope')
    for name,expected in p['original_commands'].items():
        base=p['commands_dir']+'/'+name
        literal=r.text(base+'.rc').strip();need(literal==str(expected['rc']),'legacy verbatim original command rc')
        if isinstance(expected['rc'],str):
            meta=r.doc(base+'.meta.json');need(literal=='unavailable' and ('holder-acquire' in name or 'holder-stdout-stream' in name) and meta['original_rc'] is None and (meta.get('child_still_running') is True or meta.get('stream') is True),'only live-holder/stream observations may lack terminal child rc')
        need(r.doc(base+'.cmd.json')==expected['command'] and r.doc(base+'.meta.json')==expected['meta'] and r.read(base+'.raw')==expected['raw'].encode(),'legacy original business command/raw/meta changed')
    need(result['child_original_rc']==result['source_before_original_rc']==result['source_after_original_rc']==0 and result['error'] is None,'legacy outer original status/error')
    audit=r.doc(p['audit'])
    if suite=='p24':audit={'source':r.doc(p['audit_metadata'])['source'],'files':audit}
    need(audit['source']==source,'legacy original audit source')
    expected_members={'p1':512,'p22':1578,'p23':1896,'p24':826}
    need(len(audit['files'])==expected_members[suite],'legacy exact original audit file scope')
    for relative,pin in audit['files'].items():
        path='tools/smoke/business-platform-p2-6/'+relative
        need(not relative.lower().endswith('.env'),'private env cannot be stage evidence')
        raw=r.read(path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'legacy original full member hash/bytes')
    if suite=='p22':
        value=report['evidence']['jwt_expiry_no_fallback']
        need(value['precheck_short_valid_list']==200 and value['admission_status']==202 and value['terminal']=='failed' and value['gate_entered'] is True and '401' in value['failure_detail'] and value['target_txns']=={},'P22L unchanged entered gate then Dataset401 contract')
    if suite=='p23':
        holder=report['evidence']['contention_real_db_lock'];need(holder['holder_pid']>0 and holder['probe_rc']==1 and holder['probe_55p03_line'] is True and '55P03' in holder['probe_err_tail'] and holder['holder_idle_in_transaction']=='1' and holder['holder_release_rc']==0,'P23 real held row/NOWAIT/release contract')
        acquire=[v for n,v in p['original_commands'].items() if 'holder-acquire' in n];release=[v for n,v in p['original_commands'].items() if 'holder-release' in n]
        need(len(acquire)==len(release)==1,'P23 exact real holder acquire/release')
        actual=ast.literal_eval(acquire[0]['raw']);need(actual['backend_pid']==holder['holder_pid'] and actual['client_pid']>0 and 'BEGIN;' in acquire[0]['command']['stdin'] and 'FOR UPDATE;' in acquire[0]['command']['stdin'] and 'SELECT pg_backend_pid()' in acquire[0]['command']['stdin'],'P23 actual Popen SQL holder identity')
        probes=[v for v in p['original_commands'].values() if v['rc']==1 and '55P03' in v['meta'].get('stderr','') and 'FOR UPDATE NOWAIT' in json.dumps(v['command'])]
        need(len(probes)==1 and release[0]['rc']==0 and release[0]['meta']['reaped'] is True and release[0]['meta']['final_process_rc']==0 and release[0]['command']['backend_pid']==holder['holder_pid'] and release[0]['command']['stdin']=='ROLLBACK;\n','P23 NOWAIT original55P03 + actual owned rollback/reaped')
        need(len(audit['snapshots'])==3,'P23 three original safe snapshots')
        for ref in audit['snapshots']:
            path=ref['copy'].removeprefix(p['canonical_root_label']+'/');raw=r.read(path)
            need(hashlib.sha256(raw).hexdigest()==ref['sha256'] and len(raw)==ref['bytes'],'P23 saved snapshot copy bytes')
    if suite=='p24':
        need(adapter['guard_calls']==2 and all(flag in p['actual_child_argv'] for flag in ('--official','--confirm-http','--with-restart-probe')),'P24 full guarded restart/official argument scope')
        cleanup=report['cleanup']['per_sync'];need(len(cleanup)==13 and set(cleanup)==set(report['owned_sync_ids']),'P24 full13 owned finally scope')
        need(all(v['disable_status']==v['revoke_status']==200 and all(v[k] is True for k in ('get_confirmed','persisted_confirmed','grant_gone','runs_terminal')) and v['runs_state_unresolved']=={} for v in cleanup.values()),'P24 original complete per-owned finally proof')
        copies=r.doc(p['sideproof_ledger']);need(len(copies)==6,'P24 exact6 saved safe sideproof copies')
        for row in copies:
            raw=r.read(row['copy'].removeprefix(p['canonical_root_label']+'/'));need(hashlib.sha256(raw).hexdigest()==row['sha256'] and len(raw)==row['bytes'],'P24 sideproof copy original bytes')
    return {'kind':'legacy-actual-stage','suite':suite,'named':len(names),'scope':'one full suite only; not ABC-last or full legacy aggregate'}
