"""Actual whole old+Root typed+one registered h10 held Run preservation.

No Root release is forged/rewritten. Dynamic h10 identity is derived from the
current runner's actual successful create/admission receipts, or an explicit
Root-pinned post-h10 anchor in the separate read-only freeze phase.
"""
import hashlib,json,re,os
from datetime import datetime
from pathlib import Path


class RetryableClockObservation(ValueError):
    """Only the proven expiry reason moved between SQL and public GET."""


def read_sql_bindings(bindings):
    from p26_p25_runtime import H
    observed={}
    for key in ('controlled','CP','parents','protocol'):
        proof=bindings[key]
        if proof['original_rc']!=0: raise ValueError('pre-kill SQL original rc not zero')
        for suffix,member in proof['files'].items():
            f=Path(member['path'])
            if f.is_symlink() or not f.resolve().is_relative_to(H.P26/'results/w10') or H.sha(f)!=member['sha256']: raise ValueError('pre-kill SQL receipt drift')
        if Path(proof['files']['.rc']['path']).read_text().strip()!='0' or json.loads(Path(proof['files']['.cmd.json']['path']).read_text())!=proof['argv']: raise ValueError('pre-kill actual command/rc binding')
        observed[key]=H.W.C._parse_rows_stdout(Path(proof['files']['.raw']['path']).read_text(),'pre-kill-'+key)
    return observed


def read_pre_kill_anchor(ctx,ref):
    from p26_p25_runtime import H
    path=Path(ref['path'])
    if not path.is_absolute() or path.is_symlink() or not path.resolve().is_relative_to(H.P26/'results/w10') or H.sha(path)!=ref['sha256']:
        raise ValueError('missing/changed pre-kill anchor')
    anchor=json.loads(path.read_text())
    if anchor.get('schema')!='p26-h10-pre-kill-anchor-v1' or anchor['source_S']!=ctx.release['candidate_source']: raise ValueError('pre-kill anchor schema/source')
    observed=read_sql_bindings(anchor['sql_bindings'])
    rid=anchor['row']['id'];did=anchor['parent']['def_id']
    if [r for r in observed['controlled'] if r['id']==rid]!=[anchor['row']] or [r for r in observed['CP'] if r['run_id']==rid]!=[anchor['cp_owner_row_full']] or [r for r in observed['parents'] if r['def_id']==did]!=[anchor['parent']] or observed['protocol']!=[anchor['protocol_clock']]:
        raise ValueError('pre-kill raw SQL projection differs from anchor')
    return anchor


def compare_pre_kill(anchor,row,owner,parent,protocol):
    if row!=anchor['row'] or parent!=anchor['parent'] or protocol['id']!=row['id'] or protocol['recovery_protocol']!=anchor['protocol_clock']['recovery_protocol']:
        raise ValueError('full pre-kill row/parent/protocol drift')
    if type(protocol.get('expired')) is not bool: raise ValueError('actual DB expiry evidence absent')
    if anchor['protocol_clock']['expired'] is True and protocol['expired'] is not True: raise ValueError('DB expiry regressed')
    expected=dict(anchor['cp_owner_row_full'])
    if owner.get('blocked_reason')!=expected.get('blocked_reason'):
        if expected.get('blocked_reason')=='' and owner.get('blocked_reason')=='requires_reconciliation' and protocol['expired'] is True:
            expected['blocked_reason']='requires_reconciliation'
        else: raise ValueError('CP reason changed without permitted DB expiry transition')
    if owner!=expected: raise ValueError('full pre-kill CP owner drift')


def guard_held(ctx,stage,export=False):
    from p26_p25_runtime import H,claims
    W=H.W; ctx.gate(); sql_receipts={}
    root=json.loads(Path(ctx.release['typed_roster']).read_text())
    if H.sha(ctx.release['typed_roster'])!=ctx.release['typed_roster_sha256']: raise ValueError('Root typed bytes drift')
    if H.G.candidate_hash(H.G.OPENFOUNDRY)!=ctx.release['candidate_source']: raise ValueError('actual source changed')
    if not hasattr(ctx,'preservation_baseline'): ctx.preservation_baseline=W.load_baseline(H.G.P25_FINAL_DIR)
    _,catalog,derived=ctx.preservation_baseline
    catalog['public_views']=W.public_view_baselines(derived,catalog)
    if ctx.runner is not None:
        held=ctx.runner.held; did,sid,rid=[held[k] for k in ('def_id','source_id','run_id')]
        for key,kind in ((did,'def'),(sid,'source'),(rid,'run')):
            if ctx.created.get(key,{}).get('kind')!=kind: raise ValueError('h10 held ID not actual-created here '+kind)
        target=ctx.created[did]['output_dataset_id']; name=ctx.created[sid]['name']
        identity=claims(__import__('os').environ['OPENFOUNDRY_SMOKE_JWT'])
        if any(ctx.created[x]['actor']!=identity['actor'] or ctx.created[x]['tenant']!=identity['tenant'] for x in (did,sid,rid)):
            raise ValueError('h10 actual create actor/tenant mismatch')
    else:
        anchor=ctx.release['p25_dynamic_hold']
        if H.sha(anchor['origin_report'])!=ctx.release['p25_inputs'].get(anchor['origin_report']): raise ValueError('Root h10 report not pinned')
        origin=json.loads(Path(anchor['origin_report']).read_text())['typed_entry']
        if any(origin[k]!=anchor[k] for k in ('def_id','source_id','run_id','actor','tenant')) or origin['safe_projection_27']!=anchor['safe_projection_27'] or origin['parent']['output_dataset_id']!=anchor['output_dataset_id'] or origin['parent']['source_name']!=anchor['source_name']:
            raise ValueError('Root dynamic hold tuple differs from actual h10 exported origin')
        did,sid,rid,target,name=[anchor[k] for k in ('def_id','source_id','run_id','output_dataset_id','source_name')]
        identity={'actor':anchor['actor'],'tenant':anchor['tenant']}
        origin_doc=json.loads(Path(anchor['origin_report']).read_text())
        if origin_doc.get('pre_kill_anchor')!=anchor.get('pre_kill_anchor') or origin_doc['typed_entry']['cp_owner_row_full']!=anchor.get('cp_owner_row_full'):
            raise ValueError('Root freeze full CP/pre-kill origin binding missing or changed')
        ctx.pre_kill_anchor_ref=anchor['pre_kill_anchor']
        if ctx.release['p25_inputs'].get(ctx.pre_kill_anchor_ref['path'])!=ctx.pre_kill_anchor_ref['sha256']: raise ValueError('Root pre-kill anchor not pinned')
    pre_anchor=None
    if stage!='immediately-before-kill':
        if not hasattr(ctx,'pre_kill_anchor_ref'): raise ValueError('pre-kill anchor required before post/freeze')
        pre_anchor=read_pre_kill_anchor(ctx,ctx.pre_kill_anchor_ref)
        if ctx.runner is None:
            suffixes={'controlled':'whole-controlled','CP':'whole-CP','parents':'parents','protocol':'actual-protocol-clock'};bindings={}
            for key,suffix in suffixes.items():
                matches=[v for k,v in origin_doc['actual_observation_sql_receipts'].items() if k.endswith('-'+suffix)]
                if len(matches)!=1: raise ValueError('origin export SQL observation missing/ambiguous')
                bindings[key]=matches[0]
            observed=read_sql_bindings(bindings)
            clock={'id':rid,'recovery_protocol':None,'expired':origin_doc['lease_expired_by_DB_clock']}
            if [r for r in observed['controlled'] if r['id']==rid]!=[origin['safe_projection_27']] or [r for r in observed['CP'] if r['run_id']==rid]!=[origin['cp_owner_row_full']] or [r for r in observed['parents'] if r['def_id']==did]!=[origin['parent']] or observed['protocol']!=[clock]:
                raise ValueError('origin exported full row/CP differs from actual SQL receipts')
            compare_pre_kill(pre_anchor,origin['safe_projection_27'],origin['cp_owner_row_full'],origin['parent'],clock)
    for value in (did,sid,rid,target,identity['actor'],identity['tenant']):
        if not H.G.UUID_RE.fullmatch(value): raise ValueError('unsafe held UUID')
    rec=json.loads(Path(ctx.release['deployment_record']).read_text())
    pins=rec['containers']
    if rec.get('source_sha256')!=ctx.release['candidate_source'] or not {W.Q3_FIXTURE,W.Q3_DATASET,W.Q3_PIPELINE,W.Q3_CONNECTOR,ctx.target}.issubset(pins):
        raise ValueError('held guard requires full source/base3/oldfixture/NEW C pins')
    for cname,pin in pins.items():
        if not re.fullmatch('[0-9a-f]{64}',pin['cid']) or not re.fullmatch('sha256:[0-9a-f]{64}',pin['image']) or not pin.get('started_at'):
            raise ValueError('held guard full CID/image/boot required')
        if cname==W.Q3_FIXTURE and pin['started_at']!=W.EXPECTED_FIXTURE_STARTED_AT:
            raise ValueError('held guard old fixture boot pin drift')
    if H.sha(ctx.release['deployment_record'])!=ctx.release['deployment_record_sha256']: raise ValueError('deployment pin drift')
    for cname,pin in pins.items():
        if stage=='immediately-before-kill' and cname!=ctx.target: continue
        expected=ctx.current_c_pin if cname==ctx.target and hasattr(ctx,'current_c_pin') else pin
        command=['docker','inspect',cname,'--format','{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}']
        cp=ctx.command('held-'+stage+'-identity',command,30)
        if cp.returncode or cp.stdout.decode().strip().splitlines()!=[expected['cid'],expected['image'],expected['started_at'],'true']:
            raise ValueError('current preservation container identity drift '+cname)
    def sql_rows(tag,command):
        raw=ctx.sql(tag,command)
        proof=getattr(ctx,'last_command_evidence',None)
        if not proof or proof['argv']!=list(command) or proof['original_rc']!=0: raise ValueError('actual SQL receipt binding required')
        sql_receipts[tag]=json.loads(json.dumps(proof))
        return W.C._parse_rows_stdout(raw,tag)
    if root.get('schema')!='p26-w10-typed-v1': raise ValueError('Root typed schema')
    if root['runs']:
        ids=[]
        for entry in root['runs']:
            if not H.G.UUID_RE.fullmatch(entry['run_id']): raise ValueError('unsafe Root typed Run UUID')
            evidence=entry['evidence']
            if H.sha(evidence['path'])!=evidence['sha256']: raise ValueError('Root typed original evidence drift')
            original=json.loads(Path(evidence['path']).read_text())
            for part in evidence.get('json_pointer','').strip('/').split('/'):
                if part: original=original[int(part)] if isinstance(original,list) else original[part.replace('~1','/').replace('~0','~')]
            if original!=entry['safe_projection_27']: raise ValueError('Root typed original safe27 differs')
            W.C._check_row_schema(entry['safe_projection_27'],'Root typed',0)
            ids.append(entry['run_id'])
        command=list(W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'])
        predicate="r.control_mode = 'controlled' AND r.status IN ('pending','running')"
        if command[-1].count(predicate)!=1: raise ValueError('safe27 predicate drift')
        values=','.join("'"+r+"'::uuid" for r in ids)
        command[-1]=command[-1].replace(predicate,'r.id IN ('+values+')')
        actualtyped=sql_rows('held-'+stage+'-Root-typed-full27',command)
        H.exact([e['safe_projection_27'] for e in root['runs']],actualtyped,'id')
        command=list(W.C.OLDPG_READONLY_ARGV['cp_owners']);command=command[:command.index('-c')]+['-c',"SELECT json_agg(row_to_json(t)) FROM (SELECT id::text,recovery_protocol FROM sync_runs WHERE id IN ("+values+")) t;"]
        protocols=sql_rows('held-'+stage+'-Root-typed-protocol',command)
        H.exact([{'id':e['run_id'],'recovery_protocol':None if e['protocol']=='legacy-null' else e['protocol']} for e in root['runs']],protocols,'id')
    current=sql_rows('held-'+stage+'-whole-controlled',W.C.OLDPG_READONLY_ARGV['controlled_rows_27col'])
    actual=next((r for r in current if r['id']==rid),None)
    if not actual: raise ValueError('actual held controlled Run absent')
    W.C._check_row_schema(actual,'held actual',0)
    expected_fields={'sync_def_id':did,'source_id':sid,'output_dataset_id':target,'source_name':name,'org_id':identity['tenant'],'requested_by':identity['actor'],'owner_id':identity['actor'],'control_mode':'controlled','status':'running','gate_entered':'false'}
    if any(actual[k]!=v for k,v in expected_fields.items()): raise ValueError('actual held Run shape/parent/actor changed')
    if pre_anchor is not None:
        H.exact([pre_anchor['row']],[actual],'id')
    oldrows=[r['public_projection_27'] for r in derived['protected9']]
    roots=[r['safe_projection_27'] for r in root['runs'] if r['control_mode']=='controlled' and r['expected_state'] in ('pending','running')]
    if any(r['id']==rid for r in roots): raise ValueError('dynamic held must be declared once, not duplicated into Root whole roster')
    H.exact(oldrows+roots+[actual],current,'id')  # includes all shadow extras
    cprows=sql_rows('held-'+stage+'-whole-CP',W.C.OLDPG_READONLY_ARGV['cp_owners'])
    owner=next((r for r in cprows if r['run_id']==rid),None)
    if not owner or any(owner[k]!=v for k,v in {'sync_def_id':did,'source_id':sid,'output_dataset_id':target,'run_status':'running','mode':'cursor'}.items()): raise ValueError('held full CP owner binding changed')
    H.exact([r['cp_owner_row_full'] for r in derived['cp8']]+[r['cp_owner_row_full'] for r in root['runs'] if r.get('cp_owner_row_full')]+[owner],cprows,'run_id')
    rootparents={r['parent']['def_id']:r['parent'] for r in root['runs']}
    parent={'def_id':did,'org_id':identity['tenant'],'source_id':sid,'output_dataset_id':target,'conn_id':sid,'conn_org_id':identity['tenant'],'conn_owner_id':identity['actor'],'source_name':name,'connector_type':'rest_api'}
    olddefs={v['def_id'] for v in catalog['public_views']}
    allparents=sql_rows('held-'+stage+'-parents',W.build_parents_argv(olddefs|set(rootparents)|{did}))
    expected_ids=olddefs|set(rootparents)|{did}
    if len(allparents)!=len(expected_ids) or {p['def_id'] for p in allparents}!=expected_ids: raise ValueError('parent union extra/missing')
    H.exact(list(rootparents.values())+[parent],[p for p in allparents if p['def_id'] not in olddefs],'def_id')
    spec=next(s for s in W.build_step_specs(catalog) if s['id']=='sql-cp-parents')
    check=spec['gate'][1](json.dumps([p for p in allparents if p['def_id'] in olddefs]),{'catalog':catalog,'baseline_cp8':derived['cp8']})
    if not check['ok']: raise ValueError('old safe parents drift '+str(check))
    command=list(W.C.OLDPG_READONLY_ARGV['cp_owners']); command=command[:command.index('-c')]+['-c',"SELECT json_agg(row_to_json(t)) FROM (SELECT id::text,recovery_protocol,(lease_expires_at IS NOT NULL AND lease_expires_at<=clock_timestamp()) AS expired FROM sync_runs WHERE id='"+rid+"'::uuid) t;"]
    protocol=sql_rows('held-'+stage+'-actual-protocol-clock',command)
    if len(protocol)!=1 or protocol[0]['id']!=rid or protocol[0]['recovery_protocol'] is not None: raise ValueError('held actual NULL recovery protocol not proven')
    if root.get('legacy_whole_baseline_27') is not None:
        command=list(W.C.OLDPG_READONLY_ARGV['controlled_rows_27col']); command[-1]=command[-1].replace("r.control_mode = 'controlled'","r.control_mode = 'legacy'")
        legacy=sql_rows('held-'+stage+'-whole-legacy',command)
        H.exact(root['legacy_whole_baseline_27']+[r['safe_projection_27'] for r in root['runs'] if r['control_mode']=='legacy'],legacy,'id')
    if {e['parent']['def_id'] for e in root['runs']}!={v['def_id'] for v in root.get('public_views',[])}: raise ValueError('Root typed public Def whole set missing/extra')
    secret=W.read_jwt_secret(); sub,org=W.jwt_identity_from_baseline(derived,catalog);ctx.attempt.add_secret(secret)
    for view in catalog['public_views']+root.get('public_views',[]):
        ctx.gate(); token=W.sign_testing_jwt(secret,view.get('actor',sub),view.get('tenant',org));ctx.attempt.add_secret(token)
        url=W.ORIGINS['connector']+'/api/v1/data-connection/syncs/'+view['def_id']
        responses=[]
        for suffix in ('checkpoint','runs'):
            ctx.gate(); before=ctx.attempt.step_begin('held-'+stage+'-public-'+suffix,{'method':'GET','url':url+'/'+suffix})
            try: response=W.http_call(url+'/'+suffix,auth=token)
            except Exception as exc:
                ctx.attempt.step_finish('held-'+stage+'-public-'+suffix,before,type(exc).__name__,'unavailable',{'original_rc':None});raise
            ctx.attempt.step_finish('held-'+stage+'-public-'+suffix,before,response.body,'unavailable',{'original_rc':None,'http_status':response.status})
            responses.append(response)
        cp,runs=responses; expected_cp=view.get('checkpoint_status',200)
        if cp.status!=expected_cp: raise ValueError('preserved public checkpoint status drift')
        gate=W.compare_public_view(view['def_id'],view['baseline_cp'],view['baseline_runs'],200 if expected_cp==404 else cp.status,{} if expected_cp==404 else json.loads(cp.body),runs.status,json.loads(runs.body))
        if not gate['ok']: raise ValueError('preserved full public checkpoint/Run drift '+str(gate))
    actual_parent=next(p for p in allparents if p['def_id']==did)
    if type(protocol[0].get('expired')) is not bool: raise ValueError('actual DB expiry boolean required')
    if stage=='immediately-before-kill':
        if ctx.runner is None or hasattr(ctx,'pre_kill_anchor_ref'): raise ValueError('exclusive runner pre-kill anchor required')
        if protocol[0]['expired'] is not False: raise ValueError('pre-kill valid live lease required')
        path=ctx.output/'h10-pre-kill-anchor.json'
        bindings={key:sql_receipts['held-'+stage+'-'+suffix] for key,suffix in (('controlled','whole-controlled'),('CP','whole-CP'),('parents','parents'),('protocol','actual-protocol-clock'))}
        pre_anchor={'schema':'p26-h10-pre-kill-anchor-v1','source_S':ctx.release['candidate_source'],'row':actual,'cp_owner_row_full':owner,'parent':actual_parent,'protocol_clock':protocol[0],'sql_bindings':bindings}
        with path.open('x') as stream:
            stream.write(json.dumps(pre_anchor,indent=2)+'\n');stream.flush();os.fsync(stream.fileno())
        ctx.pre_kill_anchor_ref={'path':str(path),'sha256':H.sha(path)}
        read_pre_kill_anchor(ctx,ctx.pre_kill_anchor_ref)
    else:
        compare_pre_kill(pre_anchor,actual,owner,actual_parent,protocol[0])
    result={'scope':'actual whole old9+CP8+Root typed+registered h10 held','stage':stage,'row':actual,'cp_owner_row_full':owner,'parent':parent,'lease_expired_by_DB_clock':protocol[0]['expired'],'pre_kill_anchor':ctx.pre_kill_anchor_ref,'pre_kill_sql_bindings':pre_anchor['sql_bindings'],'actual_observation_sql_receipts':sql_receipts}
    if export:
        if ctx.runner is not None:
            api=ctx.runner.api
        else:
            engine=ctx.engine(); rec=engine.AttemptRecorder(str(ctx.output/'held-public'),'held-public'); api=engine.HttpApi(ctx.bases,__import__('os').environ['OPENFOUNDRY_SMOKE_JWT'],rec,'held-public')
        cp=api.call('connector','GET','/api/v1/data-connection/syncs/'+did+'/checkpoint')
        runs=api.call('connector','GET','/api/v1/data-connection/syncs/'+did+'/runs')
        if cp['status']!=200 or runs['status']!=200 or not isinstance(cp['json'],dict) or not isinstance(runs['json'],list): raise ValueError('held public actual baseline failed')
        public_owner=cp['json'].get('owner') or {}
        since=lambda value:datetime.fromisoformat(value.replace('Z','+00:00'))
        if cp['json'].get('mode')!=owner['mode'] or (type(cp['json'].get('confirmed_epoch')) is not int or str(cp['json']['confirmed_epoch'])!=str(owner['confirmed_epoch'])) or public_owner.get('run_id')!=rid or public_owner.get('run_status')!=owner['run_status'] or since(public_owner['since'])!=since(owner['active_since']):
            raise ValueError('export public CP differs from pre-kill-checked SQL owner')
        if public_owner.get('blocked_reason','')!=owner['blocked_reason']:
            if owner['blocked_reason']=='' and public_owner.get('blocked_reason')=='requires_reconciliation' and protocol[0]['expired'] is True:
                raise RetryableClockObservation('DB-proven expiry reason advanced after SQL; resample complete guard')
            raise ValueError('export public CP reason differs without permitted clock transition')
        origin=ctx.output/('held-safe27-'+stage+'.json'); origin.write_text(json.dumps(actual,indent=2)+'\n')
        entry={'run_id':rid,'def_id':did,'source_id':sid,'tenant':identity['tenant'],'actor':identity['actor'],'control_mode':'controlled','protocol':'legacy-null','purpose':'actual h10 prepublish crash-held; no adoption or clearing','expected_state':'running','pubentered':'false','safe_projection_27':actual,'cp_owner_row_full':owner,'parent':parent,'evidence':{'path':str(origin),'sha256':H.sha(origin),'json_pointer':''},'pre_kill_anchor':ctx.pre_kill_anchor_ref}
        result['typed_entry']=entry; result['public_view']={'def_id':did,**identity,'checkpoint_status':200,'baseline_cp':cp['json'],'baseline_runs':runs['json']}
        result['frozen_for_next_phase']=bool(protocol[0]['expired'] and owner['blocked_reason']=='requires_reconciliation')
        path=ctx.output/('held-typed-'+stage+'.json'); path.write_text(json.dumps(result,indent=2)+'\n')
        result['export_path']=str(path)
    return result
