"""Current P25 supplemental reads; no historical UUID/failed-case replacement."""
import argparse,hashlib,json,os,shlex,sys,time
from pathlib import Path


def digest(data): return hashlib.sha256(data).hexdigest()
def ref(path):
    p=Path(path).resolve(); data=p.read_bytes(); return {'path':str(p),'sha256':digest(data),'bytes':len(data)}
def doc(path): return json.loads(Path(path).read_text())
def jsonout(path,value): Path(path).write_text(json.dumps(value,indent=2)+'\n')
def actual_body(record):
    value=doc(record); raw=value.get('resp_body','').encode()
    if value.get('status')!=200 or value.get('resp_sha256')!=digest(raw) or value.get('resp_bytes')!=len(raw): raise ValueError('actual HTTP body/status/hash mismatch '+str(record))
    return json.loads(raw)

def tail_binding(summary_path,bindings_path,declaration_path):
    summary=doc(summary_path); binding=doc(bindings_path); declaration=doc(declaration_path)
    if summary.get('phase')!='tail' or summary.get('failed')!=[] or summary.get('tail_adjudication',{}).get('ok') is not True: raise ValueError('actual successful tail required; no summary repair')
    if binding.get('summary_sha256')!=ref(summary_path)['sha256'] or binding.get('bindings')!=summary.get('tail_bindings') or binding.get('problems')!=[]: raise ValueError('actual tail binding/summary mismatch')
    if digest(json.dumps(binding['bindings'],sort_keys=True).encode())!=binding['bindings_sha256'] or binding['declaration_sha256']!=declaration['declaration_sha256'] or digest(json.dumps(declaration['spec'],sort_keys=True).encode())!=declaration['declaration_sha256']: raise ValueError('declaration/bindings actual hashes mismatch')
    from p26_p25_runtime import load
    modes={(o['def_id'],o['run_id']):o.get('mode') for case in summary['scenarios'] for o in case.get('owner_roster',[])}
    owners=[dict(o,mode=modes.get((o['def_id'],o['run_id']))) for o in summary['final_owner_roster']]
    actual=load('smoke_p25.py').adjudicate_tail(declaration,owners,summary['final_run_roster'])
    if not actual['ok'] or actual['bindings']!=binding['bindings']: raise ValueError('actual whole dual-roster declaration replay failed')
    return summary,binding['bindings']['h14-drop']

def collect_h14(ctx,tail_summary,bindings,declaration):
    summary,identity=tail_binding(tail_summary,bindings,declaration)
    if identity.get('mode')!='drop_after_verify' or identity.get('kind')!='owner': raise ValueError('h14 actual mode/owner binding absent')
    engine=ctx.engine(); rec=engine.AttemptRecorder(str(ctx.output/'h14-reads'),'h14-reads');api=engine.HttpApi(ctx.bases,os.environ['OPENFOUNDRY_SMOKE_JWT'],rec,'h14-reads')
    did,sid,rid=[identity[k] for k in ('def_id','source_id','run_id')]
    # Locate exactly the ORIGINAL case's before/after proxy ledger reads.
    evidence=Path(summary['attempts']['evidence_dir'])
    records=[]
    for p in sorted(evidence.glob('h14-attempt-*.json')):
        d=doc(p)
        if d.get('base')=='proxy_admin' and d.get('method')=='GET' and d.get('path','').endswith('/admin/log'):
            records.append((p,actual_body(p)))
    if len(records)!=2: raise ValueError('exact original h14 before/after ledgers required')
    made=api.call('connector','GET','/api/v1/data-connection/syncs/'+did)
    if made['status']!=200 or made['json'].get('source_id')!=sid: raise ValueError('actual Def/Source binding failed')
    target=made['json']['output_dataset_id']; before=[r for r in records[0][1] if r.get('request_target')==target]; after=[r for r in records[1][1] if r.get('request_target')==target]
    if before or len(after)!=1: raise ValueError('h14 exact one actual target dispatch required')
    entry=after[0]
    if entry.get('mode')!='drop_after_verify' or entry.get('forwarded') is not True or entry.get('verified_committed') is not True or type(entry.get('upstream_status')) is not int or not 200<=entry['upstream_status']<300 or entry.get('error') is not None:
        raise ValueError('actual corrected-proxy verified lost ACK dispatch absent')
    ack=entry['upstream_ack_raw'].encode()
    if digest(ack)!=entry['upstream_ack_sha256']: raise ValueError('actual upstream ACK hash changed')
    transaction=entry['ack_txn_id']
    run=api.call('connector','GET','/api/v1/data-connection/syncs/'+did+'/runs/'+rid)
    cp=api.call('connector','GET','/api/v1/data-connection/syncs/'+did+'/checkpoint')
    txn=api.call('dataset','GET','/v1/datasets/'+target+'/branches/main/transactions/'+transaction)
    files=api.call('dataset','GET','/v1/datasets/'+target+'/files')
    if any(r['status']!=200 for r in (run,cp,txn,files)): raise ValueError('independent h14 direct GET failed')
    candidates=[f for f in files['json'].get('files',[]) if f.get('transaction_id')==transaction]
    if len(candidates)!=1: raise ValueError('download ID must come from actual transaction-bound file index')
    file=candidates[0]
    download=api.call('dataset','GET','/v1/datasets/'+target+'/files/'+file['id']+'/download',raw=True)
    if download['status']!=200: raise ValueError('actual full download failed')
    bodies={'definition':made,'run':run,'checkpoint':cp,'transaction':txn,'files':files,'download':download}; saved={}
    for name,response in bodies.items():
        path=ctx.output/(name+('.bin' if name=='download' else '.json'));path.write_bytes(response['raw']);saved[name]=ref(path)
    proof={'schema':'p26-p25-h14-independent-read-v1','inputs':{'tail_summary':ref(tail_summary),'bindings':ref(bindings),'declaration':ref(declaration)},'identity':{**identity,'target_id':target,'transaction_id':transaction,'file_id':file['id']},'original_ledgers':[ref(p) for p,_ in records],'actual_entry':entry,'bodies':saved,'http_recorder':rec.dir,'hmac_policy':'one validated same-origin anonymous follow; no presigned query values or Authorization persisted','source_S':ctx.release['candidate_source']}
    path=ctx.output/'h14-independent-proof.json';jsonout(path,proof)
    verify_h14(path)
    return path

def verify_h14(path):
    from p26_p25_runtime import load
    proof=doc(path);engine=load('smoke_p25.py')
    if proof['schema']!='p26-p25-h14-independent-read-v1': raise ValueError('wrong proof schema')
    for group in (proof['inputs'],proof['bodies']):
        for member in group.values():
            if ref(member['path'])!=member: raise ValueError('original proof input bytes drift')
    for member in proof['original_ledgers']:
        if ref(member['path'])!=member: raise ValueError('original ledger bytes drift')
    summary,identity=tail_binding(proof['inputs']['tail_summary']['path'],proof['inputs']['bindings']['path'],proof['inputs']['declaration']['path'])
    selected=[r for r in summary['scenarios'] if r.get('id')=='h14']
    if len(selected)!=1 or selected[0].get('failed') is not False or any(g.get('ok') is not True for g in selected[0]['gates']): raise ValueError('original h14 case/gates not successful')
    mandatory=set(next(s for s in engine.SCENARIOS if s['id']=='h14')['gates'])
    if not mandatory.issubset({g.get('gate') for g in selected[0]['gates']}): raise ValueError('original h14 mandatory gates missing')
    originals=[doc(p) for p in Path(summary['attempts']['evidence_dir']).glob('h14-attempt-*.json')]
    if sum(v==selected[0] for v in originals)!=1: raise ValueError('h14 original scenario recorder missing/duplicate')
    i=proof['identity'];did,sid,rid,target,transaction,fid=[i[k] for k in ('def_id','source_id','run_id','target_id','transaction_id','file_id')]
    if any(identity[k]!=i[k] for k in ('def_id','source_id','run_id','mode')): raise ValueError('independent identity differs from actual tail binding')
    before=actual_body(proof['original_ledgers'][0]['path']);after=actual_body(proof['original_ledgers'][1]['path'])
    if any(e.get('request_target')==target for e in before): raise ValueError('target already dispatched before h14')
    entries=[e for e in after if e.get('request_target')==target]
    if entries!=[proof['actual_entry']]: raise ValueError('actual target dispatch set differs')
    e=entries[0]
    if e.get('method')!='POST' or e.get('path') not in ('/v1/datasets/'+target+'/outputs:commit','/api/v1/datasets/'+target+'/outputs:commit'):
        raise ValueError('actual commit dispatch method/path mismatch')
    proxy=load('fixtures/dataset_fault_proxy_p25.py')
    parsed=proxy.parse_ack(e['upstream_ack_raw'].encode())
    if proxy.verify_commit_ack(parsed,target).get('verified_committed') is not True or parsed.get('ack_txn_id')!=transaction:
        raise ValueError('actual upstream ACK raw is unusable/wrong target')
    if e.get('forwarded') is not True or e.get('verified_committed') is not True or e.get('mode')!='drop_after_verify' or e.get('error') is not None or not 200<=e['upstream_status']<300 or e.get('ack_txn_id')!=transaction or digest(e['upstream_ack_raw'].encode())!=e['upstream_ack_sha256']: raise ValueError('actual lost-ACK dispatch/ACK mismatch')
    values={k:Path(v['path']).read_bytes() for k,v in proof['bodies'].items()};objects={k:json.loads(v) for k,v in values.items() if k!='download'}
    d,r,c,t,f=[objects[k] for k in ('definition','run','checkpoint','transaction','files')]
    if d['id']!=did or d['source_id']!=sid or d['output_dataset_id']!=target or r['id']!=rid or r['sync_def_id']!=did or r['status']!='failed' or r.get('dataset_version_id') is not None or not r.get('publish_entered_at'): raise ValueError('local unconfirmed outcome/actual parent mismatch')
    owner=c.get('owner') or {}
    if owner.get('run_id')!=rid or owner.get('run_status')!='failed' or owner.get('blocked_reason')!='requires_reconciliation' or c.get('confirmed_epoch')!=0: raise ValueError('local CP0 retained owner lost')
    want=engine._ndjson(engine.window_values('p25h1',0));meta=t.get('metadata') or {}
    if t['id']!=transaction or t['dataset_id']!=target or t['status']!='COMMITTED' or t['branch_name']!='main' or t['tx_type']!='SNAPSHOT' or meta.get('lineage')!={'source_id':sid,'sync_def_id':did,'sync_run_id':rid} or meta.get('rows')!=len(engine.window_values('p25h1',0)) or meta.get('bytes')!=len(want) or meta.get('content_hash')!=digest(want): raise ValueError('independent transaction lineage/content mismatch')
    files=f.get('files')
    if f.get('branch')!='main' or f.get('total')!=1 or not isinstance(files,list) or len(files)!=1: raise ValueError('actual fresh-target file set not exact')
    file=files[0]
    if file.get('id')!=fid or file.get('dataset_id')!=target or file.get('transaction_id')!=transaction or file.get('size_bytes')!=len(want) or file.get('logical_path')!='sync-'+rid+'.ndjson' or values['download']!=want or engine.ndjson_values(values['download'])!=engine.window_values('p25h1',0): raise ValueError('independent file index/full bytes mismatch')
    # Rebind each independently saved body to an ACTUAL GET recorder row.
    rows=[doc(p) for p in Path(proof['http_recorder']).glob('h14-reads-attempt-*.json')]
    paths={'definition':'/api/v1/data-connection/syncs/'+did,'run':'/api/v1/data-connection/syncs/'+did+'/runs/'+rid,'checkpoint':'/api/v1/data-connection/syncs/'+did+'/checkpoint','transaction':'/v1/datasets/'+target+'/branches/main/transactions/'+transaction,'files':'/v1/datasets/'+target+'/files','download':'/v1/datasets/'+target+'/files/'+fid+'/download'}
    for name,suffix in paths.items():
        matching=[x for x in rows if x.get('method')=='GET' and x.get('status')==200 and x.get('path','').endswith(suffix) and x.get('resp_sha256')==digest(values[name]) and x.get('resp_bytes')==len(values[name])]
        if len(matching)!=1: raise ValueError('independent body lacks exact actual HTTP GET proof '+name)
    return {'ok':True,'identity':i,'download_bytes':len(want),'download_sha256':digest(want)}

def execute(ctx,mode,tail):
    from p26_p25_runtime import load,H
    p=argparse.ArgumentParser()
    if mode=='quiet':
        p.add_argument('--handles',action='append',required=True);p.add_argument('--released',action='store_true');a=p.parse_args(tail)
        if not a.released: raise ValueError('quiet requires original --released')
        q=load('collect_http_quiet_p25.py');engine=ctx.engine()
        proof,path=q.collect(a.handles,ctx.output/'quiet',api_factory=lambda m,rec:engine.HttpApi(ctx.bases,os.environ['OPENFOUNDRY_SMOKE_JWT'],rec,'final-quiet'),released=True)
        q.verify_quiet_proof(path,a.handles)
        print(json.dumps({'proof_file':str(path),'ok':proof['ok']}));return 0 if proof['ok'] else 1
    if mode=='h13-ledger':
        p.add_argument('--summary',required=True);a=p.parse_args(tail)
        script=Path(__file__).with_name('p26_h13_ledger_read.py');step=ctx.output/'ledger-step';step.mkdir()
        argv=[sys.executable,'-I','-B',str(script),'--release-file',str(ctx.release_path),'--summary',a.summary,'--output',str(ctx.output/'read')]
        (step/'cmd.txt').write_text(shlex.join(argv)+'\n')
        cp=ctx.command('actual-h13-ledger-child',argv,30)
        (step/'stdout.log').write_bytes(cp.stdout);(step/'stderr.log').write_bytes(cp.stderr);(step/'rc').write_text(str(cp.returncode)+'\n')
        if cp.returncode: return cp.returncode
        verifier=load('verify_h13_ledger_p25.py');result=verifier.verify_ledger(step,a.summary)
        jsonout(ctx.output/'h13-ledger-proof.json',result);print(json.dumps({'ok':True,'step':str(step),'proof':str(ctx.output/'h13-ledger-proof.json')}));return 0
    if mode=='h14-read':
        p.add_argument('--tail-summary',required=True);p.add_argument('--bindings',required=True);p.add_argument('--declaration',required=True);a=p.parse_args(tail)
        path=collect_h14(ctx,a.tail_summary,a.bindings,a.declaration);print(json.dumps({'ok':True,'proof':str(path)}));return 0
    if mode=='freeze-held':
        p.add_argument('--timeout',type=int,default=350);a=p.parse_args(tail)
        if not 1<=a.timeout<=350: raise ValueError('bounded freeze timeout1..350s required')
        deadline=time.monotonic()+a.timeout; sample=0
        while True:
            sample+=1
            from p26_p25_typed_guard import RetryableClockObservation
            try:result=ctx.guard_held('freeze-'+str(sample),export=True)
            except RetryableClockObservation as exc:
                ctx.attempt.record('freeze-clock-resample',{'sample':sample},str(exc),0,{'original_rc':None,'typed_export_written':False})
                if time.monotonic()>=deadline: raise ValueError('freeze clock observations did not converge before original deadline') from exc
                time.sleep(max(0,min(1,deadline-time.monotonic())))
                continue
            if result['frozen_for_next_phase']:
                print(json.dumps({'ok':True,'typed':result['export_path']}));return 0
            if time.monotonic()>=deadline: raise ValueError('actual DB lease did not expire/reconcile within bound; never clear/adopt')
            time.sleep(1)
    raise ValueError('unknown supplemental mode')
