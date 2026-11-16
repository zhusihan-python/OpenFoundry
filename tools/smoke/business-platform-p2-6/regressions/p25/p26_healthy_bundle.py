#!/usr/bin/env python3
"""Pure exact actual-source selection for a NEW healthy12 window.

No fixed failed-three identities. Every selected case is linked to its original
CLI rc/stdout/recorder; retained failures/handles/owners remain explicit.
"""
import argparse,hashlib,json,sys
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path): return json.loads(Path(path).read_text())
def canonical(obj): return (json.dumps(obj,indent=2)+'\n').encode()

def build(manifest_path):
    from p26_p25_runtime import load
    a=load('aggregate_healthy_p25.py');engine=load('smoke_p25.py');manifest=read(manifest_path)
    specs={s['id']:s for s in engine.SCENARIOS if s['phase']=='healthy' and s['id']!='h10'}
    if set(manifest['selection'])!=set(specs): raise ValueError('NEW selection is not exact healthy12')
    loaded=[];handles=[];origin_map={};failures=[]
    for index,source in enumerate(manifest['sources']):
        summary=str(Path(source['summary']).resolve());step=str(Path(source['step']).resolve())
        document,cases,rows,proof,records=a.source(summary,step,Path,source['original_rc'],source.get('case'))
        from p26_p25_runtime import SOURCE
        receipt=read(source['runtime_result'])
        capture=Path(step)/('cmd.json' if (Path(step)/'cmd.json').exists() else 'cmd.txt')
        if capture.suffix=='.json': argv=json.loads(capture.read_text())
        else:
            import shlex
            argv=shlex.split(capture.read_text().removeprefix('CMD: ').strip())
        if sum(x=='--release-file' for x in argv)!=1 or '--' not in argv: raise ValueError('actual NEW runtime command/release missing')
        release_path=argv[argv.index('--release-file')+1]; release=read(release_path)
        if not any(Path(x).name=='p26_p25_runtime.py' for x in argv) or release.get('schema')!='p26-w10-release-v1' or release.get('root_accepted') is not True or release.get('candidate_source')!=SOURCE or release.get('p25_mode')!='engine' or release.get('p25_argv')!=argv[argv.index('--')+1:] or receipt.get('source_S')!=SOURCE or receipt.get('mode')!='engine' or receipt.get('wrapper_exception') is not None or receipt.get('original_engine_or_runner_rc')!=source['original_rc'] or receipt.get('runtime_sha256')!=release['p25_runtime_sha256'] or receipt.get('initial_guard',{}).get('ok') is not True:
            raise ValueError('actual NEW same-S runtime/phase/preservation receipt mismatch')
        proof['runtime_result']={'path':str(Path(source['runtime_result']).resolve()),'sha256':sha(source['runtime_result'])}
        proof['root_phase']={'path':str(Path(release_path).resolve()),'sha256':sha(release_path)}
        loaded.append((document,cases,proof,records))
        history_only=source.get('history_only') is True
        if history_only:
            onlys=[argv[i+1] for i,v in enumerate(argv[:-1]) if v=='--only']
            rosters=[d for _,d in records if isinstance(d,dict) and set(d)=={'defs','owner_roster','run_roster','problems'}]
            if source.get('case')!='h9' or onlys!=['h9'] or set(cases)!={'h9'} or index in manifest['selection'].values() or source['original_rc']!=1 or document.get('failed')!=['finally-quiet'] or document.get('quiet',{}).get('ok') is not False or document.get('final_owner_roster')!=[] or document.get('final_run_roster')!=[] or len(rosters)!=1 or rosters[0]['owner_roster']!=[] or rosters[0]['run_roster']!=[] or rosters[0]['problems']!=[]:
                raise ValueError('history-only permits only unselected exact original h9 finally-quiet failure')
            a.good_case(cases['h9'],specs['h9'])
            if len(cases['h9']['gates'])!=8 or any(g.get('ok') is not True for g in cases['h9']['gates']):
                raise ValueError('history-only original h9 eight strict gates required')
        if source['original_rc']!=0 and (source['original_rc']!=1 or not document['failed'] or not set(document['failed']).issubset(specs)) and not history_only:
            raise ValueError('non-case/global wrapper failure cannot be selected as green business source')
        if document['failed']:
            if source['original_rc']==0: raise ValueError('failed source with process rc0')
            failures.append({'source_index':index,'summary':summary,'summary_sha256':sha(summary),'original_rc':source['original_rc'],'failed':document['failed'],'history_only':history_only})
            for owner in document['final_owner_roster']:
                if owner.get('run_status')!='failed' or owner.get('blocked_reason')!='requires_reconciliation': raise ValueError('unexpected failed history owner shape')
                origin_map.setdefault(owner['run_id'],{**owner,'origin':'failedhealthy','source_summary':summary,'source_summary_sha256':sha(summary)})
        for h in rows: handles.append({**h,'bundle_origin':{'source_index':index,'summary_path':summary,'summary_sha256':sha(summary),'handles_path':proof['handles_file'],'handles_sha256':proof['handles_sha256']}})
    selected=[];case_sources={}
    for s in engine.SCENARIOS:
        if s['id'] not in specs: continue
        index=manifest['selection'][s['id']]; document,cases,proof,records=loaded[index]
        if s['id'] not in cases: raise ValueError('selected actual source lacks case')
        if manifest['sources'][index].get('history_only') is True: raise ValueError('history-only source cannot satisfy selection')
        if s['id']=='h9' and (manifest['sources'][index]['original_rc']!=0 or document['failed']!=[] or document.get('quiet',{}).get('ok') is not True): raise ValueError('selected h9 must be fresh successful quiet source')
        row=cases[s['id']];a.good_case(row,specs[s['id']]);selected.append(row);case_sources[s['id']]={'source_index':index,'summary_path':proof['summary_path'],'summary_sha256':proof['summary_sha256'],'original_rc':manifest['sources'][index]['original_rc'],'scenario_record':proof['scenario_records'][s['id']]}
    # Preserve every successful/failed handle and every origin, deduplicating
    # actual kind+UUID only when building the import union.
    union=[];by_id={}
    for h in handles:
        key=(h.get('kind'),h.get('ref')) if h.get('ref') else ('failed',len(union))
        if key in by_id: union[by_id[key]]['bundle_origins'].append(h['bundle_origin'])
        else: by_id[key]=len(union);union.append({**h,'bundle_origins':[h['bundle_origin']]})
    last=manifest['final_source_index'];lastdoc,_,_,records=loaded[last]
    defs={h['ref'] for h in union if h.get('ok') and h.get('kind') in ('def','h10-def')}
    roster=[d for _,d in records if isinstance(d,dict) and set(d)=={'defs','owner_roster','run_roster','problems'}]
    if len(roster)!=1 or set(roster[0]['defs'])!=defs or len(roster[0]['defs'])!=len(defs) or roster[0]['problems'] or roster[0]['run_roster']!=[] or lastdoc['final_run_roster']!=[] or lastdoc['final_owner_roster']!=roster[0]['owner_roster']: raise ValueError('final actual whole owned Def/CP/Run scope mismatch; import all prior handles')
    key=lambda o:(o['def_id'],o['source_id'],o['run_id'],o['run_status'],o['blocked_reason'])
    if sorted(key(o) for o in lastdoc['final_owner_roster'])!=sorted(key(o) for o in origin_map.values()): raise ValueError('new owner extra/lost; failed history may not be cleared/adopted')
    for o in lastdoc['final_owner_roster']:
        bodies=[]
        for _,d in records:
            if d.get('status')==200 and isinstance(d.get('resp_body'),str):
                try: bodies.append(json.loads(d['resp_body']))
                except ValueError: pass
        if not any(isinstance(b,dict) and b.get('id')==o['def_id'] and b.get('source_id')==o['source_id'] for b in bodies): raise ValueError('actual retained owner Source parent missing')
        if not any(isinstance(b,list) and any(r.get('id')==o['run_id'] and r.get('sync_def_id')==o['def_id'] and r.get('status')=='failed' for r in b if isinstance(r,dict)) for b in bodies): raise ValueError('actual retained owner full Run proof missing')
    handles_inputs=[proof['handles_file'] for _,_,proof,_ in loaded]
    quiet=a.q.verify_quiet_proof(manifest['quiet_proof'],handles_inputs)
    quiet_members,quiet_argv,quiet_stdout=a.step_proof(manifest['quiet_step'],Path,0)
    actual_handles=[str(Path(quiet_argv[i+1]).resolve()) for i,arg in enumerate(quiet_argv[:-1]) if arg=='--handles']
    if actual_handles!=list(map(lambda p:str(Path(p).resolve()),handles_inputs)) or quiet_stdout.get('ok') is not True or str(Path(quiet_stdout.get('proof_file','')).resolve())!=str(Path(manifest['quiet_proof']).resolve()):
        raise ValueError('actual global quiet CLI/handle union binding mismatch')
    h13_index=manifest['selection']['h13']; h13summary=loaded[h13_index][2]['summary_path']
    if set(loaded[h13_index][1])!={'h13'}: raise ValueError('h13 supplemental ledger requires an actual single-case h13 source')
    h13=load('verify_h13_ledger_p25.py').verify_ledger(manifest['h13_ledger_step'],h13summary)
    output={'schema':'p26-p25-healthy-bundle-v1','phase':'healthy','released':True,'failed':[],'scenarios':selected,'quiet':quiet,'final_owner_roster':lastdoc['final_owner_roster'],'final_run_roster':[],'retained_owner_origins':list(origin_map.values()),'original_failures_preserved':failures,'case_sources':case_sources,'sources':[p for _,_,p,_ in loaded],'h13_ledger_supplement':h13,'source_manifest':{'path':str(Path(manifest_path).resolve()),'sha256':sha(manifest_path)},'attempts':{'handles_file':str(Path(manifest['handles_output']).resolve())}}
    return output,canonical({'handles':union,'count':len(union)})

def verify_bundle(path):
    actual=read(path);rebuilt,handles=build(actual['source_manifest']['path'])
    if rebuilt!=actual or Path(actual['attempts']['handles_file']).read_bytes()!=handles: raise ValueError('NEW bundle differs from actual source recomputation')
    return {'ok':True,'selected':len(actual['scenarios']),'retained_failed':len(actual['retained_owner_origins'])}

def main():
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);p.add_argument('--output',required=True);p.add_argument('--verify',action='store_true');a=p.parse_args()
    if a.verify: print(json.dumps(verify_bundle(a.output)));return
    manifest=read(a.manifest);out=Path(a.output);handles=Path(manifest['handles_output'])
    if out.exists() or handles.exists(): raise ValueError('exclusive NEW bundle/handles output required')
    document,data=build(a.manifest);handles.write_bytes(data);out.write_bytes(canonical(document));print(json.dumps(verify_bundle(out)))
if __name__=='__main__': main()
