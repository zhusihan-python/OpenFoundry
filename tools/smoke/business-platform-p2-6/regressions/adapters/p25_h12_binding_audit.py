#!/usr/bin/env python3
"""Read original accepted S-PG receipts; never fabricate missing h12 stages."""
import argparse,hashlib,json,sys
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[2]; ROOT=P26.parents[2]
sys.path.insert(0,str(P26/'archive'))
import p26_original_proofs as P
NAME='TestPGCheckpointAdvanceAllOrNothingRollback'

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--root-accepted',required=True); ap.add_argument('--output',required=True); args=ap.parse_args()
    out=Path(args.output).resolve()
    if not out.is_relative_to((P26/'results/w10').resolve()) or out.exists(): raise ValueError('exclusive NEW results/w10 output required')
    accepted=json.loads(Path(args.root_accepted).read_text()); source=accepted['source']
    roster=json.loads((P26/'archive/p26_formal_roster.json').read_text())
    contracts={key:value for item in roster['items'] for key,value in item.get('contracts',{}).items()}
    chosen=[('new-normal','pg-new-connector-normal'),('new-race','pg-new-connector-race'),('unified','pg-unified-connector-normal')]
    reader=P.Reader(ROOT); stages=[]
    for stage,key in chosen:
        if key not in contracts:
            # The roster uses the stable satisfier ID declared in each item.
            matches=[(k,v) for k,v in contracts.items() if v.get('proof',{}).get('inner','').endswith({'new-normal':'test-connector-normal_20261115T053633Z','new-race':'test-connector-race_20261115T053659Z','unified':'test-unified-connector-normal_20261115T053838Z'}[stage])]
            if len(matches)!=1: raise ValueError('ambiguous accepted stage '+stage)
            key,contract=matches[0]
        else: contract=contracts[key]
        proof=contract['proof']; P.pg(reader,proof,source)
        original=next((p for p in accepted['seven_actual_chains_all0'] if Path(p['inner']).name==Path(proof['inner']).name),None)
        if not original or original['source']!=source or original['process_rc']!=0 or original['wrapper_rc']!=0 or not original['allgates0']:
            raise ValueError('Root accepted stage binding missing')
        base=proof['inner']+'/01-go-test'; raw=reader.text(base+'.raw'); lines=raw.splitlines()
        counts={'run':sum(l=='=== RUN   '+NAME for l in lines),'pass':sum(l.startswith('--- PASS: '+NAME+' (') for l in lines),'skip':sum(l.startswith('--- SKIP: '+NAME+' (') for l in lines),'fail':sum(l.startswith('--- FAIL: '+NAME+' (') for l in lines)}
        stages.append({'stage':stage,'satisfier':key,'original_proof':proof,'actual_pinned_rollback_parent_counts':counts,'has_exact_rollback':counts=={'run':1,'pass':1,'skip':0,'fail':0}})
    complete=all(s['has_exact_rollback'] for s in stages)
    doc={'schema':'p26-p25-h12-compatibility-audit-v1','source_S':source,'root_accepted':{'path':str(Path(args.root_accepted).resolve()),'sha256':sha(args.root_accepted)},'required_rollback':NAME,'stages':stages,'original_files':reader.files,'actual_http_sql_docker_calls':0,'compatible_with_unchanged_old_h12':complete,'verdict':'READY' if complete else 'NOTREADY','gap':'normal/race P26-only raw lacks old rollback; do not relabel unified raw as three stages' if not complete else None}
    out.parent.mkdir(parents=True,exist_ok=True)
    with out.open('x') as f: json.dump(doc,f,indent=2)
    print(json.dumps({'output':str(out),'verdict':doc['verdict'],'stages':[{'stage':s['stage'],'counts':s['actual_pinned_rollback_parent_counts']} for s in stages],'original_files_read':len(reader.files)}))
    return 0 # successful truthful audit; readiness is explicit NOTREADY.
if __name__=='__main__': sys.exit(main())
