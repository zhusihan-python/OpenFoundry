#!/usr/bin/env python3
"""Freeze NEW tail semantics from actual h10 bindings, with no future UUIDs."""
import argparse,hashlib,json,sys
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def build(binding_path,frozen_path):
    binding=json.loads(Path(binding_path).read_text());frozen=json.loads(Path(frozen_path).read_text())
    known=binding['known_bindings']; held=known['h10-held']; entry=frozen['typed_entry']
    if frozen.get('frozen_for_next_phase') is not True or frozen.get('lease_expired_by_DB_clock') is not True or frozen['cp_owner_row_full']['blocked_reason']!='requires_reconciliation' or any(held[k]!=entry[k] for k in ('def_id','source_id','run_id')): raise ValueError('actual expired h10 held proof/known binding mismatch')
    owners=[{'def_tag':'h11-'+mode,'run_ref':'terminal-failed','mode':mode,'blocked_reason':'requires_reconciliation'} for mode in ('no_txn_200','not_committed','wrong_target','http_500')]
    owners += [{'def_tag':'h14-drop','run_ref':'terminal-failed','mode':'drop_after_verify','blocked_reason':'requires_reconciliation'},{'def_tag':'h10-held','run_ref':'same','blocked_reason':'requires_reconciliation'}]
    # Preserve actual retained healthy history under its own actual known tags.
    # Never assume the old two/h13/failed-three identities exist this round.
    for row in binding.get('retained_owner_rows',[]):
        if row['def_tag'] not in known or row['origin']!='failedhealthy': raise ValueError('actual retained origin/known tuple missing')
        if sha(row['source_summary'])!=row['source_summary_sha256']: raise ValueError('retained failed source bytes changed')
        owners.append(row)
    spec={'owner_rows':owners,'run_rows':[{'def_tag':'h10-held','source_tag':'h10-src','semantics':'p25-crash-held-prepublish','count':1}],'produced_by':'p25-tail'}
    return {'schema':'p25-tail-roster-v1','ok':True,'spec':spec,'declaration_sha256':hashlib.sha256(json.dumps(spec,sort_keys=True).encode()).hexdigest(),'known_bindings':known,'source_proofs':{'h10_binding':{'path':str(Path(binding_path).resolve()),'sha256':sha(binding_path)},'actual_frozen_held':{'path':str(Path(frozen_path).resolve()),'sha256':sha(frozen_path)}}}

def main():
    p=argparse.ArgumentParser();p.add_argument('--h10-binding',required=True);p.add_argument('--frozen-held',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    out=Path(a.output)
    if out.exists(): raise ValueError('exclusive declaration required')
    result=build(a.h10_binding,a.frozen_held);out.write_text(json.dumps(result,indent=2)+'\n')
    from p26_p25_runtime import load
    _,err=load('smoke_p25.py').load_semantic_declaration(out)
    if err: raise ValueError(err)
    print(json.dumps({'ok':True,'declaration':str(out.resolve()),'owner_semantics':len(result['spec']['owner_rows']),'nonterminal_semantics':1}))
if __name__=='__main__': main()
