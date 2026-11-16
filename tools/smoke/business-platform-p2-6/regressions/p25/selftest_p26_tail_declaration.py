#!/usr/bin/env python3
import copy,json,sys,tempfile,uuid,hashlib
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
from p26_tail_declaration import build
import p26_p25_runtime as R

def main():
    with tempfile.TemporaryDirectory(dir=R.P26/'results/w10') as td:
        td=Path(td);held={k:str(uuid.uuid4()) for k in ('def_id','source_id','run_id')};binding=td/'binding.json';binding.write_text(json.dumps({'known_bindings':{'h10-held':held},'retained_owner_rows':[]}));frozen=td/'held.json';data={'frozen_for_next_phase':True,'lease_expired_by_DB_clock':True,'cp_owner_row_full':{'blocked_reason':'requires_reconciliation'},'typed_entry':held};frozen.write_text(json.dumps(data));decl=build(binding,frozen);out=td/'declaration.json';out.write_text(json.dumps(decl));engine=R.load('smoke_p25.py');_,err=engine.load_semantic_declaration(out);assert not err
        assert len(decl['spec']['owner_rows'])==6 and len(decl['spec']['run_rows'])==1 and decl['spec']['run_rows'][0]['count']==1
        assert not any(k in r for r in decl['spec']['owner_rows']+decl['spec']['run_rows'] for k in ('run_id','def_id','source_id'))
        for key,value in [('frozen_for_next_phase',False),('lease_expired_by_DB_clock',False)]:
            wrong=copy.deepcopy(data);wrong[key]=value;frozen.write_text(json.dumps(wrong))
            try:build(binding,frozen)
            except ValueError:pass
            else:assert False
        wrong=copy.deepcopy(data);wrong['typed_entry']['run_id']=str(uuid.uuid4());frozen.write_text(json.dumps(wrong))
        try:build(binding,frozen)
        except ValueError:pass
        else:assert False
        # A rehashed invented UUID in a future semantic row still fails the
        # ORIGINAL copied engine's semantic contract.
        bad=copy.deepcopy(decl);bad['spec']['owner_rows'][0]['run_id']=str(uuid.uuid4());bad['declaration_sha256']=hashlib.sha256(json.dumps(bad['spec'],sort_keys=True).encode()).hexdigest();out.write_text(json.dumps(bad));_,err=engine.load_semantic_declaration(out);assert err
    print(json.dumps({'ok':True,'actual_HTTP_SQL_Docker_calls':0,'checks':['fresh actual binding yields exact six owner semantics + one held run, no futureUUIDs','unfrozen/nonexpired/mismatchedknownheld refuses','even rehashed future UUID semantic row refused by unchanged copied engine']},indent=2))
if __name__=='__main__':main()
