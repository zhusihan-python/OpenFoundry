#!/usr/bin/env python3
import copy,json,sys
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import a_schedule_readonly as S
ID='11111111-1111-4111-8111-111111111111'
base={'protected_defs':[{'id':ID,'schedule_enabled':False,'schedule_next_fire_at':None}], 'global_enabled':[], 'global_due_count':0, 'global_grants_count':0, 'nonterminal_runs':[]}
checks=[]
def reject(label, data):
    try:S.validate(json.dumps(data),[ID])
    except (ValueError,AssertionError,KeyError,TypeError):checks.append(label)
    else:raise AssertionError(label)
assert S.validate(json.dumps(base),[ID])==base
for key,value in [('global_enabled',[ID]),('global_due_count',1),('global_grants_count',1)]:
    data=copy.deepcopy(base);data[key]=value;reject(key,data)
for key,value in [('schedule_enabled',True),('schedule_next_fire_at','2026-11-15T00:00:00Z')]:
    data=copy.deepcopy(base);data['protected_defs'][0][key]=value;reject(key,data)
for rows in [[],base['protected_defs']*2,[{**base['protected_defs'][0],'id':'22222222-2222-4222-8222-222222222222'}]]:
    reject('missing/duplicate/foreign protected Def',{**base,'protected_defs':rows})
cmd=S.argv('a'*64,[ID]);assert cmd[:4]==['docker','exec','a'*64,'psql']
assert 'BEGIN READ ONLY;' in cmd[-1] and 'SET LOCAL statement_timeout' in cmd[-1]
assert not any(x in cmd[-1] for x in ('UPDATE ','DELETE ','INSERT '))
print(json.dumps({'ok':True,'actual_calls':0,'negative_checks':checks,'readonly_argv':cmd},indent=2))
