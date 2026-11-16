#!/usr/bin/env python3
import ast,copy,json,sys
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import final_quiesce_new_owned as Q
import final_owned_aux_cleanup as C
import final_main_restart as R

def main(out):
 out.mkdir(exist_ok=False,parents=True);cases=[]
 def reject(name,fn):
  try:fn()
  except (AssertionError,KeyError,ValueError):pass
  else:raise AssertionError(name+' accepted')
  cases.append(name+'-rejected')
 pins={name:{'cid':Q.HTTP_CIDS.get(name,format(i+20,'064x')),'image':'sha256:'+'a'*64,'started_at':'2026-11-15T01:00:00Z','running':True,'ownership':{'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':Q.ROLES[name]}} for i,name in enumerate(Q.CONNECTORS)}
 Q.validate_connector_ledger(pins,set());cases.append('exact-five-correct-a2b2-role-accepted')
 for name,mut in [('drop',lambda p:p.pop('p26connectora2')),('extra',lambda p:p.update(p26unknown=copy.deepcopy(p['p26connectora']))),('main-name',lambda p:p.update({Q.W.Q3_CONNECTOR:p.pop('p26connectora')})),('role2',lambda p:p['p26connectora2']['ownership'].update({'of-p26-role':'connector_2'})),('wrong-httpCID',lambda p:p['p26connectora2'].update(cid='e'*64)),('duplicateCID',lambda p:p['p26connectora'].update(cid=p['p26connectorb']['cid']))]:
  bad=copy.deepcopy(pins);mut(bad);reject('quiesce-'+name,lambda:Q.validate_connector_ledger(bad,set()))
 reject('quiesce-old-main-CID',lambda:Q.validate_connector_ledger(pins,{pins['p26connectorc']['cid']}))
 aux={name:{'cid':C.HTTP_CIDS.get(name,format(i+40,'064x')),'image':'sha256:'+'a'*64,'started_at':'2026-11-15T01:00:00Z','running':name not in C.CONNECTORS,'ownership':{'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':role}} for i,(name,role) in enumerate(C.OWNED_ROLES.items())}
 for mod in [C,R]:
  mod.validate_aux_ledger(aux,set());cases.append(mod.__name__+'-exact-ten-five-stopped-accepted')
  for label,mut in [('unknown-extra',lambda p:p.update(p26unknown=copy.deepcopy(p['p26fixture']))),('missing-a2',lambda p:p.pop('p26connectora2')),('live-b2',lambda p:p['p26connectorb2'].update(running=True)),('wrongrole',lambda p:p['p26connectorb2']['ownership'].update({'of-p26-role':'connector_2'}))]:
   bad=copy.deepcopy(aux);mut(bad);reject(mod.__name__+'-'+label,lambda:mod.validate_aux_ledger(bad,set()))
  reject(mod.__name__+'-oldCID',lambda:mod.validate_aux_ledger(aux,{aux['p26fixture']['cid']}))
 for name,pin in aux.items():
  host,inner=C.OWNED_PORTS[name];actual={k:pin[k] for k in ['cid','image','started_at','running']};actual.update(name='/'+name,labels=pin['ownership'],ports={str(inner)+'/tcp':[{'HostIp':'127.0.0.1','HostPort':str(host)}]});C.validate_target(name,pin,actual,set())
 cases.append('cleanup-exact-ten-runtime-targets-fixedports-accepted')
 reject('cleanup-old-fixture-name',lambda:C.validate_target(Q.W.Q3_FIXTURE,aux['p26fixture'],{},set()))
 did='11111111-1111-4111-8111-111111111111';entry={'def_id':did,'source_id':'22222222-2222-4222-8222-222222222222','output_dataset_id':'33333333-3333-4333-8333-333333333333','actor_id':'44444444-4444-4444-8444-444444444444','org_id':None,'instance':'p26connectora2'}
 for key,body in [('definition_creation',{'id':did,'source_id':entry['source_id'],'output_dataset_id':entry['output_dataset_id'],'org_id':None}),('source_creation',{'id':entry['source_id'],'owner_id':entry['actor_id'],'org_id':None})]:
  f=out/(key+'.json');f.write_text(json.dumps(body));entry[key]={'path':str(f),'sha256':Q.G.sha256_file(f)}
 coverage={'schema':'p26-final-owned-def-coverage-v1','source_sha256':'s','definition_ids':[did],'creators':[{k:entry[k] for k in ['def_id','definition_creation','source_creation']}]}
 Q.validate_definition(entry,set());Q.validate_coverage([entry],coverage,set(),'s');cases.append('controlled-newheld-Def-proven-creation-coverage-accepted')
 reject('old-or-N-Def-protected',lambda:Q.validate_definition(entry,{did}))
 for label,mut in [('missing',lambda d:d.update(definition_ids=[])),('unknown-extra',lambda d:d['definition_ids'].append('66666666-6666-4666-8666-666666666666')),('no-creator-origins',lambda d:d.update(creators=[])),('creator-sha-drift',lambda d:d['creators'][0]['source_creation'].update(sha256='0'*64))]:
  bad=copy.deepcopy(coverage);mut(bad);reject('coverage-'+label,lambda:Q.validate_coverage([entry],bad,set(),'s'))
 assert Q.schedule_actions({'schedule_enabled':False,'schedule_next_fire_at':None},False)==[];cases.append('already-quiet-controlledheld-no-HTTP-mutation')
 assert Q.schedule_actions({'schedule_enabled':True,'schedule_next_fire_at':'future'},True)==[('DELETE','/schedule:arm',None),('PATCH','',{'schedule_enabled':False})];cases.append('active-new-schedule-only-disarm-disable')
 for mod in [Q,C,R]:
  ast.parse(Path(mod.__file__).read_text())
  with patch.object(mod.G,'load_release',return_value=None),patch.object(mod.subprocess,'run') as proc:
   reject(mod.__name__+'-noRoot',lambda:mod.run('synthetic-no-root'));assert proc.call_count==0
 result={'pure_only':True,'actual_HTTP_SQL_Docker_calls':0,'passed':len(cases),'cases':cases,'tool_shas':{str(Path(m.__file__).relative_to(Q.P26)):Q.G.sha256_file(m.__file__) for m in [Q,C,R]}};(out/'RESULT.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
if __name__=='__main__':main(Path(sys.argv[1]).resolve())
