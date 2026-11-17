#!/usr/bin/env python3
"""Real single-Dataset failure keeps the last committed product."""
import base64,datetime,json,time
from canonical_acceptance import ROOT,forward,http,pg,inject,drain,save
from verify_results import derive_event_id

def publish(pbs,token,seed,transform):
 return http(pbs,'/v1/job-specs/TRANSFORM',token,'POST',{'pipeline_rid':seed['pipeline']['id'],'branch_name':'main','inputs':[{'dataset_rid':seed['input']['id']}],'output_dataset_rids':[seed['output']['id']],'logic_payload':{'transform_type':transform,'dataset_rid':seed['input']['id']}},evidence='fault-case-http')
def prepare(pbs,token,seed,key):
 g=http(pbs,'/api/v1/scheduling/grants',token,'POST',{'schedule_key':key,'pipeline_rid':seed['pipeline']['id'],'token':token},evidence='fault-case-http')
 http(pbs,'/api/v1/scheduling/definitions',token,'POST',{'schedule_key':key,'pipeline_rid':seed['pipeline']['id'],'build_branch':'main','cron':'0 0 1 1 *','grant_id':g['id'],'grant_generation':g['generation']},evidence='fault-case-http')
 claims=json.loads(base64.urlsafe_b64decode(token.split('.')[1]+'=='));tenant=claims.get('org_id') or claims['sub']
 e=json.loads((ROOT/'duplicate-event.ndjson').read_text());p=e['payload'];p.update(tenant_kind='org' if claims.get('org_id') else 'personal',tenant_id=tenant,schedule_key=key,pipeline_rid=seed['pipeline']['id'],grant_ref=g['id'],grant_generation=g['generation'],scheduled_for=datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')+'000Z');e['event_id']=str(derive_event_id(p));e['aggregate_id']=p['tenant_kind']+':'+p['tenant_id']+'/'+key
 save(key+'-event.ndjson',json.dumps(e,separators=(',',':'))+'\n');return e

def dispatch(e):
 return pg("SELECT coalesce(json_agg(row_to_json(d)),json_build_array()) FROM scheduling.dispatches d WHERE event_id='"+e['event_id']+"';")
def await_state(e,states,timeout=75):
 end=time.monotonic()+timeout
 while time.monotonic()<end:
  rows=dispatch(e)
  if rows and rows[0]['state'] in states:return rows[0]
  if rows and rows[0]['state'] in {'skipped','blocked','failed','succeeded'}:raise AssertionError(rows)
  time.sleep(.5)
 raise TimeoutError((e['event_id'],rows))
def main():
 seed=json.loads((ROOT/'canonical-seed.json').read_text());token=(ROOT/'org-token.txt').read_text().strip();key='runtime-single-failure-r2'
 with forward('ofwp32-pbs',50081) as pbs,forward('ofwp32-dvs',50078) as dvs:
  path='/v1/datasets/'+seed['output']['id'];before={'versions':http(dvs,path+'/versions',token,evidence='failure-product'),'rows':http(dvs,path+'/files/rows?branch=main&mode=preview&limit=10',token,evidence='failure-product')};save('failure-pre-product.json',before)
  try:
   publish(pbs,token,seed,'unsupported_fixture_transform');e=prepare(pbs,token,seed,key);inject((ROOT/(key+'-event.ndjson')).read_bytes(),key+'-producer');drain();d=await_state(e,{'failed'});save('failure-dispatch.json',d);assert d['error'],d
   after={'versions':http(dvs,path+'/versions',token,evidence='failure-product'),'rows':http(dvs,path+'/files/rows?branch=main&mode=preview&limit=10',token,evidence='failure-product')};save('failure-post-product.json',after);assert before==after
  finally:publish(pbs,token,seed,'dataset_input')
 print('PASS single Dataset failed dispatch preserves versions and exact previous rows')
if __name__=='__main__':main()
