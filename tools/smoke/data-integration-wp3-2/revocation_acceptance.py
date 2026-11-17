#!/usr/bin/env python3
"""Current-generation revocation for org/personal through real HTTP/Kafka/PG."""
import copy, datetime, json, os, pathlib, subprocess, sys, time
from canonical_acceptance import ROOT,forward,http,pg,inject,drain,save
from verify_results import derive_event_id

def test(domain,token,seed,tenant,old=None):
 key='runtime-v1-20261117'
 with forward('ofwp32-pbs',50081) as pbs,forward('ofwp32-dvs',50078) as dvs:
  if old is None:
   old=http(pbs,'/api/v1/scheduling/grants',token,'POST',{'schedule_key':key,'pipeline_rid':seed['pipeline']['id'],'token':token},evidence=domain+'-revocation')
   http(pbs,'/api/v1/scheduling/definitions',token,'POST',{'schedule_key':key,'pipeline_rid':seed['pipeline']['id'],'build_branch':'main','cron':'* * * * *','grant_id':old['id'],'grant_generation':old['generation']},evidence=domain+'-revocation')
   http(pbs,'/api/v1/scheduling/definitions/'+key+'/pause',token,'PATCH',{},evidence=domain+'-revocation')
  before=http(dvs,'/v1/datasets/'+seed['output']['id']+'/versions',token,evidence=domain+'-revocation')
  # The queued event contains the original generation; regrant is ACTIVE
  # at a newer generation when this old event reaches the real consumer.
  http(pbs,'/api/v1/scheduling/grants/'+key,token,'DELETE',evidence=domain+'-revocation')
  fresh=http(pbs,'/api/v1/scheduling/grants',token,'POST',{'schedule_key':key,'pipeline_rid':seed['pipeline']['id'],'token':token},evidence=domain+'-revocation');assert fresh['generation']>old['generation']
  e=json.loads((ROOT/'duplicate-event.ndjson').read_text());p=e['payload'];p.update(tenant_kind=domain,tenant_id=tenant,pipeline_rid=seed['pipeline']['id'],grant_ref=old['id'],grant_generation=old['generation'],scheduled_for='2026-11-18T01:00:00.000000000Z');e['event_id']=str(derive_event_id(p));e['aggregate_id']=domain+':'+tenant+'/'+key
  raw=(json.dumps(e,separators=(',',':'))+'\n').encode();save(domain+'-revoked-event.ndjson',raw.decode());inject(raw,domain+'-revoked-producer');drain()
  rows=pg("SELECT coalesce(json_agg(row_to_json(d)),json_build_array()) FROM scheduling.dispatches d WHERE event_id='"+e['event_id']+"';");save(domain+'-revoked-dispatch.json',rows);assert len(rows)==1 and rows[0]['state']=='skipped' and rows[0]['error']=='grant_ref_mismatch',rows
  assert not pg("SELECT to_json(EXISTS(SELECT 1 FROM builds WHERE id='"+rows[0]['build_id']+"'));"),rows
  p.update(grant_ref=fresh['id'],scheduled_for='2026-11-18T01:01:00.000000000Z');e['event_id']=str(derive_event_id(p));raw=(json.dumps(e,separators=(',',':'))+'\n').encode();save(domain+'-stale-generation-event.ndjson',raw.decode());inject(raw,domain+'-stale-generation-producer');drain()
  stale=pg("SELECT coalesce(json_agg(row_to_json(d)),json_build_array()) FROM scheduling.dispatches d WHERE event_id='"+e['event_id']+"';");save(domain+'-stale-generation-dispatch.json',stale);assert len(stale)==1 and stale[0]['state']=='skipped' and stale[0]['error']=='grant_generation_mismatch',stale
  assert not pg("SELECT to_json(EXISTS(SELECT 1 FROM builds WHERE id='"+stale[0]['build_id']+"'));"),stale
  after=http(dvs,'/v1/datasets/'+seed['output']['id']+'/versions',token,evidence=domain+'-revocation');assert before==after
  save(domain+'-revocation-summary.json',{'old_generation':old['generation'],'current_generation':fresh['generation'],'state':rows[0]['state'],'build_absent':True,'versions_unchanged':True})

def main():
 orgtoken=(ROOT/'org-token.txt').read_text().strip();orgseed=json.loads((ROOT/'canonical-seed.json').read_text());old=json.loads((ROOT/'canonical-schedule.json').read_text())['grant'];test('org',orgtoken,orgseed,'77777777-2222-4222-8222-222222222222',old)
 token= subprocess.check_output([sys.executable,'tools/smoke/business-platform-p1/mint_token.py','--secret','ofwp32-runtime-synthetic-jwt-20261117-only','--sub','77777777-2222-4222-8222-222222222222','--roles','admin','--hours','2'],text=True).strip();(ROOT/'personal-token.txt').write_text(token+'\n')
 with forward('ofwp32-pbs',50081) as pbs,forward('ofwp32-dvs',50078) as dvs:
  http(pbs,'/api/v1/scheduling/definitions/runtime-v1-20261117/pause',token,'PATCH',{},allowed=(404,),evidence='cross-kind-isolation')
  http(dvs,'/v1/datasets/'+orgseed['output']['id'],token,allowed=(404,),evidence='cross-kind-isolation')
  env={**os.environ,'PBS':pbs,'DVS':dvs,'TOKEN':token}
  p=subprocess.run([sys.executable,'tools/smoke/data-integration-wp3-2/canonical_seed.py'],env=env,capture_output=True,text=True);save('personal-seed-http.log',p.stderr);save('personal-seed.rc',str(p.returncode)+'\n');assert p.returncode==0,p.stderr
  seed=json.loads(p.stdout);save('personal-seed.json',seed)
 test('personal',token,seed,'77777777-2222-4222-8222-222222222222')
 print('PASS org/personal stale grant generations skipped; no Build/product; same UUID cross-kind HTTP isolation')
if __name__=='__main__':main()
