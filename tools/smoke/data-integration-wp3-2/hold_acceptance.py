#!/usr/bin/env python3
"""Live-lease cross-opportunity Forbid and authorization revocation after successful real Dataset read."""
import datetime,json,sys,time
from canonical_acceptance import ROOT,forward,http,pg,inject,drain,save
from failure_acceptance import prepare,publish,await_state,dispatch
from lost_ack_acceptance import proxy_events
from verify_results import derive_event_id

def hold_case(kind,domain):
 seed=json.loads((ROOT/('canonical-seed.json' if domain=='org' else 'personal-seed.json')).read_text());token=(ROOT/('org-token.txt' if domain=='org' else 'personal-token.txt')).read_text().strip();key='runtime-hold-r3-'+kind+'-'+domain
 with forward('ofwp32-pbs',50081) as pbs,forward('ofwp32-dvs',50078) as dvs,forward('ofwp32-fault-proxy',8080) as proxy:
  path='/v1/datasets/'+seed['output']['id'];rows_path='/v1/datasets/'+seed['input']['id']+'/files/rows?limit=1000&mode=execution';before=http(dvs,path+'/versions',token,evidence=key+'-http');save(key+'-before-versions.json',before)
  publish(pbs,token,seed,'dataset_input');e=prepare(pbs,token,seed,key);seq=max([x.get('sequence',0) for x in proxy_events()] or [0]);http(proxy,'/__faults/hold-next','','POST',{'method':'GET','path':rows_path},evidence=key+'-control');inject((ROOT/(key+'-event.ndjson')).read_bytes(),key+'-producer');drain()
  end=time.monotonic()+30
  while time.monotonic()<end:
   events=[x for x in proxy_events() if x.get('sequence',0)>seq and x.get('path')==rows_path]
   if any(x.get('action')=='hold_started' for x in events):break
   time.sleep(.2)
  else:raise TimeoutError('exact Dataset rows hold not started')
  active=pg("SELECT json_build_object('dispatch',row_to_json(d),'lease_live',d.lease_expires_at>now(),'activity',(SELECT row_to_json(a) FROM scheduling.pipeline_activity a WHERE a.active_dispatch_id=d.event_id)) FROM scheduling.dispatches d WHERE d.event_id='"+e['event_id']+"';");save(key+'-live-held-dispatch.json',active);assert active['dispatch']['state']=='executing' and active['lease_live'] and active['activity'],active
  try:
   if kind=='forbid':
    second=json.loads(json.dumps(e));stamp=datetime.datetime.fromisoformat(second['payload']['scheduled_for'].replace('Z','+00:00'))+datetime.timedelta(seconds=1);second['payload']['scheduled_for']=stamp.strftime('%Y-%m-%dT%H:%M:%S.%f')+'000Z';second['event_id']=str(derive_event_id(second['payload']));raw=(json.dumps(second,separators=(',',':'))+'\n').encode();save(key+'-second-event.ndjson',raw.decode());inject(raw,key+'-second-producer');drain();blocked=await_state(second,{'blocked'});save(key+'-blocked-dispatch.json',blocked);assert blocked['error']=='overlap',blocked
    assert not pg("SELECT to_json(EXISTS(SELECT 1 FROM builds WHERE id='"+blocked['build_id']+"'));"),blocked
   else:http(pbs,'/api/v1/scheduling/grants/'+key,token,'DELETE',evidence=key+'-revoke')
  finally:http(proxy,'/__faults/release','','POST',{},evidence=key+'-control')
  final=await_state(e,{'succeeded'} if kind=='forbid' else {'failed'});save(key+'-final-dispatch.json',final)
  events=[x for x in proxy_events() if x.get('sequence',0)>seq and x.get('path')==rows_path];save(key+'-proxy-events.json',events);assert any(x.get('action')=='hold_released' for x in events) and any(x.get('action')=='forwarded' and x.get('upstream_status')==200 for x in events),events
  after=http(dvs,path+'/versions',token,evidence=key+'-http');save(key+'-after-versions.json',after)
  if kind=='forbid':assert len(after['data'])==len(before['data'])+1
  else:assert after==before and any(reason in final['error'] for reason in ('grant_state_revoked','scheduling grant stale at publish gate')),final
 print('PASS',key)
if __name__=='__main__':
 if len(sys.argv)>1 and sys.argv[1]=='revoke':hold_case('revoke','org');hold_case('revoke','personal')
 else:hold_case('forbid','org')
