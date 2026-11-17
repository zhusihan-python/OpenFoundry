#!/usr/bin/env python3
"""Real DVS committed/lost ACK -> PBS restart -> same original bytes/key receipt recovery."""
import hashlib,json,time
from canonical_acceptance import ROOT,K,command,forward,http,pg,inject,drain,save
from failure_acceptance import prepare,publish,await_state

def receipts(e):return pg("SELECT coalesce(json_agg(row_to_json(r)),json_build_array()) FROM scheduling.publish_receipts r WHERE dispatch_event_id='"+e['event_id']+"';")
def proxy_events():return [json.loads(s) for s in command(K+['exec','deploy/ofwp32-fault-proxy','--','cat','/evidence/events.jsonl'],text=True).splitlines()]
def main():
 token=(ROOT/'org-token.txt').read_text().strip();seed=json.loads((ROOT/'canonical-seed.json').read_text());key='runtime-lost-ack-r3';down=False
 with forward('ofwp32-pbs',50081) as pbs,forward('ofwp32-dvs',50078) as dvs,forward('ofwp32-fault-proxy',8080) as proxy:
  path='/v1/datasets/'+seed['output']['id'];before=http(dvs,path+'/versions',token,evidence='lost-ack-r3-product');save('lost-ack-r3-before-versions.json',before)
  publish(pbs,token,seed,'dataset_input');e=prepare(pbs,token,seed,key);http(proxy,'/__faults/drop-next','', 'POST',{'method':'POST','path':'/v1/datasets/ri.foundry.main.dataset.'+seed['output']['id']+'/outputs:commit','hold_replay':True},evidence='lost-ack-r3-control')
  inject((ROOT/(key+'-event.ndjson')).read_bytes(),key+'-producer');drain()
  end=time.monotonic()+75
  while time.monotonic()<end:
   rr=receipts(e)
   if rr:
    events=[x for x in proxy_events() if x.get('key')==rr[0]['key']]
    if any(x.get('action')=='ack_dropped' for x in events) and any(x.get('action')=='hold_started' for x in events):break
   time.sleep(.3)
  else:raise TimeoutError('real dropped ACK and replay hold not observed')
  save('lost-ack-r3-before-restart-dispatch.json',pg("SELECT row_to_json(d) FROM scheduling.dispatches d WHERE event_id='"+e['event_id']+"';"))
  try:
   command(K+['scale','deployment/ofwp32-pbs','--replicas=0'],text=True);down=True
   command(K+['wait','--for=delete','pod','-l','app=ofwp32-pbs','--timeout=45s'],text=True)
   pending=receipts(e);save('lost-ack-r3-pending-receipts.json',pending);assert len(pending)==1 and pending[0]['state'] in ('pending','unknown'),pending
   r=pending[0];body=bytes.fromhex(r['request_body'][2:]);assert hashlib.sha256(body).hexdigest()==r['request_sha256']
   ev=[x for x in proxy_events() if x.get('key')==r['key']];save('lost-ack-r3-before-restart-proxy-events.json',ev);assert any(x.get('action')=='ack_dropped' and x.get('committed_transaction_id') for x in ev),ev
   committed=http(dvs,path+'/versions',token,evidence='lost-ack-r3-product');save('lost-ack-r3-remote-committed-versions.json',committed);assert len(committed['data'])==len(before['data'])+1
   http(proxy,'/__faults/release','', 'POST',{},evidence='lost-ack-r3-control')
   command(K+['scale','deployment/ofwp32-pbs','--replicas=1'],text=True);down=False
   command(K+['rollout','status','deployment/ofwp32-pbs','--timeout=90s'],text=True)
   final=await_state(e,{'succeeded'});save('lost-ack-r3-final-dispatch.json',final);rr=receipts(e);save('lost-ack-r3-final-receipts.json',rr);assert len(rr)==1 and rr[0]['state']=='committed' and rr[0]['key']==r['key'] and rr[0]['request_body']==r['request_body']
   after=http(dvs,path+'/versions',token,evidence='lost-ack-r3-product');save('lost-ack-r3-after-versions.json',after);assert after==committed,(after,committed)
   ev=[x for x in proxy_events() if x.get('key')==r['key']];save('lost-ack-r3-final-proxy-events.json',ev);drops=[x for x in ev if x.get('action')=='ack_dropped'];passes=[x for x in ev if x.get('action')=='forwarded' and x.get('committed_transaction_id')];assert len(drops)==1 and passes
   assert all(x['request_sha256']==r['request_sha256'] for x in drops+passes)
   assert all(x['committed_transaction_id']==drops[0]['committed_transaction_id'] for x in passes)
  finally:
   if down:command(K+['scale','deployment/ofwp32-pbs','--replicas=1'],text=True)
 print('PASS real DVS committed ACK lost, PBS restarted, same bytes/key recovered original txn with no extra version')
if __name__=='__main__':main()
