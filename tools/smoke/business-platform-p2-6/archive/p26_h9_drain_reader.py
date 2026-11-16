"""Actual h9 retry natural GET drain chain; preserves first failed history."""
import hashlib,json

def need(ok,why):
 if not ok:raise ValueError(why)

def local(p,path):
 prefix=p['canonical_root_label']+'/';need(path.startswith(prefix),'drain local original');return path[len(prefix):]

def verify(r,p,audit,runtime,summary,handles):
 chainraw=r.read(p['drain_chain']);need(hashlib.sha256(chainraw).hexdigest()==audit['drain_chain_sha256'],'h9 exact drain chain original');chain=json.loads(chainraw);need(len(chain)==13,'h9 exact13 original drain samples');elapsed=[];prefixes=[]
 for index,item in enumerate(chain):
  raw=r.read(local(p,item['derived_file']));need(hashlib.sha256(raw).hexdigest()==item['sha256'] and json.loads(raw)==item['sample'],'h9 derived sample links original');sample=item['sample'];elapsed.append(sample['elapsed_seconds']);inputs=item['actual_GET_inputs'];need(len(inputs)==2,'h9 sample actual GET pair')
  for role,obs,url in zip(('fixture','proxy'),inputs,('http://127.0.0.1:17103/admin/src/p25h6','http://127.0.0.1:17104/admin/stats')):
   base=local(p,obs['prefix']);prefixes.append(base);command=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json');bodyfile=r.read(base+'.raw');need(r.text(base+'.rc').strip()=='unavailable','h9 HTTP no original Unix rc')
   need(command['method']=='GET' and command['url']=={'url':url,'query_digest':None} and command['request_body_sha256'] is None,'h9 drain fixed actual GET only')
   need(meta['original_rc'] is None and meta['http_status']==obs['http_status']==200 and obs['original_child_rc'] is None and meta['error'] is None and meta['scrub_hits']==0,'h9 HTTP transport original status, not Unix child rc')
   need(hashlib.sha256(bodyfile).hexdigest()==obs['body_sha256'],'h9 original raw FILE hash includes recorder newline')
   need(bodyfile.endswith(b'\n') and hashlib.sha256(bodyfile[:-1]).hexdigest()==meta['response_sha256'],'h9 original HTTP RESPONSE hash excludes recorder newline')
   need(json.loads(bodyfile)==sample[role],'h9 derived sample facts are actual raw GET facts')
  f=sample['fixture'];proxy=sample['proxy'];need(f['name']=='p25h6' and f['request_count']==f['inventory_started']==6 and f['inventory_finished'] in (5,6) and f['active']==6-f['inventory_finished'],'h9 no dispatch/reset during original natural drain')
  need(proxy['commit_started']==proxy['commit_finished']==21 and proxy['commit_inflight']==0,'h9 unchanged proxy drain')
  need(sample['quiet']==(f['active']==0),'h9 actual drain quiet derived correctly')
 need(len(set(prefixes))==26 and elapsed==sorted(set(elapsed)) and 0<elapsed[0]<elapsed[-1]<=10,'h9 distinct26 GETs and bounded monotonic sample times')
 need(chain[0]['sample']['fixture']['active']==1 and chain[-1]['sample']['quiet'] is True and runtime['h9_natural_drain']==audit['natural_drain']=={'samples':13,'elapsed_seconds':elapsed[-1]} and runtime['h9_natural_drain_failure'] is None,'h9 actual drain completion without excusing strict finally')
 need(summary['quiet']==audit['strict_finally_quiet'] and summary['quiet']['ok'] is True,'h9 original strict finally independently succeeds')
 old=r.doc(p['failed_launch']+'/FAILED-SUMMARY.json');original=r.read(local(p,old['summary']['path']));need(hashlib.sha256(original).hexdigest()==audit['original_failed_summary_unchanged'] and json.loads(original)['failed']==['finally-quiet'],'h9 original failed history remains failed')
 raw=r.read(local(p,old['handles']['path']));need(hashlib.sha256(raw).hexdigest()==old['handles']['sha256'] and json.loads(raw)['count']==731,'h9 full731 original failed handles retained separately')
 imported=[h for h in handles['handles'] if h.get('imported_from')==old['handles']['path']];oldparents=[h for h in json.loads(raw)['handles'] if h['kind'] in ('source','def')];need(len(imported)==len(oldparents)==728 and all(h['imported_sha256']==old['handles']['sha256'] for h in imported),'h9 retry imports728 parent references from full failed731; no fake re-creations')
 return {'actual_samples':13,'actual_GET_count':26,'HTTP_child_rc':None,'last_elapsed_seconds':elapsed[-1],'failed_history_retained':True}
