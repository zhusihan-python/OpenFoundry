"""Frozen failed h10 integrity and exact new-created import subset only."""
import hashlib,json

def need(ok,why):
 if not ok:raise ValueError(why)
def local(p,path):
 prefix=p['canonical_root_label']+'/';need(path.startswith(prefix),'failedh10 local original');return path[len(prefix):]
def ref(r,p,entry):
 raw=r.read(local(p,entry['path']));need(hashlib.sha256(raw).hexdigest()==entry['sha256'],'failedh10 original SHA')
 if 'bytes' in entry:need(len(raw)==entry['bytes'],'failedh10 original size')
 return raw

def history(r,p,source):
 need(p['classification']=='failed-history-only','failedh10 cannot satisfy success')
 a=r.doc(p['audit']);need(len(a['files'])==963,'failedh10 original963 closure')
 for path,pin in a['files'].items():
  need(not path.endswith('.env'),'failedh10 private env forbidden');raw=r.read('tools/smoke/business-platform-p2-6/'+path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'failedh10 original member drift')
 need(a['original_outer_rc']==a['original_child_rc']==a['original_runner_rc']==1 and a['pre_subprocess_original_rc'] is None and a['pre_rc_artifact_literal']=='unavailable','failedh10 original rc1 versus absent pre Unix rc')
 need(a['pre_summary'] is a['post_summary'] is a['pre_kill_anchor'] is a['typed_afterpost'] is None and a['lifecycle_counts']=={'kill':0,'start':0},'failedh10 no pre/post/anchor/crash success')
 pre=a['pre_command'];need(pre['rc'] is None and r.text(local(p,pre['rc_file'])).strip()=='unavailable' and 'TimeoutExpired' in r.text(local(p,pre['stderr_artifact'])),'failedh10 actual TimeoutExpired/original unavailable')
 r.rc(p['launch_rc'],1);r.rc(p['outer']+'/child/rc',1)
 for side in ('source-before','source-after'):
  r.rc(p['outer']+'/'+side+'/rc');need(source in r.text(p['outer']+'/'+side+'/stdout.log'),'failedh10 actual source stable')
 need(r.doc(p['outer']+'/tools-before.json')==r.doc(p['outer']+'/tools-after.json'),'failedh10 actual tool map stable')
 runtime=json.loads(ref(r,p,a['runtime_result']));runner=json.loads(ref(r,p,a['runner_result']));need(runtime['source_S']==source and runtime['original_engine_or_runner_rc']==1 and runtime['h10_command_counts']=={'kill':0,'start':0} and runtime['pre_kill_anchor'] is None and runner['ok'] is False,'failedh10 actual runtime/runner failure')
 h=json.loads(ref(r,p,a['actual_handles']));need(h['count']==len(h['handles'])==44,'failedh10 original44 handles preserved');created=runtime['created_safe'];need(len(created)==4 and {x['kind'] for x in created.values()}=={'dataset','source','def','run'} and a['quiet_def']['def_id'] not in created,'failedh10 only actual4 new objects; quietDef imported')
 selected=[(i,row) for i,row in enumerate(h['handles']) if row.get('ref') in created];need([i for i,_ in selected]==[40,41,42,43] and all(not row.get('imported_from') and row['kind']==created[row['ref']]['kind'] for _,row in selected),'failedh10 exact genuine newly-created row identities')
 imports=[row for row in h['handles'] if row.get('ref') not in created];need(len(imports)==40 and all(row.get('imported_from') for row in imports),'failedh10 healthy import background retained')
 subset=r.doc(p['retained']);ledger=r.doc(p['retained_ledger']);need(subset=={'handles':[row for _,row in selected],'count':4},'retainedfailedh10 subset must exactly preserve original full semantic rows')
 need(ledger['prepare_handles']==a['actual_handles'] and len(ledger['records'])==4,'retainedfailedh10 exact origin ledger scope')
 bodies={}
 for index,((original_index,row),origin) in enumerate(zip(selected,ledger['records'])):
  need(origin['output_index']==index and origin['original_index']==original_index and origin['ref']==row['ref'] and origin['kind']==row['kind'] and origin['original_prepare_handles']==a['actual_handles'] and origin['original_full_row_sha256']==hashlib.sha256(json.dumps(row,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'retainedfailedh10 exact pathSHA/index/fullrowSHA')
  event=json.loads(ref(r,p,origin['actual_POST_event']));body=event['resp_body'].encode();need(event['method']=='POST' and event['status']==(202 if row['kind']=='run' else 201) and hashlib.sha256(body).hexdigest()==event['resp_sha256'] and len(body)==event['resp_bytes'],'retainedfailedh10 actual new POST201/202 proof');obj=json.loads(body);need(obj['id']==row['ref'] and created[obj['id']]['original_http_step']==origin['actual_runtime_HTTP_step'],'retainedfailedh10 POST and runtimecreated identity')
  step=origin['actual_runtime_HTTP_step'];meta=r.doc(local(p,step+'.meta.json'));need(meta['original_rc'] is None and meta['http_status']==event['status'] and meta['response_sha256']==event['resp_sha256'] and json.loads(r.read(local(p,step+'.raw')))==obj,'retainedfailedh10 actual runtime HTTP body agrees');bodies[row['kind']]=obj
 d,s,run,ds=[bodies[k] for k in ('def','source','run','dataset')];need(d['source_id']==s['id'] and d['output_dataset_id']==ds['id'] and run['sync_def_id']==d['id'] and s['owner_id']==ds['owner_id']==run['owner_id']==run['requested_by'] and d['org_id']==s['org_id']==ds['org_id']==run['org_id'],'retainedfailedh10 exact creation parent/actor/tenant chain')
 return {'kind':p['kind'],'failed':True,'outer_child_runner_rc':1,'pre_subprocess_Unix_rc':None,'pre_timeout':True,'kill_start':{'kill':0,'start':0},'retained_created_rows':4,'original_import_rows_preserved':40,'quiet_Def_new':False,'CURRENT_state_proven':False,'formal_success_eligible':False,'full_P25_15':False}
