"""Post-failure CURRENT-only natural convergence. Never an h10 crash pass."""
import hashlib,json
from datetime import datetime

def need(ok,why):
 if not ok:raise ValueError(why)
def local(p,path):
 prefix=p['canonical_root_label']+'/';need(path.startswith(prefix),'CURRENT local original');return path[len(prefix):]
def ref(r,p,e):
 raw=r.read(local(p,e['path']));need(hashlib.sha256(raw).hexdigest()==e['sha256'],'CURRENT original ref SHA');return raw

def current(r,p,source):
 need(p['classification']=='CURRENT-readonly-only','CURRENT cannot relabel historical failure')
 a=r.doc(p['audit']);need(len(a['files'])==188,'CURRENT exact188 original member closure')
 for path,pin in a['files'].items():
  need(not path.endswith('.env'),'CURRENT private env forbidden');raw=r.read('tools/smoke/business-platform-p2-6/'+path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'CURRENT original member changed')
 base=p['base'];r.rc(base+'/launch-actual/rc');toolmap=r.doc(base+'/current-actual/tools-before.json');need(toolmap==r.doc(base+'/current-actual/tools-after.json'),'CURRENT original tool map drift')
 for path,digest in toolmap.items():
  entry=p['helper_source_snapshots'][path];need(entry['sha256']==digest and hashlib.sha256(r.read(entry['workspace_path'])).hexdigest()==digest,'CURRENT actual helper/source bytes retained')
 result=json.loads(ref(r,p,a['actual_result']));need(result['ok'] is True and result['schema']=='p26-h10-failed-current-v1' and result['root_release_sha256']==a['root_release_sha256'],'CURRENT original helper result/release metadata')
 failed={}
 for path,digest in result['failed_original_unchanged'].items():
  raw=r.read(local(p,path));need(hashlib.sha256(raw).hexdigest()==digest,'CURRENT original failure unchanged');failed[path]=json.loads(raw)
 failure=next(v for path,v in failed.items() if path.endswith('/FAILED-ORIGINAL-AUDIT.json'));need(failure['original_runner_rc']==1 and failure['pre_subprocess_original_rc'] is None and failure['lifecycle_counts']=={'kill':0,'start':0},'CURRENT retains failed non-crash history')
 created=next(v['created_safe'] for path,v in failed.items() if path.endswith('/RESULT.json'));need(result['creators']==a['known_creators']==created and len(created)==4,'CURRENT only same original four creation identities')
 ids={v['kind']:v['id'] for v in created.values()};did=ids['def'];rid=ids['run'];sid=ids['source'];dsid=ids['dataset'];bodies=result['bodies'];directory=local(p,a['actual_result']['path']).rsplit('/',1)[0];suffixes={'def':'http://127.0.0.1:17100/api/v1/data-connection/syncs/'+did,'run':'http://127.0.0.1:17100/api/v1/data-connection/syncs/'+did+'/runs/'+rid,'cp':'http://127.0.0.1:17100/api/v1/data-connection/syncs/'+did+'/checkpoint','runs':'http://127.0.0.1:17100/api/v1/data-connection/syncs/'+did+'/runs','fixture':'http://127.0.0.1:17103/admin/src/p25h13','proxy':'http://127.0.0.1:17104/admin/stats'}
 for index,(key,url) in enumerate(suffixes.items(),1):
  stem=directory+'/'+str(index).zfill(2)+'-GET-'+key;cmd=r.doc(stem+'.cmd.json');meta=r.doc(stem+'.meta.json');raw=r.read(stem+'.raw');need(cmd=={'method':'GET','url':url} and meta['original_rc'] is None and meta['http_status']==200 and meta['error'] is None and r.text(stem+'.rc').strip()=='unavailable' and json.loads(raw)==bodies[key],'CURRENT exact original six GET/raw/status/no Unix rc')
 need(a['http_original_rc'] is None and a['original_http_operation_count']==6 and a['http_statuses']==[200]*6,'CURRENT no invented HTTP Unix success')
 definition,run,cp,rows,fixture,proxy=[bodies[k] for k in ('def','run','cp','runs','fixture','proxy')]
 need(definition['id']==did and definition['source_id']==sid and definition['output_dataset_id']==dsid and definition['schedule_authorization']['locally_executable'] is False,'CURRENT actual same known Def/Source/output no executable schedule')
 actor=created[rid]['actor'];tenant=created[rid]['tenant'];need(run['id']==rid and run['sync_def_id']==did and run['status']=='succeeded' and run['rows_written']==12 and run['bytes_written']==270 and run['files_written']==1 and run['owner_id']==run['requested_by']==actor and run['org_id']==definition['org_id']==tenant,'CURRENT naturally succeeded original Run/parent/actor/tenant')
 clock=lambda s:datetime.fromisoformat(s.replace('Z','+00:00'));need(clock(run['finished_at'])>clock(failure['failure_time']),'CURRENT natural success happened after failed pre timeout')
 need(len(rows)==result['full_known_run_count']==1 and all(run[k]==v for k,v in rows[0].items()),'CURRENT complete one-row Run list binds same full Run')
 confirmed=cp['last_confirmed'];need(cp['mode']=='cursor' and cp['confirmed_epoch']==1 and cp['initialized'] is True and cp.get('owner') in (None,{}) and result['cp_owner']=={} and confirmed['run_id']==rid and confirmed['txn_id']==run['dataset_version_id'] and confirmed['rows']==run['rows_written'] and confirmed['bytes']==run['bytes_written'] and confirmed['content_hash']==run['content_hash'],'CURRENT cursor1/released owner exact confirmed Run/Txn facts')
 need(fixture==result['fixture']==a['fixture'] and fixture['request_count']==fixture['inventory_started']==fixture['inventory_finished']==5 and fixture['active']==0 and proxy==result['proxy']==a['proxy'] and proxy['commit_started']==proxy['commit_finished']==27 and proxy['commit_inflight']==0,'CURRENT original owned fixture/proxy quiet')
 guard=result['initial_whole_guard'];need(guard['ok'] is True and guard['controlled_count']==9 and guard['cp_count']==8,'CURRENT original oldguard9CP8 preserved')
 record=json.loads(ref(r,p,p['accepted_runtime_record']));need(record['source_sha256']==source and len(record['containers'])==15,'CURRENT accepted S2 runtime15 record')
 inspections={x['argv'][2]:x for x in guard['records'] if isinstance(x.get('argv'),list) and x['argv'][:2]==['docker','inspect']}
 need(set(inspections)==set(record['containers']),'CURRENT original guard exact15 identity facts')
 for name,pin in record['containers'].items():need(inspections[name]['rc']==0 and inspections[name]['stdout'].strip().splitlines()==[pin['cid'],pin['image'],pin['started_at'],'true'],'CURRENT actual runtime identity unchanged')
 return {'kind':p['kind'],'CURRENT_natural_convergence_only':True,'helper_original_Unix_rc':0,'HTTP_original_Unix_rc':None,'source_check_Unix_rc':None,'source_checks':'internal helper validate before/each GET/after; no standalone source process','original_GET_count':6,'run_status':'succeeded','confirmed_epoch':1,'CP_owner_absent':True,'full_Run_list_count':1,'original_runtime15_unchanged':True,'historical_h10_still_failed':True,'h10_crash_pass':False,'full_P25_15':False}
