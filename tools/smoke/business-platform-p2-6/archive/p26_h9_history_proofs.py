"""Read failed h9 and later CURRENT quiet as separate non-satisfying history."""
import hashlib,json
P='tools/smoke/business-platform-p2-6/'

def need(ok,why):
 if not ok:raise ValueError(why)

def ref(r,p,entry):
 prefix=p['canonical_root_label']+'/';need(entry['path'].startswith(prefix),'h9 local original ref');raw=r.read(entry['path'][len(prefix):]);need(hashlib.sha256(raw).hexdigest()==entry['sha256'],'h9 referenced original SHA');return json.loads(raw)

def members(r,files,count):
 need(len(files)==count,'h9 exact original member count')
 for path,pin in files.items():
  need(not path.lower().endswith('.env'),'h9 private file forbidden');raw=r.read(P+path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'h9 original member SHA/size')

def outer(r,base,source,expected):
 r.rc(base+'/child/rc',expected);need(r.doc(base+'/tools-before.json')==r.doc(base+'/tools-after.json'),'h9 original tools stable')
 for side in ('source-before','source-after'):
  r.rc(base+'/'+side+'/rc');need(source in r.text(base+'/'+side+'/stdout.log'),'h9 original source stable')
 for label,digest in r.doc(base+'/tools-before.json').items():need(hashlib.sha256(r.read(base+'/tools-before/'+label)).hexdigest()==digest,'h9 actual historical tool SHA')

def failed(r,p,source):
 need(p['classification']=='failed-history-only','h9 failure cannot satisfy pass scope')
 summary=r.doc(p['launch']+'/FAILED-SUMMARY.json');files=r.doc(p['launch']+'/original-files.json');members(r,files,447);r.rc(p['launch']+'/rc',1);outer(r,p['outer'],source,1)
 need(summary['source']==source and summary['original_launch_rc']==summary['original_child_rc']==summary['runtime_original_engine_rc']==1 and summary['runtime_exception'] is None,'h9 original failure RC/source')
 runtime=ref(r,p,summary['runtime_terminal']);need(runtime['source_S']==source and runtime['original_engine_or_runner_rc']==1 and runtime['wrapper_exception'] is None and runtime['h10_command_counts']=={'kill':0,'start':0},'h9 failed actual runtime original')
 original=ref(r,p,summary['summary']);need(original['failed']==['finally-quiet'] and len(original['scenarios'])==1,'h9 original global finally failure retained')
 case=original['scenarios'][0];need(case['id']=='h9' and case['failed'] is False and len(case['gates'])==8 and all(g['gate']=='G_RETAIN_UNKNOWN' and g['ok'] is True and g['detail'] for g in case['gates']),'h9 original eight business gates remain distinct from failed finally')
 need(original['quiet']==summary['original_finally_quiet'] and original['quiet']['ok'] is False and original['quiet']['fixture']['p25h6']['inventory_started']==4 and original['quiet']['fixture']['p25h6']['inventory_finished']==3 and original['quiet']['fixture']['p25h6']['active']==1,'h9 original active1 not overwritten')
 need(original['final_owner_roster']==original['final_run_roster']==[],'h9 original empty final rosters')
 handles=ref(r,p,summary['handles']);need(handles['count']==len(handles['handles'])==731,'h9 whole original handles')
 export=ref(r,p,summary['typed_export']);need(export['entries']==export['public_views']==[],'h9 empty typed new owners')
 return {'kind':p['kind'],'original_rc':1,'business_gates_true':8,'original_finally_quiet':False,'formal_success_eligible':False,'full_P25_15':False}

def current(r,p,source):
 need(p['classification']=='CURRENT-readonly-only','CURRENT cannot satisfy historical h9')
 audit=r.doc(p['audit']);members(r,audit['files'],266);need(audit['source']==source and audit['original_launch_rc']==audit['original_child_rc']==audit['runtime_rc']==0 and audit['HTTP_methods']=={'GET':7},'CURRENT read-only original status')
 r.rc(p['base']+'/outer-launch-actual/rc');outer(r,p['base']+'/outer-actual',source,0)
 outer_result=r.doc(p['base']+'/outer-actual/RESULT.json');runtime=ref(r,p,outer_result['links']['runtime_result']);need(runtime['source_S']==source and runtime['original_engine_or_runner_rc']==0 and runtime['wrapper_exception'] is None and runtime['created_safe']=={} and runtime['h10_command_counts']=={'kill':0,'start':0},'CURRENT original runtime source/status/no new creation or lifecycle')
 guard=runtime['initial_guard'];need(guard['ok'] is True and guard['controlled_count']==9 and guard['cp_count']==8,'CURRENT own original old guard9CP8')
 proof=ref(r,p,audit['proof']);need(proof['ok'] is True and proof['bases']=={'fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'} and len(proof['raw_records'])==7,'CURRENT exact fixed bases/get7')
 need(proof['quiet']==audit['current_quiet'] and proof['quiet']['ok'] is True and proof['quiet']['problems']==[],'CURRENT own quiet proof only')
 need(set(proof['quiet']['fixture'])==set(proof['source_names'])=={'p25empty','p25h1','p25h2','p25h3','p25h4','p25h6'},'CURRENT exact6 source scope')
 for entry in proof['handles_inputs']:ref(r,p,entry)
 observations={}
 for entry in proof['raw_records']:
  event=ref(r,p,entry);need(event['method']=='GET' and event['status']==200 and event['error'] is None and event['request_body_sha256'] is None,'CURRENT seven successful GET only')
  body=event['resp_body'].encode();need(hashlib.sha256(body).hexdigest()==event['resp_sha256'] and len(body)==event['resp_bytes'],'CURRENT original observed body SHA/size');need(event['path'] not in observations,'CURRENT duplicate event');observations[event['path']]=json.loads(body)
 for name,row in proof['quiet']['fixture'].items():
  need(row['active']==0 and row['inventory_started']==row['inventory_finished'] and row==observations['http://127.0.0.1:17103/admin/src/'+name],'CURRENT complete original fixture observation')
 proxy=proof['quiet']['proxy'];need(proxy==observations['http://127.0.0.1:17104/admin/stats'] and proxy['commit_started']==proxy['commit_finished']==20 and proxy['commit_inflight']==0,'CURRENT original proxy quiet')
 h6=proof['quiet']['fixture']['p25h6'];need(h6['request_count']==h6['inventory_started']==h6['inventory_finished']==4,'CURRENT existing request drained without extra dispatch')
 failed_summary=ref(r,p,p['failed_summary']);need(failed_summary['failed']==['finally-quiet'] and p['failed_summary']['sha256']==audit['original_failed_summary_unchanged_sha256'],'CURRENT cannot amend original failure')
 return {'kind':p['kind'],'original_rc':0,'CURRENT_quiet_only':True,'read_only_GET_count':7,'original_h9_still_failed':True,'formal_success_eligible':False,'full_P25_15':False}
