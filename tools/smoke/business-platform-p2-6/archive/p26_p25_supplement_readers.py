"""Independent stdlib readers for actual P25 supplemental components only."""
import hashlib,json,re
from datetime import datetime

def need(ok,why):
 if not ok:raise ValueError(why)
def local(p,path):
 prefix=p['canonical_root_label']+'/';need(path.startswith(prefix),'supplement local original');return path[len(prefix):]
def readref(r,p,entry):
 raw=r.read(local(p,entry['path']));need(hashlib.sha256(raw).hexdigest()==entry['sha256'],'supplement original ref SHA')
 if 'bytes' in entry:need(len(raw)==entry['bytes'],'supplement original ref size')
 return raw

def closed(r,p,source,count):
 audit=r.doc(p['audit']);need(len(audit['files'])==count,'supplement exact original audit member closure')
 for path,pin in audit['files'].items():
  raw=r.read('tools/smoke/business-platform-p2-6/'+path);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'supplement original member changed')
 r.rc(p['launch_rc']);base=p['outer'];r.rc(base+'/child/rc')
 for side in ('source-before','source-after'):
  r.rc(base+'/'+side+'/rc');need(source in r.text(base+'/'+side+'/stdout.log'),'supplement original source before/after')
 tools=r.doc(base+'/tools-before.json');need(tools==r.doc(base+'/tools-after.json'),'supplement actual tools stable')
 for path,digest in tools.items():need(hashlib.sha256(r.read(base+'/tools-before/'+path)).hexdigest()==digest,'supplement historical imported tool source')
 runtime=json.loads(readref(r,p,audit['runtime_result']));need(runtime['source_S']==source and runtime['original_engine_or_runner_rc']==0 and runtime['wrapper_exception'] is None and runtime['created_safe']=={} and runtime['h10_command_counts']=={'kill':0,'start':0},'supplement actual runtime source/rc/no creation/lifecycle')
 return audit,runtime

def ledger(r,p,source):
 audit,runtime=closed(r,p,source,245);execution=audit['verifier_execution'];need(execution['returned_ok'] is True and execution['separate_process_rc'] is None and execution['enclosing_runtime_original_rc']==0,'h13 verifier was in-process; no fabricated Unix verifier rc')
 proof=json.loads(readref(r,p,audit['ledger_proof']));body=json.loads(readref(r,p,audit['ledger_body']));need(proof['ok'] is True and proof['schema']=='p25-h13-ledger-verdict-v1' and len(body)==4,'h13 exact original ledger body/verdict')
 inputs=[]
 for entry in proof['inputs']:inputs.append((entry['path'],readref(r,p,entry)))
 stepmeta=json.loads(next(raw for path,raw in inputs if path.endswith('/ledger-step/stdout.log')));need(stepmeta['schema']=='p25-h13-ledger-read-v1' and stepmeta['status']==200 and stepmeta['original_rc']==0 and stepmeta['body']==body and stepmeta['body_sha256']==audit['ledger_body']['sha256'],'h13 actual original reader/HTTP status and body')
 r.rc(local(p,next(path for path,_ in inputs if path.endswith('/ledger-step/rc'))));summaryraw=r.read(local(p,stepmeta['h13_summary']));need(hashlib.sha256(summaryraw).hexdigest()==stepmeta['h13_summary_sha256'],'h13 successful actual summary immutable')
 summary=json.loads(summaryraw);need(summary['failed']==[] and [x['id'] for x in summary['scenarios']]==['h13'] and len(summary['scenarios'][0]['gates'])==42 and all(x['ok'] is True for x in summary['scenarios'][0]['gates']),'h13 actual42 successful source gates')
 runs={};defs={};ticks=set();manual=set()
 for path,raw in inputs:
  if not re.search(r'-attempt-[0-9]+\.json$',path):continue
  event=json.loads(raw)
  if event.get('status') not in (200,201,202) or not isinstance(event.get('resp_body'),str):continue
  observed=event['resp_body'].encode();need(hashlib.sha256(observed).hexdigest()==event['resp_sha256'] and len(observed)==event['resp_bytes'],'h13 exact source event body/size')
  try:value=json.loads(observed)
  except ValueError:continue
  if isinstance(value,dict) and value.get('status')=='succeeded' and value.get('sync_def_id') and value.get('incremental'):
   need(value['id'] not in runs or runs[value['id']]==value,'h13 actual successful Run immutable');runs[value['id']]=value
  if isinstance(value,dict) and value.get('id') and value.get('source_id') and value.get('output_dataset_id'):defs[value['id']]=value
  if isinstance(value,list) and '/schedule/ticks' in event.get('path',''):ticks|={x['run_id'] for x in value if x.get('outcome')=='enqueued'}
  if event.get('method')=='POST' and event.get('path','').endswith('/run') and event.get('query')=='async=true' and isinstance(value,dict):manual.add(value['id'])
 need(len(runs)==4 and len(set(runs)&ticks)==3 and len(set(runs)&manual)==1 and not ticks&manual,'h13 actual3 scheduled plus1 manual Run')
 stamp=lambda text:datetime.fromisoformat(text.replace('Z','+00:00')).timestamp();used=set();result=[];windows=[]
 for rid,run in sorted(runs.items(),key=lambda item:stamp(item[1]['started_at'])):
  start,end=stamp(run['started_at']),stamp(run['publish_entered_at']);need(start<end and all(not(start<=b and end>=a) for a,b in windows),'h13 actual disjoint fetch/publish windows');windows.append((start,end));epoch=run['incremental']['base_epoch'];need(type(epoch) is int and epoch in (0,1),'h13 actual captured base epoch')
  group=[(i,e) for i,e in enumerate(body) if start<=e['ts']<=end];need(len(group)==1,'h13 one actual page per successful window')
  i,page=group[0];need(i not in used and page['window']==epoch and page['since_param']=='since_token' and page['token']==(None if epoch==0 else 'p25h13-tok-0') and page['cursor']=='' and type(page['page_size']) is int and page['page_size']==50,'h13 actual fixed cursor/token/window page origin');used.add(i);parent=defs[run['sync_def_id']]
  result.append({'run_id':rid,'def_id':run['sync_def_id'],'source_id':parent['source_id'],'base_epoch':epoch,'started_at':run['started_at'],'publish_entered_at':run['publish_entered_at'],'ledger_indices':[i],'expected_pages':1})
 need(result==proof['runs']==audit['proved_runs'] and sorted(used)==proof['used_ledger_indices']==[0,1,2,3] and proof['excluded_historical_indices']==[],'h13 actual ledger independently recomputed exactly')
 need(len({x['source_id'] for x in result})==1 and len({x['def_id'] for x in result})==2 and all(sorted(x['base_epoch'] for x in result if x['def_id']==did)==[0,1] for did in {x['def_id'] for x in result}),'h13 exact parent/two-batch lineage')
 return {'kind':p['kind'],'actual_ledger_GET':1,'actual_reader_Unix_rc':0,'enclosing_runtime_Unix_rc':0,'in_process_verifier_returned_ok':True,'separate_verifier_Unix_rc':None,'proved_runs':4,'full_P25_15':False}

def quiet(r,p,source):
 audit,runtime=closed(r,p,source,271);proof=json.loads(readref(r,p,audit['quiet_proof']));need(proof['schema']=='p25-owned-quiet-proof-v1' and proof['ok'] is True,'global quiet actual proof schema/status')
 need(proof['handles_inputs']==audit['handles_inputs_exact13']==p['expected_original_handles'] and len(proof['handles_inputs'])==13,'global quiet exact13 ORIGINAL source order; derived union forbidden')
 argv=r.doc(p['outer']+'/child/cmd.json');handles=[argv[i+1] for i,v in enumerate(argv[:-1]) if v=='--handles'];need(handles==[x['path'] for x in p['expected_original_handles']],'global quiet original child argv exact handle order')
 names=set()
 for entry in proof['handles_inputs']:
  doc=json.loads(readref(r,p,entry));need(doc['count']==len(doc['handles']),'global quiet complete original handles count')
  for h in doc['handles']:
   if h.get('ok') and h.get('kind')=='source':
    name=h.get('config',{}).get('sync',{}).get('query_params',{}).get('src')
    if name:names.add(name)
 need(names==set(proof['source_names'])=={'p25empty','p25h1','p25h13','p25h2','p25h3','p25h4','p25h6'} and proof['source_names']==sorted(names),'global quiet exact7 fixture sources derived from ALL13 original inputs')
 need(proof['bases']=={'fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'} and proof['engine_sha256']=='f931dca20e5be294e250f551063f282c97233fe812ed54ced56d32809a6e254d','global quiet fixed S2 engine/origins')
 need(len(proof['raw_records'])==8,'global quiet exact8 original GET records');observations={}
 for entry in proof['raw_records']:
  event=json.loads(readref(r,p,entry));need(event['method']=='GET' and event['status']==200 and event['error'] is None and event['request_body_sha256'] is None,'global quiet original GET status/no new writes')
  body=event['resp_body'].encode();need(hashlib.sha256(body).hexdigest()==event['resp_sha256'] and len(body)==event['resp_bytes'],'global quiet exact original response body/hash/size');need(event['path'] not in observations,'global quiet duplicate path');observations[event['path']]=json.loads(body)
 whole=proof['quiet'];need(whole==audit['quiet'] and whole['ok'] is True and whole['problems']==[] and set(whole['fixture'])==names,'global quiet exact complete original observed fixtures')
 for name,row in whole['fixture'].items():need(row==observations['http://127.0.0.1:17103/admin/src/'+name] and row['name']==name and row['active']==0 and row['inventory_started']==row['inventory_finished'],'global quiet actual fixture all requests drained')
 proxy=whole['proxy'];need(proxy==observations['http://127.0.0.1:17104/admin/stats'] and proxy['commit_started']==proxy['commit_finished']==26 and proxy['commit_inflight']==0,'global quiet actual proxy drained')
 guard=runtime['initial_guard'];need(guard['ok'] is True and guard['controlled_count']==9 and guard['cp_count']==8,'global quiet own original whole guard9CP8')
 return {'kind':p['kind'],'original_handles_input_count':13,'actual_GET_count':8,'fixture_sources':7,'enclosing_runtime_Unix_rc':0,'separate_quiet_verifier_Unix_rc':None,'original_HTTP_child_rc':None,'global_fixture_proxy_quiet':True,'full_P25_15':False}
