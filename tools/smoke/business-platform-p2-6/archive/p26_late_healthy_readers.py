"""Specific original h12 PG compatibility, h13 scheduler, h15 busy readers."""
import hashlib,json,re
from datetime import datetime

def need(ok,why):
 if not ok:raise ValueError(why)
def local(p,path):
 prefix=p['canonical_root_label']+'/';need(path.startswith(prefix),'late healthy local original');return path[len(prefix):]

def h12(r,p,source,case):
 evidence=case['gates'][0]['evidence'];need(len(evidence)==10,'h12 exact original ten compatibility evidence refs')
 for ref in evidence:need(hashlib.sha256(r.read(local(p,ref['file']))).hexdigest()==ref['sha256'],'h12 pinned actual compatibility evidence changed')
 base=local(p,evidence[0]['file']).rsplit('/',1)[0];name='TestPGCheckpointAdvanceAllOrNothingRollback';need(r.text(base+'/p25-h12-rollback-names.txt').strip()==name,'h12 exact rollback name')
 proof=r.doc(base+'/proof.json');need(proof['candidate_source_sha256']==source and proof['required_names_count']==1 and [x['stage'] for x in proof['stages']]==['new-normal','new-race','unified'],'h12 frozen same-S2 original proof stages')
 chains=r.doc(base+'/original-chains.json');need([x['stage'] for x in chains]==['new-normal','new-race','unified'],'h12 actual original stage chains')
 for stage,chain in zip(proof['stages'],chains):
  for path,pin in chain['original_files'].items():
   raw=r.read(local(p,path));need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'h12 original same-S PG command/raw/rc chain')
  stem=local(p,stage['attempt_base']);r.rc(stem+'.rc');raw=r.text(stem+'.raw');kv=dict(line.split('=',1) for line in r.text(stem+'.meta').splitlines() if '=' in line)
  need(kv['source_content_sha256_before']==kv['source_content_sha256_after']==source,'h12 transparent derived source metadata')
  meta_raw=r.read(local(p,kv['original_meta_path']));need(hashlib.sha256(meta_raw).hexdigest()==kv['original_meta_sha256'],'h12 derived meta links real original JSON');meta=json.loads(meta_raw);need(meta['source_before']==meta['source_after']==source and meta['rc']==meta['wrapper_rc']==0,'h12 original source and rc')
  original=local(p,chain['original_inner'])+'/01-go-test';need(r.read(original+'.raw')==raw.encode() and r.read(original+'.rc')==r.read(stem+'.rc'),'h12 raw/rc copies actual byteequal')
  need(len(re.findall(r'^=== RUN\s+'+name+r'\s*$',raw,re.M))==len(re.findall(r'^--- PASS: '+name+r'\b',raw,re.M))==1 and not re.search(r'^--- (?:FAIL|SKIP): '+name+r'\b',raw,re.M),'h12 each original stage exactly1RUN1PASS0SKIP0FAIL')

def h13(r,p,audit,records,bodies,defs,sources,runs):
 ticks={x['id']:x for obs,body in bodies if '/schedule/ticks' in obs['path'] and isinstance(body,list) for x in body if x.get('sync_def_id') in defs}
 admitted=[x for x in ticks.values() if x['outcome']=='enqueued'];busy=[x for x in ticks.values() if x['outcome']=='skipped_incremental_busy'];need(len(defs)==2 and len(sources)==1 and len(admitted)==3 and len(busy)==1 and not busy[0].get('run_id'),'h13 exact scheduler3 admitted/1 nonadmitted busy')
 need(all(x['run_id'] in runs and runs[x['run_id']]['sync_def_id']==x['sync_def_id'] and runs[x['run_id']]['requested_by']==x['actor'] for x in admitted),'h13 scheduler original tick-to-Run/parent/actor')
 snapshots=[]
 for path in audit['files']:
  if '/engine-evidence/' not in path or '/sql-facts-attempt-' not in path or not path.endswith('.json'):continue
  doc=r.doc('tools/smoke/business-platform-p2-6/'+path);need(doc['original_rc']==doc['rc']==0 and doc['parsed_ok'] is True,'h13 actual baseline/post SQL original rc')
  root=path.rsplit('/',1)[0]+'/';cmd=r.read('tools/smoke/business-platform-p2-6/'+root+doc['cmd_file']);need(hashlib.sha256(cmd).hexdigest()==doc['cmd_sha256'],'h13 original SQL command changed')
  for key in ('stdout','stderr'):
   raw=r.read(local(p,doc[key+'_file']));need(raw.decode()==doc[key] and hashlib.sha256(raw).hexdigest()==doc[key+'_sha256'],'h13 original SQL stdout/stderr binding')
  facts={}
  for line in doc['stdout'].splitlines():tag,_,rest=line.partition('|');facts.setdefault(tag,[]).append(rest)
  snapshots.append((doc['tag'],facts))
 need(len(snapshots)==2,'h13 exact original before/post SQL facts');facts=dict(snapshots);before=facts['h13-busy-baseline'];after=facts['h13-busy-post']
 for key in ('FACT_STAMP','FACT_CP','FACT_RUN'):need(before[key]==after[key],'h13 admission stamp/CP/full Run set unchanged across busy tick')
 added=set(after['FACT_TICK'])-set(before['FACT_TICK']);need(len(added)==1 and added.pop().endswith('|skipped_incremental_busy|NULL'),'h13 new busy tick never admitted Run')
 dt=lambda s:datetime.fromisoformat(s.replace('Z','+00:00'));need(dt(after['FACT_NEXTFIRE'][0])>dt(before['FACT_NEXTFIRE'][0]),'h13 cron cursor advanced without admission')
 for did in defs:
  need(any(x['method']=='DELETE' and x.get('status')==200 and x['path'].endswith('/'+did+'/schedule:arm') for x in records),'h13 own original grant revoke')
  need(any(x['method']=='PATCH' and x.get('status')==200 and x['path'].endswith('/'+did) for x in records),'h13 own original schedule disable')
  views=[body for obs,body in bodies if obs['method']=='GET' and obs['path'].endswith('/'+did) and isinstance(body,dict)];need(views and views[-1]['schedule_authorization']['locally_executable'] is False,'h13 own final public scheduler no longer executable')

def verify(r,p,source,audit,case,records,bodies,defs,sources,created_runs,runs):
 sid=p['case']
 if sid=='h12':
  need(not defs and not sources and not created_runs,'h12 no new creations or business runs');h12(r,p,source,case)
 if sid=='h13':h13(r,p,audit,records,bodies,defs,sources,runs)
 if sid=='h15':
  denied=[json.loads(x['resp_body']) for x in records if x.get('status')==409 and 'resp_body' in x];need(len(defs)==len(sources)==len(runs)==len(denied)==1 and denied[0]['code']=='sync_incremental_busy' and denied[0]['active_run_id'] in runs,'h15 original one-run busy refusal')
  did=next(iter(defs));cp0=[body for obs,body in bodies if '/syncs/'+did+'/checkpoint' in obs['path'] and isinstance(body,dict) and body.get('confirmed_epoch')==0];need(cp0,'h15 actual untouched epoch0 during held busy window')
