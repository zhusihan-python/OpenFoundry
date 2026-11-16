#!/usr/bin/env python3
"""Pure, pinned h14 lost-201-ACK supplement. Never performs HTTP or mutates original evidence."""
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex

HERE=Path(__file__).resolve().parent
TAIL_SHA='c340abeb0b74da9b02dbeee25781187a8688b9fb003714ebbfbfc9989646ee34'
ACK_SHA='a04489f83f1a6410cbf4817dac8376bb185446e7653f03e9f8219e6c516b5027'
LEDGER_SHA='c2d283f8aa4b0227a75f3636af541efd62add567ab7b5fa5e424f6d0fa341e3f'
OLD_PROXY_SHA='a6a4aa06535c6b5eafdb9b49243b30a67eb9c4cdfc52befa5086f1e937a557f3'
IDENTITY={'run_id':'90b39f97-9bf9-41f6-a65f-c84a23b2d508','def_id':'e6ef1ec8-315e-41d0-8c8c-dbe6e3bbfd94',
 'source_id':'530544e2-b1b4-41ab-aa18-872c17854ea0','target_id':'cb5f193c-a5ca-4512-8f72-9f054aa9312e',
 'transaction_id':'bac73c15-b4ae-4a58-be24-0b1d5bfc0d78','ack_file_id':'852fde85-9f3a-4c97-8853-0d92791a5287',
 'file_id':'a6fef0ca-5709-405d-a82a-a14524f73415'}
BASE='http://127.0.0.1:17078/v1/datasets/'+IDENTITY['target_id']


def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def digest(raw):return hashlib.sha256(raw).hexdigest()
def data(value):return (json.dumps(value,indent=2)+'\n').encode()
def absolute(p):return str(Path(p).absolute())
def load(path,name):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m


def build(arguments,resolve_path=None):
 resolve=resolve_path or Path;links=[]
 def linked(path):
  p=resolve(str(path));links.append({'path':str(path),'sha256':sha(p)});return p
 def read(path):return json.loads(linked(path).read_text())
 q=load(HERE/'collect_http_quiet_p25.py','h14_engine_loader');m=q.engine()
 original=read(arguments['original_tail'])
 if sha(resolve(arguments['original_tail']))!=TAIL_SHA or original.get('phase')!='tail' or original.get('failed')!=['h14']:
  raise ValueError('only the pinned original failed h14 tail is eligible')
 step=Path(arguments['original_step'])
 if linked(step/'rc').read_text().strip()!='1':raise ValueError('original tail rc1 must remain preserved')
 command=linked(step/'cmd.txt').read_text().strip();argv=shlex.split(command[5:] if command.startswith('CMD: ') else command)
 linked(step/'stderr.log');stdout=read(step/'stdout.log')
 def flag(name):
  if argv.count(name)!=1:raise ValueError('source tail argv missing/duplicate '+name)
  return argv[argv.index(name)+1]
 if flag('--phase')!='tail' or '--released' not in argv or absolute(flag('--result-file'))!=arguments['original_tail'] or absolute(stdout.get('result_file',''))!=arguments['original_tail'] or stdout.get('failed')!=['h14']:
  raise ValueError('actual original tail CLI/raw stdout not bound')
 old_proxy=linked(arguments['old_proxy'])
 if sha(old_proxy)!=OLD_PROXY_SHA:raise ValueError('original proxy source bytes/SHA mismatch')
 proxy=load(old_proxy,'h14_original_proxy_parser')
 cases={r['id']:r for r in original['scenarios']}
 if set(cases)!={'h11','h14'} or cases['h11'].get('failed') is not False or any(g.get('ok') is not True for g in cases['h11']['gates']):
  raise ValueError('original h11 successful facts changed')
 h14=cases['h14'];failed=[i for i,g in enumerate(h14['gates']) if g.get('ok') is not True]
 if failed!=[0,2] or any(h14['gates'][i].get('gate')!='G_ACK_STRICT' for i in failed) or h14.get('failed') is not True:
  raise ValueError('only two known h14 ACK assertion gaps can be supplemented')
 if len(h14['gates'])!=5 or h14['gates'][0]['detail']!='exactly ONE forwarded+verified drop entry for THIS target (request_target filter; delta=1 incr=1)' or not h14['gates'][2]['detail'].endswith(': no ack_txn_id'):
  raise ValueError('original h14 failure trigger differs from pinned status201 parser defect')
 records=[];evidence=original['attempts']['evidence_dir']
 for p in sorted(resolve(evidence).glob('*-attempt-*.json')):
  logical=str(Path(evidence)/p.name);records.append((logical,read(logical)))
 if any(sum(d==row for _,d in records)!=1 for row in original['scenarios']):raise ValueError('original scenario recorder rows differ')
 def response(d):
  raw=d.get('resp_body','').encode()
  if d.get('status')!=200 or d.get('resp_sha256')!=digest(raw) or d.get('resp_bytes')!=len(raw):raise ValueError('saved HTTP status/body SHA/bytes mismatch')
  return json.loads(raw)
 did,rid,target=IDENTITY['def_id'],IDENTITY['run_id'],IDENTITY['target_id']
 logs=[(p,response(d)) for p,d in records if d.get('base')=='proxy_admin' and d.get('method')=='GET' and d.get('path','').endswith('/admin/log') and Path(p).name.startswith('h14-')]
 if len(logs)!=2:raise ValueError('expected exact before/after h14 proxy ledger reads')
 before=[e for e in logs[0][1] if e.get('request_target')==target];after=[e for e in logs[1][1] if e.get('request_target')==target]
 if before or len(after)!=1 or sha(resolve(logs[1][0]))!=LEDGER_SHA:raise ValueError('expected exact one actual target201 ACK drop dispatch')
 entry=after[0]
 if entry.get('mode')!='drop_after_verify' or entry.get('method')!='POST' or entry.get('path')!='/api/v1/datasets/'+target+'/outputs:commit' or entry.get('forwarded') is not True or entry.get('upstream_status')!=201 or entry.get('error') is not None or entry.get('verified_committed') is not False or entry.get('ack_txn_id') is not None:
  raise ValueError('old proxy status201/verifiedfalse raw history changed')
 ack_raw=entry.get('upstream_ack_raw','').encode()
 if digest(ack_raw)!=ACK_SHA or entry.get('upstream_ack_sha256')!=ACK_SHA:raise ValueError('actual raw upstream201 ACK SHA mismatch')
 parsed=proxy.parse_ack(ack_raw)
 if proxy.verify_commit_ack(parsed,target).get('verified_committed') is not True or parsed.get('ack_txn_id')!=IDENTITY['transaction_id']:
  raise ValueError('raw201 ACK is not strictly usable')
 ack=json.loads(ack_raw);acktxn=ack.get('transaction') or {}
 if acktxn.get('dataset_id')!=target or acktxn.get('status')!='COMMITTED':raise ValueError('nested ACK transaction target/status mismatch')
 def body_for(path):
  hits=[response(d) for _,d in records if d.get('method')=='GET' and d.get('path','').endswith(path)]
  if not hits:raise ValueError('required original HTTP observation missing: '+path)
  return hits[-1]
 local=body_for('/syncs/'+did+'/runs/'+rid);cp=body_for('/syncs/'+did+'/checkpoint');dv=body_for('/syncs/'+did)
 if local.get('id')!=rid or local.get('sync_def_id')!=did or local.get('status')!='failed' or local.get('dataset_version_id') is not None or not local.get('publish_entered_at') or not str(local.get('error','')).endswith(': EOF') or '/datasets/'+target+'/outputs:commit' not in local['error'] or local.get('incremental',{}).get('base_epoch')!=0:
  raise ValueError('original local outcome is not captured postpublish lost ACK / no local transaction')
 owner=cp.get('owner') or {}
 if cp.get('mode')!='cursor' or cp.get('initialized') is not False or cp.get('confirmed_epoch')!=0 or owner.get('run_id')!=rid or owner.get('run_status')!='failed' or owner.get('blocked_reason')!='requires_reconciliation' or dv.get('id')!=did or dv.get('source_id')!=IDENTITY['source_id'] or dv.get('output_dataset_id')!=target:
  raise ValueError('original actual parent / CP0 retained owner changed')
 old_bind_path=original['tail_bindings_file'];old_bind=read(old_bind_path);decl=read(flag('--tail-declaration'))
 if old_bind.get('summary_sha256')!=TAIL_SHA or old_bind.get('bindings')!=original.get('tail_bindings') or old_bind.get('problems')!=[] or old_bind.get('bindings_sha256')!=digest(json.dumps(old_bind['bindings'],sort_keys=True).encode()) or old_bind.get('declaration_sha256')!=decl.get('declaration_sha256'):
  raise ValueError('original actual bindings/declaration/summary integrity mismatch')
 if len(original.get('final_owner_roster',[]))!=8 or len(original.get('final_run_roster',[]))!=1 or original.get('tail_adjudication',{}).get('ok') is not True or original.get('quiet',{}).get('ok') is not True:
  raise ValueError('original retained dual roster/quiet/adjudication changed')
 mode={(r.get('def_id'),r.get('run_id')):r.get('mode') for case in original['scenarios'] for r in case['owner_roster']}
 owners=[dict(o,mode=mode.get((o.get('def_id'),o.get('run_id')))) for o in original['final_owner_roster']]
 adjudicated=m.adjudicate_tail(decl,owners,original['final_run_roster'])
 if not adjudicated['ok'] or adjudicated['bindings']!=old_bind['bindings']:raise ValueError('original immutable declaration/actual exact dual rosters fail replay')
 remote=read(arguments['remote_summary'])
 if remote.get('schema')!='p25-h14-read-supplement-v1' or remote.get('exact_binding')!=IDENTITY or remote.get('upstream_ack_sha256')!=ACK_SHA:raise ValueError('remote supplement actual identity contract mismatch')
 for name,pin in (('original_tail',TAIL_SHA),('original_proxy_ledger_record',LEDGER_SHA)):
  ref=remote['inputs'][name]
  if sha(linked(ref['path']))!=pin or ref['sha256']!=pin:raise ValueError('remote supplement original input mismatch')
 previous=remote['previous_attempt']
 if sha(linked(previous['path']))!=previous['sha256']:raise ValueError('previous failed download attempt SHA changed')
 def artifact(group,url,status=200,rc=0):
  if set(group)!={'cmd','body','status','rc','stderr'}:raise ValueError('remote raw group not exact')
  raw={}
  for key,ref in group.items():
   p=linked(ref['path']);b=p.read_bytes()
   if digest(b)!=ref['sha256'] or len(b)!=ref['bytes']:raise ValueError('remote original raw bytes/SHA mismatch')
   raw[key]=b
  command=json.loads(raw['cmd'])
  if command.get('method')!='GET' or command.get('url')!=url:raise ValueError('remote source must be exact direct readonly GET')
  if raw['status'].decode().strip()!=str(status) or raw['rc'].decode().strip()!=str(rc):raise ValueError('remote actual HTTP status/originalrc mismatch')
  return raw['body']
 groups=remote.get('artifacts') or {}
 if set(groups)!={'transaction','files','download'}:raise ValueError('exact three independent remote reads required')
 txn=json.loads(artifact(groups['transaction'],BASE+'/branches/main/transactions/'+IDENTITY['transaction_id']))
 env=json.loads(artifact(groups['files'],BASE+'/files'))
 download=artifact(groups['download'],BASE+'/files/'+IDENTITY['file_id']+'/download')
 artifact(remote['failed_ack_download'],BASE+'/files/'+IDENTITY['ack_file_id']+'/download',404,1)
 expected=m._ndjson(m.window_values('p25h1',0));meta=txn.get('metadata') or {};lineage=meta.get('lineage') or {}
 if txn.get('id')!=IDENTITY['transaction_id'] or txn.get('dataset_id')!=target or txn.get('branch_name')!='main' or txn.get('tx_type')!='SNAPSHOT' or txn.get('status')!='COMMITTED' or lineage!={'source_id':IDENTITY['source_id'],'sync_def_id':did,'sync_run_id':rid} or meta.get('rows')!=24 or meta.get('bytes')!=558 or meta.get('content_hash')!=digest(expected):raise ValueError('independent flat transaction COMMITTED/main/SNAPSHOT/lineage/rows/bytes/hash mismatch')
 files=env.get('files')
 if env.get('branch')!='main' or env.get('total')!=1 or not isinstance(files,list) or len(files)!=1:raise ValueError('independent fresh-target files envelope not exact')
 file=files[0]
 if file.get('id')!=IDENTITY['file_id'] or file.get('dataset_id')!=target or file.get('transaction_id')!=IDENTITY['transaction_id'] or file.get('size_bytes')!=558 or file.get('logical_path')!='sync-'+rid+'.ndjson' or download!=expected or len(download)!=558 or m.ndjson_values(download)!=m.window_values('p25h1',0):raise ValueError('actual file-index transaction binding / independent exact downloaded bytes differ')
 if local.get('content_hash')!=digest(expected) or acktxn.get('metadata',{}).get('content_hash')!=digest(expected):raise ValueError('local/rawACK content hash differs from independent bytes')
 proof={'schema':'p25-h14-supplement-proof-v1','ok':True,'source_arguments':arguments,'inputs':links,
  'identity':IDENTITY,'original_tail_sha256':TAIL_SHA,'original_tail_rc':1,'original_failed':['h14'],
  'original_proxy_verified_committed':False,'strict_raw_201_ack_usable':True,'raw_ack_sha256':ACK_SHA,
  'independent_download_sha256':digest(download),'independent_download_bytes':558,'rows':24,
  'preserved_failed_ack_download_status':404,'supplemented_gate_indices':[0,2]}
 proof_bytes=data(proof);aggregate=copy.deepcopy(original)
 aggregate['schema']='p25-tail-aggregate-v1';aggregate['failed']=[];aggregate['tail_bindings_file']=arguments['bindings_output']
 selected=next(r for r in aggregate['scenarios'] if r['id']=='h14');selected['failed']=False
 for i in (0,2):
  selected['gates'][i]={'gate':'G_ACK_STRICT','ok':True,'detail':'h14 savedraw supplement: usable upstream201 ACK and independent committed transaction/file bytes verified',
                       'evidence':[{'proof_file':arguments['proof_output'],'proof_sha256':digest(proof_bytes),'original_gate_index':i}]}
 aggregate['supplement']={'proof_file':arguments['proof_output'],'proof_sha256':digest(proof_bytes),'source_arguments':arguments,
  'original_tail':{'path':arguments['original_tail'],'sha256':TAIL_SHA,'original_rc':1},
  'original_bindings':{'path':old_bind_path,'sha256':sha(resolve(old_bind_path))},'replaced_gates':[{'case':'h14','index':i,'original_gate':h14['gates'][i]} for i in (0,2)]}
 aggregate_bytes=data(aggregate);binding=copy.deepcopy(old_bind);binding['summary_sha256']=digest(aggregate_bytes)
 binding['supplement']={'original_bindings_file':old_bind_path,'original_bindings_sha256':sha(resolve(old_bind_path)),
                       'original_summary_sha256':TAIL_SHA,'proof_file':arguments['proof_output'],'proof_sha256':digest(proof_bytes)}
 return aggregate_bytes,data(binding),proof_bytes


def verify_supplement(path,resolve_path=None):
 resolve=resolve_path or Path
 try:
  doc=json.loads(resolve(str(path)).read_text())
  if doc.get('schema')!='p25-tail-aggregate-v1':raise ValueError('typed tail aggregate required')
  args=doc['supplement']['source_arguments'];aggregate,bindings,proof=build(args,resolve)
  if resolve(str(path)).read_bytes()!=aggregate or resolve(args['bindings_output']).read_bytes()!=bindings or resolve(args['proof_output']).read_bytes()!=proof:
   raise ValueError('derived tail aggregate / selected bindings / proof differ from complete source replay')
  return {'ok':True,'problems':[]}
 except Exception as exc:return {'ok':False,'problems':[str(exc)]}


def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__)
 for name in ('original-tail','original-step','remote-summary','old-proxy','output','bindings-output','proof-output'):p.add_argument('--'+name,required=True)
 a=p.parse_args(argv);args={k:absolute(v) for k,v in vars(a).items() if k!='output'}
 try:
  outputs=[Path(a.output),Path(a.bindings_output),Path(a.proof_output)]
  if len(set(p.absolute() for p in outputs))!=3 or any(p.exists() for p in outputs):raise ValueError('fresh distinct derived outputs required')
  aggregate,bindings,proof=build(args)
  for path,raw in zip(outputs,(aggregate,bindings,proof)):path.write_bytes(raw)
  result=verify_supplement(absolute(a.output));print(json.dumps({'aggregate_file':absolute(a.output),**result}));return 0 if result['ok'] else 1
 except Exception as exc:print(json.dumps({'ok':False,'problems':[str(exc)]}));return 1

if __name__=='__main__':raise SystemExit(main())
