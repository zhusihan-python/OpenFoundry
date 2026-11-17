#!/usr/bin/env python3
"""Real isolated HTTP/PG/Kafka acceptance helpers; no production fault endpoints."""
import concurrent.futures, contextlib, datetime, importlib.util, json, os, pathlib, socket, subprocess, sys, time, urllib.error, urllib.request
K=['kubectl','--kubeconfig','/private/tmp/ofwp32-acc/kubeconfig','-n','ofwp32']
ROOT=pathlib.Path(os.environ.get('OFWP32_EVIDENCE','/private/tmp/ofwp32-runtime-sol-evidence'))
def save(name,value):
 (ROOT/name).write_text(json.dumps(value,indent=2) if not isinstance(value,str) else value)
def command(args,**kw):
 p=subprocess.run(args,capture_output=True,**kw)
 if p.returncode:raise RuntimeError((args,p.returncode,p.stderr))
 return p.stdout
@contextlib.contextmanager
def forward(service,port):
 with socket.socket() as s:s.bind(('127.0.0.1',0));local=s.getsockname()[1]
 log=open(ROOT/(service+'-forward.log'),'ab');p=subprocess.Popen(K+['port-forward','svc/'+service,f'{local}:{port}'],stdout=log,stderr=log)
 try:
  for _ in range(50):
   if p.poll() is not None:raise RuntimeError('port-forward exited')
   try:
    with socket.create_connection(('127.0.0.1',local),timeout=.2):break
   except OSError:time.sleep(.1)
  else:raise TimeoutError('port-forward listener')
  yield f'http://127.0.0.1:{local}'
 finally:p.terminate();p.wait(timeout=5);log.close()
def http(base,path,token,method='GET',body=None,allowed=(200,201,202,204),evidence='http'):
 req=urllib.request.Request(base+path,method=method,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'},data=None if body is None else json.dumps(body).encode())
 try:
  with urllib.request.urlopen(req,timeout=35) as f:code,raw=f.status,f.read()
 except urllib.error.HTTPError as e:code,raw=e.code,e.read()
 rec={'path':path,'method':method,'status':code,'body':raw.decode()}
 with (ROOT/(evidence+'.jsonl')).open('a') as f:f.write(json.dumps(rec)+'\n')
 assert code in allowed,rec
 return json.loads(raw) if raw else None
def pg(sql):return json.loads(command(K+['exec','ofwp32-pg-0','--','psql','-v','ON_ERROR_STOP=1','-U','ofwp32','-d','ofwp32','-Atc',sql],text=True))
def inject(raw,name):
 p=subprocess.run(K+['exec','-i','ofwp32-kafka-0','--','/opt/kafka/bin/kafka-console-producer.sh','--bootstrap-server','localhost:9092','--topic','pipeline.builds.v1'],input=raw,capture_output=True)
 save(name+'.json',{'rc':p.returncode,'stdout':p.stdout.decode(),'stderr':p.stderr.decode()});assert p.returncode==0

def drain():
 for _ in range(30):
  raw=command(K+['exec','ofwp32-kafka-0','--','/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','localhost:9092','--group','ofwp32-runtime-v1','--describe'],text=True)
  save('consumer-offsets-latest.txt',raw)
  rows=[s.split() for s in raw.splitlines() if s.startswith('ofwp32-runtime-v1 ')]
  if rows and all(s[5]=='0' for s in rows):return
  time.sleep(1)
 raise TimeoutError('consumer did not reach zero lag')
def counts():return pg("SELECT json_build_object('dispatches',(SELECT count(*) FROM scheduling.dispatches WHERE schedule_key='runtime-v1-20261117'),'builds',(SELECT count(*) FROM builds WHERE id IN (SELECT build_id FROM scheduling.dispatches WHERE schedule_key='runtime-v1-20261117')),'receipts',(SELECT count(*) FROM scheduling.publish_receipts WHERE dispatch_event_id IN (SELECT event_id FROM scheduling.dispatches WHERE schedule_key='runtime-v1-20261117')));")
def duplicate():
 token=(ROOT/'org-token.txt').read_text().strip();seed=json.loads((ROOT/'canonical-seed.json').read_text());raw=(ROOT/'duplicate-event.ndjson').read_bytes()
 with forward('ofwp32-dvs',50078) as dvs:
  path='/v1/datasets/'+seed['output']['id']+'/versions';before={'pg':counts(),'versions':http(dvs,path,token,evidence='duplicate-complete')};save('duplicate-complete-pre.json',before)
  with concurrent.futures.ThreadPoolExecutor(2) as pool:list(pool.map(lambda i:inject(raw,f'duplicate-complete-producer-{i}'),range(2)))
  drain();after={'pg':counts(),'versions':http(dvs,path,token,evidence='duplicate-complete')};save('duplicate-complete-post.json',after);assert before==after,(before,after)
if __name__=='__main__':
 ROOT.mkdir(exist_ok=True,parents=True)
 assert sys.argv[1]=='duplicate'
 duplicate();print('PASS duplicate: consumed concurrent duplicates, PG and real DVS versions unchanged')
