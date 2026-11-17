#!/usr/bin/env python3
"""Read-only event→Build→receipt→actual DVS transaction/version/file bytes for counted v3 cycles."""
import base64,datetime,hashlib,hmac,json,pathlib,time,urllib.parse,urllib.request
from canonical_acceptance import ROOT,forward,http,pg,save

def main():
 token=(ROOT/'org-token.txt').read_text().strip();snap=json.loads((ROOT/'v3-after-restart-db.json').read_text());cycles=['2026-11-17T09:02:00+00:00','2026-11-17T09:03:00+00:00','2026-11-17T09:04:00+00:00'];summaries=[]
 with forward('ofwp32-dvs',50078) as dvs:
  for n,slot in enumerate(cycles):
   d=next(d for d in snap['dispatches'] if d['scheduled_for']==slot);b=next(b for b in snap['builds'] if b['id']==d['build_id']);r=next(r for r in snap['receipts'] if r['dispatch_event_id']==d['event_id']);ack=r['receipt'];tx=ack['transaction']['id'];ds=ack['dataset_id'];assert d['state']=='succeeded' and r['state']=='committed'
   request=bytes.fromhex(r['request_body'][2:]);assert hashlib.sha256(request).hexdigest()==r['request_sha256'];req=json.loads(request)
   txn=http(dvs,f'/v1/datasets/{ds}/branches/main/transactions/{tx}',token,evidence='counted-v3-cycle-http');assert txn['status']=='COMMITTED' and txn['id']==tx
   versions=http(dvs,f'/v1/datasets/{ds}/versions',token,evidence='counted-v3-cycle-http');version=next(v for v in versions['data'] if v['transaction_id']==tx)
   view=http(dvs,f'/v1/datasets/{ds}/views/at?branch=main&transaction_id={tx}',token,evidence='counted-v3-cycle-http');save(f'counted-v3-cycle-{n}-view.json',view)
   files=[]
   for i,f in enumerate(ack['files']):
    # Runtime transaction files use the documented local backing-FS URI;
    # the legacy files/{id}/download catalog does not contain these IDs.
    key=f['storage_path'].removeprefix('local:///');expires=int(time.time())+300
    stamp=datetime.datetime.fromtimestamp(expires,datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    sig=hmac.new(b'ofwp32-runtime-synthetic-jwt-20261117-only',(key+'\n'+stamp).encode(),hashlib.sha256).hexdigest()
    path='/v1/_internal/local-fs/'+urllib.parse.quote(key,safe='/')+'?'+urllib.parse.urlencode({'expires':expires,'sig':sig})
    with urllib.request.urlopen(dvs+path) as response:code,raw=response.status,response.read()
    assert code==200 and len(raw)==f['size_bytes'] and hashlib.sha256(raw).hexdigest()==f['metadata']['sha256']
    source=next(v for v in req['files'] if v['logical_path']==f['path']);assert raw==base64.b64decode(source['content_base64'])
    rows=[json.loads(line) for line in raw.splitlines()];assert rows==[{'qty':2,'sku':'A'},{'qty':5,'sku':'B'}],rows
    (ROOT/f'counted-v3-cycle-{n}-file-{i}.ndjson').write_bytes(raw)
    files.append({'status':code,'storage_path':f['storage_path'],'sha256':hashlib.sha256(raw).hexdigest(),'row_count':len(rows),'bytes':len(raw),'download_method':'DVS signed local-fs HTTP using synthetic fixture signing key'})
   save(f'counted-v3-cycle-{n}-files.json',files)
   save(f'counted-v3-cycle-{n}.json',{'dispatch':d,'build':b,'receipt':r,'dvs_transaction':txn,'dvs_version':version})
   summaries.append({'scheduled_for':slot,'event_id':d['event_id'],'build_id':b['id'],'transaction_id':tx,'version':version['version'],'receipt_sha':r['request_sha256'],'files':files})
 save('counted-v3-cycles-summary.json',summaries);print('READ ONLY cycle linkage captured')
if __name__=='__main__':main()
