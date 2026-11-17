#!/usr/bin/env python3
"""Fresh real HTTP canonical fixture; TOKEN/DVS/PBS environment required."""
import json, os, sys, urllib.request, urllib.error, uuid
DVS, PBS, TOKEN = (os.environ[k].rstrip('/') for k in ('DVS','PBS','TOKEN'))
def call(base,path,body=None,raw=None,ctype='application/json',allowed=(200,201)):
    data=raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req=urllib.request.Request(base+path,data=data,headers={'Authorization':'Bearer '+TOKEN,'Content-Type':ctype},method='POST' if data is not None else 'GET')
    try:
        with urllib.request.urlopen(req) as r: status,buf=r.status,r.read()
    except urllib.error.HTTPError as e: status,buf=e.code,e.read()
    print(json.dumps({"path":path,"status":status,"body":buf.decode("utf-8",errors="replace")}),file=sys.stderr,flush=True)
    assert status in allowed,(path,status,buf)
    return json.loads(buf) if buf else {}
suffix=uuid.uuid4().hex[:8]
inp=call(DVS,'/v1/datasets',{'name':'sched-in-'+suffix,'format':'csv'})
out=call(DVS,'/v1/datasets',{'name':'sched-out-'+suffix,'format':'json'})
for ds in (inp,out):
    call(DVS,f"/v1/datasets/{ds['id']}/branches",{'name':'main'},allowed=(200,201,409))
csv=b'\xef\xbb\xbfsku,qty\nA,2\nB,5\n'
boundary='Fixture'+suffix
raw=(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="inventory.csv"\r\nContent-Type: text/csv\r\n\r\n'.encode()+csv+f'\r\n--{boundary}--\r\n'.encode())
up=call(DVS,f"/v1/datasets/{inp['id']}/upload",raw=raw,ctype='multipart/form-data; boundary='+boundary)
physical=up.get('physical_uri','')
assert physical,up
storage=physical[len('local:///'):] if physical.startswith('local:///') else physical
commit=call(DVS,f"/v1/datasets/{inp['id']}/outputs:commit",{
 'branch':'main','transaction_type':'SNAPSHOT','summary':'canonical fixture',
 'schema':{'fields':[{'name':'sku','type':'string'},{'name':'qty','type':'integer'}],'file_format':'TEXT'},
 'files':[{'logical_path':up.get('path') or 'inventory.csv','size_bytes':up.get('size_bytes') or len(csv),'storage_path':storage}]})
assert commit.get('branch')=='main',commit
call(DVS,f"/v1/datasets/{inp['id']}/schema?branch=main")
call(DVS,f"/v1/datasets/{inp['id']}/files/rows?branch=main&mode=preview&limit=10")
nodes=[{'id':'input','label':'Dataset','transform_type':'dataset_input','input_dataset_ids':[inp['id']],'config':{'source_kind':'dataset','dataset_id':inp['id']}},
 {'id':'filter','label':'Quantity over 3','transform_type':'filter','depends_on':['input'],'config':{'predicate':'qty > 3'}},
 {'id':'output','label':'Filtered inventory','transform_type':'output_dataset','depends_on':['filter'],'output_dataset_id':out['id'],'config':{}}]
pipeline=call(PBS,'/api/v1/pipelines',{'name':'sched-pipe-'+suffix,'pipeline_type':'FASTER','nodes':nodes})
spec=call(PBS,'/v1/job-specs/TRANSFORM',{'pipeline_rid':pipeline['id'],'branch_name':'main','inputs':[{'dataset_rid':inp['id']}],'output_dataset_rids':[out['id']],'logic_payload':{'transform_type':'dataset_input','dataset_rid':inp['id']}},allowed=(201,))
assert spec.get('rid'),spec
print(json.dumps({'input':inp,'output':out,'pipeline':pipeline,'spec':spec,'branch':'main'}))
