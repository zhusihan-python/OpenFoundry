#!/usr/bin/env python3
"""Use copied Engine create/admit methods and real ROUTES, with fake HTTP."""
import base64,json,sys
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_p25_runtime as R

def main():
 with patch('subprocess.run',side_effect=AssertionError('no child')),patch('socket.socket',side_effect=AssertionError('no network')):
  ctx=object.__new__(R.Context);ctx.created={};m=R.load('smoke_p25.py')
  ids={k:str(i)*8+'-'+str(i)*4+'-4'+str(i)*3+'-8'+str(i)*3+'-'+str(i)*12 for i,k in enumerate(('source','dataset','def','run','actor','org'),1)}
  token='a.'+base64.urlsafe_b64encode(json.dumps({'sub':ids['actor'],'org_id':ids['org']}).encode()).decode().rstrip('=')+'.PURE'
  class Rec:
   def persist_handle(self,*a,**k):pass
   def persist_failed_handle(self,*a,**k):pass
  calls=[]
  # Response fields mirror Go models Connection/SyncJob/SyncRun; SyncRun is
  # flat and has sync_def_id, requested_by/owner_id/org_id (no source_id).
  class API:
   def call(self,base,method,path,body=None,**kw):
    if path==m.ROUTES['source_create']:obj={'id':ids['source'],'name':body['name'],'owner_id':ids['actor'],'org_id':ids['org'],'connector_type':'rest_api'};status=201
    elif path==m.DATASET_ROUTES['dataset_create']:obj={'id':ids['dataset'],'name':body['name']};status=201
    elif path==m.ROUTES['sync_create']:obj={'id':ids['def'],'source_id':body['source_id'],'output_dataset_id':body['output_dataset_id']};status=201
    elif path==m.route('sync_run',id=ids['def'])+'?async=true':obj={'id':ids['run'],'sync_def_id':ids['def'],'status':'pending','requested_by':ids['actor'],'owner_id':ids['actor'],'org_id':ids['org'],'control_mode':'controlled'};status=202
    else:raise AssertionError((base,method,path))
    result={'status':status,'json':obj,'error':None};calls.append((base,method,path));ctx.observe(base,method,path,body,result,'actual-fake-http-step-'+str(len(calls)),token);return result
  e=m.P25Engine(None,{},Rec());api=API();source=e.create_source(api,'new-source',{});dataset=e.create_dataset(api,'new-dataset');definition,_=e.make_sync_def(api,'new-def',source['id'],dataset['id']);run,_=e.trigger_run(api,definition['id'],async_mode=True,wait=False)
  assert {ctx.created[ids[k]]['kind'] for k in ('source','dataset','def','run')}=={'source','dataset','def','run'}
  for kind in ('source','dataset','def','run'):
   row=ctx.created[ids[kind]];assert row['actor']==ids['actor'] and row['tenant']==ids['org'] and row['original_http_step'].startswith('actual-fake-http-step-')
  assert ctx.created[ids['source']]['name']=='new-source';assert ctx.created[ids['def']]['source_id']==source['id'] and ctx.created[ids['def']]['output_dataset_id']==dataset['id'];assert ctx.created[ids['run']]['sync_def_id']==definition['id']
  before=dict(ctx.created);ctx.observe('connector','POST',m.ROUTES['source_create'],{'name':'denied'},{'status':403,'json':{'id':ids['source']}},'denied',token);ctx.observe('connector','GET',m.route('run_get',id=ids['def'],run_id=ids['run']),None,{'status':200,'json':run},'read',token);assert ctx.created==before
 print(json.dumps({'ok':True,'checks':['real copied engine create_source/create_dataset/make_sync_def/trigger_run methods','actual ROUTES and flat response field identity chain','async query route registration','denied create and canonical GET do not overwrite create origin'],'calls_fake':calls,'actual_calls':0},indent=2))
if __name__=='__main__':main()
