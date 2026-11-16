#!/usr/bin/env python3
"""Full guard pre→post→freeze with fake Docker/SQL and actual raw receipts."""
import base64,copy,json,subprocess,sys,tempfile,types,uuid
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True;sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_p25_runtime as R
from p26_p25_typed_guard import guard_held

def main(clock_only=False):
 W=R.H.W;sealed=W.load_baseline(R.H.G.P25_FINAL_DIR);_,cat,der=sealed;cat['public_views']=W.public_view_baselines(der,cat);actor,org,rid,did,sid,target=[str(uuid.uuid4()) for _ in range(6)];row=copy.deepcopy(der['protected9'][0]['public_projection_27']);row.update(id=rid,sync_def_id=did,source_id=sid,output_dataset_id=target,owner_id=actor,requested_by=actor,run_owner_id=actor,org_id=org,run_org_id=org,conn_org_id=org,def_org_id=org,source_name='NEW-held',fixture_src='p25h13',gate_entered='false',status='running',finished_at='',cancel_requested='false',has_hash='false',has_version='false');parent={'def_id':did,'org_id':org,'source_id':sid,'output_dataset_id':target,'conn_id':sid,'conn_org_id':org,'conn_owner_id':actor,'source_name':row['source_name'],'connector_type':'rest_api'};cp=copy.deepcopy(der['cp8'][0]['cp_owner_row_full']);cp.update(sync_def_id=did,source_id=sid,output_dataset_id=target,run_id=rid,run_status='running',mode='cursor',blocked_reason='');oldparents=[{**p,**cat['owner_parents']['source_baseline'][p['source_id']]} for p in cat['owner_parents']['by_def'].values()];token='a.'+base64.urlsafe_b64encode(json.dumps({'sub':actor,'org_id':org}).encode()).decode().rstrip('=')+'.PURE';checks=[]
 with tempfile.TemporaryDirectory(dir=R.P26/'results/w10') as td,patch('socket.socket',side_effect=AssertionError('no network')),patch('subprocess.run',side_effect=AssertionError('no child')),patch.dict(__import__('os').environ,OPENFOUNDRY_SMOKE_JWT=token):
  td=Path(td)
  for fault in ((None,) if clock_only else (None,'expiry-before-reason','pre-expired','attempt','heartbeat_at','has_token','cancel_requested','finished_at','CPepoch','CPactive_since','reason-without-expiry','wrong-anchor','missing-anchor','raw-drift')):
   out=td/str(fault);out.mkdir();typed=out/'typed.json';typed.write_text(json.dumps({'schema':'p26-w10-typed-v1','runs':[],'public_views':[]}));pins={n:{'cid':'a'*64,'image':'sha256:'+'b'*64,'started_at':W.EXPECTED_FIXTURE_STARTED_AT if n==W.Q3_FIXTURE else '2026-11-15T08:00:00Z'} for n in [W.Q3_FIXTURE,W.Q3_DATASET,W.Q3_PIPELINE,W.Q3_CONNECTOR,'p26connectorc']};dep=out/'deployment.json';dep.write_text(json.dumps({'source_sha256':R.SOURCE,'containers':pins}));state={'row':copy.deepcopy(row),'cp':copy.deepcopy(cp),'parent':copy.deepcopy(parent),'expired':False};seq=[0];rel={'candidate_source':R.SOURCE,'typed_roster':str(typed),'typed_roster_sha256':R.H.sha(typed),'deployment_record':str(dep),'deployment_record_sha256':R.H.sha(dep),'p25_inputs':{}}
   def held_api(base,method,path,**kw):
    body={'mode':state['cp']['mode'],'confirmed_epoch':state.get('public_epoch',int(state['cp']['confirmed_epoch'])),'owner':{'run_id':rid,'run_status':'running','since':state['cp']['active_since'],'blocked_reason':state.get('public_reason',state['cp']['blocked_reason'])}} if path.endswith('/checkpoint') else [{'id':rid,'sync_def_id':did,'status':'running'}]
    return {'status':200,'json':body}
   ctx=types.SimpleNamespace(release=rel,runner=types.SimpleNamespace(held={'def_id':did,'source_id':sid,'run_id':rid},api=types.SimpleNamespace(call=held_api)),created={sid:{'kind':'source','actor':actor,'tenant':org,'name':row['source_name']},did:{'kind':'def','actor':actor,'tenant':org,'output_dataset_id':target},rid:{'kind':'run','actor':actor,'tenant':org}},target='p26connectorc',preservation_baseline=sealed,output=out,attempt=W.Attempt(out,'fake'),gate=lambda:None)
   ctx.command=lambda tag,argv,timeout:subprocess.CompletedProcess(argv,0,('\n'.join([pins[argv[2]]['cid'],pins[argv[2]]['image'],pins[argv[2]]['started_at'],'true'])+'\n').encode(),b'')
   def sql(tag,argv):
    if tag.endswith('-whole-controlled'):data=[r['public_projection_27'] for r in der['protected9']]+[state['row']]
    elif tag.endswith('-whole-CP'):data=[r['cp_owner_row_full'] for r in der['cp8']]+[state['cp']]
    elif tag.endswith('-parents'):data=oldparents+[state['parent']]
    elif tag.endswith('-actual-protocol-clock'):data=[{'id':rid,'recovery_protocol':None,'expired':state['expired']}]
    else:raise AssertionError(tag)
    raw=json.dumps(data);seq[0]+=1;prefix=out/('sql-'+str(seq[0]));contents={'.cmd.json':json.dumps(argv),'.raw':raw,'.rc':'0\n','.meta.json':'{"original_rc":0}'}
    for suffix,text in contents.items():Path(str(prefix)+suffix).write_text(text)
    ctx.last_command_evidence={'argv':list(argv),'original_rc':0,'files':{s:{'path':str(prefix)+s,'sha256':R.H.sha(str(prefix)+s)} for s in contents}};return raw
   ctx.sql=sql
   def http(url,**kw):
    did2=url.split('/')[-2];v=next(v for v in cat['public_views'] if v['def_id']==did2);body=v['baseline_cp'] if url.endswith('/checkpoint') else v['baseline_runs'];return W.HttpResponse(200,{},json.dumps(body).encode(),url,True)
   with patch.object(R.H.G,'candidate_hash',return_value=R.SOURCE),patch.object(W,'read_jwt_secret',return_value='FAKE'),patch.object(W,'http_call',side_effect=http):
    if fault=='pre-expired':
     state['expired']=True
     try:guard_held(ctx,'immediately-before-kill')
     except ValueError:pass
     else:raise AssertionError('expired lease accepted before kill')
     checks.append('pre-expired lease refused');continue
    pre=guard_held(ctx,'immediately-before-kill');ref=dict(ctx.pre_kill_anchor_ref);assert Path(ref['path']).exists()
    if fault in row:state['row'][fault]='99' if fault=='attempt' else 'changed'
    elif fault=='CPepoch':state['cp']['confirmed_epoch']=str(int(state['cp']['confirmed_epoch'])+1)
    elif fault=='CPactive_since':state['cp']['active_since']='2026-11-15T09:00:00Z'
    elif fault=='reason-without-expiry':state['cp']['blocked_reason']='requires_reconciliation'
    elif fault=='wrong-anchor':
     ap=Path(ref['path']);doc=json.loads(ap.read_text());doc['row']['attempt']='99';ap.write_text(json.dumps(doc));ctx.pre_kill_anchor_ref['sha256']=R.H.sha(ap)
    elif fault=='missing-anchor':del ctx.pre_kill_anchor_ref
    elif fault=='raw-drift':
     ap=json.loads(Path(ref['path']).read_text());rawp=Path(ap['sql_bindings']['CP']['files']['.raw']['path']);rawp.write_text(rawp.read_text()+' ')
    elif fault=='expiry-before-reason':state['expired']=True
    elif clock_only:pass
    else:state['expired']=True;state['cp']['blocked_reason']='requires_reconciliation'
    try:post=guard_held(ctx,'after-post',export=True)
    except (ValueError,KeyError):assert fault is not None,fault
    else:
     assert fault in (None,'expiry-before-reason') and post['pre_kill_anchor']==ref and post['row']==row
     # Separate freeze context must be Root-bound to the original CP + anchor.
     origin=post['export_path'];rel['p25_inputs']={origin:R.H.sha(origin),ref['path']:ref['sha256']};rel['p25_dynamic_hold']={'origin_report':origin,'def_id':did,'source_id':sid,'run_id':rid,'actor':actor,'tenant':org,'output_dataset_id':target,'source_name':row['source_name'],'safe_projection_27':post['typed_entry']['safe_projection_27'],'cp_owner_row_full':post['typed_entry']['cp_owner_row_full'],'pre_kill_anchor':ref};ctx.runner=None
     engine=R.load('smoke_p25.py');engine.HttpApi=lambda *a,**k:types.SimpleNamespace(call=held_api);ctx.engine=lambda:engine;ctx.bases=R.STAGE_BASES
     if clock_only:
      import p26_p25_supplements as S
      for mode in ('resample-success','public-epoch-drift','deadline'):
       clock={'t':0.};samples=[];sql_deltas=[]
       def sample(stage,export):
        samples.append(stage);state['expired']=True;state['cp']['blocked_reason']='' if mode!='resample-success' or len(samples)==1 else 'requires_reconciliation';state['public_reason']='requires_reconciliation'
        if mode=='public-epoch-drift':state['public_epoch']=int(state['cp']['confirmed_epoch'])+1
        else:state.pop('public_epoch',None)
        before_sql=seq[0]
        try:return guard_held(ctx,mode+'-'+stage,export)
        finally:sql_deltas.append(seq[0]-before_sql)
       ctx.guard_held=sample
       with patch.object(S.time,'monotonic',side_effect=lambda:clock['t']),patch.object(S.time,'sleep',side_effect=lambda seconds:clock.__setitem__('t',clock['t']+seconds)):
        try:rc=S.execute(ctx,'freeze-held',['--timeout','2'])
        except ValueError:
         assert mode!='resample-success'
         if mode=='public-epoch-drift':assert len(samples)==1
         else:assert len(samples)==3 and clock['t']==2
        else:assert mode=='resample-success' and rc==0 and len(samples)==2
       assert sql_deltas==[4]*len(samples)
       assert not (out/('held-typed-'+mode+'-freeze-1.json')).exists()
       checks.append(mode+' complete guard flow')
      continue
     frozen=guard_held(ctx,'freeze-1',export=True)
     if fault=='expiry-before-reason':
      assert frozen['frozen_for_next_phase'] is False
      state['cp']['blocked_reason']='requires_reconciliation';frozen=guard_held(ctx,'freeze-2',export=True)
     assert frozen['frozen_for_next_phase'] and frozen['pre_kill_anchor']==ref
     original_origin=Path(origin).read_text();bad_origin=json.loads(original_origin);bad_origin['typed_entry']['cp_owner_row_full']['confirmed_epoch']='99';Path(origin).write_text(json.dumps(bad_origin));rel['p25_inputs'][origin]=R.H.sha(origin);rel['p25_dynamic_hold']['cp_owner_row_full']=bad_origin['typed_entry']['cp_owner_row_full']
     try:guard_held(ctx,'freeze-bad-origin')
     except (ValueError,KeyError):pass
     else:raise AssertionError('self-reported post CP drift accepted')
     Path(origin).write_text(original_origin);rel['p25_inputs'][origin]=R.H.sha(origin);rel['p25_dynamic_hold']['cp_owner_row_full']=post['typed_entry']['cp_owner_row_full']
     rel['p25_dynamic_hold'].pop('pre_kill_anchor')
     try:guard_held(ctx,'freeze-bad')
     except (ValueError,KeyError):pass
     else:raise AssertionError('missing Root anchor accepted')
   checks.append(fault or 'exact full pre→post expiry-only CP reason + Root freeze')
 print(json.dumps({'ok':True,'actual_calls':0,'checks':checks},indent=2))
if __name__=='__main__':main(clock_only='--clock-flow' in sys.argv)
