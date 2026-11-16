#!/usr/bin/env python3
"""Real sealed baseline + fake actual whole SQL/container/public reads."""
import copy,json,sys,tempfile,uuid,subprocess,types
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_p25_runtime as R
from p26_p25_typed_guard import guard_held

def uid():return str(uuid.uuid4())
def main():
    W=R.H.W;sealed=W.load_baseline(R.H.G.P25_FINAL_DIR);_,catalog,derived=sealed;catalog['public_views']=W.public_view_baselines(derived,catalog)
    actor,org,rid,did,sid,target=[uid() for _ in range(6)]
    held=copy.deepcopy(derived['protected9'][0]['public_projection_27']);held.update(id=rid,sync_def_id=did,source_id=sid,output_dataset_id=target,owner_id=actor,requested_by=actor,run_owner_id=actor,org_id=org,run_org_id=org,conn_org_id=org,def_org_id=org,source_name='p26-new-h10',fixture_src='p26-h10',gate_entered='false',status='running')
    parent={'def_id':did,'org_id':org,'source_id':sid,'output_dataset_id':target,'conn_id':sid,'conn_org_id':org,'conn_owner_id':actor,'source_name':held['source_name'],'connector_type':'rest_api'}
    cp=copy.deepcopy(derived['cp8'][0]['cp_owner_row_full']);cp.update(sync_def_id=did,source_id=sid,output_dataset_id=target,run_id=rid,run_status='running',mode='cursor',blocked_reason='requires_reconciliation')
    terminal=copy.deepcopy(held);terminal.update(id=uid(),sync_def_id=uid(),source_id=uid(),output_dataset_id=uid(),source_name='p26-terminal',status='failed',gate_entered='true')
    tp={**parent,'def_id':terminal['sync_def_id'],'source_id':terminal['source_id'],'conn_id':terminal['source_id'],'output_dataset_id':terminal['output_dataset_id'],'source_name':terminal['source_name']}
    oldparents=[{**p,**catalog['owner_parents']['source_baseline'][p['source_id']]} for p in catalog['owner_parents']['by_def'].values()]
    checks=[]
    with tempfile.TemporaryDirectory(dir=R.P26/'results/w10') as td,patch('socket.socket',side_effect=AssertionError('live forbidden')):
        td=Path(td);origin=td/'held-origin.json';origin.write_text(json.dumps({'typed_entry':{'def_id':did,'source_id':sid,'run_id':rid,'actor':actor,'tenant':org,'safe_projection_27':held,'parent':parent}}));terminalfile=td/'terminal-origin.json';terminalfile.write_text(json.dumps(terminal))
        entry={'run_id':terminal['id'],'def_id':terminal['sync_def_id'],'source_id':terminal['source_id'],'actor':actor,'tenant':org,'control_mode':'controlled','protocol':'legacy-null','expected_state':'failed','pubentered':'true','purpose':'pure terminal baseline','safe_projection_27':terminal,'parent':tp,'evidence':{'path':str(terminalfile),'sha256':R.H.sha(terminalfile),'json_pointer':''}}
        def proof(prefix,argv,data):
            contents={'.cmd.json':json.dumps(argv),'.raw':json.dumps(data),'.rc':'0\n','.meta.json':'{"original_rc":0}'}
            for suffix,text in contents.items():Path(str(prefix)+suffix).write_text(text)
            return {'argv':list(argv),'original_rc':0,'files':{suffix:{'path':str(prefix)+suffix,'sha256':R.H.sha(str(prefix)+suffix)} for suffix in contents}}
        pre_cp={**cp,'blocked_reason':''};base_data={'controlled':[held],'CP':[pre_cp],'parents':[parent],'protocol':[{'id':rid,'recovery_protocol':None,'expired':False}]};bindings={key:proof(td/('fake-pre-'+key),['fake-readonly-'+key],data) for key,data in base_data.items()};preanchor=td/'pre-kill.json';preanchor.write_text(json.dumps({'schema':'p26-h10-pre-kill-anchor-v1','source_S':R.SOURCE,'row':held,'cp_owner_row_full':pre_cp,'parent':parent,'protocol_clock':base_data['protocol'][0],'sql_bindings':bindings}));ref={'path':str(preanchor),'sha256':R.H.sha(preanchor)};post_data={'controlled':[held],'CP':[cp],'parents':[parent],'protocol':[{'id':rid,'recovery_protocol':None,'expired':True}]};suffixes={'controlled':'whole-controlled','CP':'whole-CP','parents':'parents','protocol':'actual-protocol-clock'};postbindings={'held-after-post-'+suffixes[key]:proof(td/('fake-post-'+key),['fake-readonly-'+key],data) for key,data in post_data.items()};origin.write_text(json.dumps({'typed_entry':{'def_id':did,'source_id':sid,'run_id':rid,'actor':actor,'tenant':org,'safe_projection_27':held,'cp_owner_row_full':cp,'parent':parent},'pre_kill_anchor':ref,'lease_expired_by_DB_clock':True,'actual_observation_sql_receipts':postbindings}))
        typed=td/'typed.json';typed.write_text(json.dumps({'schema':'p26-w10-typed-v1','runs':[entry],'public_views':[{'def_id':tp['def_id'],'actor':actor,'tenant':org,'checkpoint_status':404,'baseline_cp':{},'baseline_runs':[]}]}))
        for fault in [None,'controlled-shadow','CP-shadow','protocol','old-parent','boot','missing-pins','typed-terminal-drift','public-shadow']:
            out=td/str(fault);out.mkdir();pins={name:{'cid':'a'*64,'image':'sha256:'+'b'*64,'started_at':W.EXPECTED_FIXTURE_STARTED_AT if name==W.Q3_FIXTURE else '2026-11-15T08:00:00Z'} for name in [W.Q3_FIXTURE,W.Q3_DATASET,W.Q3_PIPELINE,W.Q3_CONNECTOR,'p26connectorc']}
            if fault=='missing-pins':pins.pop(W.Q3_DATASET)
            rec=out/'deployment.json';rec.write_text(json.dumps({'source_sha256':R.SOURCE,'containers':pins}));ctx=types.SimpleNamespace(release={'candidate_source':R.SOURCE,'typed_roster':str(typed),'typed_roster_sha256':R.H.sha(typed),'deployment_record':str(rec),'deployment_record_sha256':R.H.sha(rec),'p25_inputs':{str(origin):R.H.sha(origin),str(preanchor):R.H.sha(preanchor)},'p25_dynamic_hold':{'origin_report':str(origin),'def_id':did,'source_id':sid,'run_id':rid,'actor':actor,'tenant':org,'output_dataset_id':target,'source_name':held['source_name'],'safe_projection_27':held,'cp_owner_row_full':cp,'pre_kill_anchor':ref}},runner=None,target='p26connectorc',preservation_baseline=sealed,output=out,attempt=W.Attempt(out,'pure'),gate=lambda:None)
            def command(tag,argv,timeout):
                p=pins[argv[2]];lines=[p['cid'],p['image'],p['started_at'],'true'];
                if fault=='boot':lines[2]='wrongboot'
                return subprocess.CompletedProcess(argv,0,('\n'.join(lines)+'\n').encode(),b'')
            ctx.command=command
            def sql(tag,argv):
                if tag.endswith('-Root-typed-full27'):
                    r=copy.deepcopy(terminal)
                    if fault=='typed-terminal-drift':r['generation']='99'
                    result=[r]
                elif tag.endswith('-Root-typed-protocol'):result=[{'id':terminal['id'],'recovery_protocol':None}]
                elif tag.endswith('-whole-controlled'):
                    result=[r['public_projection_27'] for r in derived['protected9']]+[held]
                    if fault=='controlled-shadow':result=result+[{**held,'id':uid()}]
                elif tag.endswith('-whole-CP'):
                    result=[r['cp_owner_row_full'] for r in derived['cp8']]+[cp]
                    if fault=='CP-shadow':result=result+[{**cp,'run_id':uid()}]
                elif tag.endswith('-parents'):
                    result=copy.deepcopy(oldparents)+[parent,tp]
                    if fault=='old-parent':result[0]['source_name']='changed'
                elif tag.endswith('-actual-protocol-clock'):result=[{'id':rid,'recovery_protocol':'p26-v1' if fault=='protocol' else None,'expired':True}]
                else:raise AssertionError(tag)
                ctx.last_command_evidence=proof(out/('fake-actual-'+tag),argv,result)
                return json.dumps(result)
            ctx.sql=sql
            def http(url,**kwargs):
                did2=url.split('/')[-2];suffix=url.split('/')[-1];view=next(v for v in catalog['public_views'] if v['def_id']==did2) if did2!=tp['def_id'] else {'baseline_cp':{},'baseline_runs':[]}
                value=view['baseline_cp'] if suffix=='checkpoint' else copy.deepcopy(view['baseline_runs'])
                if fault=='public-shadow' and suffix=='runs':value.append({'id':uid(),'sync_def_id':did2})
                status=404 if did2==tp['def_id'] and suffix=='checkpoint' else 200
                return W.HttpResponse(status,{},json.dumps(value).encode(),url,True)
            with patch.object(R.H.G,'candidate_hash',return_value=R.SOURCE),patch.object(W,'read_jwt_secret',return_value='pure-secret'),patch.object(W,'http_call',side_effect=http):
                try:result=guard_held(ctx,'pure',export=False)
                except ValueError:assert fault is not None,fault
                else:assert fault is None and result['row']==held and result['lease_expired_by_DB_clock'] is True
            checks.append(fault or 'exact old9+CP8+NEWterminal+dynamicheld')
    print(json.dumps({'ok':True,'actual_HTTP_SQL_Docker_calls':0,'checks':checks},indent=2))
if __name__=='__main__':main()
