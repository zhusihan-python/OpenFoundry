#!/usr/bin/env python3
"""Actual guard branches over sealed filesystem baseline + injected live I/O."""
import sys,json,tempfile,uuid
from pathlib import Path
from types import SimpleNamespace as NS
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_guard_shim as H
import w10_boot_transition as B
W=H.W; G=H.G

def main():
    historical,catalog,derived=W.load_baseline(G.P25_FINAL_DIR)
    catalog['public_views']=W.public_view_baselines(derived,catalog)
    runs=[e['public_projection_27'] for e in derived['protected9']]
    cp=[e['cp_owner_row_full'] for e in derived['cp8']]
    parents=[]
    for did,row in catalog['owner_parents']['by_def'].items():
        src=catalog['owner_parents']['source_baseline'][row['source_id']]
        parents.append({**{k:row[k] for k in ('def_id','org_id','source_id','output_dataset_id')},**{k:src[k] for k in ('conn_id','conn_org_id','conn_owner_id','source_name','connector_type')}})
    views={v['def_id']:v for v in catalog['public_views']}
    S=G.candidate_hash(G.OPENFOUNDRY)
    with tempfile.TemporaryDirectory(dir=H.P26/'results/w10') as td:
        tmp=Path(td); roster=tmp/'typed.json'; rec=tmp/'rec.json'; release=tmp/'release.json'
        roster.write_text(json.dumps({'schema':'p26-w10-typed-v1','runs':[],'public_views':[]}))
        pins={name:{'cid':'a'*64,'image':'sha256:'+'b'*64,'started_at':W.EXPECTED_FIXTURE_STARTED_AT if name==W.Q3_FIXTURE else '2026-11-15T00:00:00Z'} for name in (W.Q3_FIXTURE,W.Q3_DATASET,W.Q3_PIPELINE,W.Q3_CONNECTOR)}
        rec.write_text(json.dumps({'source_sha256':S,'containers':pins}))
        release.write_text(json.dumps({'schema':'p26-w10-release-v1','root_accepted':True,'suite':'p23','stage':'p23full8','expires_utc':'2099-01-01T00:00:00Z','evidence_root':td,'candidate_source':S,'deployment_record':str(rec),'deployment_record_sha256':H.sha(rec),'typed_roster':str(roster),'typed_roster_sha256':H.sha(roster)}))
        mode={'value':'good'}; protocol_value={'value':'p26-v1'}; legacy_rows=[]; calls=[]
        def runner(cmd):
            calls.append(cmd)
            if cmd[:2]==['docker','inspect']:
                p=pins[cmd[2]]
                out='\n'.join([p['cid'],p['image'],p['started_at'],str(p.get('expected_running',True)).lower()])
                if mode['value']=='cid': out=out.replace('a'*64,'c'*64)
            elif 'recovery_protocol FROM sync_runs' in cmd[-1]:
                out=json.dumps([{'id':runs[-1]['id'],'recovery_protocol':'WRONG' if mode['value']=='protocol' else protocol_value['value']}]+[{'id':r['id'],'recovery_protocol':None} for r in legacy_rows])
            elif 'AS fixture_src' in cmd[-1] and 'r.id IN (' in cmd[-1]:
                out=json.dumps([runs[-1]]+legacy_rows)
            elif "r.control_mode = 'legacy'" in cmd[-1]:
                out=json.dumps(legacy_rows+([{**legacy_rows[0],'id':str(uuid.uuid4())}] if mode['value']=='legacyshadow' else []))
            elif cmd==W.C.OLDPG_READONLY_ARGV['controlled_rows_27col']:
                rows=runs+([{**runs[0],'id':'11111111-1111-4111-8111-111111111111'}] if mode['value']=='shadow' else [])
                out=json.dumps(rows)
            elif cmd==W.C.OLDPG_READONLY_ARGV['cp_owners']: out=json.dumps(cp)
            else: out=json.dumps(parents)
            return NS(returncode=9 if mode['value']=='rc' else 0,stdout=out,stderr='fake')
        def http(url,**kwargs):
            did=url.split('/')[-2]; v=views[did]
            body=v['baseline_cp'] if url.endswith('/checkpoint') else v['baseline_runs']
            return W.HttpResponse(v.get('checkpoint_status',200) if url.endswith('/checkpoint') else 200,{},json.dumps(body).encode(),url,True)
        secret_before=W.read_jwt_secret
        W.read_jwt_secret=lambda:'pure-fake-secret'
        try:
            assert H.check(release,runner,http)['ok']
            # Optional Root-pinned main restart receipt chain changes ONLY
            # Dataset boot. Actual current inspect must still match new boot.
            ledger=tmp/'main-ledger';ledger.mkdir();proofout=tmp/'restart-receipts';proofout.mkdir()
            doc0=json.loads(release.read_text());doc0['release_id']='pure-guard-mainrestart';doc0['main_restart_output']=td
            doc0['main_boot_transition']={'executor_tool':str(Path(__file__).resolve()),'executor_tool_sha256':H.sha(__file__),'guard_extension_sha256':H.sha(B.__file__),'ledger_dir':str(ledger),'allowed_service_order':['dataset','pipeline','connector'],'restart_commands':{svc:['docker','restart',pins[getattr(W,'Q3_'+svc.upper())]['cid']] for svc in ('dataset','pipeline','connector')}}
            (ledger/'00-anchor.json').write_text(json.dumps({'schema':'p26-main-boot-transitions-v1','release_id':doc0['release_id'],'executor_tool_sha256':H.sha(__file__),'base_deployment_record_sha256':H.sha(rec),'allowed_service_order':['dataset','pipeline','connector']}))
            beforepin=dict(pins[W.Q3_DATASET]);afterpin=dict(beforepin,started_at='2026-11-15T00:00:01Z');step={'schema':'p26-main-boot-transition-v1','release_id':doc0['release_id'],'service':'dataset','previous':beforepin,'current':afterpin}
            def receipt(tag,cmd,raw):
                p=proofout/tag
                for suffix,data in [('.cmd.json',json.dumps(cmd)),('.raw',raw),('.rc','0\n'),('.meta.json','{"original_rc":0}')]:Path(str(p)+suffix).write_text(data)
                return str(p)
            for side,pin in [('before',beforepin),('after',afterpin)]:
                values={'identity':'\n'.join([pin['cid'],pin['image'],pin['started_at'],'true']),'mounts':'dataset-files:/home/nonroot:volume','ports':'{}\n{}','env_names':'SAFE_NAME'}
                step[side]={k:receipt(side+'-'+k,['docker','inspect',pin['cid'],'--format',fmt],values[k]) for k,fmt in [('identity',B.IDENTITY),('mounts',B.MOUNTS),('ports',B.PORTS),('env_names',B.ENVNAMES)]}
            step['restart_step']=receipt('restart',doc0['main_boot_transition']['restart_commands']['dataset'],afterpin['cid']);(ledger/'01-dataset.json').write_text(json.dumps(step));release.write_text(json.dumps(doc0));pins[W.Q3_DATASET]=afterpin
            verified=H.check(release,runner,http);assert verified['ok'] and len(verified['verified_main_boot_transition']['steps'])==1
            pins[W.Q3_DATASET]=beforepin;doc0.pop('main_boot_transition');doc0.pop('main_restart_output');release.write_text(json.dumps(doc0))
            pins['p26connectora']={'cid':'d'*64,'image':'sha256:'+'e'*64,'started_at':'2026-11-15T00:00:03Z','expected_running':False,'ownership':{'of-p26-owner':'p26-http','of-p26-stage':'p2-6'}}
            rec.write_text(json.dumps({'source_sha256':S,'containers':pins}));doc0['deployment_record_sha256']=H.sha(rec);release.write_text(json.dumps(doc0));assert H.check(release,runner,http)['ok']
            pins[W.Q3_CONNECTOR]['expected_running']=False;rec.write_text(json.dumps({'source_sha256':S,'containers':pins}));doc0['deployment_record_sha256']=H.sha(rec);release.write_text(json.dumps(doc0))
            try:H.check(release,runner,http)
            except ValueError:pass
            else:raise AssertionError('Root main stopped expectedstate accepted')
            pins[W.Q3_CONNECTOR].pop('expected_running');rec.write_text(json.dumps({'source_sha256':S,'containers':pins}));doc0['deployment_record_sha256']=H.sha(rec);release.write_text(json.dumps(doc0))
            # Real typed extension: control_mode stays controlled, recovery
            # protocol is independently SELECTed as p26-v1, never conflated.
            newrow=dict(next(r for r in runs if r['id']==derived['h10_run_id']))
            newrow.update(id=str(uuid.uuid4()),sync_def_id=str(uuid.uuid4()),source_id=str(uuid.uuid4()),output_dataset_id=str(uuid.uuid4()),source_name='p26-pure-held')
            newparent=dict(parents[0]); newparent.update(def_id=newrow['sync_def_id'],source_id=newrow['source_id'],conn_id=newrow['source_id'],output_dataset_id=newrow['output_dataset_id'],org_id=newrow['org_id'],conn_org_id=newrow['org_id'],conn_owner_id=newrow['requested_by'],source_name=newrow['source_name'])
            origin=tmp/'origin-row.json'; origin.write_text(json.dumps(newrow))
            view={'def_id':newrow['sync_def_id'],'actor':newrow['requested_by'],'tenant':newrow['org_id'],'checkpoint_status':404,'baseline_cp':{},'baseline_runs':[{'id':newrow['id'],'sync_def_id':newrow['sync_def_id'],'status':newrow['status']}]}
            typed={'schema':'p26-w10-typed-v1','runs':[{'run_id':newrow['id'],'def_id':newrow['sync_def_id'],'source_id':newrow['source_id'],'tenant':newrow['org_id'],'actor':newrow['requested_by'],'control_mode':'controlled','protocol':'p26-v1','purpose':'pure fake new held','expected_state':newrow['status'],'pubentered':newrow['gate_entered'],'safe_projection_27':newrow,'parent':newparent,'evidence':{'path':str(origin),'sha256':H.sha(origin),'json_pointer':''}}],'public_views':[view]}
            roster.write_text(json.dumps(typed)); doc=json.loads(release.read_text()); doc['typed_roster_sha256']=H.sha(roster); release.write_text(json.dumps(doc))
            runs.append(newrow); parents.append(newparent); views[view['def_id']]=view
            assert H.check(release,runner,http)['ok']
            typed['runs'][0]['protocol']='legacy-null'; protocol_value['value']=None
            roster.write_text(json.dumps(typed)); doc['typed_roster_sha256']=H.sha(roster); release.write_text(json.dumps(doc))
            assert H.check(release,runner,http)['ok']
            mode['value']='protocol'
            try: H.check(release,runner,http)
            except ValueError: pass
            else: raise AssertionError('wrong recovery protocol accepted')
            mode['value']='good'
            legacy=dict(newrow,id=str(uuid.uuid4()),sync_def_id=str(uuid.uuid4()),source_id=str(uuid.uuid4()),output_dataset_id=str(uuid.uuid4()),source_name='p26-new-N-legacy',control_mode='legacy',gate_entered='false',has_holder='false',has_token='false',heartbeat_at='',lease_expires_at='',generation='',attempt='')
            legacy_parent=dict(newparent,def_id=legacy['sync_def_id'],source_id=legacy['source_id'],conn_id=legacy['source_id'],output_dataset_id=legacy['output_dataset_id'],source_name=legacy['source_name'])
            original_legacy=tmp/'legacy-row.json'; original_legacy.write_text(json.dumps(legacy))
            legacy_view=dict(view,def_id=legacy['sync_def_id'],baseline_runs=[{'id':legacy['id'],'sync_def_id':legacy['sync_def_id'],'status':'running'}])
            legacy_entry=dict(typed['runs'][0],run_id=legacy['id'],def_id=legacy['sync_def_id'],source_id=legacy['source_id'],control_mode='legacy',pubentered=legacy['gate_entered'],safe_projection_27=legacy,parent=legacy_parent,evidence={'path':str(original_legacy),'sha256':H.sha(original_legacy),'json_pointer':''})
            typed['runs'].append(legacy_entry); typed['public_views'].append(legacy_view); typed['legacy_whole_baseline_27']=[]
            legacy_rows.append(legacy); parents.append(legacy_parent); views[legacy_view['def_id']]=legacy_view
            roster.write_text(json.dumps(typed)); doc['typed_roster_sha256']=H.sha(roster); release.write_text(json.dumps(doc))
            assert H.check(release,runner,http)['ok']
            for m in ('cid','shadow','rc','legacyshadow'):
                mode['value']=m
                try: H.check(release,runner,http)
                except ValueError: pass
                else: raise AssertionError(m+' falsely accepted')
        finally: W.read_jwt_secret=secret_before
        print(json.dumps({'ok':True,'actual_http_sql_docker_calls':0,'sealed_files_read_only':True,'actual_source_sha256':S,'checks':['full actual guard with fake I/O','Root-pinned immutable Dataset sameCID restart receipts derive only boot; full actual whole guard accepts that actual new boot','Root-pinned NEW owned aux expected_running false accepted; stopped main forbidden','wrong CID rejected','whole SQL shadow rejected','real injected rc9 rejected','typed p26-v1 with control_mode controlled positive','independent wrong recovery protocol rejected','explicit legacy-null matches actual SQL NULL','actual legacy control_mode separate from protocol; whole legacy shadow rejected'],'injected_runner_calls':len(calls)},indent=2))
if __name__=='__main__': main()
