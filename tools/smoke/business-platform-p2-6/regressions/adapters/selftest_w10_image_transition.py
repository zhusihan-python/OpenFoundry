#!/usr/bin/env python3
"""Root-authority/original receipt/current-read image bridge tests; no live IO."""
import copy,hashlib,json,sys,tempfile
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_image_transition as I
import w10_guard_shim as H
W=H.W
ACCEPTED=Path('/Users/allen/.codex/visualizations/2026/11/13/01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs/2026-11-15-p26/S2-build-two-original-accepted.json')

def main():
    accepted=json.loads(ACCEPTED.read_text());source=accepted['source'];root=H.P26
    _,catalog,derived=W.load_baseline(H.G.P25_FINAL_DIR);catalog['public_views']=W.public_view_baselines(derived,catalog)
    oldrows=[e['public_projection_27'] for e in derived['protected9']];cp=[e['cp_owner_row_full'] for e in derived['cp8']]
    oldparents=[{**p,**catalog['owner_parents']['source_baseline'][p['source_id']]} for p in catalog['owner_parents']['by_def'].values()]
    npath=root/'results/w10/Nsetup-P22-P23-ready-attempt1/actual-Nsetup/typed-new-N-candidate.json';typed=json.loads(npath.read_text());nr=typed['runs'][0];views={v['def_id']:v for v in catalog['public_views']+typed['public_views']}
    checks=[]
    with tempfile.TemporaryDirectory(dir=root/'results/w10') as td,patch('socket.socket',side_effect=AssertionError('actual socket forbidden')),patch.object(W,'read_jwt_secret',return_value='pure-secret'):
        td=Path(td)
        def fixture(service,fault=None):
            out=td/(service+'-'+str(fault));out.mkdir();receipts=out/'receipts';receipts.mkdir()
            names=list(H.G.Q3_CONTAINERS)+['p26connectorc','p26connectora','p26connectorb'];pins={n:{'cid':format(i+1,'064x'),'image':'sha256:'+'a'*64,'started_at':W.EXPECTED_FIXTURE_STARTED_AT if n==W.Q3_FIXTURE else '2026-11-15T06:00:00Z'} for i,n in enumerate(names)};name=I.target_name(service,W);old=copy.deepcopy(pins[name]);image=next(c['image'] for c in accepted['candidates'] if c['service']==('dataset' if service=='dataset' else 'connector'));current={**old,'cid':'f'*64,'image':image,'started_at':'2026-11-15T09:00:00Z'}
            dep=out/'deployment.json';dep.write_text(json.dumps({'source_sha256':source,'containers':pins}));output=out/'transition';output.mkdir();proofpath=output/'proof.json'
            override=root/('deployment/override-S2-main-imageonly.yaml' if service in ('dataset','connector') else 'deployment/legacy-stage/override-S2-imageonly.yaml')
            if service in ('dataset','connector'):prefix=['docker','compose','--env-file',str(W.Q3_ENV_FILE),'-f',str(W.COMPOSE_FILE),'-f',str(override),'--project-name',W.G.Q3_PROJECT]
            else:prefix=['docker','compose','-f',str(root/'deployment/legacy-stage/compose.yaml'),'-f',str(override),'--project-name','openfoundry-p26-legacy']
            argv=prefix+['up','-d','--no-deps','--no-build','--pull=never',service];files={prefix[i+1] for i,x in enumerate(prefix[:-1]) if x in ('-f','--env-file')}
            authority={'service':service,'target_name':name,'candidate_image':image,'executor_tool':str(Path(__file__).resolve()),'executor_tool_sha256':H.sha(__file__),'guard_extension_sha256':H.sha(I.__file__),'approved_argv':argv,'config_shas':{f:H.sha(f) for f in files},'candidate_build':{'path':str(ACCEPTED),'sha256':H.sha(ACCEPTED)},'output_dir':str(output),'proof_file':str(proofpath)}
            if service.startswith('p26'):
                actual_compose=json.loads((root/'deployment/legacy-stage/compose.yaml').read_text())
                assert actual_compose['services'][service]['env_file']==['/private/tmp/openfoundry-p26-aux.env']
                authority['aux_env_sha256']=H.sha(actual_compose['services'][service]['env_file'][0])
            r={'schema':'p26-w10-release-v1','root_accepted':True,'release_id':'pure-'+service,'expires_utc':'2035-01-01T00:00:00Z','stage':I.STAGE,'suite':I.SUITE,'candidate_source':source,'deployment_record':str(dep),'deployment_record_sha256':H.sha(dep),'typed_roster':str(npath),'typed_roster_sha256':H.sha(npath),'evidence_root':str(out/'guard'),'image_transition':authority}
            if fault=='scope':r['stage']='unknown-phase'
            if fault=='tool-sha':authority['executor_tool_sha256']='0'*64
            if fault=='guard-sha':authority['guard_extension_sha256']='0'*64
            if fault=='candidate':authority['candidate_image']='sha256:'+'b'*64
            release=out/'Root.json';release.write_text(json.dumps(r))
            labels={'com.docker.compose.project':W.G.Q3_PROJECT if service in ('dataset','connector') else 'openfoundry-p26-legacy','com.docker.compose.service':service}
            if service.startswith('p26'):labels.update({'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_'+service[-1]})
            before={**old,'name':'/'+name,'config_image':old['image'],'running':True,'restart_count':0,'labels':labels,'networks':{W.G.Q3_PROJECT+'_p1net':{'NetworkID':'network','EndpointID':'old','IPAddress':'172.19.0.2'}},'ports':{'tcp':[{'HostIp':'127.0.0.1','HostPort':'17078'}]},'mounts':[{'Name':'same-files','Destination':'/home/nonroot','Type':'volume'}]}
            after=copy.deepcopy(before);after.update(current);after['config_image']=image;after['networks'][W.G.Q3_PROJECT+'_p1net'].update(EndpointID='new',IPAddress='172.19.0.3')
            env=['PATH','SAFE']+(list(I.AUX_CONFIG_KEYS) if service.startswith('p26') else [])
            def receipt(key,cmd,raw,rc=0):
                p=output/key
                for suffix,value in [('.cmd.json',json.dumps(cmd)),('.raw',raw),('.rc',str(rc)+'\n'),('.meta.json',json.dumps({'original_child_rc':rc}))]:Path(str(p)+suffix).write_text(value)
                return str(p)
            if fault=='config-image':after['config_image']='short-tag'
            if fault=='ports':after['ports']={}
            if fault=='mounts':after['mounts']=[]
            if fault=='network':after['networks'][W.G.Q3_PROJECT+'_p1net']['NetworkID']='changed'
            proof={'schema':'p26-image-transition-proof-v1','release_id':r['release_id'],'root_release_sha256':H.sha(release),'deployment_record_sha256':H.sha(dep),'executor_tool_sha256':authority['executor_tool_sha256'],'service':service,'previous':old,'current':current,'receipts':{}}
            for side,pin,fact in [('before',old,before),('after',current,after)]:
                proof['receipts'][side+'_identity']=receipt(side+'-identity',['docker','inspect',pin['cid'],'--format',I.IDENTITY_JSON],json.dumps(fact))
                proof['receipts'][side+'_env_names']=receipt(side+'-names',['docker','inspect',pin['cid'],'--format',I.ENVNAMES],'\n'.join(env))
                if service.startswith('p26'):
                    config=I.config_for(service)
                    if fault=='aux-loop' and side=='after':config['OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS']='1'
                    proof['receipts'][side+'_aux_config']=receipt(side+'-aux-config',['docker','inspect',pin['cid'],'--format',I.AUX_CONFIG_FORMAT],'\n'.join(k+'='+v for k,v in config.items()))
            proof['receipts']['command']=receipt('command',argv,'up-complete',7 if fault=='rc' else 0)
            proof['receipts']['discovery']=receipt('discovery',prefix+['ps','-q',service],current['cid']+'\n')
            proofpath.write_text(json.dumps(proof));return out,r,release,proofpath,pins,before,after,env
        def runner_for(pins,target,after,env,service,mode='post',fault=None):
            calls=[]
            def runner(cmd):
                calls.append(cmd)
                if cmd[:2]==['docker','inspect']:
                    if cmd[-1]==I.IDENTITY_JSON:
                        fact=copy.deepcopy(after)
                        if fault=='actual-current-drift':fact['mounts']=[]
                        raw=json.dumps(fact)
                    elif cmd[-1]==I.ENVNAMES:raw='\n'.join(env)
                    elif cmd[-1]==I.AUX_CONFIG_FORMAT:raw='\n'.join(k+'='+v for k,v in I.config_for(service).items())
                    else:
                        p={k:after[k] for k in ('cid','image','started_at')} if cmd[2]==target and mode=='post' else pins[cmd[2]];raw='\n'.join([p['cid'],p['image'],p['started_at'],'true'])
                elif cmd==W.C.OLDPG_READONLY_ARGV['controlled_rows_27col']:raw=json.dumps(oldrows+([{**oldrows[0],'id':'11111111-1111-4111-8111-111111111111'}] if fault=='whole-shadow' else []))
                elif cmd==W.C.OLDPG_READONLY_ARGV['cp_owners']:raw=json.dumps(cp)
                elif 'recovery_protocol FROM sync_runs' in cmd[-1]:raw=json.dumps([{'id':nr['run_id'],'recovery_protocol':None}])
                elif 'r.id IN (' in cmd[-1]:raw=json.dumps([nr['safe_projection_27']])
                elif "r.control_mode = 'legacy'" in cmd[-1]:raw=json.dumps(typed['legacy_whole_baseline_27']+[nr['safe_projection_27']])
                else:raw=json.dumps(oldparents+[nr['parent']])
                return NS(returncode=0,stdout=raw,stderr='')
            return runner,calls
        def http(url,**kw):
            v=views[url.split('/')[-2]];checkpoint=url.endswith('/checkpoint');return W.HttpResponse(v.get('checkpoint_status',200) if checkpoint else 200,{},json.dumps(v['baseline_cp'] if checkpoint else v['baseline_runs']).encode(),url,True)
        for service in I.SERVICES:
            out,r,release,path,pins,before,after,env=fixture(service)
            # No projection: original authority is checked BEFORE mutation and
            # the actual guard still uses the old initial pins.
            run,calls=runner_for(pins,I.target_name(service,W),after,env,service,'pre');pre=H.check(release,run,http);assert not pre['verified_image_projection']['applied']
            run,calls=runner_for(pins,I.target_name(service,W),after,env,service);post=H.check(release,run,http,deployment_projection=str(path));assert post['ok'] and post['controlled_count']==9 and post['cp_count']==8 and post['verified_image_projection']['applied']
            assert json.loads(Path(r['deployment_record']).read_text())['containers']==pins
            checks.append(service+' old+typedN actual guard pre/post passes; Root record unchanged')
        for fault in ['scope','tool-sha','guard-sha','candidate','config-image','ports','mounts','network','rc','actual-current-drift','whole-shadow','aux-loop']:
            service='p26connectorc' if fault=='aux-loop' else 'dataset';out,r,release,path,pins,before,after,env=fixture(service,fault);run,calls=runner_for(pins,I.target_name(service,W),after,env,service,fault=fault)
            try:H.check(release,run,http,deployment_projection=str(path))
            except (ValueError,KeyError):pass
            else:raise AssertionError('accepted '+fault)
            if fault not in ('actual-current-drift','whole-shadow'):assert not calls,fault
            checks.append(fault+' rejected')
    print(json.dumps({'ok':True,'actual_HTTP_SQL_Docker_calls':0,'checks':checks},indent=2))
if __name__=='__main__':main()
