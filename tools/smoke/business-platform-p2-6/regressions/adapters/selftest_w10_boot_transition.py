#!/usr/bin/env python3
import copy,hashlib,json,sys,tempfile
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_boot_transition as B
import w10_guard_shim as H

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
    cases=[]
    with tempfile.TemporaryDirectory(dir=H.P26/'results/w10') as td:
        td=Path(td)
        for fault,count in [(None,0),(None,1),(None,3),('env-order',1),('env-duplicate',1),('env-illegal',1),('env-extra',1),('CID',1),('boot',1),('before',1),('rc',1),('mounts',1),('loops',3),('gap',3),('extra-file',1)]:
            out=td/(str(fault)+'-'+str(count));out.mkdir();ledger=out/'ledger';ledger.mkdir();record={'source_sha256':'b'*64,'containers':{getattr(H.W,'Q3_'+svc.upper()):{'cid':str(i)*64,'image':'sha256:'+'a'*64,'started_at':'2026-11-15T08:00:00Z'} for i,svc in enumerate(['dataset','pipeline','connector'],1)}}
            record['containers'][H.W.Q3_FIXTURE]={'cid':'d'*64,'image':'sha256:'+'f'*64,'started_at':H.W.EXPECTED_FIXTURE_STARTED_AT};dep=out/'deployment.json';dep.write_text(json.dumps(record));r={'release_id':'pure','deployment_record_sha256':sha(dep),'main_restart_output':str(out),'main_boot_transition':{'executor_tool':str(Path(__file__).resolve()),'executor_tool_sha256':sha(__file__),'guard_extension_sha256':sha(B.__file__),'ledger_dir':str(ledger),'allowed_service_order':['dataset','pipeline','connector'],'restart_commands':{svc:['docker','restart',record['containers'][getattr(H.W,'Q3_'+svc.upper())]['cid']] for svc in ['dataset','pipeline','connector']}}}
            (ledger/'00-anchor.json').write_text(json.dumps({'schema':'p26-main-boot-transitions-v1','release_id':'pure','executor_tool_sha256':sha(__file__),'base_deployment_record_sha256':sha(dep),'allowed_service_order':['dataset','pipeline','connector']}))
            def receipt(name,cmd,raw,rc=0):
                p=out/'receipts'/name;p.parent.mkdir(exist_ok=True)
                for suffix,data in [('.cmd.json',json.dumps(cmd)),('.raw',raw),('.rc',str(rc)+'\n'),('.meta.json',json.dumps({'original_rc':rc}))]:Path(str(p)+suffix).write_text(data)
                return str(p)
            for i,svc in enumerate(['dataset','pipeline','connector'][:count],1):
                old=record['containers'][getattr(H.W,'Q3_'+svc.upper())];current={**old,'started_at':'2026-11-15T09:00:00Z'}
                if fault=='CID':current['cid']='e'*64
                if fault=='boot':current['started_at']=old['started_at']
                step={'schema':'p26-main-boot-transition-v1','release_id':'pure','service':svc,'previous':old,'current':current}
                for side,pin in [('before',old),('after',current)]:
                    values={'identity':'\n'.join([pin['cid'],pin['image'],pin['started_at'],'true'])+'\n','mounts':'volume:/home/nonroot:volume ','ports':'{}\n{}\n','env_names':'SAFE_NAME\nOTHER_NAME\n'}
                    if side=='after':
                        if fault=='env-order':values['env_names']='OTHER_NAME\nSAFE_NAME\n\n'
                        if fault=='env-duplicate':values['env_names']='SAFE_NAME\nSAFE_NAME\nOTHER_NAME\n'
                        if fault=='env-illegal':values['env_names']='SAFE_NAME\nOTHER_NAME \n'
                        if fault=='env-extra':values['env_names']+='EXTRA_NAME\n'
                    if fault=='before' and side=='before':values['identity']='wrong'
                    if fault=='mounts' and side=='after':values['mounts']='newvolume:/home/nonroot:volume '
                    if fault=='loops' and svc=='connector':values['env_names']+='\n'+H.W.LOOP_ENV_NAMES[0]+'\n'
                    step[side]={k:receipt(str(i)+'-'+side+'-'+k,['docker','inspect',old['cid'],'--format',fmt],values[k]) for k,fmt in [('identity',B.IDENTITY),('mounts',B.MOUNTS),('ports',B.PORTS),('env_names',B.ENVNAMES)]}
                step['restart_step']=receipt(str(i)+'-restart',r['main_boot_transition']['restart_commands'][svc],old['cid']+'\n',1 if fault=='rc' else 0)
                if fault!='gap' or i!=1:(ledger/(str(i).zfill(2)+'-'+svc+'.json')).write_text(json.dumps(step))
            if fault=='extra-file':(ledger/'04-fixture.json').write_text('{}')
            try:new,proof=B.derive(r,record,H.W)
            except ValueError:assert fault is not None
            else:
                assert fault in (None,'env-order') and len(proof['steps'])==count
                assert record['containers'][H.W.Q3_FIXTURE]==new['containers'][H.W.Q3_FIXTURE]
                for i,svc in enumerate(['dataset','pipeline','connector']):
                    name=getattr(H.W,'Q3_'+svc.upper());assert new['containers'][name]['cid']==record['containers'][name]['cid'] and new['containers'][name]['image']==record['containers'][name]['image'];assert (new['containers'][name]['started_at']!=record['containers'][name]['started_at'])==(i<count)
            cases.append([fault or 'sameCID boot-only prefix',count])
    print(json.dumps({'ok':True,'actual_HTTP_SQL_Docker_calls':0,'cases':cases},indent=2))
if __name__=='__main__':main()
