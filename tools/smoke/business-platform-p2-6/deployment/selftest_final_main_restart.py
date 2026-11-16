#!/usr/bin/env python3
"""Pure exact main3 receipt-chain fixtures; zero restart/HTTP/SQL calls."""
import importlib.util,json,copy,sys
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('mainrestart_pure',HERE/'final_main_restart.py');M=importlib.util.module_from_spec(s);s.loader.exec_module(M)
b=importlib.util.spec_from_file_location('bootvalidator_pure',M.BOOT);B=importlib.util.module_from_spec(b);b.loader.exec_module(B)


def run(out):
    out.mkdir(parents=True,exist_ok=False);cases=[]
    for name,change,ok in [('sameCID-threeboots',None,True),('differentCID',lambda p:p['current'].update(cid='f'*64),False),('sameboot',lambda p:p['current'].update(started_at=p['previous']['started_at']),False),('wrongimage',lambda p:p['current'].update(image='sha256:'+'f'*64),False)]:
        root=out/name;root.mkdir();ledger=root/'ledger';ledger.mkdir();receipts=root/'receipts';receipts.mkdir()
        pins={getattr(M.W,'Q3_'+v.upper()):{'cid':str(i)*64,'image':'sha256:'+str(i)*64,'started_at':'2026-11-15T01:00:00Z'} for i,v in enumerate(M.ORDER,1)}
        record={'containers':pins};r={'release_id':'pure-restart','deployment_record_sha256':'base-record-pure','main_restart_output':str(root),'main_boot_transition':{'executor_tool':str(HERE/'final_main_restart.py'),'executor_tool_sha256':M.G.sha256_file(HERE/'final_main_restart.py'),'guard_extension_sha256':M.G.sha256_file(M.BOOT),'ledger_dir':str(ledger),'allowed_service_order':M.ORDER,'restart_commands':{v:['docker','restart','--time','120',pins[getattr(M.W,'Q3_'+v.upper())]['cid']] for v in M.ORDER}}}
        M.exclusive_json(ledger/'00-anchor.json',{'schema':'p26-main-boot-transitions-v1','release_id':r['release_id'],'executor_tool_sha256':r['main_boot_transition']['executor_tool_sha256'],'base_deployment_record_sha256':r['deployment_record_sha256'],'allowed_service_order':M.ORDER})
        def receipt(prefix,argv,raw):
            Path(str(prefix)+'.cmd.json').write_text(json.dumps(argv));Path(str(prefix)+'.raw').write_text(raw);Path(str(prefix)+'.rc').write_text('0\n');Path(str(prefix)+'.meta.json').write_text(json.dumps({'original_child_rc':0,'stderr':''}))
        for i,v in enumerate(M.ORDER,1):
            old=pins[getattr(M.W,'Q3_'+v.upper())];current=dict(old,started_at='2026-11-15T02:00:00Z');step={'schema':'p26-main-boot-transition-v1','release_id':r['release_id'],'service':v,'previous':old,'current':current,'before':{},'after':{}}
            for side,pin in [('before',old),('after',current)]:
                for key,fmt in [('identity',B.IDENTITY),('mounts',B.MOUNTS),('ports',B.PORTS),('env_names',B.ENVNAMES)]:
                    prefix=receipts/(v+'-'+side+'-'+key);step[side][key]=str(prefix)
                    raw='\n'.join([pin['cid'],pin['image'],pin['started_at'],'true'])+'\n' if key=='identity' else {'mounts':'dataset-files:/home/nonroot:volume \n','ports':'{}\n{}\n','env_names':'PATH\nJWT_SECRET\n'}[key]
                    receipt(prefix,['docker','inspect',old['cid'],'--format',fmt],raw)
            prefix=receipts/(v+'-restart');receipt(prefix,r['main_boot_transition']['restart_commands'][v],old['cid']+'\n');step['restart_step']=str(prefix)
            if i==1 and change:change(step)
            M.exclusive_json(ledger/(str(i).zfill(2)+'-'+v+'.json'),step)
        accepted=False
        try:
            derived,proof=B.derive(r,record,M.W);assert len(proof['steps'])==3;accepted=True
        except (AssertionError,ValueError):pass
        assert accepted==ok;cases.append({'case':name,'pass':True,'accepted':accepted})
    previous={'cid':'1'*64,'image':'sha256:'+'1'*64,'started_at':'2026-11-15T01:00:00Z'};current=dict(previous,started_at='2026-11-15T02:00:00Z')
    for key in ['mounts','ports','env_names']:
        before={'mounts':'x','ports':'x','env_names':'PATH'};after=dict(before);after[key]+='changed'
        try:M.validate_change(previous,current,before,after,'connector')
        except AssertionError:pass
        else:raise AssertionError('invariant drift accepted')
        cases.append({'case':'reject-'+key+'-drift','pass':True})
    with patch.object(M.G,'load_release',return_value=None),patch.object(M.subprocess,'run',side_effect=AssertionError('actualprocessforbidden')) as process:
        try:M.run('pure-no-release')
        except AssertionError:pass
        else:raise AssertionError('unreleased allowed')
        assert process.call_count==0
    cases.append({'case':'unreleased-zero-calls','pass':True})
    (out/'RESULT.json').write_text(json.dumps({'pure_only':True,'passed':len(cases),'actual_resource_calls':0,'cases':cases,'writer_sha256':M.G.sha256_file(HERE/'final_main_restart.py'),'boot_helper_sha256':M.G.sha256_file(M.BOOT),'scope':'actual NEW validator reads synthetic full original receipts; no claim of real restart'},indent=2)+'\n');print(out);print(str(len(cases))+' restart contract cases passed; actual calls=0')


if __name__=='__main__':run(Path(sys.argv[1]).resolve())
