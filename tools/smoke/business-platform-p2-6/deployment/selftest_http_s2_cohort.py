#!/usr/bin/env python3
"""Pure fixed cohort2/name/image/ports/config guards, no actual resources."""
import copy,importlib.util,json,sys
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('aux2_pure',HERE/'create_http_s2_cohort.py');M=importlib.util.module_from_spec(s);s.loader.exec_module(M)


def run(out):
    out.mkdir(parents=True,exist_ok=False);results=[];image='sha256:'+'2'*64
    for inst in ['a','b']:
        argv=M.argv_for(inst,image);assert argv[-1]==image and argv[argv.index('--name')+1]==M.NAMES[inst]
        assert '127.0.0.1:'+str(M.PORTS[inst])+':50088' in argv and '--pull=never' in argv
        assert argv[argv.index('--network')+1]==M.NET and argv[argv.index('--env-file')+1]==str(M.AUX_ENV)
        assert 'DATASET_SERVICE_URL=http://p26proxy:8091' in argv and 'of-p26-role=connector_'+inst in argv
        assert 'of-p26-role=connector_'+inst+'2' not in argv
        assert not any(x in argv for x in ['p26connectora','p26connectorb','p26connectorc','stop','rm','restart','--build'])
        pin={'cid':inst*64 if inst=='a' else 'b'*64,'name':'/'+M.NAMES[inst],'image':image,'running':True,'labels':{'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_'+inst},'networks':{M.NET:{}},'ports':{'50088/tcp':[{'HostIp':'127.0.0.1','HostPort':str(M.PORTS[inst])}]}}
        M.validate(inst,pin,image);results.append({'case':inst+'-exactargv-and-ownedidentity','pass':True})
        for case,change in [('oldname',lambda f:f.update(name='/p26connector'+inst)),('wrongimage',lambda f:f.update(image='sha256:'+'3'*64)),('wrongrole-with2',lambda f:f['labels'].update({'of-p26-role':'connector_'+inst+'2'})),('wrongport',lambda f:f['ports']['50088/tcp'][0].update(HostPort='17097')),('publicbind',lambda f:f['ports']['50088/tcp'][0].update(HostIp='0.0.0.0'))]:
            f=copy.deepcopy(pin);change(f)
            try:M.validate(inst,f,image)
            except AssertionError:pass
            else:raise AssertionError(case+' accepted')
            results.append({'case':inst+'-'+case+'-reject','pass':True})
    with patch.object(M.G,'load_release',return_value=None),patch.object(M.subprocess,'run',side_effect=AssertionError('actualresourceforbidden')) as process:
        try:M.run('pure-no-release')
        except AssertionError:pass
        else:raise AssertionError('unreleased startup allowed')
        assert process.call_count==0
    results.append({'case':'noRootphase-zero-calls','pass':True})
    (out/'RESULT.json').write_text(json.dumps({'pure_only':True,'passed':len(results),'cases':results,'actual_Docker_HTTP_SQL_calls':0,'helper_sha256':M.G.sha256_file(HERE/'create_http_s2_cohort.py')},indent=2)+'\n');print(out);print(str(len(results))+' cohort2 pure cases passed; actual calls=0')


if __name__=='__main__':run(Path(sys.argv[1]).resolve())
