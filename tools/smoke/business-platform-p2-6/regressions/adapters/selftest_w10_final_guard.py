#!/usr/bin/env python3
"""Pure full final entry using actual sealed artifacts and fake live reads."""
import hashlib,json,sys,tempfile,subprocess
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_final_guard as F
H=F.H;W=F.W
S='b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c'

def main():
    historical,catalog,derived=W.load_baseline(H.G.P25_FINAL_DIR);files=W._manifest(H.G.P25_FINAL_DIR)
    counter={name:W._verified_read(H.G.P25_FINAL_DIR,'postfacts-attempts/attempt-1/03-counters.read1.'+name+'.body',files) for name in catalog['counter_names']}
    download=W._verified_read(H.G.P25_FINAL_DIR,W.SEAL_MEMBERS['h14_download_body'],files);txn=W._verified_read(H.G.P25_FINAL_DIR,W.SEAL_MEMBERS['h14_txn_body'],files)
    prior=H.P26/'results/w8/run-20261115T075850Z-d9bc01';recorded={}
    for p in prior.glob('*.cmd.json'):
        doc=json.loads(p.read_text())
        if isinstance(doc,dict) and 'argv' in doc:
            raw=Path(str(p).removesuffix('.cmd.json')+'.raw')
            if raw.exists():recorded[json.dumps(doc['argv'])]=raw.read_text()
    identity=json.loads((H.P26/'results/w6/deployment/legacy-AB-transition-outer-20261115T083635Z/deployment-record.json').read_text())
    identity['containers']={k:v for k,v in identity['containers'].items() if k in H.G.Q3_CONTAINERS}
    cases=[]
    with tempfile.TemporaryDirectory(dir=H.P26/'results/w10') as td,patch('socket.socket',side_effect=AssertionError('actual network forbidden')):
        td=Path(td)
        for fault in [None,'aux-running','loops','grant-extra','counter','file','wrong-origin']:
            root=td/str(fault);root.mkdir();typed=root/'typed.json';typed.write_text('{"schema":"p26-w10-typed-v1","runs":[],"public_views":[]}');dep=root/'deployment.json';dep.write_text(json.dumps(identity))
            aux={'schema':'p26-final-owned-aux-v1','source_sha256':S,'containers':{name:{'cid':str(i)*64,'image':'sha256:'+'a'*64,'started_at':'2026-11-15T09:00:00Z','running':False,'ownership':{'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_'+name[-1]}} for i,name in enumerate(['p26connectora','p26connectorb','p26connectorc'],1)}}
            ap=root/'aux.json';ap.write_text(json.dumps(aux));out=root/'actual';r={'schema':'p26-w10-release-v1','root_accepted':True,'expires_utc':'2035-01-01T00:00:00Z','release_id':'pure','stage':F.STAGE,'suite':'p26-final','candidate_source':S,'final_output':str(out),'evidence_root':str(out),'final_tools_sha256':{p:H.sha(H.P26/p) for p in F.TOOLS}}
            for k,p in [('typed_roster',typed),('deployment_record',dep),('final_aux_ledger',ap)]:r[k]=str(p);r[k+'_sha256']=H.sha(p)
            release=root/'release.json';release.write_text(json.dumps(r));calls=[]
            def runner(argv):
                calls.append(argv)
                if argv[:2]==['docker','ps'] and '--all' in argv:raw='\n'.join(p['cid']+'|'+name+'|exited' for name,p in aux['containers'].items())
                elif argv[:3]==['docker','image','inspect']:raw=argv[3]+'\n'
                elif argv[:2]==['docker','inspect'] and argv[2] in {p['cid'] for p in aux['containers'].values()}:
                    p=next(v for v in aux['containers'].values() if v['cid']==argv[2]);raw='\n'.join([p['cid'],p['image'],p['started_at'],'true' if fault=='aux-running' else 'false',json.dumps(p['ownership'])])
                elif any('SELECT COALESCE(json_agg' in part for part in argv):raw='[{"sync_def_id":"unexpected"}]' if fault=='grant-extra' else '[]'
                else:
                    raw=recorded[json.dumps(argv)]
                    if fault=='loops' and argv[:3]==['docker','inspect',W.Q3_CONNECTOR] and 'Config.Env' in argv[-1]:raw+='\n'+W.LOOP_ENV_NAMES[0]+'\n'
                return subprocess.CompletedProcess(argv,0,raw,'')
            follow='http://127.0.0.1:17078'+W.DOWNLOAD_REDIRECT_PREFIX+'fake?signature=PURE_HMAC_NEVER_PERSIST'
            auths=[]
            def http(url,auth=None):
                auths.append((url,auth))
                if url==catalog['h14_download_url']:return W.HttpResponse(302,{'Location':follow if fault!='wrong-origin' else 'http://evil.invalid/file?signature=SECRET'},b'',url,True)
                if url==follow:
                    assert auth is None
                    return W.HttpResponse(200,{},download+(b'changed' if fault=='file' else b''),url,False)
                if url==catalog['h14_txn_url']:return W.HttpResponse(200,{},txn,url,True)
                name=url.rsplit('/',1)[-1]
                body=counter[name] if name in counter else b'{"status":"ok"}'
                if name in counter and fault=='counter':body+=b'changed'
                return W.HttpResponse(200,{},body,url,bool(auth))
            with patch.object(H,'check',return_value={'ok':True,'scope':'delegated real whole-set guard tested independently'}) as whole,patch.object(W,'read_jwt_secret',return_value='pure-test-secret'),patch.object(H.G,'candidate_hash',return_value=S):
                rc=F.execute(str(release),str(out),runner,http)
            result=json.loads((out/'RESULT.json').read_text());assert rc==(0 if fault is None else 2),(fault,result)
            assert whole.call_count==1
            assert not any('sql-controlled-9' in str(c) for c in calls)
            assert all(not any(word in part for word in ['INSERT INTO','UPDATE sync_','ALTER TABLE','DROP TRIGGER']) for c in calls for part in c)
            if fault is None:assert result['ok'] and result['whole_current_schedule_grants']==0 and (out/'old-h14-download.bin').read_bytes()==download
            allbytes=b''.join(p.read_bytes() for p in out.rglob('*') if p.is_file());assert b'PURE_HMAC_NEVER_PERSIST' not in allbytes
            cases.append(fault or 'all actual W8 readonly specs with delegated old+typed whole guard')
    print(json.dumps({'ok':True,'actual_HTTP_SQL_Docker_calls':0,'cases':cases},indent=2))
if __name__=='__main__':main()
