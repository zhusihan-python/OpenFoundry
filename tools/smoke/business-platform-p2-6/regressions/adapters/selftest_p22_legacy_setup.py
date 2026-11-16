#!/usr/bin/env python3
"""Pure execution of the real setup/SQL builder with fake HTTP, PG and guard."""
import base64,copy,json,re,sys,tempfile,uuid,subprocess
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import p22_legacy_probe_entry as E
S='b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c'
def uid():return str(uuid.uuid4())
def row(rid,did,sid,tid,actor,org):
    r={k:'' for k in E.H.W.C.SCHEMA_27}
    r.update(id=rid,sync_def_id=did,source_id=sid,output_dataset_id=tid,owner_id=actor,run_owner_id=actor,requested_by=actor,conn_org_id=org,def_org_id=org,run_org_id=org,org_id=org,control_mode='legacy',status='running',source_name='pure-new-source',has_holder='false',has_token='false',gate_entered='false',cancel_requested='false',has_hash='false',has_version='false')
    E.H.W.C._check_row_schema(r,'fake',0);return r

def parent(r):return {'def_id':r['sync_def_id'],'source_id':r['source_id'],'conn_id':r['source_id'],'output_dataset_id':r['output_dataset_id'],'org_id':r['org_id'],'conn_org_id':r['org_id'],'conn_owner_id':r['owner_id'],'source_name':r['source_name'],'connector_type':'rest_api'}

class Fake:
    def __init__(self,fault=None):
        self.fault=fault;self.calls=[];self.rows=[row(*[uid() for _ in range(6)]) for _ in range(2)];self.parents=[parent(r) for r in self.rows];self.created={};self.insert=0
        self.baseline={'schema':'p26-w10-legacy-baseline-v1','runs':copy.deepcopy(self.rows),'parents':copy.deepcopy(self.parents),'protocols':[{'id':r['id'],'recovery_protocol':None} for r in self.rows],'crosscheck13_parent_protocolNULL':True,'whole_active_outside_old9_exact_verified_legacy2':True}
        if fault=='shadow':self.rows.append(row(*[uid() for _ in range(6)]))
    def http(self,url,method,raw,token):
        self.calls.append(('http',method,url));body=json.loads(raw) if raw else None
        if method=='POST':
            assert not url.endswith('/run') and not url.endswith('/cancel')
            rid=uid()
            if url.endswith('/sources'):self.created['sid']=rid; self.created['source_name']=body['name'];assert body['config']['base_url']=='http://p26fixture:8090'
            elif url.endswith('/datasets'):self.created['tid']=rid
            else:
                self.created['did']=rid;assert body['schedule_enabled'] is False
                claims=json.loads(base64.urlsafe_b64decode(token.split('.')[1]+'==='))
                new=row(uid(),rid,self.created['sid'],self.created['tid'],claims['sub'],claims['org_id']);new['source_name']=self.created['source_name'];self.parents.append(parent(new))
            return 201,json.dumps({'id':rid}).encode()
        if url.endswith('/checkpoint'):return 404,b'{"error":"not found"}'
        r=self.rows[-1];return 200,json.dumps([{'id':r['id'],'sync_def_id':r['sync_def_id'],'org_id':r['org_id'],'requested_by':r['requested_by'],'status':'running','control_mode':'legacy'}]).encode()
    def pg(self,argv,**kwargs):
        self.calls.append(('pg',argv));sql=' '.join(argv)
        if 'INSERT INTO sync_runs' in sql:
            self.insert+=1;assert self.insert==1
            assert "d.schedule_enabled=false AND d.schedule_next_fire_at IS NULL" in sql
            rid=re.search("SELECT '([^']+)'::uuid,d.id",sql)[1]
            actor=re.search("c.owner_id='([^']+)'",sql)[1];org=re.search("d.org_id='([^']+)'",sql)[1]
            r=row(rid,self.created['did'],self.created['sid'],self.created['tid'],actor,org);r['source_name']=self.created['source_name'];self.rows.append(r)
            if self.fault=='timeout':raise subprocess.TimeoutExpired(argv,30)
            return subprocess.CompletedProcess(argv,0,rid+'\n','')
        if 'schedule_enabled' in sql:
            result=[{'id':self.created['did'],'schedule_enabled':False,'next_fire_null':True}]
        elif 'recovery_protocol' in sql:
            ids=re.findall("'([a-f0-9-]{36})'",sql);result=[{'id':r['id'],'recovery_protocol':'wrong' if self.fault=='protocol' else None} for r in self.rows if r['id'] in ids]
        elif 'AS conn_owner_id' in sql:
            ids=re.findall("'([a-f0-9-]{36})'",sql);result=[p for p in self.parents if p['def_id'] in ids]
        elif "r.control_mode = 'legacy'" in sql:result=self.rows
        else:
            ids=re.findall("r.id='([^']+)'::uuid",sql);result=[r for r in self.rows if r['id'] in ids]
        return subprocess.CompletedProcess(argv,0,json.dumps(result)+'\n','')

def main():
    root=E.H.P26/'results/w10'
    with tempfile.TemporaryDirectory(prefix='N-pure-',dir=root) as td:
        td=Path(td);results=[]
        for fault in [None,'shadow','protocol','timeout']:
            f=Fake(fault);r={'schema':'p26-w10-release-v1','root_accepted':True,'expires_utc':'2035-01-01T00:00:00Z','stage':E.STAGE,'suite':'p22','candidate_source':S,'allow_new_legacy_fixture_insert':True,'setup_tools_sha256':{p:E.digest(E.H.P26/p) for p in E.SETUP_TOOLS},'setup_entry_sha256':E.digest(E.__file__),'evidence_root':str(td)}
            for key,obj in [('deployment_record',{}),('typed_roster',{'schema':'p26-w10-typed-v1','runs':[],'public_views':[]}),('legacy_baseline',f.baseline)]:
                p=td/(str(fault)+key+'.json');p.write_text(json.dumps(obj));r[key]=str(p);r[key+'_sha256']=E.digest(p)
            out=td/(str(fault)+'output');r['setup_output']=str(out);rp=td/(str(fault)+'release.json');rp.write_text(json.dumps(r))
            with patch.object(E.H.G,'candidate_hash',return_value=S),patch.object(E.H.W,'read_jwt_secret',return_value='pure-test-secret'),patch.object(E.H,'check',return_value={'ok':True,'controlled_count':9,'cp_count':8}) as guard:
                rc=E.execute(str(rp),str(out),f.pg,f.http)
                result=json.loads((out/'RESULT.json').read_text())
                assert result['tools_unchanged'] and result['source_unchanged']
                if fault:
                    assert rc==2 and not result['ok']
                    if fault in ('shadow','protocol'):assert not any(c[0]=='http' for c in f.calls) and f.insert==0
                    else:
                        assert f.insert==1 and result['insert_attempt']['acknowledged'] is False
                        assert any(p.read_text().strip()=='unavailable' for p in out.rglob('*.rc'))
                else:
                    assert rc==0 and result['ok'] and f.insert==1 and len(result['legacy_after']['rows'])==3 and len(result['actual_owned_receipts'])==3
                    assert guard.call_count==2
                    typed=json.loads(Path(result['typed_candidate']).read_text());assert len(typed['runs'])==1 and len(typed['legacy_whole_baseline_27'])==2
                    assert typed['runs'][0]['protocol']=='legacy-null' and typed['public_views'][0]['checkpoint_status']==404
                results.append({'fault':fault,'fake_inserts':f.insert,'fake_HTTP_calls':sum(c[0]=='http' for c in f.calls)})
        calls=[]
        try:E.execute(td/'missing-release',td/'never',runner=lambda *a,**kw:calls.append(a),http=lambda *a:calls.append(a))
        except FileNotFoundError:pass
        else:assert False
        assert not calls
    print(json.dumps({'ok':True,'actual_HTTP_SQL_Docker_calls':0,'cases':results,'checks':['real setup creates owned Source/Dataset/disabled Def then single exact INSERT','historic BC whole27/parent/protocol unchanged','new legacy public CP404+Run full baseline and typed candidate','shadow/wrong protocol fail before HTTP or SQL write','INSERT timeout original_rc unavailable, no retry or cleanup','missing release zero calls']},indent=2))
if __name__=='__main__':main()
