#!/usr/bin/env python3
"""Current dynamic UUID proof fixture; no old h14 identities or summary repair."""
import copy,json,os,sys,tempfile,uuid
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_p25_runtime as R
import p26_p25_supplements as S

def dump(path,obj):path.write_text(json.dumps(obj));return path

def fixture(root):
    m=R.load('smoke_p25.py'); owners=[];spec=[]
    for mode in ('no_txn_200','not_committed','wrong_target','http_500','drop_after_verify'):
        owner={'def_id':str(uuid.uuid4()),'source_id':str(uuid.uuid4()),'run_id':str(uuid.uuid4()),'run_status':'failed','blocked_reason':'requires_reconciliation','mode':mode}
        owners.append(owner);spec.append({'def_tag':'h14-drop' if mode=='drop_after_verify' else 'h11-'+mode,'run_ref':'terminal-failed','mode':mode,'blocked_reason':'requires_reconciliation'})
    held={'def_id':str(uuid.uuid4()),'source_id':str(uuid.uuid4()),'run_id':str(uuid.uuid4()),'run_status':'running','blocked_reason':'requires_reconciliation'}
    spec.append({'def_tag':'h10-held','run_ref':'same','blocked_reason':'requires_reconciliation'})
    decl={'schema':'p25-tail-roster-v1','ok':True,'spec':{'owner_rows':spec,'run_rows':[{'def_tag':'h10-held','source_tag':'h10-src','semantics':'p25-crash-held-prepublish','count':1}],'produced_by':'p25-tail'},'known_bindings':{'h10-held':{k:held[k] for k in ('def_id','source_id','run_id')}}}
    decl['declaration_sha256']=S.digest(json.dumps(decl['spec'],sort_keys=True).encode());declaration=dump(root/'decl.json',decl)
    runs=[{**held,'id':held['run_id'],'sync_def_id':held['def_id'],'status':'running','control_mode':'controlled','publish_entered_at':None,'generation':1,'attempt':1}]
    adjudication=m.adjudicate_tail(decl,owners+[held],runs);assert adjudication['ok'],adjudication
    cases=[{'id':'h11','phase':'tail','failed':False,'gates':[{'gate':g,'ok':True} for g in ('G_ACK_STRICT','G_RETAIN_UNKNOWN')],'owner_roster':owners[:4]},{'id':'h14','phase':'tail','failed':False,'gates':[{'gate':g,'ok':True} for g in ('G_ACK_STRICT','G_RETAIN_UNKNOWN','G_NO_REPLAY')],'owner_roster':[owners[-1]]}]
    ev=root/'original-tail';ev.mkdir();dump(ev/'h14-attempt-0001.json',cases[1]);dump(ev/'h11-attempt-0001.json',cases[0])
    i=owners[-1];did,sid,rid=[i[k] for k in ('def_id','source_id','run_id')];target,txn,fid=[str(uuid.uuid4()) for _ in range(3)]
    want=m._ndjson(m.window_values('p25h1',0));meta={'rows':24,'bytes':len(want),'content_hash':S.digest(want),'lineage':{'source_id':sid,'sync_def_id':did,'sync_run_id':rid}}
    ack={'dataset_id':target,'transaction':{'id':txn,'dataset_id':target,'status':'COMMITTED','metadata':meta}}
    entry={'request_target':target,'method':'POST','path':'/api/v1/datasets/'+target+'/outputs:commit','mode':'drop_after_verify','forwarded':True,'verified_committed':True,'upstream_status':201,'error':None,'ack_txn_id':txn,'upstream_ack_raw':json.dumps(ack),'upstream_ack_sha256':S.digest(json.dumps(ack).encode())}
    for n,ledger in ((2,[]),(3,[entry])):
        raw=json.dumps(ledger).encode();dump(ev/('h14-attempt-%04d.json'%n),{'base':'proxy_admin','method':'GET','path':'http://127.0.0.1:17096/admin/log','status':200,'resp_body':raw.decode(),'resp_sha256':S.digest(raw),'resp_bytes':len(raw)})
    summary={'phase':'tail','failed':[],'scenarios':cases,'owner_roster':owners,'final_owner_roster':owners+[held],'final_run_roster':runs,'tail_adjudication':{'ok':True},'tail_bindings':adjudication['bindings'],'attempts':{'evidence_dir':str(ev)}}
    sp=dump(root/'tail.json',summary)
    binding={'schema':'p25-tail-bindings-v1','summary_sha256':S.ref(sp)['sha256'],'bindings':adjudication['bindings'],'problems':[],'bindings_sha256':S.digest(json.dumps(adjudication['bindings'],sort_keys=True).encode()),'declaration_sha256':decl['declaration_sha256']}
    bp=dump(root/'bindings.json',binding)
    data={
        '/api/v1/data-connection/syncs/'+did:{'id':did,'source_id':sid,'output_dataset_id':target},
        '/api/v1/data-connection/syncs/'+did+'/runs/'+rid:{'id':rid,'sync_def_id':did,'status':'failed','dataset_version_id':None,'publish_entered_at':'2026-11-15T00:00:01Z','content_hash':S.digest(want)},
        '/api/v1/data-connection/syncs/'+did+'/checkpoint':{'confirmed_epoch':0,'owner':{'run_id':rid,'run_status':'failed','blocked_reason':'requires_reconciliation'}},
        '/v1/datasets/'+target+'/branches/main/transactions/'+txn:{'id':txn,'dataset_id':target,'status':'COMMITTED','branch_name':'main','tx_type':'SNAPSHOT','metadata':meta},
        '/v1/datasets/'+target+'/files':{'branch':'main','total':1,'files':[{'id':fid,'dataset_id':target,'transaction_id':txn,'size_bytes':len(want),'logical_path':'sync-'+rid+'.ndjson'}]},
        '/v1/datasets/'+target+'/files/'+fid+'/download':want}
    class API:
        def __init__(self,bases,jwt,rec,scenario):self.bases=bases;self.rec=rec;self.scenario=scenario
        def call(self,base,method,path,**kwargs):
            assert method=='GET';obj=data[path];raw=obj if isinstance(obj,bytes) else json.dumps(obj).encode()
            self.rec.record(self.scenario,'http',{'base':base,'method':method,'path':self.bases[base]+path,'status':200,'resp_sha256':S.digest(raw),'resp_bytes':len(raw),**({'resp_body':raw.decode()} if not isinstance(obj,bytes) else {})})
            return {'status':200,'json':obj,'raw':raw}
    out=root/'reads';out.mkdir();ctx=NS(engine=lambda:NS(AttemptRecorder=m.AttemptRecorder,HttpApi=API),output=out,bases={'connector':'http://127.0.0.1:17100','dataset':'http://127.0.0.1:17078'},release={'candidate_source':R.SOURCE})
    return S.collect_h14(ctx,sp,bp,declaration)

def main():
    with tempfile.TemporaryDirectory(dir=R.P26/'results/w10') as td,patch('socket.socket',side_effect=AssertionError('live forbidden')),patch('subprocess.run',side_effect=AssertionError('child forbidden')),patch('urllib.request.urlopen',side_effect=AssertionError('HTTP forbidden')),patch.dict(os.environ,OPENFOUNDRY_SMOKE_JWT='PURE-IN-MEMORY'):
        proof=fixture(Path(td));assert S.verify_h14(proof)['ok'];original=S.doc(proof)
        # Rehashed independent data still must satisfy actual lineage/bytes,
        # not merely a self-consistent file hash or summary ok flag.
        for change in ('lineage','file_id','bytes','wrong_ack'):
            value=copy.deepcopy(original);saved=[]
            if change=='lineage':
                p=Path(value['bodies']['transaction']['path']);saved.append((p,p.read_bytes()));d=S.doc(p);d['metadata']['lineage']['source_id']=str(uuid.uuid4());dump(p,d);value['bodies']['transaction']=S.ref(p)
            if change=='file_id':
                p=Path(value['bodies']['files']['path']);saved.append((p,p.read_bytes()));d=S.doc(p);d['files'][0]['id']=str(uuid.uuid4());dump(p,d);value['bodies']['files']=S.ref(p)
            if change=='bytes':
                p=Path(value['bodies']['download']['path']);saved.append((p,p.read_bytes()));p.write_bytes(b'wrong\n');value['bodies']['download']=S.ref(p)
            if change=='wrong_ack':
                value['actual_entry']['upstream_ack_raw']='{}';value['actual_entry']['upstream_ack_sha256']=S.digest(b'{}')
            altered=Path(td)/('bad-'+change+'.json');dump(altered,value)
            try:S.verify_h14(altered)
            except ValueError:pass
            else:assert False,change
            for p,raw in saved:p.write_bytes(raw)
    print(json.dumps({'ok':True,'actual_http_sql_docker_calls':0,'checks':['future dynamic UUID h14 true201 ACK + original whole dual-roster replay','independent COMMITTED lineage + file-index ID + exact 24-row/full-byte download','rehashed wrong lineage/file/bytes/ACK refused; no summary replacement']},indent=2))
if __name__=='__main__':main()
