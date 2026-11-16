#!/usr/bin/env python3
"""Pure source/raw/rc/roster replay tests for repaired healthy aggregation."""
import contextlib
import importlib.util
import json
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('aggregate',HERE/'aggregate_healthy_p25.py')
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)
q=a.q;m=q.engine()


def dump(path,obj):path.write_bytes(a.raw_json(obj));return path
class API:
    def __init__(self,rec):self.rec=rec
    def call(self,base,method,path,**kw):
        body=({'commit_inflight':0,'commit_started':2,'commit_finished':2} if base=='proxy_admin' else
              {'name':path.rsplit('/',1)[-1],'active':0,'inventory_started':2,'inventory_finished':2})
        raw=json.dumps(body).encode()
        self.rec.record('final-quiet','http',{'base':base,'method':method,'path':q.BASES[base]+path,
            'status':200,'resp_body':raw.decode(),'resp_bytes':len(raw),'resp_sha256':a.digest(raw)})
        return {'status':200,'json':body}


def fixture(root):
    source=m._new_uuid();defs=[m._new_uuid() for _ in range(6)];runs=[m._new_uuid() for _ in range(2)]
    original_handles=[{'kind':'source','ref':source,'ok':True,'config':m.incremental_source_config('http://p25fixture:8090','p25h1')}]+[
        {'kind':'def','ref':d,'ok':True} for d in defs[:2]]
    owners=[dict(def_id=defs[i],source_id=source,run_id=runs[i],run_status='failed',blocked_reason='requires_reconciliation',source='finally-actual') for i in range(2)]
    scopes={};summaries={};steps={}
    def create(tag,rows,handles,failed,rc,final=False):
        evidence=root/(tag+'-evidence');evidence.mkdir();hp=dump(evidence/'handles.json',{'handles':handles,'count':len(handles)})
        for n,row in enumerate(rows):dump(evidence/(row['id']+'-attempt-%04d.json'%n),row)
        if final:
            dump(evidence/'finally-attempt-9999.json',dict(owner_roster=owners,run_roster=[],defs=defs,problems=[]))
            for i,owner in enumerate(owners):
                pairs=[({'id':owner['def_id'],'source_id':source},'/syncs/'+owner['def_id']),
                       ([{'id':owner['run_id'],'sync_def_id':owner['def_id'],'status':'failed'}],'/syncs/'+owner['def_id']+'/runs'),
                       ({'owner':{'run_id':owner['run_id'],'run_status':'failed','blocked_reason':'requires_reconciliation'}},'/syncs/'+owner['def_id']+'/checkpoint')]
                for j,(body,path) in enumerate(pairs):
                    dump(evidence/('finally-attempt-%04d.json'%(100+i*3+j)),{'status':200,'resp_body':json.dumps(body),'path':'http://local'+path})
        summary=root/(tag+'.json');dump(summary,dict(phase='healthy',released=True,failed=failed,scenarios=rows,
            quiet={'ok':True},attempts={'evidence_dir':str(evidence),'handles_file':str(hp)},final_owner_roster=owners,final_run_roster=[]))
        step=root/(tag+'-step');step.mkdir()
        argv=['python3','smoke_p25.py','--phase','healthy','--released','--result-file',str(summary)]
        if tag!='original':argv+=['--only',tag]
        (step/'cmd.txt').write_text('CMD: '+shlex.join(argv));dump(step/'stdout.log',{'result_file':str(summary),'failed':failed})
        (step/'stderr.log').write_text('');(step/'rc').write_text(str(rc)+'\n')
        scopes[tag]=str(hp);summaries[tag]=str(summary);steps[tag]=str(step)
    rows=[dict(id=s['id'],phase='healthy',failed=s['id'] in ('h7','h13','h15'),
        gates=[dict(gate=g,ok=s['id'] not in ('h7','h13','h15'),detail='pure',evidence=[]) for g in {'h2':['G_SNAPSHOT_ONLY'],'h3':['G_EMPTY_BATCH','G_LEDGER'],'h13':['G_CAS_ATOMIC','G_OWNER_DURABLE']}.get(s['id'],s['gates'])]) for s in m.SCENARIOS if s['phase']=='healthy' and s['id']!='h10']
    create('original',rows,original_handles,['h7','h13','h15'],1)
    union=list(original_handles)
    for i,tag in enumerate(('h7','h15','h13')):
        sid=m._new_uuid();src={'h7':'p25h4','h15':'p25h6','h13':'p25h13'}[tag]
        fresh=[{'kind':'source','ref':sid,'ok':True,'config':m.incremental_source_config('http://p25fixture:8090',src)}, {'kind':'def','ref':defs[i+2],'ok':True}]
        if tag=='h13':fresh.append({'kind':'def','ref':defs[5],'ok':True})
        union+=fresh
        row=next(r for r in rows if r['id']==tag);row=json.loads(json.dumps(row));row['failed']=False
        for g in row['gates']:g['ok']=True
        create(tag,[row],list(union) if tag=='h13' else fresh,[],0,final=tag=='h13')
    inputs=[scopes[t] for t in ('original','h7','h15','h13')]
    proof,path=q.collect(inputs,root/'quiet',api_factory=lambda engine,rec:API(rec))
    step=root/'quiet-step';step.mkdir()
    argv=['python3','collect_http_quiet_p25.py','--released']
    for hp in inputs:argv+=['--handles',hp]
    argv+=['--evidence-root',str(root/'quiet')]
    (step/'cmd.txt').write_text('CMD: '+shlex.join(argv));dump(step/'stdout.log',{'proof_file':str(path),'ok':True});(step/'stderr.log').write_text('');(step/'rc').write_text('0\n')
    import datetime as dt
    h13summary=Path(summaries['h13']);h13doc=json.loads(h13summary.read_text());he=Path(h13doc['attempts']['evidence_dir'])
    h13source=next(h['ref'] for h in union if h.get('kind')=='source' and h.get('config',{}).get('sync',{}).get('query_params',{}).get('src')=='p25h13')
    ledger=[];success=[]
    for i in range(4):
        start=dt.datetime(2026,11,15,tzinfo=dt.timezone.utc)+dt.timedelta(seconds=10*i)
        run=dict(id=m._new_uuid(),sync_def_id=defs[4+i//2],status='succeeded',incremental={'mode':'cursor','base_epoch':i%2},
                 started_at=start.isoformat(),publish_entered_at=(start+dt.timedelta(seconds=2)).isoformat())
        success.append(run)
        dump(he/('h13-attempt-%04d.json'%(1000+i)),{'status':200,'method':'GET','path':'/runs/'+run['id'],'resp_body':json.dumps(run)})
        ledger.append({'ts':start.timestamp()+1,'since_param':'since_token','token':None if i%2==0 else 'p25h13-tok-0',
                       'cursor':'','window':i%2,'page_size':50})
    for i,did in enumerate(defs[4:]):
        dump(he/('h13-attempt-%04d.json'%(1010+i)),{'status':200,'resp_body':json.dumps({'id':did,'source_id':h13source,'output_dataset_id':m._new_uuid()})})
    dump(he/'h13-attempt-1020.json',{'status':200,'path':'/schedule/ticks','resp_body':json.dumps([{'outcome':'enqueued','run_id':r['id']} for r in success[:3]])})
    dump(he/'h13-attempt-1021.json',{'status':202,'method':'POST','path':'/run','query':'async=true','resp_body':json.dumps({'id':success[3]['id']})})
    ld=root/'ledger';ld.mkdir();body=dump(ld/'body.json',ledger);(ld/'http_status').write_text('200');(ld/'original.rc').write_text('0')
    ls=root/'ledger-step';ls.mkdir();script=root/'ledger-read.py';script.write_text('# pure mocked collector')
    (ls/'cmd.txt').write_text('CMD: python3 -I '+str(script));(ls/'stderr.log').write_text('');(ls/'rc').write_text('0')
    dump(ls/'stdout.log',{'schema':'p25-h13-ledger-read-v1','status':200,'original_rc':0,'body':ledger,'body_file':str(body),
                          'body_sha256':q.sha(body),'h13_summary':str(h13summary),'h13_summary_sha256':q.sha(h13summary)})
    return dict(original_summary=summaries['original'],original_step=steps['original'],
        replacements=[dict(case=t,summary=summaries[t],step=steps[t]) for t in ('h7','h15','h13')],
        final_quiet_proof=str(path),final_quiet_step=str(step),h13_ledger_step=str(ls),handles_output=str(root/'union.json'))


class Aggregate(unittest.TestCase):
    def setUp(self):
        self.stack=contextlib.ExitStack();self.addCleanup(self.stack.close)
        for target in ('socket.socket','subprocess.run','urllib.request.urlopen'):
            self.stack.enter_context(patch(target,side_effect=AssertionError('live IO forbidden')))
    def prepare(self):
        td=tempfile.TemporaryDirectory();self.addCleanup(td.cleanup);root=Path(td.name);arguments=fixture(root)
        doc,handles=a.build(arguments);Path(arguments['handles_output']).write_bytes(handles);path=dump(root/'aggregate.json',doc)
        return root,arguments,doc,path
    def test_actual_source_rebuild_and_preserved_failure(self):
        root,args,doc,path=self.prepare();self.assertTrue(a.verify_aggregate(path)['ok'])
        self.assertEqual(doc['original_failure_preserved']['original_rc'],1)
        self.assertEqual(len(doc['final_owner_roster']),2);self.assertEqual(doc['final_run_roster'],[])
        self.assertEqual(set(doc['case_sources']),{s['id'] for s in m.SCENARIOS if s['phase']=='healthy' and s['id']!='h10'})
    def test_modified_source_raw_rc_union_or_aggregate_fail(self):
        for kind in ('rc','raw','union','aggregate','quiet'):
            with self.subTest(kind=kind):
                root,args,doc,path=self.prepare()
                if kind=='rc':Path(args['replacements'][0]['step'],'rc').write_text('1\n')
                elif kind=='raw':Path(doc['sources']['h13']['evidence_records'][0]['path']).write_text('{}')
                elif kind=='union':Path(args['handles_output']).write_text('{}')
                elif kind=='aggregate':doc['scenarios'][0]['gates'][0]['detail']='edited';dump(path,doc)
                else:
                    proof=json.loads(Path(args['final_quiet_proof']).read_text());raw=Path(proof['raw_records'][0]['path']);raw.write_text('{}')
                self.assertFalse(a.verify_aggregate(path)['ok'])
    def test_missing_replacement_or_changed_baseline_fail(self):
        for mode in ('missing','owner','import'):
            with self.subTest(mode=mode):
                root,args,doc,path=self.prepare()
                if mode=='missing':args['replacements']=args['replacements'][:-1]
                elif mode=='owner':
                    p=Path(args['replacements'][-1]['summary']);s=json.loads(p.read_text());s['final_owner_roster']=[];dump(p,s)
                else:
                    p=Path(doc['sources']['h13']['handles_file']);h=json.loads(p.read_text());h['handles']=h['handles'][1:];h['count']=len(h['handles']);dump(p,h)
                with self.assertRaises((ValueError,KeyError)):a.build(args)
    def test_quiet_rejects_wrong_self_identified_source_even_with_consistent_hashes(self):
        root,args,doc,path=self.prepare()
        pp=Path(args['final_quiet_proof']);proof=json.loads(pp.read_text())
        member=next(x for x in proof['raw_records'] if json.loads(Path(x['path']).read_text()).get('base')=='fixture')
        rp=Path(member['path']);record=json.loads(rp.read_text());body=json.loads(record['resp_body']);body['name']='other-source'
        raw=json.dumps(body).encode();record.update(resp_body=raw.decode(),resp_sha256=a.digest(raw),resp_bytes=len(raw))
        dump(rp,record);member['sha256']=q.sha(rp);dump(pp,proof)
        with self.assertRaisesRegex(ValueError,'body.name'):q.verify_quiet_proof(str(pp),[doc['sources'][tag]['handles_file'] for tag in ('original','h7','h15','h13')])

    def test_missing_mandatory_gate_fails_with_synchronized_summary_and_recorder(self):
        root,args,doc,path=self.prepare();summary=Path(args['replacements'][-1]['summary'])
        source=json.loads(summary.read_text());row=source['scenarios'][0]
        removed='G_OWNER_DURABLE';row['gates']=[g for g in row['gates'] if g['gate']!=removed]
        dump(summary,source)
        recorder=Path(doc['case_sources']['h13']['scenario_record']['path']);dump(recorder,row)
        with self.assertRaisesRegex(ValueError,'mandatory registry gate'):a.build(args)

    def test_h13_ledger_token_or_actual_parent_rejected_with_rehashed_body(self):
        for mode in ('token','base','parent'):
            with self.subTest(mode=mode):
                root,args,doc,path=self.prepare()
                if mode=='token':
                    mp=Path(args['h13_ledger_step'])/'stdout.log';meta=json.loads(mp.read_text());bp=Path(meta['body_file'])
                    ledger=json.loads(bp.read_text());ledger[0]['token']='forged';dump(bp,ledger)
                    meta.update(body=ledger,body_sha256=q.sha(bp));dump(mp,meta)
                else:
                    hp=Path(args['replacements'][-1]['summary']);he=Path(json.loads(hp.read_text())['attempts']['evidence_dir'])
                    rp=he/'h13-attempt-1000.json';record=json.loads(rp.read_text());run=json.loads(record['resp_body'])
                    if mode=='base':run['incremental']['base_epoch']=9
                    else:run['sync_def_id']=m._new_uuid()
                    record['resp_body']=json.dumps(run);dump(rp,record)
                with self.assertRaises(ValueError):a.build(args)

    def test_collector_default_refuses_before_io(self):
        with self.assertRaises(ValueError):q.collect(['/none'],'/none')
        for args in ([],['--released']):
            with patch.dict('os.environ',P25_CONFIRM='no'),contextlib.redirect_stderr(__import__('io').StringIO()):
                self.assertEqual(q.main(['--handles','/none','--evidence-root','/none']+args),3)

if __name__=='__main__':unittest.main()
