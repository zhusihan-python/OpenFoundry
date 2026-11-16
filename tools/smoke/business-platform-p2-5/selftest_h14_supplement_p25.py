#!/usr/bin/env python3
"""Pure proxy 201 ACK regression and supplement tamper checks."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import types
import unittest
from unittest.mock import Mock, patch

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('proxy_fixed',HERE/'fixtures/dataset_fault_proxy_p25.py')
proxy=importlib.util.module_from_spec(spec);spec.loader.exec_module(proxy)
TARGET='cb5f193c-a5ca-4512-8f72-9f054aa9312e'

class Proxy201(unittest.TestCase):
    def call(self,status,body,error=None):
        core=proxy.ProxyCore();core.set_mode('drop_after_verify')
        h=object.__new__(proxy.Handler);h.core=core;h.path='/api/v1/datasets/'+TARGET+'/outputs:commit'
        h.headers={'Content-Length':'2'};h.rfile=io.BytesIO(b'{}');h.connection=types.SimpleNamespace(close=Mock())
        h._upstream=lambda *args:(status,body,error);h._send=Mock()
        with patch('socket.socket',side_effect=AssertionError('no sockets')),patch('urllib.request.urlopen',side_effect=AssertionError('no network')):
            h.do_POST()
        self.assertTrue(core.quiet());return h,core.ledger[0]
    def actual_ack(self):
        p=HERE/'results/2026-11-14-p25-final/tail/p25-tail-20261114T173622/h14-attempt-0049.json'
        rows=json.loads(json.loads(p.read_text())['resp_body'])
        return next(e['upstream_ack_raw'].encode() for e in rows if e.get('request_target')==TARGET)
    def test_real_201_ack_strictly_verified_then_dropped(self):
        raw=self.actual_ack();h,entry=self.call(201,raw)
        self.assertTrue(entry['verified_committed']);self.assertEqual(entry['ack_txn_id'],'bac73c15-b4ae-4a58-be24-0b1d5bfc0d78')
        h.connection.close.assert_called_once();h._send.assert_not_called()
    def test_bad_ack_or_upstream_failure_never_misdrop(self):
        actual=json.loads(self.actual_ack())
        cases=[]
        for field,value in (('status','OPEN'),('id','00000000-0000-0000-0000-000000000000'),('id','not-uuid')):
            body=copy.deepcopy(actual);body['transaction'][field]=value;cases.append((201,json.dumps(body).encode(),None))
        body=copy.deepcopy(actual);body['dataset_id']='00000000-0000-4000-8000-000000000000';cases.append((201,json.dumps(body).encode(),None))
        cases += [(201,b'invalid JSON',None),(503,self.actual_ack(),None),(502,b'',RuntimeError('upstream failed'))]
        for status,body,error in cases:
            with self.subTest(status=status,body=body[:30]):
                h,entry=self.call(status,body,error);self.assertFalse(entry['verified_committed'])
                h.connection.close.assert_not_called();self.assertEqual(h._send.call_args.args[0],502 if error else status)

class Supplement(unittest.TestCase):
    def setup_sources(self,td):
        sp=importlib.util.spec_from_file_location('h14_verifier',HERE/'verify_h14_supplement_p25.py')
        m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
        ev=HERE/'results/2026-11-14-p25-final'
        args=dict(original_tail=str(ev/'tail-result.json'),original_step=str(ev/'steps/tail-attempt1'),
          remote_summary=str(ev/'tail-h14-supplement/attempt2/summary.json'),
          old_proxy=str(HERE/'results/h14-repair-tools-sol/attempt1/dataset_fault_proxy_p25.before.py'),
          bindings_output=str(td/'tail-selected-bindings.json'),proof_output=str(td/'proof.json'))
        overrides={}
        def resolve(path):return overrides.get(str(path),Path(path))
        def mutate(path,value):
            target=td/('mutation-%d.json'%len(overrides));target.write_bytes(m.data(value));overrides[str(path)]=target
        return m,args,resolve,mutate
    def test_exact_savedraw_remote_replay_only_replaces_two_gates(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d,patch('socket.socket',side_effect=AssertionError('network forbidden')),patch('subprocess.run',side_effect=AssertionError('process forbidden')),patch('urllib.request.urlopen',side_effect=AssertionError('HTTP forbidden')):
            td=Path(d);m,args,resolve,_=self.setup_sources(td);agg,bind,proof=m.build(args,resolve)
            selected=td/'aggregate.json';selected.write_bytes(agg);Path(args['bindings_output']).write_bytes(bind);Path(args['proof_output']).write_bytes(proof)
            self.assertTrue(m.verify_supplement(selected,resolve)['ok'])
            old=json.loads(Path(args['original_tail']).read_text());new=json.loads(agg)
            self.assertEqual(new['final_owner_roster'],old['final_owner_roster']);self.assertEqual(new['final_run_roster'],old['final_run_roster'])
            self.assertEqual(new['tail_bindings'],old['tail_bindings']);self.assertEqual(new['scenarios'][0],old['scenarios'][0])
            old14=next(x for x in old['scenarios'] if x['id']=='h14');new14=next(x for x in new['scenarios'] if x['id']=='h14')
            self.assertEqual([i for i,(x,y) in enumerate(zip(old14['gates'],new14['gates'])) if x!=y],[0,2])
            self.assertEqual(json.loads(bind)['summary_sha256'],m.digest(agg))
            chosen=json.loads(bind);chosen['bindings']['h14-drop']['run_id']=m.IDENTITY['def_id']
            Path(args['bindings_output']).write_bytes(m.data(chosen));self.assertFalse(m.verify_supplement(selected,resolve)['ok'])
    def test_rehashed_remote_tampering_and_local_success_cannot_fake_supplement(self):
        import tempfile
        for kind in ('lineage','files_txn','download','wrong_cmd','local_success','old_summary','old_proxy'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as d:
                td=Path(d);m,args,resolve,mutate=self.setup_sources(td)
                remote=json.loads(Path(args['remote_summary']).read_text())
                if kind in ('lineage','files_txn','download','wrong_cmd'):
                    group=remote['artifacts']['transaction' if kind=='lineage' else 'files' if kind=='files_txn' else 'download']
                    member='cmd' if kind=='wrong_cmd' else 'body';ref=group[member];path=ref['path']
                    if kind=='download':raw=b'"forged"\n'
                    else:
                        body=json.loads(Path(path).read_text())
                        if kind=='lineage':body['metadata']['lineage']['sync_run_id']=m.IDENTITY['def_id']
                        elif kind=='files_txn':body['files'][0]['transaction_id']=m.IDENTITY['def_id']
                        else:body['url']=m.BASE+'/files/'+m.IDENTITY['ack_file_id']+'/download'
                        raw=m.data(body)
                    target=td/'remote.raw';target.write_bytes(raw)
                    prior_resolve=resolve
                    def resolve(p,previous=prior_resolve,key=path,target=target):return target if str(p)==key else previous(p)
                    ref['sha256']=m.digest(raw);ref['bytes']=len(raw);mutate(args['remote_summary'],remote)
                elif kind=='local_success':
                    original=json.loads(Path(args['original_tail']).read_text());path=str(Path(original['attempts']['evidence_dir'])/'h14-attempt-0048.json')
                    record=json.loads(Path(path).read_text());body=json.loads(record['resp_body']);body['status']='succeeded'
                    raw=json.dumps(body).encode();record.update(resp_body=raw.decode(),resp_bytes=len(raw),resp_sha256=m.digest(raw));mutate(path,record)
                elif kind=='old_summary':
                    original=json.loads(Path(args['original_tail']).read_text());original['failed']=[];mutate(args['original_tail'],original)
                else:
                    target=td/'bad-old-proxy.py';target.write_text('# changed')
                    old=resolve
                    def resolve(p,previous=old):return target if str(p)==args['old_proxy'] else previous(p)
                with self.assertRaises((ValueError,KeyError)):m.build(args,resolve)

if __name__=='__main__':unittest.main()
