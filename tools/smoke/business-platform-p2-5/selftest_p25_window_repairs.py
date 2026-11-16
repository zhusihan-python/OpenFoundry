#!/usr/bin/env python3
"""Pure h7/h15 wire regressions; no sockets or subprocesses."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('window_engine',HERE/'smoke_p25.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class Repairs(unittest.TestCase):
    def test_h7_and_h15_actual_wire_paths(self):
        for sid in ('h7','h15'):
            with self.subTest(case=sid), tempfile.TemporaryDirectory() as td, \
                patch('socket.socket',side_effect=AssertionError('network forbidden')), \
                patch('subprocess.run',side_effect=AssertionError('process forbidden')):
                fixture=m.FakeIncrementalFixture(); proxy=m.FakeProxyDataset()
                connector=m.FakeConnector(fixture,proxy);rec=m.AttemptRecorder(td,'pure')
                class API(m.FakeApi):
                    def call(self,base,method,path,**kw):
                        if sid=='h15' and method=='GET' and path.endswith('/checkpoint'):
                            did=path.split('/syncs/')[1].split('/')[0]
                            if not any(r['sync_def_id']==did for r in connector.runs.values()):
                                return dict(status=404,json={'error':'incremental checkpoint not initialized for this sync definition'},error=None)
                        return super().call(base,method,path,**kw)
                factory=lambda case,auth='main':API(connector,fixture,proxy,rec,case,auth=auth)
                engine=m.P25Engine(factory,{'fixture_src_url':'http://p25fixture:8090','jwt_org2':'pure'},rec)
                with patch.object(m.time,'sleep'):
                    result=engine.run_scenario(sid).to_json()
                self.assertFalse(result['failed'],result)

    def test_fresh404_does_not_swallow_other_errors(self):
        good={'status':404,'json':{'error':'incremental checkpoint not initialized for this sync definition'}}
        data=m._h15_data_position(good);self.assertEqual(data['confirmed_epoch'],0)
        for response in ({'status':404,'json':{'error':'sync job not found'}}, {'status':403,'json':good['json']},
                         {'status':None,'json':None}, {'status':200,'json':{}}):
            self.assertIsNone(m._h15_data_position(response))
        self.assertEqual(data,m._h15_data_position({'status':200,'json':dict(data,owner={'run_id':'actual-owner'})}))
        self.assertNotEqual(data,m._h15_data_position({'status':200,'json':dict(data,initialized=True,confirmed_epoch=1)}))

    def test_fence_error_binds_actual_parent_and_run(self):
        d,r=m._new_uuid(),m._new_uuid()
        good={'status':409,'json':{'error':m._owner_fence_message(d,r)}}
        self.assertTrue(m._mutation_fence_error(good,'target',d,r))
        self.assertFalse(m._mutation_fence_error(good,'target',d,m._new_uuid()))
        self.assertFalse(m._mutation_fence_error({'status':409,'json':{'error':'unrelated conflict'}},'target',d,r))
        self.assertFalse(m._mutation_fence_error({'status':200,'json':good['json']},'target',d,r))

if __name__=='__main__':unittest.main()
