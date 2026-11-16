#!/usr/bin/env python3
"""HTTP evidence pure tests. No sockets or subprocess product execution."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_http_lib as lib
import p26_scenarios as scenarios

class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.ev=lib.EvidenceDir(Path(self.tmp.name),'evidence-pure')
    def tearDown(self):
        self.tmp.cleanup()
    def result(self,tag,resp,**kwargs):
        rec=self.ev.step(tag);rec.cmd(['GET','/safe'])
        rec.result(resp,**kwargs)
        return json.loads((self.ev.path/(tag+'.meta')).read_text())
    def test_original_status_separate_from_normalized_rc(self):
        for status in (200,201,302,401,403,404,409,502):
            tag=str(status)
            meta=self.result(tag,lib.HTTPResponse(status=status,body=b'{"error":"safe refusal"}'))
            self.assertEqual(meta['original_http_status'],status)
            self.assertEqual(meta['rc_kind'],'normalized_harness')
            self.assertEqual(meta['original_child_rc'],'unavailable')
            self.assertEqual(int((self.ev.path/(tag+'.rc')).read_text()),0 if status<300 else status)
            self.assertEqual(meta['safe_response']['error'],'safe refusal')
    def test_full_safe_counters_and_opaque_projection(self):
        body={'status':'running','recovery':{'send_attempts':2,'query_attempts':1},
              'confirmed_epoch':1,'owner':{'run_id':'new-owned'},
              'config':{'secret':'private-config'},'completed_token':'opaque-original',
              'publish_body':'raw-publish-original','content_base64':'raw-inline-original',
              'next_cursor':'opaque-page','lease_token':'lease-original'}
        meta=self.result('safe',lib.HTTPResponse(status=200,body=json.dumps(body).encode()))
        self.assertEqual(meta['safe_response']['completed_token'],lib.opaque_digest('opaque-original'))
        self.assertEqual(meta['safe_response']['recovery'],body['recovery'])
        self.assertEqual(meta['safe_response']['confirmed_epoch'],1)
        files=''.join(p.read_text() for p in self.ev.path.iterdir())
        for secret in ('private-config','opaque-original','raw-publish-original','raw-inline-original','opaque-page','lease-original'):
            self.assertNotIn(secret,files)
    def test_exact_env_secret_redacts_arbitrary_error(self):
        with patch.dict(os.environ,{'P26_HTTP_ADMIN_TOKEN':'opaque-admin-original'}):
            meta=self.result('error',lib.HTTPResponse(status=502,body=b'{"error":"opaque-admin-original"}'))
        self.assertEqual(meta['safe_response']['error'],'<redacted>')
    def test_transport_failure_has_no_http_or_child_status(self):
        meta=self.result('failed',lib.HTTPResponse(err=OSError('private exception message')))
        self.assertIsNone(meta['original_http_status'])
        self.assertEqual(meta['transport_exception'],'OSError')
        self.assertEqual(meta['original_child_rc'],'unavailable')
        self.assertNotIn('private exception message',(self.ev.path/'failed.raw').read_text())
    def test_explicit_download_artifact_recomputable(self):
        body=b'"record one"\n"record two"\n'
        meta=self.result('download',lib.HTTPResponse(status=200,body=body),artifact_bytes=True)
        artifact=self.ev.path/meta['download_artifact']
        self.assertEqual(artifact.read_bytes(),body)
        self.assertEqual(lib.sha256_hex(artifact.read_bytes()),meta['response_sha256'])
        self.assertEqual(len(artifact.read_bytes().splitlines()),2)
    def test_unexpected_client_exception_still_has_evidence(self):
        class Broken:
            def request(self,*args,**kwargs):
                raise RuntimeError('original failure')
        bases={k:'http://'+k for k in ('fixture','proxy_admin','dvs','connector')}
        ctx=scenarios.LiveCtx(None,bases,'Bearer test','admin',Path(self.tmp.name),'broken')
        client=Broken();client.base='http://connector'
        with self.assertRaisesRegex(RuntimeError,'original failure'):
            ctx.http('broken',client,'GET','/safe')
        self.assertEqual(len(list(ctx.evidence.path.glob('*.cmd'))),1)
        meta=json.loads(next(ctx.evidence.path.glob('*.meta')).read_text())
        self.assertEqual(meta['transport_exception'],'RuntimeError')
    def test_dropack_baseline_delta_not_global_one(self):
        import selftest_p26_http as mechanics
        stack=mechanics.FakeStackTransport(behavior='auto')
        stack.proxy.stats['drops']=3
        ctx=scenarios.LiveCtx(stack,mechanics.FAKE_BASES,"Bearer "+mechanics.PLATFORM_JWT,
                              mechanics.ADMIN_TOKEN,Path(self.tmp.name),'baseline',
                              approved_files_origin='http://'+mechanics.FakeStackTransport.DVS_FILES)
        facts=scenarios.run_scenario('dropack_auto_retry_exact_replay',ctx)
        self.assertEqual(facts['outcome'],'pass')
        self.assertEqual(facts['proxy_stats_baseline']['drops'],3)
        self.assertEqual(facts['proxy_stats_final']['drops'],4)
        self.assertTrue(facts['fixture_snapshots'])

if __name__=='__main__':
    unittest.main(verbosity=2)
