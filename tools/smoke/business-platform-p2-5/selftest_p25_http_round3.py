#!/usr/bin/env python3
"""Pure round-3 regressions; no HTTP, Docker, DB, Go, or sleeping."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('p25_http', HERE / 'smoke_p25.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

D, S, R = [str(m._new_uuid()) for _ in range(3)]
OWNER = dict(def_id=D, source_id=S, run_id=R, run_status='failed',
             blocked_reason='requires_reconciliation', mode='no_txn_200')
DECL = {'spec': {'owner_rows': [dict(def_tag='negative', run_ref='terminal-failed',
                 mode='no_txn_200', blocked_reason='requires_reconciliation')], 'run_rows': []}}

class Round3(unittest.TestCase):
    def setUp(self):
        self.net = patch.object(socket, 'socket', side_effect=AssertionError('network forbidden'))
        self.net.start()
        self.addCleanup(self.net.stop)

    def test_real_p25_fixture_hold_15_seconds_with_30_second_cap(self):
        spec = importlib.util.spec_from_file_location('p25_fixture_pure', HERE / 'fixtures/rest_source_p25.py')
        fxmod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fxmod)
        fx = fxmod.IncrementalFixture()
        for millis, seconds in ((15000, 15), (90000, 30)):
            fx.set_knob('p25h13', {'slow_terminal_ms': millis})
            with patch.object(fxmod.time, 'sleep') as sleep:
                status, _ = fx.data('p25h13', {})
            self.assertEqual(status, 200)
            sleep.assert_called_once_with(seconds)

    def test_crash_record_binds_inspected_identity_and_real_artifacts(self):
        import copy
        with tempfile.TemporaryDirectory() as td:
            prior = {D: {'source_id': S, 'checkpoint': {'owner': {'run_id': R}}}}
            prior_path = Path(td, 'prior.json')
            prior_path.write_text(json.dumps({'snapshot': prior, 'taken_at': '2026-11-14T01:00:00Z'}))
            image = 'sha256:' + 'a' * 64
            rec = dict(schema='p25-crash-record-v1', project='openfoundry-platform-p1-q3v9',
                       service='p25connector', container='b' * 64, container_id='b' * 64,
                       image=image, image_before=image, image_after=image,
                       method='sigkill+start-same-image', performed_by='pure-test',
                       started_at_before='2026-11-14T00:00:00Z', started_at_after='2026-11-14T01:00:02Z',
                       timestamps=dict(pre_at='2026-11-14T01:00:00Z', kill_at='2026-11-14T01:00:01Z',
                                       started_after_at='2026-11-14T01:00:02Z', post_at='2026-11-14T01:00:03Z'),
                       held=dict(def_id=D, source_id=S, run_id=R), pre_snapshot_sha256=m._file_sha256(prior_path))
            for action in ('kill', 'start', 'health'):
                raw = Path(td, action + '.raw'); raw.write_bytes(action.encode())
                cmd = {'kill': 'docker kill -s KILL ' + rec['container'], 'start': 'docker start ' + rec['container'], 'health': 'curl --fail http://local/health'}[action]
                rec[action] = dict(cmd=cmd, rc=0, artifact=str(raw), artifact_sha256=m._file_sha256(raw))
            for stage in ('before', 'after'):
                raw = Path(td, stage + '.json')
                raw.write_text(json.dumps([dict(Id=rec['container'], Image=image,
                    Config={'Labels': {'com.docker.compose.project': rec['project'], 'com.docker.compose.service': 'p25connector'}},
                    State={'StartedAt': rec['started_at_' + stage]})]))
                rec['inspect_' + stage] = dict(artifact=str(raw), artifact_sha256=m._file_sha256(raw))
            check = lambda r, img=image: m._validate_crash_record(r, [D], D, prior, str(prior_path), img)
            self.assertEqual(check(rec), [])
            for field in ('project', 'service', 'container_id', 'health', 'inspect_before', 'inspect_after', 'pre_snapshot_sha256', 'held'):
                bad = copy.deepcopy(rec); bad.pop(field)
                with self.subTest(missing=field): self.assertTrue(check(bad))
            for action in ('kill', 'start', 'health'):
                bad = copy.deepcopy(rec); bad[action]['rc'] = False
                with self.subTest(bool_rc=action): self.assertTrue(check(bad))
                bad = copy.deepcopy(rec); bad[action]['artifact_sha256'] = 'c' * 64
                with self.subTest(fake_sha=action): self.assertTrue(check(bad))
            bad = copy.deepcopy(rec); bad['timestamps']['kill_at'] = bad['timestamps']['pre_at']
            self.assertTrue(check(bad))
            bad = copy.deepcopy(rec); bad['kill']['cmd'] = 'docker restart ' + rec['container']
            self.assertTrue(check(bad))
            self.assertTrue(check(rec, 'sha256:' + 'c' * 64))

    def test_h13_pure_success_and_schedule_disabled_even_on_failure(self):
        for force_failure in (False, True):
            with self.subTest(force_failure=force_failure), tempfile.TemporaryDirectory() as td:
                fixture = m.FakeIncrementalFixture(); proxy = m.FakeProxyDataset()
                connector = m.FakeConnector(fixture, proxy)
                rec = m.AttemptRecorder(td, 'h13-pure')
                polls = {}
                class DelayedRunApi(m.FakeApi):
                    def call(self, base_key, method, path, **kw):
                        result = super().call(base_key, method, path, **kw)
                        if method == 'GET' and '/runs/' in path and result.get('json', {}).get('status') == 'succeeded':
                            polls[path] = polls.get(path, 0) + 1
                            if polls[path] == 1:
                                result = dict(result, json=dict(result['json'], status='running'))
                        return result
                factory = lambda sid, auth='main': DelayedRunApi(connector, fixture, proxy, rec, sid, auth=auth)
                cfg = dict(fixture_src_url='http://p25fixture:8090', sql_fact_reader=m.FakeSqlFactReader(connector, rec))
                engine = m.P25Engine(factory, cfg, rec)
                if force_failure:
                    connector.create_source(S, m.incremental_source_config('http://fixture', 'p25h13'))
                    engine.h13 = lambda api, res: (engine.make_sync_def(api, 'abort', S, D,
                        extra_cfg={'schedule_enabled': True}, scenario='h13'), (_ for _ in ()).throw(ValueError('forced')))
                with patch.object(m.time, 'sleep'):
                    result = engine.run_scenario('h13').to_json()
                self.assertEqual(result['failed'], force_failure, result)
                if not force_failure:
                    self.assertGreaterEqual(len(polls), 3)
                    self.assertTrue(all(count >= 2 for count in polls.values()))
                self.assertTrue(connector.defs)
                self.assertTrue(all(not d.get('schedule_enabled') for d in connector.defs.values()))

    def test_terminal_post_still_reads_get_only_incremental(self):
        with tempfile.TemporaryDirectory() as td:
            rec = m.AttemptRecorder(td, 'wire')
            calls = []
            class API:
                def call(self, base, method, path, **kw):
                    calls.append(method)
                    body = dict(id=R, sync_def_id=D, status='succeeded')
                    if method == 'GET': body['incremental'] = dict(mode='cursor', base_epoch=0)
                    return dict(status=200, json=body, error=None)
            api = API()
            engine = m.P25Engine(lambda *a, **kw: api, {}, rec)
            run, _ = engine.trigger_run(api, D)
            self.assertEqual(calls, ['POST', 'GET'])
            self.assertEqual(run['incremental']['base_epoch'], 0)
            self.assertNotIn('incremental', m._public_run(run))
            self.assertIn('incremental', m._public_run(run, incremental=True))

    def test_tail_owner_is_terminal_independent_of_run_roster(self):
        self.assertTrue(m.adjudicate_tail(DECL, [OWNER], [])['ok'])

    def test_tail_rejects_duplicate_owner(self):
        self.assertFalse(m.adjudicate_tail(DECL, [OWNER, OWNER], [])['ok'])

    def test_tail_rejects_wrong_mode_without_fallback(self):
        self.assertFalse(m.adjudicate_tail(DECL, [dict(OWNER, mode='http_500')], [])['ok'])

    def test_tail_rejects_wrong_business_state(self):
        self.assertFalse(m.adjudicate_tail(DECL, [dict(OWNER, blocked_reason='')], [])['ok'])

    def test_tail_rejects_extra_or_missing_owner(self):
        for rows in ([], [OWNER, dict(OWNER, def_id=m._new_uuid(), run_id=m._new_uuid())]):
            self.assertFalse(m.adjudicate_tail(DECL, rows, [])['ok'])

    def test_tail_rejects_orphan_nonterminal(self):
        row = dict(def_id=D, source_id=S, run_id=R, sync_def_id=D, status='running')
        self.assertFalse(m.adjudicate_tail(DECL, [OWNER], [row])['ok'])

    def test_tail_rejects_missing_run_parent(self):
        decl = {'spec': {'owner_rows': [], 'run_rows': [dict(def_tag='held', count=1)]}}
        row = dict(def_id=D, source_id=S, run_id=R, status='running')
        self.assertFalse(m.adjudicate_tail(decl, [], [row])['ok'])

    def test_uuid_requires_real_layout(self):
        self.assertFalse(m._is_uuidish('----' + 'a' * 32))
        self.assertTrue(m._is_uuidish(D))

    def test_sql_no_dsn_in_command_and_raw_bytes_retained(self):
        with tempfile.TemporaryDirectory() as td:
            Path(td, 'compose.yaml').write_text('# pure mock compose\n')
            rec = m.AttemptRecorder(td, 'pure')
            rec.persist_handle('source', S)
            rec.persist_handle('def', D)
            reader = m.PsqlFactReader(td, rec)
            def run(argv, **kw):
                self.assertNotIn('SECRET', repr(argv))
                self.assertIn('BEGIN READ ONLY;', argv[-1])
                self.assertEqual(5, argv[-1].count('SELECT'))
                self.assertEqual(argv[:9], ['docker', 'compose', '-f', str(Path(td, 'compose.yaml')), '--project-name', 'openfoundry-platform-p1-q3v9', 'exec', '-T', 'postgres'])
                self.assertIn('connectors', argv)
                self.assertTrue(list(Path(rec.dir).glob('*.cmd')))
                return type('Proc', (), dict(returncode=7, stdout=b'raw-out', stderr=b'raw-err'))()
            with patch.dict(m._SQL_RELEASE_ARMED, armed=True), patch('subprocess.run', side_effect=run):
                result = reader.snapshot('test', D, S)
            self.assertEqual(7, result['rc'])
            record = json.loads(next(Path(rec.dir).glob('*attempt*.json')).read_text())
            for stream in ('stdout', 'stderr'):
                data = Path(record[stream + '_file']).read_bytes()
                self.assertEqual(record[stream + '_sha256'], m._sha256(data))
            self.assertNotIn('SECRET', ''.join(p.read_text() for p in Path(rec.dir).iterdir() if p.is_file()))

    def test_sql_owned_uuid_and_unreleased_tripwires_before_process(self):
        with tempfile.TemporaryDirectory() as td:
            Path(td, 'compose.yaml').write_text('# pure\n')
            rec = m.AttemptRecorder(td, 'pure')
            reader = m.PsqlFactReader(td, rec)
            with patch('subprocess.run', side_effect=AssertionError('IO forbidden')):
                with self.assertRaises(ValueError): reader.snapshot('x', D, S)
                rec.persist_handle('source', S); rec.persist_handle('def', D)
                with patch.dict(m._SQL_RELEASE_ARMED, armed=False):
                    with self.assertRaises(m.SqlFactRefused): reader.snapshot('x', D, S)

    def test_tail_cli_final_summary_hash_and_failed_marker(self):
        with tempfile.TemporaryDirectory() as td:
            spec = {'owner_rows': [], 'run_rows': []}
            decl = Path(td, 'decl.json')
            decl.write_text(json.dumps(dict(schema='p25-tail-roster-v1', ok=True, spec=spec,
                declaration_sha256=m._sha256(json.dumps(spec, sort_keys=True)))))
            summary = Path(td, 'summary.json')
            class FakeEngine:
                def __init__(self, *a): pass
                def run_scenario(self, sid, **kw): return m.ScenarioResult(sid, 'tail')
                def final_quiet(self, *a): return {'ok': True}
                def final_rosters(self, *a): return dict(owner_roster=[OWNER], run_roster=[], problems=[])
            argv = ['smoke', '--phase', 'tail', '--released', '--tail-declaration', str(decl),
                    '--evidence-dir', td, '--result-file', str(summary)]
            with patch.object(m, 'P25Engine', FakeEngine), patch('sys.argv', argv), \
                 patch.dict(os.environ, P25_CONFIRM='yes'), contextlib.redirect_stdout(io.StringIO()):
                rc = m.main()
            self.assertEqual(rc, 1)
            doc = json.loads(summary.read_text())
            self.assertIn('tail-adjudication', doc['failed'])
            bindings = json.loads(Path(doc['tail_bindings_file']).read_text())
            self.assertEqual(bindings['summary_sha256'], m._file_sha256(summary))

if __name__ == '__main__':
    unittest.main()
