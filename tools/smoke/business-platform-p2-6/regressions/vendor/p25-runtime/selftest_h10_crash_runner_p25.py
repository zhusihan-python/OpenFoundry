#!/usr/bin/env python3
"""Pure injected IO; socket, urlopen and subprocess are tripwired."""
import contextlib
import datetime as dt
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.request
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('h10_runner', HERE / 'h10_crash_runner_p25.py')
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
m = r.load_engine()
IMAGE = 'sha256:' + 'a' * 64
CID = 'b' * 64


class FakeIO:
    def __init__(self, root, fault=None):
        self.clock, self.base, self.fault = 0., time.time() + 10, fault
        self.calls = []; self.started = '2026-11-13T00:00:00Z'
        self.fixture, self.proxy = m.FakeIncrementalFixture(), m.FakeProxyDataset()
        outer = self
        class Connector(m.FakeConnector):
            def trigger(self, did, async_mode=False):
                status, row = super().trigger(did, async_mode)
                if async_mode and status == 202:
                    live = self.runs[row['id']]
                    live['status'] = row['status'] = 'running'
                    counters = outer.fixture.counters['p25h13']
                    counters['inventory_started'] += 1; counters['active'] += 1
                    if outer.fault == 'never_active': counters['active'] = 0
                return status, row
            def poll_run(self, rid):
                run = self.runs.get(rid)
                if run and run.get('_pending'): return run
                return super().poll_run(rid)
        self.connector = Connector(self.fixture, self.proxy)
        self.root = root

    def now(self): return self.clock
    def timestamp(self):
        return dt.datetime.fromtimestamp(self.base + self.clock, dt.timezone.utc).isoformat().replace('+00:00', 'Z')
    def sleep(self, seconds): self.clock += seconds
    def api(self, engine, bases, recorder):
        return m.FakeApi(self.connector, self.fixture, self.proxy, recorder, 'prepare')

    def command(self, argv, timeout):
        self.calls.append(argv)
        self.clock += .01
        if argv[:2] == ['docker', 'ps']:
            raw = ((CID + '\n' + 'c' * 64) if self.fault == 'duplicate_container' else CID).encode()
        elif argv[:2] == ['docker', 'inspect']:
            labels = {'com.docker.compose.project': r.PROJECT, 'com.docker.compose.service': r.SERVICE}
            if self.fault == 'bad_labels': labels['com.docker.compose.service'] = 'connector'
            raw = json.dumps([dict(Id=CID, Image=IMAGE if self.fault != 'wrong_image' else 'sha256:' + 'c' * 64,
                Config={'Labels': labels}, State={'StartedAt': self.started, 'Running': True})]).encode()
        elif len(argv) > 2 and argv[2].endswith('smoke_p25.py'):
            stage = argv[argv.index('--h10-stage') + 1]
            if stage == 'pre' and self.fault == 'pre_fail': return subprocess.CompletedProcess(argv, 1, b'pre-failed', b'')
            if stage == 'pre' and self.fault == 'slow_pre': self.clock += 6
            with patch.object(m, 'HttpApi', side_effect=lambda bases, jwt, rec, sid: m.FakeApi(
                self.connector, self.fixture, self.proxy, rec, sid)), \
                patch.object(m.time, 'sleep', side_effect=self.sleep), patch('sys.argv', argv[2:]), \
                patch.dict(os.environ, P25_CONFIRM='yes', OPENFOUNDRY_SMOKE_JWT='pure'), \
                contextlib.redirect_stdout(io.StringIO()) as out:
                rc = m.main()
            if stage == 'post' and self.fault == 'post_fail': rc = 1
            if stage == 'pre' and self.fault == 'lost_afterpre': self.fixture.counters['p25h13']['active'] = 0
            return subprocess.CompletedProcess(argv, rc, out.getvalue().encode(), b'')
        elif argv[:4] == ['docker', 'kill', '-s', 'KILL']:
            if self.fault == 'kill_fail': return subprocess.CompletedProcess(argv, 1, b'', b'kill failed')
            assert argv[-1] == CID
            counters = self.fixture.counters['p25h13']
            counters['inventory_finished'] += 1; counters['active'] = 0
            raw = CID.encode()
        elif argv[:2] == ['docker', 'start']:
            assert argv[-1] == CID
            if self.fault == 'start_fail': return subprocess.CompletedProcess(argv, 1, b'', b'start failed')
            self.started = self.timestamp(); raw = CID.encode()
            if self.fault == 'takeover':
                for run in self.connector.runs.values():
                    if run.get('_pending'): run['generation'] += 1
            if self.fault == 'baseline_takeover':
                next(run for run in self.connector.runs.values() if run['status']=='failed')['generation'] += 1
        elif '-c' in argv and '/healthz' in argv[-1]:
            if self.fault == 'health_fail':
                self.clock += 3
                return subprocess.CompletedProcess(argv, 1, b'', b'unhealthy')
            raw = b'{"ok":true}'
        else:
            raise AssertionError('unexpected IO argv ' + repr(argv))
        return subprocess.CompletedProcess(argv, 0, raw, b'')


def inputs(root, fio):
    env = root / 'q3'; env.mkdir(); (env / 'compose.yaml').write_text('# mock; not executed\n')
    selected = root / 'selected.json'
    r.write(selected, dict(schema='p25-selected-build-v1', ok=True, all_ok=True,
                          source_sha256=r.SOURCE_SHA, images={r.SERVICE: IMAGE}))
    rec = m.AttemptRecorder(str(root / 'healthy'), 'healthy')
    source, did, dsid = [m._new_uuid() for _ in range(3)]
    config = m.incremental_source_config(r.FIXTURE_URL, 'p25h1')
    fio.connector.create_source(source, config)
    dsid = fio.proxy.create_dataset("quiet")["id"]
    fio.connector.create_def(did, source, dsid)
    fio.connector.trigger(did)
    rec.persist_handle('source', source, {'config': config})
    rec.persist_handle('def', did)
    summary = root / 'healthy.json'
    r.write(summary, dict(phase='healthy', released=True, failed=[], quiet={'ok': True},
        scenarios=[dict(id=s['id'], failed=False) for s in m.SCENARIOS if s['phase'] == 'healthy' and s['id'] != 'h10'],
        attempts={'handles_file': rec.handles_path}))
    return r.parser().parse_args(['--env-dir', str(env), '--selected-build', str(selected),
        '--healthy-summary', str(summary), '--evidence-root', str(root / 'attempt'), '--released'])


class H10(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack(); self.addCleanup(self.stack.close)
        for target in ('socket.socket', 'subprocess.run', 'urllib.request.urlopen'):
            self.stack.enter_context(patch(target, side_effect=AssertionError('live IO forbidden')))
        self.stack.enter_context(patch.dict(os.environ, P25_CONFIRM='yes'))

    def run_case(self, fault=None):
        td = tempfile.TemporaryDirectory(); self.addCleanup(td.cleanup)
        root = Path(td.name)
        fio = FakeIO(root, fault); args = inputs(root, fio)
        runner = r.Runner(args, m, fio)
        code = runner.run()
        return code, runner, fio

    def test_full_injected_flow_uses_real_engine_pre_post(self):
        code, runner, fio = self.run_case()
        self.assertEqual(code, 0, runner.report)
        self.assertEqual([a for a in fio.calls if a[:2] == ['docker', 'kill']], [['docker', 'kill', '-s', 'KILL', CID]])
        self.assertEqual([a for a in fio.calls if a[:2] == ['docker', 'start']], [['docker', 'start', CID]])
        crash = json.loads((runner.root / 'crash.json').read_text())
        self.assertEqual(crash['held'], runner.held)
        self.assertEqual(crash['pre_snapshot_sha256'], r.sha(runner.root / 'prior.json'))
        binding = json.loads((runner.root / 'tail-known-binding.json').read_text())
        self.assertEqual(binding['known_bindings']['h10-held'], runner.held)
        handles = json.loads(Path(runner.report['merged_handles_file']).read_text())['handles']
        self.assertTrue(any(h['kind'] == 'run' and h['ref'] == runner.held['run_id'] for h in handles))
        self.assertLess(runner.report['kill_elapsed_seconds'], 5)
        for action in crash['kill'], crash['start'], crash['health']:
            self.assertEqual(action['artifact_sha256'], r.sha(action['artifact']))
            self.assertEqual(action['stderr_sha256'], r.sha(action['stderr_artifact']))

    def test_identity_pre_and_budget_fail_before_kill(self):
        for fault in ('bad_labels', 'wrong_image', 'duplicate_container', 'pre_fail', 'slow_pre', 'never_active', 'lost_afterpre'):
            with self.subTest(fault=fault):
                code, runner, fio = self.run_case(fault)
                self.assertEqual(code, 1)
                self.assertFalse(any(a[:2] == ['docker', 'kill'] for a in fio.calls))
                self.assertTrue((runner.root / 'result.json').is_file())
                self.assertTrue(Path(runner.rec.handles_path).is_file())

    def test_postcrash_failures_preserve_actions_and_handles_without_retrigger(self):
        for fault in ('kill_fail', 'start_fail', 'health_fail', 'post_fail', 'takeover'):
            with self.subTest(fault=fault):
                code, runner, fio = self.run_case(fault)
                self.assertEqual(code, 1)
                self.assertFalse((runner.root / 'tail-known-binding.json').exists())
                runs = [h for h in runner.rec.handles if h['kind'] == 'run']
                self.assertEqual(len(runs), 1)
                self.assertEqual(len([a for a in fio.calls if a[:2] == ['docker', 'kill']]), 1)
                self.assertNotIn('SECRET', (runner.root / 'result.json').read_text())

    def test_rejects_h1_trial_or_unaccepted_build_before_docker_or_http(self):
        for field in ('healthy', 'selected'):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as td:
                root = Path(td); fio = FakeIO(root); args = inputs(root, fio)
                path = Path(args.healthy_summary if field == 'healthy' else args.selected_build)
                data = json.loads(path.read_text())
                if field == 'healthy': data['scenarios'] = data['scenarios'][:1]
                else: data['ok'] = False
                r.write(path, data)
                runner = r.Runner(args, m, fio)
                self.assertEqual(runner.run(), 1)
                self.assertEqual(fio.calls, [])

    def test_preserved_two_failedhealthy_owners_exactly_protected_not_ignored(self):
        spec=importlib.util.spec_from_file_location('aggregate_fixture', HERE/'selftest_aggregate_healthy_p25.py')
        fixture_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture_module)
        for fault in (None,'baseline_takeover'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as td:
                root=Path(td); ar=root/'aggregate';ar.mkdir();arguments=fixture_module.fixture(ar)
                doc,raw_handles=fixture_module.a.build(arguments)
                Path(arguments['handles_output']).write_bytes(raw_handles)
                healthy=ar/'aggregate.json';fixture_module.dump(healthy,doc)
                fio=FakeIO(root,fault)
                handles=json.loads(raw_handles)['handles']
                for h in handles:
                    if h.get('kind')=='source' and h.get('ok'):fio.connector.create_source(h['ref'],h['config'])
                source=doc['final_owner_roster'][0]['source_id']
                for h in handles:
                    if h.get('kind')=='def' and h.get('ok'):
                        ds=fio.proxy.create_dataset('baseline-target')['id'];fio.connector.create_def(h['ref'],source,ds)
                for owner in doc['final_owner_roster']:
                    rid,did=owner['run_id'],owner['def_id']
                    fio.connector.runs[rid]={'id':rid,'sync_def_id':did,'status':'failed','generation':1,'attempt':1,
                        'control_mode':'controlled','publish_entered_at':'2026-11-14T00:00:01Z','started_at':'2026-11-14T00:00:00Z'}
                    fio.connector.defs[did]['owner']=rid
                quiet=next(did for did,d in fio.connector.defs.items() if not d['owner']);fio.connector.trigger(quiet)
                env=root/'q3';env.mkdir();(env/'compose.yaml').write_text('# pure')
                selected=root/'selected.json';r.write(selected,dict(schema='p25-selected-build-v1',ok=True,all_ok=True,
                    source_sha256=r.SOURCE_SHA,images={r.SERVICE:IMAGE}))
                args=r.parser().parse_args(['--env-dir',str(env),'--selected-build',str(selected),
                    '--healthy-summary',str(healthy),'--evidence-root',str(root/'attempt'),'--released'])
                runner=r.Runner(args,m,fio);code=runner.run()
                self.assertEqual(code,1 if fault else 0,runner.report)
                self.assertEqual(len(runner.baseline_rosters['owner_roster']),2)
                self.assertEqual(runner.baseline_rosters['run_roster'],[])
                if not fault:
                    binding=json.loads((runner.root/'tail-known-binding.json').read_text())
                    self.assertEqual(len(binding['known_bindings']),3)
                    self.assertEqual(len(binding['retained_owner_rows']),2)
                    self.assertTrue(all(row['origin']=='failedhealthy' for row in binding['retained_owner_rows']))
                    after=json.loads((runner.root/'baseline-after.json').read_text())
                    self.assertEqual(after['snapshot'],runner.baseline_snapshot)
                else:
                    self.assertFalse((runner.root/'tail-known-binding.json').exists())
                    self.assertIn('baseline changed',runner.report['error'])

    def test_both_release_gates_trip_before_io(self):
        argv = ['--env-dir', '/nonexistent', '--selected-build', '/none', '--healthy-summary', '/none', '--evidence-root', '/none']
        for release, env in (([], 'yes'), (['--released'], 'no')):
            with patch.dict(os.environ, P25_CONFIRM=env), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(r.main(argv + release), 3)
        with self.assertRaises(ValueError): r.LiveIO(False)


if __name__ == '__main__': unittest.main()
