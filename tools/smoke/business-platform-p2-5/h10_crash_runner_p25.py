#!/usr/bin/env python3
"""Narrow P25 auxiliary crash runner. Refuses live IO without both release gates."""
import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
ENGINE_SHA = 'de6eb2bcbc6be34b57f6f7ab4da3be535fec9b140c2646bcbfcc9214395259a1'
SOURCE_SHA = 'a1bff07f3760fce7b5661b169ba26a165712c950035d4e4ec9948639d51ffd27'
PROJECT = 'openfoundry-platform-p1-q3v9'
SERVICE = 'p25connector'
FIXTURE_URL = 'http://p25fixture:8090'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, obj):
    Path(path).write_text(json.dumps(obj, indent=2) + '\n', encoding='utf-8')


def load_engine():
    path = HERE / 'smoke_p25.py'
    if sha(path) != ENGINE_SHA:
        raise ValueError('frozen smoke SHA mismatch')
    spec = importlib.util.spec_from_file_location('p25_h10_engine', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class LiveIO:
    def __init__(self, released):
        if not released or os.environ.get('P25_CONFIRM') != 'yes':
            raise ValueError('live IO requires --released and P25_CONFIRM=yes')

    def command(self, argv, timeout):
        return subprocess.run(argv, capture_output=True, timeout=timeout)

    def api(self, engine, bases, recorder):
        return engine.HttpApi(bases, os.environ['OPENFOUNDRY_SMOKE_JWT'], recorder, 'h10-prepare')

    def now(self):
        return time.monotonic()

    def timestamp(self):
        return dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')

    def sleep(self, seconds):
        time.sleep(seconds)


class BudgetAPI:
    def __init__(self, api, io):
        self.api, self.io, self.deadline = api, io, None

    def call(self, *args, **kwargs):
        if self.deadline is not None:
            left = self.deadline - self.io.now()
            if left <= 0:
                raise RuntimeError('held pre/kill budget exhausted')
            kwargs['timeout'] = min(kwargs.get('timeout', 30), left, 1)
        return self.api.call(*args, **kwargs)


class Runner:
    def __init__(self, args, engine, io):
        self.a, self.m, self.io = args, engine, io
        self.root = Path(args.evidence_root).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.rec = engine.AttemptRecorder(str(self.root / 'prepare'), 'h10-prepare')
        self.api = BudgetAPI(io.api(engine, {'connector': args.connector_base,
            'dataset': args.dataset_base, 'fixture': args.fixture_base,
            'proxy_admin': args.proxy_base}, self.rec), io)
        self.engine = engine.P25Engine(lambda *a, **kw: self.api,
            {'fixture_src_url': FIXTURE_URL, 'live': True}, self.rec)
        self.commands, self.sequence = [], 0
        self.report = {'schema': 'p25-h10-runner-v1', 'ok': False,
                       'commands': self.commands, 'handles_file': self.rec.handles_path}

    def command(self, tag, argv, timeout=30):
        self.sequence += 1
        directory = self.root / ('%02d-%s' % (self.sequence, tag))
        directory.mkdir()
        write(directory / 'argv.json', argv)
        (directory / 'cmd.txt').write_text(shlex.join(argv) + '\n')  # before IO
        begin = self.io.timestamp()
        try:
            proc = self.io.command(argv, timeout)
            rc, out, err = proc.returncode, proc.stdout, proc.stderr
        except subprocess.TimeoutExpired as exc:
            rc, out, err = -1, exc.stdout or b'', exc.stderr or b''
            err += b'\nTimeoutExpired'
        except Exception as exc:
            rc, out, err = -1, b'', type(exc).__name__.encode()
        (directory / 'stdout.raw').write_bytes(out)
        (directory / 'stderr.raw').write_bytes(err)
        (directory / 'rc').write_text(str(rc) + '\n')
        record = dict(cmd=shlex.join(argv), rc=rc, artifact=str(directory / 'stdout.raw'),
            artifact_sha256=sha(directory / 'stdout.raw'), stderr_artifact=str(directory / 'stderr.raw'),
            stderr_sha256=sha(directory / 'stderr.raw'), cmd_file=str(directory / 'cmd.txt'),
            cmd_sha256=sha(directory / 'cmd.txt'), argv_file=str(directory / 'argv.json'),
            rc_file=str(directory / 'rc'), rc_sha256=sha(directory / 'rc'),
            started_at=begin, finished_at=self.io.timestamp())
        write(directory / 'record.json', record)
        self.commands.append(record)
        write(self.root / 'result.json', self.report)
        return record, out

    def successful(self, tag, argv, timeout=30):
        record, raw = self.command(tag, argv, timeout)
        if record['rc'] != 0:
            raise RuntimeError(tag + ' command failed (raw rc retained)')
        return record, raw

    def inspect(self, cid, stage):
        # Only necessary identity fields; no Config.Env or secret inspection output.
        template = '[{"Id":{{json .Id}},"Image":{{json .Image}},"Config":{"Labels":{' \
            '"com.docker.compose.project":{{json (index .Config.Labels "com.docker.compose.project")}},' \
            '"com.docker.compose.service":{{json (index .Config.Labels "com.docker.compose.service")}}}},' \
            '"State":{"StartedAt":{{json .State.StartedAt}},"Running":{{json .State.Running}}}}]'
        record, raw = self.successful('inspect-' + stage,
                                      ['docker', 'inspect', '--format', template, cid])
        data = json.loads(raw)
        if not isinstance(data, list) or len(data) != 1:
            raise ValueError('inspect must identify exactly one auxiliary container')
        item = data[0]
        labels = item.get('Config', {}).get('Labels', {})
        if (item.get('Id') != cid or item.get('Image') != self.image
            or labels.get('com.docker.compose.project') != PROJECT
            or labels.get('com.docker.compose.service') != SERVICE
            or item.get('State', {}).get('Running') is not True
            or not self.m._rfc3339(item.get('State', {}).get('StartedAt'))):
            raise ValueError('inspected auxiliary identity/image/labels/running/StartedAt mismatch')
        return record, item

    def input_checks(self):
        if not (Path(self.a.env_dir) / 'compose.yaml').is_file():
            raise ValueError('q3 env dir must contain compose.yaml')
        selected = json.loads(Path(self.a.selected_build).read_text())
        if (selected.get('schema') != 'p25-selected-build-v1' or selected.get('ok') is not True
            or selected.get('all_ok') is not True or selected.get('source_sha256') != SOURCE_SHA):
            raise ValueError('selected accepted build invalid')
        self.image = selected.get('images', {}).get(SERVICE)
        if not isinstance(self.image, str) or not self.image.startswith('sha256:') or not self.m._is_sha256hex(self.image[7:]):
            raise ValueError('selected auxiliary full image ID missing')
        healthy = json.loads(Path(self.a.healthy_summary).read_text())
        if healthy.get('schema') == 'p25-healthy-aggregate-v1':
            spec = importlib.util.spec_from_file_location('p25_h10_aggregate', HERE / 'aggregate_healthy_p25.py')
            aggregate = importlib.util.module_from_spec(spec); spec.loader.exec_module(aggregate)
            verdict = aggregate.verify_aggregate(self.a.healthy_summary)
            if verdict.get('ok') is not True:
                raise ValueError('healthy aggregate source proof rejected: ' + '; '.join(verdict['problems']))
        expected = {s['id'] for s in self.m.SCENARIOS if s['phase'] == 'healthy' and s['id'] != 'h10'}
        rows = healthy.get('scenarios', [])
        if (healthy.get('phase') != 'healthy' or healthy.get('released') is not True
            or healthy.get('failed') != [] or healthy.get('quiet', {}).get('ok') is not True
            or len(rows) != len(expected) or {r.get('id') for r in rows} != expected
            or any(r.get('failed') is not False for r in rows)):
            raise ValueError('requires successful fullhealthy summary, not h1 trial')
        referenced = Path(healthy['attempts']['handles_file']).resolve()
        handles_path = Path(self.a.healthy_handles).resolve() if self.a.healthy_handles else referenced
        if sha(handles_path) != sha(referenced):
            raise ValueError('healthy handles differ from actual summary reference')
        handles = json.loads(handles_path.read_text())['handles']
        for h in handles:
            if h.get('ok') is True and h.get('kind') in ('source', 'def', 'h10-def'):
                if not self.m._is_uuidish(h.get('ref')):
                    raise ValueError('invalid actual healthy handle UUID')
                self.rec.persist_handle(h['kind'], h['ref'], dict(h, imported_from=str(handles_path),
                                                                  imported_sha256=sha(handles_path)))
        self.report['inputs'] = {str(Path(p).resolve()): sha(p) for p in
                                (self.a.selected_build, self.a.healthy_summary, handles_path, HERE / 'smoke_p25.py', Path(__file__).resolve())}
        self.report['accepted_image'] = self.image
        self.healthy_handles = handles
        self.expected_baseline_owners = healthy.get('final_owner_roster', [])
        if healthy.get('final_run_roster', []) != []:
            raise ValueError('healthy baseline has unexpected nonterminal runs')
        self.baseline_origins = healthy.get('retained_owner_origins', [])
        if self.expected_baseline_owners and healthy.get('schema') != 'p25-healthy-aggregate-v1':
            raise ValueError('retained healthy owners require exact aggregate provenance')
        _, raw = self.successful('discover-aux', ['docker', 'ps', '--no-trunc', '--quiet',
            '--filter', 'label=com.docker.compose.project=' + PROJECT,
            '--filter', 'label=com.docker.compose.service=' + SERVICE])
        ids = raw.decode().split()
        if len(ids) != 1 or not self.m._is_sha256hex(ids[0]):
            raise ValueError('need exactly one running full-ID disposable auxiliary container')
        self.cid = ids[0]
        self.before_record, self.before = self.inspect(self.cid, 'before')

    @staticmethod
    def owner_key(row):
        return tuple(row.get(k) for k in ('def_id', 'source_id', 'run_id', 'run_status', 'blocked_reason'))

    def capture_baseline(self):
        self.baseline_defs = sorted({h['ref'] for h in self.rec.handles
                                   if h.get('ok') and h.get('kind') in ('def', 'h10-def')})
        rosters = self.engine.final_rosters(self.api)
        expected = sorted(self.owner_key(o) for o in self.expected_baseline_owners)
        observed = sorted(self.owner_key(o) for o in rosters['owner_roster'])
        if (rosters['problems'] or rosters['run_roster'] or observed != expected
            or len(observed) != len(set(observed))
            or any(o.get('run_status') != 'failed' or o.get('blocked_reason') != 'requires_reconciliation'
                   for o in rosters['owner_roster'])):
            raise ValueError('actual baseline differs from exact preserved failedhealthy owners / unknown run')
        snapshot, ok = self.engine._h10_snapshot(self.api, self.baseline_defs)
        if not ok or set(snapshot) != set(self.baseline_defs):
            raise ValueError('full protected baseline snapshot incomplete')
        self.baseline_snapshot, self.baseline_rosters = snapshot, rosters
        raw_records = sorted(Path(self.rec.dir).glob('h10-prepare-attempt-*.json'))
        facts = {'owner_roster': rosters['owner_roster'], 'run_roster': rosters['run_roster'],
                 'snapshot': snapshot, 'defs': self.baseline_defs,
                 'origins': self.baseline_origins,
                 'raw_records': [{'path': str(p), 'sha256': sha(p)} for p in raw_records]}
        write(self.root / 'baseline.json', facts)
        self.report['baseline_file'] = str(self.root / 'baseline.json')
        self.report['baseline_sha256'] = sha(self.root / 'baseline.json')

    def quiet_def(self):
        sources = {h['ref']: h.get('config', {}) for h in self.healthy_handles
                   if h.get('kind') == 'source' and h.get('ok')}
        for h in self.healthy_handles:
            if h.get('kind') != 'def' or not h.get('ok'):
                continue
            did = h['ref']
            dv = self.m.def_view(self.api, did)
            if not dv:
                continue
            src = sources.get(dv.get('source_id'), {}).get('sync', {}).get('query_params', {}).get('src')
            if src not in self.m.SRC_WINDOWS or src == 'p25h13':
                continue
            cp = self.m.checkpoint_of(self.api, did)
            runs = self.m.runs_list(self.api, did)
            fx = self.m.fixture_snapshot(self.api, src)
            if (cp is not None and not (cp.get('owner') or {}).get('run_id') and runs
                and all(r.get('status') == 'succeeded' and r.get('sync_def_id') == did for r in runs)
                and fx and type(fx.get('active')) is int and fx['active'] == 0
                and type(fx.get('inventory_started')) is int
                and fx['inventory_started'] == fx.get('inventory_finished')):
                return {'def_id': did, 'source_id': dv['source_id'], 'fixture_src': src}
        raise ValueError('no actual quiet succeeded def with owned source/config found')

    def held_state(self):
        h = self.held
        run = self.api.call('connector', 'GET', self.m.route('run_get', id=h['def_id'], run_id=h['run_id']))
        cp = self.api.call('connector', 'GET', self.m.route('checkpoint_view', id=h['def_id']))
        fx = self.api.call('fixture', 'GET', '/admin/src/p25h13')
        r, c, f = run.get('json') or {}, cp.get('json') or {}, fx.get('json') or {}
        return (all(x['status'] == 200 for x in (run, cp, fx))
                and r.get('id') == h['run_id'] and r.get('sync_def_id') == h['def_id']
                and r.get('status') == 'running' and r.get('control_mode') == 'controlled'
                and r.get('publish_entered_at') is None
                and (c.get('owner') or {}).get('run_id') == h['run_id']
                and type(f.get('active')) is int and f['active'] > 0)

    def smoke_argv(self, stage):
        return [sys.executable, '-I', str(HERE / 'smoke_p25.py'), '--phase', 'healthy',
            '--only', 'h10', '--h10-stage', stage, '--released', '--h10-defs',
            self.quiet['def_id'] + ',' + self.held['def_id'], '--h10-held-def', self.held['def_id'],
            '--owned-handles-file', self.rec.handles_path, '--h10-prior', str(self.root / 'prior.json'),
            '--h10-expected-image', self.image, '--connector-base', self.a.connector_base,
            '--dataset-base', self.a.dataset_base, '--fixture-base', self.a.fixture_base,
            '--proxy-base', self.a.proxy_base, '--fixture-src-url', FIXTURE_URL,
            '--evidence-dir', str(self.root / stage), '--result-file', str(self.root / (stage + '-result.json'))] \
            + (['--h10-restart-record', str(self.root / 'crash.json')] if stage == 'post' else [])

    def run(self):
        try:
            self.input_checks()
            self.capture_baseline()
            self.quiet = self.quiet_def()
            if not self.m.proxy_set_mode(self.api, 'passthrough'):
                raise RuntimeError('proxy passthrough failed')
            ds = self.engine.create_dataset(self.api, 'p25-h10-held', scenario='h10')
            if not ds or not self.m._is_uuidish(ds.get('id')):
                raise RuntimeError('held dataset create failed')
            config = self.m.incremental_source_config(FIXTURE_URL, 'p25h13',
                        {'sync': {'limits': {'request_timeout_ms': 30000}}})
            src = self.engine.create_source(self.api, 'p25-h10-held', config, scenario='h10')
            if not src or not self.m._is_uuidish(src.get('id')):
                raise RuntimeError('held source create failed')
            sync, _ = self.engine.make_sync_def(self.api, 'p25-h10-held', src['id'], ds['id'], scenario='h10')
            if not sync or not self.m._is_uuidish(sync.get('id')):
                raise RuntimeError('held def create failed')
            self.held = {'def_id': sync['id'], 'source_id': src['id']}
            self.report['quiet'], self.report['held'] = self.quiet, self.held
            pre_argv, post_argv = self.smoke_argv('pre'), self.smoke_argv('post')
            if not self.m.fixture_set_knob(self.api, 'p25h13', {'slow_terminal_ms': 15000}):
                raise RuntimeError('held fixture knob failed')
            self.started = self.io.now()
            self.api.deadline = self.started + 5
            self.report['held_trigger_at'] = self.io.timestamp()
            run, _ = self.engine.trigger_run(self.api, sync['id'], async_mode=True, wait=False, scenario='h10')
            if not run or not self.m._is_uuidish(run.get('id')):
                raise RuntimeError('held async admission failed')
            self.held['run_id'] = run['id']
            while self.io.now() - self.started < 3 and not self.held_state():
                self.io.sleep(.05)
            if self.io.now() - self.started >= 3:
                raise RuntimeError('held running/active proof missed 3s pre budget')
            self.successful('pre', pre_argv, timeout=max(.001, self.started + 5 - self.io.now()))
            pre_result = json.loads((self.root / 'pre-result.json').read_text())
            if pre_result.get('failed') != []:
                raise RuntimeError('h10 pre verdict failed')
            if not self.held_state() or self.io.now() >= self.started + 5:
                raise RuntimeError('held proof lost or 5s kill budget missed; no kill')
            prior = json.loads((self.root / 'prior.json').read_text())
            if prior.get('held_def') != self.held['def_id']:
                raise RuntimeError('prior held identity mismatch')
            kill_at = self.io.timestamp()
            kill, _ = self.successful('kill', ['docker', 'kill', '-s', 'KILL', self.cid],
                                      timeout=max(.001, self.started + 5 - self.io.now()))
            self.api.deadline = None
            self.report['kill_elapsed_seconds'] = self.io.now() - self.started
            start, _ = self.successful('start', ['docker', 'start', self.cid])
            after_record, after = self.inspect(self.cid, 'after')
            deadline = self.io.now() + 45
            health = None
            while self.io.now() < deadline:
                script = 'import sys,urllib.request; r=urllib.request.urlopen(sys.argv[1],timeout=2); assert r.status==200; sys.stdout.buffer.write(r.read())'
                health, _ = self.command('health', [sys.executable, '-I', '-c', script,
                                          self.a.connector_base.rstrip('/') + '/healthz'], timeout=3)
                if health['rc'] == 0:
                    break
                self.io.sleep(.25)
            if not health or health['rc'] != 0:
                raise RuntimeError('auxiliary health failed')
            crash = dict(schema='p25-crash-record-v1', project=PROJECT, service=SERVICE,
                container=self.cid, container_id=self.cid, image=self.image,
                image_before=self.before['Image'], image_after=after['Image'],
                method='sigkill+start-same-image', kill=kill, start=start, health=health,
                inspect_before=self.before_record, inspect_after=after_record,
                started_at_before=self.before['State']['StartedAt'], started_at_after=after['State']['StartedAt'],
                timestamps={'pre_at': prior['taken_at'], 'kill_at': kill_at,
                            'started_after_at': after['State']['StartedAt'], 'post_at': self.io.timestamp()},
                held=dict(self.held), pre_snapshot_sha256=sha(self.root / 'prior.json'), performed_by='runtime-h10-runner')
            write(self.root / 'crash.json', crash)
            problems = self.m._validate_crash_record(crash, prior['defs'], self.held['def_id'],
                            prior['snapshot'], str(self.root / 'prior.json'), self.image)
            if problems:
                raise ValueError('crash record: ' + '; '.join(problems))
            self.successful('post', post_argv, timeout=90)
            result = json.loads((self.root / 'post-result.json').read_text())
            owners, runs = result.get('final_owner_roster', []), result.get('final_run_roster', [])
            key = lambda row: (row.get('def_id'), row.get('source_id'), row.get('run_id'))
            expected = key(self.held)
            baseline_identity = [(o['def_id'], o['source_id'], o['run_id']) for o in self.baseline_rosters['owner_roster']]
            if (result.get('failed') != [] or result.get('quiet', {}).get('ok') is not True
                or sorted(key(o) for o in owners) != sorted(baseline_identity + [expected])
                or [key(r) for r in runs] != [expected]
                or runs[0].get('sync_def_id') != self.held['def_id']
                or runs[0].get('control_mode') != 'controlled' or runs[0].get('publish_entered_at') is not None):
                raise ValueError('h10 post failed or actual retained rosters differ')
            after_snapshot, after_ok = self.engine._h10_snapshot(self.api, self.baseline_defs)
            after_rosters = self.engine.final_rosters(self.api)
            existing = [o for o in after_rosters['owner_roster'] if key(o) != expected]
            if (not after_ok or after_snapshot != self.baseline_snapshot or after_rosters['problems']
                or sorted(self.owner_key(o) for o in existing) != sorted(self.owner_key(o) for o in self.baseline_rosters['owner_roster'])
                or sorted(key(o) for o in after_rosters['owner_roster']) != sorted(baseline_identity + [expected])
                or [key(r) for r in after_rosters['run_roster']] != [expected]):
                raise ValueError('existing protected baseline changed or post crash has unknown extra owner/run')
            srcs = sorted({h.get('config', {}).get('sync', {}).get('query_params', {}).get('src')
                for h in self.rec.handles if h.get('kind') == 'source' and h.get('ok')})
            if any(src not in self.m.SRC_WINDOWS for src in srcs):
                raise ValueError('invalid owned fixture source in post quiet scope')
            quiet = self.engine.final_quiet(self.api, srcs)
            if quiet.get('ok') is not True:
                raise ValueError('full owned source post quiet failed')
            after_facts = {'snapshot': after_snapshot, 'rosters': after_rosters, 'quiet': quiet,
                'raw_records': [{'path': str(p), 'sha256': sha(p)} for p in sorted(Path(self.rec.dir).glob('h10-prepare-attempt-*.json'))]}
            write(self.root / 'baseline-after.json', after_facts)
            self.report['baseline_after_file'] = str(self.root / 'baseline-after.json')
            self.report['baseline_after_sha256'] = sha(self.root / 'baseline-after.json')
            known, retained_rows = {'h10-held': dict(self.held)}, []
            for owner in self.baseline_rosters['owner_roster']:
                tag = 'failedhealthy-h13-' + owner['def_id']
                origin = next((o for o in self.baseline_origins if key(o) == key(owner)), None)
                if not origin or origin.get('origin') != 'failedhealthy':
                    raise ValueError('baseline owner actual failedhealthy provenance missing')
                known[tag] = {k: owner[k] for k in ('def_id','source_id','run_id')}
                retained_rows.append({'def_tag': tag, 'run_ref': 'terminal-failed',
                    'blocked_reason': owner['blocked_reason'], 'origin': 'failedhealthy',
                    'source_summary': origin['source_summary'], 'source_summary_sha256': origin['source_summary_sha256']})
            binding = {'known_bindings': known, 'retained_owner_rows': retained_rows,
                       'semantics': 'p25-crash-held-prepublish', 'post_summary_sha256': sha(self.root / 'post-result.json'),
                       'baseline_sha256': self.report['baseline_sha256'],
                       'baseline_after_sha256': self.report['baseline_after_sha256']}
            write(self.root / 'tail-known-binding.json', binding)
            self.report.update(ok=True, tail_known_binding_file=str(self.root / 'tail-known-binding.json'),
                merged_handles_file=self.rec.handles_path, crash_record=str(self.root / 'crash.json'),
                post_summary=str(self.root / 'post-result.json'))
            return 0
        except Exception as exc:
            self.report['error'] = '%s: %s' % (type(exc).__name__, exc)
            return 1
        finally:
            # Keep all partial handles/actions even when pre/kill/start/post fails.
            self.api.deadline = None
            self.report['handles_sha256'] = sha(self.rec.handles_path)
            write(self.root / 'result.json', self.report)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--released', action='store_true')
    for key in ('env-dir', 'selected-build', 'healthy-summary', 'evidence-root'):
        p.add_argument('--' + key, required=True)
    p.add_argument('--healthy-handles', default='')
    for name, port in (('connector', 17094), ('dataset', 17078), ('fixture', 17092), ('proxy', 17093)):
        p.add_argument('--' + name + '-base', default='http://127.0.0.1:%d' % port)
    p.add_argument('--fixture-src-url', default=FIXTURE_URL)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    if not args.released or os.environ.get('P25_CONFIRM') != 'yes':
        print('REFUSED: requires --released and P25_CONFIRM=yes', file=sys.stderr)
        return 3
    if args.fixture_src_url != FIXTURE_URL:
        print('REFUSED: fixture source URL is fixed to p25fixture', file=sys.stderr)
        return 3
    for name in ('OPENFOUNDRY_SMOKE_JWT', 'OPENFOUNDRY_SMOKE_JWT_ORG2', 'OPENFOUNDRY_SMOKE_ARM_JWT'):
        if not os.environ.get(name):
            print('REFUSED: missing required JWT environment', file=sys.stderr)
            return 3
    try:
        runner = Runner(args, load_engine(), LiveIO(args.released))
        rc = runner.run()
        print(json.dumps({'rc': rc, 'result_file': str(runner.root / 'result.json')}))
        return rc
    except Exception as exc:
        print('h10 runner refused: ' + type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
