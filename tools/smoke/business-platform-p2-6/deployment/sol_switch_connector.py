#!/usr/bin/env python3
"""One authorized Connector image switch, with append-only original evidence."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import urllib.request

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[4]
P26 = ROOT / 'tools/smoke/business-platform-p2-6'
sys.path.insert(0, str(P26))
from common import p26_gates as G

SOURCE = 'b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c'
OLD = 'sha256:6b556702b34ba399f4ee434963c798faf86b12af294657ec3b144309c0097592'
NEW = 'sha256:f23f69161c6f86f7402a18dbd14e24fdcad581ecbd9968e7b964ef1baf11df34'
PROJECT = 'openfoundry-platform-p1-q3v9'
RELEASE = Path('/private/tmp/openfoundry-p26-release.json')
ACCEPT = Path('/Users/allen/.codex/visualizations/2026/11/13/01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs/2026-11-15-p26/Dataset-switch-and-mid-accepted.json')


def main():
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = P26 / 'results/w6/deployment' / ('sol-connector-' + stamp)
    out.mkdir(parents=True, exist_ok=False)
    def save(name, obj):
        (out / name).write_text(json.dumps(obj, indent=2) + '\n')
    def run(name, argv):
        save(name + '.cmd-before.json', argv)
        p = subprocess.run(argv, cwd=ROOT, capture_output=True, timeout=120)
        (out / (name + '.stdout')).write_bytes(p.stdout)
        (out / (name + '.stderr')).write_bytes(p.stderr)
        (out / (name + '.raw')).write_bytes(p.stdout + p.stderr)
        (out / (name + '.rc')).write_text(str(p.returncode) + '\n')
        assert p.returncode == 0, name + ' original rc=' + str(p.returncode)
        return p.stdout.decode()
    def inspect(name, svc):
        fmt = '{"id":{{json .Id}},"image":{{json .Image}},"started":{{json .State.StartedAt}},"running":{{json .State.Running}},"mounts":{{json .Mounts}},"net":{{json .NetworkSettings.Networks}},"ports":{{json .NetworkSettings.Ports}}}'
        return json.loads(run(name, ['docker', 'inspect', PROJECT + '-' + svc + '-1', '--format', fmt]))
    try:
        release = G.load_release(str(RELEASE))
        assert release and 'switch-connector' in release['released_for']
        assert release['frozen_source_sha256'] == SOURCE
        expected = ['docker', 'compose', '--env-file', '/private/tmp/' + PROJECT + '/.env', '-f', '/private/tmp/' + PROJECT + '/compose.yaml', '-f', 'tools/smoke/business-platform-p2-6/deployment/override-imageonly-p26.yaml', '--project-name', PROJECT, 'up', '-d', '--no-deps', '--no-build', '--pull=never', 'connector']
        assert release['switch_connector_argv'] == expected
        accepted = json.loads(ACCEPT.read_text())
        assert accepted['root_accepted'] and accepted['source_S'] == SOURCE
        assert json.loads((Path(accepted['switch']) / 'RESULT.json').read_text())['ok']
        inner = Path(accepted['mid'])
        verdict = json.loads((inner / 'RESULT.json').read_text())
        assert verdict['ok'] and verdict['current']['steps_recorded'] == 131
        rcs = list(inner.rglob('*.rc'))
        assert len(rcs) == 131 and len(list(inner.glob('*.gate.rc'))) == 59
        assert all(p.read_text().strip() == '0' for p in rcs)
        assert G.candidate_hash(ROOT) == SOURCE
        save('preconditions.json', {'source': SOURCE, 'oldpre_original_rcs': len(rcs), 'release_id': release['release_id'], 'tool_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'preflight_sha256': G.sha256_file(P26 / 'preflight/p26_current_preflight.py'), 'override_sha256': G.sha256_file(P26 / 'deployment/override-imageonly-p26.yaml')})
        before = {s: inspect('before-' + s, s) for s in ['dataset', 'connector', 'pipeline', 'postgres', 'fixture']}
        assert before['connector']['image'] == OLD
        assert before['fixture']['started'] == '2026-11-13T18:29:14.176785055Z'
        assert all(v['running'] for v in before.values())
        mounts = before['connector']['mounts']
        assert before['dataset']['image'] == accepted['dataset_image']
        assert before['dataset']['id'] == accepted['dataset_cid']
        assert before['dataset']['started'] == accepted['dataset_started_at']
        assert set(before['connector']['net']) == {PROJECT + '_p1net'}
        assert run('candidate-image', ['docker', 'image', 'inspect', NEW, '--format', '{{.Id}}']).strip() == NEW
        assert G.load_release(str(RELEASE)) == release
        run('switch-connector', expected)
        after = {s: inspect('after-' + s, s) for s in before}
        assert after['connector']['image'] == NEW and after['connector']['running']
        assert after['connector']['id'] != before['connector']['id']
        assert after['connector']['started'] != before['connector']['started']
        assert after['connector']['mounts'] == mounts
        assert set(after['connector']['net']) == set(before['connector']['net'])
        assert after['connector']['ports'] == before['connector']['ports']
        assert all(after[s] == before[s] for s in before if s != 'connector')
        save('health.cmd-before.json', {'method': 'GET', 'url': 'http://127.0.0.1:17088/healthz'})
        with urllib.request.urlopen('http://127.0.0.1:17088/healthz', timeout=15) as response:
            body = response.read(1048576)
            (out / 'health.raw').write_bytes(body)
            save('health.meta.json', {'status': response.status})
            assert response.status == 200
        (out / 'health.rc').write_text('0\n')
        assert G.candidate_hash(ROOT) == SOURCE
        save('RESULT.json', {'ok': True, 'connector_image': NEW, 'other_four_unchanged': True, 'source': SOURCE})
        print(out)
        return 0
    except Exception as exc:
        save('RESULT.json', {'ok': False, 'error': type(exc).__name__ + ': ' + str(exc), 'rollback_executed': False})
        print(out)
        raise


if __name__ == '__main__':
    sys.exit(main())
