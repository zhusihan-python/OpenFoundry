#!/usr/bin/env python3
"""Execute reviewed NEW-four argv only; no removal/restart/rollback paths."""
import datetime
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib import request

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
sys.path.insert(0, str(P26 / 'deployment'))
from common import p26_gates as G
import p26_deploy_prep as D


def plan():
    p = D.http_plan()
    p['private_env_phase'] = 'http-private-env'
    p['private_env_argv'] = ['python3', '-I', '-B', str(P26 / 'deployment/sol_prepare_private_env.py')]
    return p


def execute():
    p = plan()
    release = G.load_release()
    assert release and all(s['phase'] in release['released_for'] for s in p['serves']), 'all three new4 phases required'
    assert G.candidate_hash(P26.parents[2]) == D.S
    out = P26 / 'results/w6/deployment' / ('sol-new4-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    out.mkdir(parents=True, exist_ok=False)
    def save(name, value):
        (out / name).write_text(json.dumps(value, indent=2) + '\n')
    def run(name, argv, accepted=(0,)):
        save(name + '.cmd-before.json', argv)
        r = subprocess.run(argv, capture_output=True, timeout=120)
        (out / (name + '.stdout')).write_bytes(r.stdout)
        (out / (name + '.stderr')).write_bytes(r.stderr)
        (out / (name + '.raw')).write_bytes(r.stdout + r.stderr)
        (out / (name + '.rc')).write_text(str(r.returncode) + '\n')
        assert r.returncode in accepted, name + ' original rc=' + str(r.returncode)
        return r.stdout.decode()
    ledger = []
    try:
        save('plan.json', p)
        names = run('collision-names', ['docker', 'ps', '-a', '--format', '{{.Names}}']).splitlines()
        assert not set(names).intersection(D.ROLES), 'NEW name collision; preserve'
        ports = run('collision-ports', ['lsof', '-nP', '-iTCP:17095-17098', '-sTCP:LISTEN'], (0, 1))
        assert not ports.strip(), 'NEW port collision; preserve'
        for env in [D.ADMIN_ENV, D.AUX_ENV, D.SMOKE_ENV]:
            path = Path(env)
            assert path.is_file() and not path.is_symlink() and path.stat().st_mode & 0o777 == 0o600
        for image in [D.PY_FULL, D.CAND['connector']['image_full']]:
            assert run('image-' + image[7:19], ['docker', 'image', 'inspect', image, '--format', '{{.Id}}']).strip() == image
        for step in p['serves']:
            vectors = step['argv'] if isinstance(step['argv'][0], list) else [step['argv']]
            for argv in vectors:
                assert G.load_release() == release, 'Root release changed; preserve created resources'
                name = argv[argv.index('--name') + 1]
                cid = run('create-' + name, argv).strip()
                assert re.fullmatch('[0-9a-f]{64}', cid), 'Docker did not return full CID'
                # Persist creation immediately, before subsequent inspection.
                ledger.append({'name': name, 'cid': cid, 'phase': step['phase']})
                save('created-ledger.json', ledger)
                fmt = '{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"labels":{{json .Config.Labels}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"networks":{{json .NetworkSettings.Networks}},"ports":{{json .NetworkSettings.Ports}},"mounts":{{json .Mounts}}}'
                facts = json.loads(run('inspect-' + name, ['docker', 'inspect', cid, '--format', fmt]))
                expected_image = next(v for v in argv if v.startswith('sha256:'))
                assert facts['cid'] == cid and facts['name'] == '/' + name and facts['image'] == expected_image and facts['running']
                assert set(facts['networks']) == {D.Q3_NET}
                for key, val in {'owner': 'p26-http', 'stage': 'p2-6', 'role': D.ROLES[name]}.items():
                    assert facts['labels']['of-p26-' + key] == val
                ledger[-1].update(facts)
                ledger[-1]['name'] = name
                save('created-ledger.json', ledger)
        for name, port in [('p26fixture',17095), ('p26proxy',17096), ('p26connectora',17097), ('p26connectorb',17098)]:
            url = 'http://127.0.0.1:' + str(port) + '/healthz'
            # Readiness-only retries, each recorded independently.
            for attempt in range(1, 11):
                sid = 'health-' + name + '-' + str(attempt)
                save(sid + '.cmd-before.json', {'method': 'GET', 'url': url})
                try:
                    with request.urlopen(url, timeout=5) as response:
                        (out / (sid + '.raw')).write_bytes(response.read(1048576))
                        save(sid + '.meta.json', {'status': response.status})
                        assert response.status == 200
                    (out / (sid + '.rc')).write_text('0\n')
                    break
                except Exception as exc:
                    (out / (sid + '.raw')).write_text(type(exc).__name__ + '\n')
                    (out / (sid + '.rc')).write_text('1\n')
                    if attempt == 10:
                        raise
                    time.sleep(1)
        aux = {inst: {'cid': f['cid'], 'name': f['name'], 'image': f['image'],
                      'labels': {k: f['labels']['of-p26-' + k] for k in ['owner', 'stage', 'role']}}
               for inst, f in zip(['a', 'b'], [x for x in ledger if x['name'] in ['p26connectora', 'p26connectorb']])}
        save('w7_aux.json', aux)
        save('RESULT.json', {'ok': True, 'created': len(ledger), 'source': D.S})
        print(out)
    except Exception as exc:
        save('RESULT.json', {'ok': False, 'error': type(exc).__name__ + ': ' + str(exc), 'created': len(ledger), 'cleanup_executed': False})
        print(out)
        raise


if __name__ == '__main__':
    if sys.argv[1:] == ['plan']:
        print(json.dumps(plan(), indent=2))
    elif sys.argv[1:] == ['execute']:
        execute()
    else:
        raise SystemExit('usage: sol_start_new4.py plan|execute')
