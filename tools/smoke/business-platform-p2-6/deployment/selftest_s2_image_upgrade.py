#!/usr/bin/env python3
"""Single-target upgrade boundary controls with synthetic identities only."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
from unittest.mock import patch

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('upgrade_pure', HERE / 's2_image_upgrade.py')
U = importlib.util.module_from_spec(spec)
spec.loader.exec_module(U)


def main(output):
    output.mkdir(parents=True, exist_ok=False)
    before = {'cid': 'a' * 64, 'name': '/' + U.name_for('dataset'),
              'image': 'sha256:' + 'b' * 64, 'config_image': 'sha256:' + 'b' * 64,
              'started_at': '2026-11-15T06:45:25.373428845Z', 'running': True,
              'restart_count': 0, 'labels': {'com.docker.compose.project': U.G.Q3_PROJECT,
                                            'com.docker.compose.service': 'dataset'},
              'networks': {U.NET: {'NetworkID': 'network-pin', 'IPAddress': '172.0.0.2'}},
              'ports': {'50078/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '17078'}]},
              'mounts': [{'Name': 'q3_dataset_files', 'Destination': '/home/nonroot'}]}
    after = copy.deepcopy(before)
    after.update(cid='c' * 64, image='sha256:' + 'd' * 64, config_image='sha256:' + 'd' * 64,
                 started_at='2026-11-15T10:00:00.123456789Z')
    after['networks'][U.NET]['IPAddress'] = '172.0.0.3'
    image = after['image']
    U.validate_replacement('dataset', before, after, image, 'PATH\nJWT_SECRET\n', 'PATH\nJWT_SECRET\n')
    cases = ['single-approved-replacement-allows-new-ip']
    changes = [('sameCID', lambda f: f.update(cid=before['cid'])),
               ('oldboot', lambda f: f.update(started_at=before['started_at'])),
               ('wrongimage', lambda f: f.update(image=before['image'])),
               ('config-tag-instead-of-fullimage', lambda f: f.update(config_image='some-tag')),
               ('stopped', lambda f: f.update(running=False)),
               ('wrongservice', lambda f: f['labels'].update({'com.docker.compose.service': 'postgres'})),
               ('mountdrift', lambda f: f['mounts'][0].update(Destination='/other')),
               ('publicport', lambda f: f['ports']['50078/tcp'][0].update(HostIp='0.0.0.0')),
               ('different-network', lambda f: f['networks'][U.NET].update(NetworkID='other-network'))]
    for name, change in changes:
        bad = copy.deepcopy(after)
        change(bad)
        try:
            U.validate_replacement('dataset', before, bad, image, 'PATH\n', 'PATH\n')
        except AssertionError:
            pass
        else:
            raise AssertionError(name + ' accepted')
        cases.append(name + '-reject')
    with patch.object(U.G, 'load_release', return_value=None), patch.object(U.subprocess, 'run') as process:
        try:
            U.run('dataset', 'synthetic-no-root-release')
        except AssertionError:
            pass
        else:
            raise AssertionError('unreleased execution accepted')
        assert process.call_count == 0
    cases.append('no-phase-zero-subprocess-calls')
    for service in U.SERVICES:
        cmd = U.upgrade_argv(service)
        assert cmd[-6:] == ['up', '-d', '--no-deps', '--no-build', '--pull=never', service]
        assert cmd.count(service) == 1
    cases.append('all-five-commands-single-target-no-deps-no-build-never-pull')
    proof = output / 'synthetic-transition.json'
    proof.write_text('{"synthetic":true}\n')
    prefix = output / 'synthetic-after'
    for suffix in ['.cmd.json', '.raw', '.rc', '.meta.json']:
        Path(str(prefix) + suffix).write_text('synthetic\n')
    name = U.name_for('dataset')
    original = {'containers': {name: {k: before[k] for k in ['cid', 'image', 'started_at']}},
                'origins': {name: {'old': 'immutable original origin'}}}
    preserved = copy.deepcopy(original)
    current = {k: after[k] for k in ['cid', 'image', 'started_at']}
    derived = U.candidate_record_with_origins(original, 'dataset', current, proof, {'facts': prefix})
    assert original == preserved and derived['containers'][name] == current and derived['origins'][name]['priorOrigin'] == preserved['origins'][name] and derived['origins'][name]['transition_proof']['sha256'] == U.G.sha256_file(proof) and len(derived['origins'][name]['actual_after_receipts']) == 4 and all(x['sha256'] == U.G.sha256_file(x['path']) for x in derived['origins'][name]['actual_after_receipts'])
    cases.append('new-target-origin-binds-actual-proof-and-after-receipts-retains-prior-original-unchanged')
    result = {'pure_only': True, 'actual_Docker_HTTP_SQL_calls': 0, 'passed': len(cases),
              'cases': cases, 'dataset_argv': U.upgrade_argv('dataset'),
              'executor_sha256': U.G.sha256_file(HERE / 's2_image_upgrade.py')}
    (output / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': len(cases), 'actual_calls': 0, 'output': str(output)}))


if __name__ == '__main__':
    main(Path(sys.argv[1]).resolve())
