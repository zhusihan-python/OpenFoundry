#!/usr/bin/env python3
"""Complete the known Connector replacement with reads only; retain original rc1.

Root separately pins the observed b38/current candidate EXPECTATION record.
This tool never invokes compose/up/restart/stop/rm, nor creates a Root release.
"""
import argparse
import copy
import json
from pathlib import Path
import re
import subprocess
import sys

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
sys.path.insert(0, str(P26 / 'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
import s2_image_upgrade as U
from final_old_artifact_readonly import evidenced_http, terminal_error

PHASE = 's2-image-readonly-completion-connector'
TOOLS = {'deployment/connector_image_readonly_completion.py', 'deployment/s2_image_upgrade.py',
         'deployment/final_old_artifact_readonly.py', 'common/p26_gates.py',
         'preflight/p26_current_preflight.py', 'regressions/adapters/w10_guard_shim.py',
         'regressions/adapters/w10_image_transition.py'}
CURRENT = {'cid': 'b38db2471649ac53a9db73ebb40b2e2a7694369fdf0d7e548353b252e0ddd881',
           'image': 'sha256:ea8244f03a3b07e72b77bd7539e13ae1d39cf91bf90a26d01675f008c64685d6',
           'started_at': '2026-11-15T10:03:19.135785133Z'}


def ref(path):
    return {'path': str(Path(path).resolve()), 'sha256': G.sha256_file(path)}


def read_pin(binding):
    path = Path(binding['path'])
    assert path.is_file() and not path.is_symlink()
    assert G.sha256_file(path) == binding['sha256']
    return json.loads(path.read_bytes())


def original_receipt(prefix):
    files = {suffix: Path(str(prefix) + suffix) for suffix in ['.cmd.json', '.raw', '.rc', '.meta.json']}
    assert all(path.is_file() and not path.is_symlink() for path in files.values())
    assert files['.rc'].read_text().strip() == '0'
    return json.loads(files['.cmd.json'].read_bytes()), files['.raw'].read_text(), [ref(p) for p in files.values()]


def classify_step_receipts(directory):
    counts = {'step_receipts_zero': 0, 'unix_child_rc0': 0, 'http_status200_transport0': 0}
    for path in sorted(Path(directory).glob('*.rc')):
        assert path.read_text().strip() == '0'
        meta = json.loads(Path(str(path)[:-3] + '.meta.json').read_bytes())
        if meta.get('kind') == 'http':
            assert meta.get('original_child_rc') is None and meta['status'] == 200 and meta['transport_outcome'] == 0
            counts['http_status200_transport0'] += 1
        else:
            assert type(meta.get('original_child_rc')) is int and meta['original_child_rc'] == 0
            counts['unix_child_rc0'] += 1
        counts['step_receipts_zero'] += 1
    return counts


def validate_origin(authority):
    """Validate immutable prior attempt from disk only; no runtime calls."""
    old_root = read_pin(authority['original_root_release'])
    failure = read_pin(authority['failure_summary'])
    snapshot = read_pin(authority['used_tools_snapshot'])
    assert old_root['image_transition']['service'] == 'connector'
    assert old_root['image_transition']['approved_argv'] == U.upgrade_argv('connector')
    assert failure['outer_original_rc'] == 1 and failure['ok'] is False
    assert failure['health_executed'] is False and failure['post_typed_guard_executed'] is False
    assert failure['source_before'] == failure['source_after'] == old_root['candidate_source']
    assert Path(failure['outer']).is_relative_to(P26 / 'results/w6/deployment')
    assert (Path(failure['outer']) / 'rc').read_text().strip() == '1'
    for item in failure['outer_originals'] + failure['inner_originals']:
        assert G.sha256_file(item['path']) == item['sha256']
    inner = Path(failure['inner'])
    assert inner.is_relative_to(Path(old_root['image_transition']['output_dir']))
    rcs = sorted(inner.glob('*.rc'))
    assert len(rcs) == 32 and {int(path.name.split('-', 1)[0]) for path in rcs} == set(range(1, 33))
    assert all(path.read_text().strip() == '0' for path in rcs)
    counts = classify_step_receipts(inner)
    assert counts == {'step_receipts_zero': 32, 'unix_child_rc0': 23, 'http_status200_transport0': 9}
    assert snapshot['original_release_path'] == authority['original_root_release']['path']
    assert snapshot['original_release_sha256'] == authority['original_root_release']['sha256']
    old_tools = dict(old_root['image_upgrade_tools_sha256'])
    old_tools['regressions/adapters/w10_image_transition.py'] = old_root['image_transition']['guard_extension_sha256']
    assert len(snapshot['files']) == len(old_tools) == 9
    seen = set()
    for item in snapshot['files']:
        relative = str(Path(item['origin']).relative_to(P26))
        assert relative not in seen and item['sha256'] == old_tools[relative]
        assert G.sha256_file(item['copy']) == item['sha256']
        assert Path(item['copy']).is_relative_to(Path(authority['used_tools_snapshot']['path']).parent)
        seen.add(relative)
    assert seen == set(old_tools)
    # The ORIGINAL executor hash is proven by its archived bytes, never by
    # asserting the current corrected executor still has the old digest.
    assert old_root['image_transition']['executor_tool_sha256'] == old_tools['deployment/s2_image_upgrade.py']
    for path, digest in old_root['image_transition']['config_shas'].items():
        assert G.sha256_file(path) == digest
    base = read_pin({'path': old_root['deployment_record'], 'sha256': old_root['deployment_record_sha256']})
    artifacts = []
    captured = {}
    for key, number, suffix, fmt in [('before', 25, 'facts', U.FACTS), ('after', 31, 'facts', U.FACTS),
                                    ('before_names', 26, 'env_names', U.ENV_NAMES),
                                    ('after_names', 32, 'env_names', U.ENV_NAMES)]:
        side = 'before' if key.startswith('before') else 'after'
        prefix = inner / (str(number) + '-target-' + side + '-' + suffix)
        cmd, raw, refs = original_receipt(prefix)
        pin = base['containers'][W.Q3_CONNECTOR] if side == 'before' else authority['expected_current']
        assert cmd == ['docker', 'inspect', pin['cid'], '--format', fmt]
        captured[key] = raw if key.endswith('names') else json.loads(raw)
        artifacts.extend(refs)
    before, after = captured['before'], captured['after']
    expected = authority['expected_current']
    assert expected == CURRENT
    U.validate_fact('connector', before, base['containers'][W.Q3_CONNECTOR]['image'], base['containers'][W.Q3_CONNECTOR])
    U.validate_replacement('connector', before, after, expected['image'], captured['before_names'], captured['after_names'])
    assert {key: after[key] for key in expected} == expected
    up, _, refs = original_receipt(inner / '28-upgrade-only-connector')
    assert up == old_root['image_transition']['approved_argv']
    artifacts.extend(refs)
    discovery, cid, refs = original_receipt(inner / '29-resolve-command-created-cid')
    assert discovery == U.compose_prefix('connector') + ['ps', '-q', 'connector'] and cid.strip() == expected['cid']
    artifacts.extend(refs)
    return {'original_outer_rc': 1, 'original_32_step_receipts_zero': True, 'original_receipt_counts': counts,
            'original_root_release': authority['original_root_release'],
            'failure_summary': authority['failure_summary'], 'used_tools_snapshot': authority['used_tools_snapshot'],
            'original_executor_sha256': old_tools['deployment/s2_image_upgrade.py'],
            'corrected_executor_sha256': G.sha256_file(U.__file__),
            'source_sha256': old_root['candidate_source'], 'old_base_record': base,
            'before': before, 'after': after, 'original_six_receipt_artifacts': artifacts}


def run(release_file):
    r = G.load_release(release_file)
    assert r and r.get('root_accepted') is True and PHASE in r['released_for']
    assert r['stage'] == 'runtime-image-completion' and r['suite'] == 'p26-runtime'
    assert 'image_transition' not in r and 'main_boot_transition' not in r
    assert set(r['completion_tools_sha256']) == TOOLS
    for rel, digest in r['completion_tools_sha256'].items():
        assert G.sha256_file(P26 / rel) == digest
    origin = validate_origin(r['connector_readonly_completion'])
    source = r['candidate_source']
    assert source == origin['source_sha256'] == G.candidate_hash(G.OPENFOUNDRY)
    record = read_pin({'path': r['deployment_record'], 'sha256': r['deployment_record_sha256']})
    assert record['source_sha256'] == source
    assert record['containers'][W.Q3_CONNECTOR] == CURRENT
    expected_record = copy.deepcopy(origin['old_base_record']['containers'])
    expected_record[W.Q3_CONNECTOR] = CURRENT
    assert record['containers'] == expected_record
    output = Path(r['connector_readonly_completion']['output_dir']).resolve()
    assert output.is_relative_to(P26 / 'results/w6/deployment')
    output.mkdir(parents=True, exist_ok=False)
    att = W.Attempt(output, 'commands')
    root_sha = G.sha256_file(release_file)
    for line in W.Q3_ENV_FILE.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            att.add_secret(line.split('=', 1)[1].strip().strip('\"').strip("'"))

    def gate():
        assert G.sha256_file(release_file) == root_sha and G.load_release(release_file) == r
        assert G.candidate_hash(G.OPENFOUNDRY) == source
        for rel, digest in r['completion_tools_sha256'].items():
            assert G.sha256_file(P26 / rel) == digest

    def command(tag, argv):
        gate()
        assert (argv[:2] == ['docker', 'inspect'] or argv[:3] == ['docker', 'ps', '-a'] or
                argv[:2] == ['docker', 'exec'] or argv[:4] == ['python3', '-I', '-B', str(U.GUARD)])
        prefix = att.step_begin(tag, argv)
        try:
            cp = subprocess.run(argv, capture_output=True, text=True, timeout=180)
        except Exception as exc:
            att.step_finish(tag, prefix, type(exc).__name__, 'unavailable', {'original_child_rc': None})
            raise RuntimeError(tag + ' spawn/timeout') from None
        att.step_finish(tag, prefix, cp.stdout, cp.returncode, {'stderr': cp.stderr, 'original_child_rc': cp.returncode}, kind='argv')
        assert cp.returncode == 0
        return cp.stdout, prefix

    def inspect(tag, cid):
        text, prefix = command(tag, ['docker', 'inspect', cid, '--format', U.FACTS])
        return json.loads(text), prefix

    try:
        actual, after_ref = inspect('current-known-Connector', CURRENT['cid'])
        assert actual == origin['after']
        names, names_ref = command('current-Connector-env-names', ['docker', 'inspect', CURRENT['cid'], '--format', U.ENV_NAMES])
        # Validate against the immutable original after NAME receipt; keep its
        # order on disk, compare only validated duplicate-free NAME sets.
        old_inner = Path(read_pin(origin['failure_summary'])['inner'])
        assert set(U.env_names(names)) == set(U.env_names((old_inner / '32-target-after-env_names.raw').read_text()))
        gate()
        health, _ = evidenced_http(att, W.ORIGINS['connector'] + '/healthz')
        assert health.status == 200
        before_facts = {json.loads(path.read_bytes())['name'].lstrip('/'): json.loads(path.read_bytes())
                        for path in old_inner.glob('*-before-*.raw') if path.name.split('-', 1)[0].isdigit()
                        and 13 <= int(path.name.split('-', 1)[0]) <= 23}
        assert len(before_facts) == 11
        for name, previous in before_facts.items():
            if name != W.Q3_CONNECTOR:
                now, _ = inspect('unchanged-' + name, previous['cid'])
                assert now == previous
        _, catalog, _ = W.load_baseline(G.P25_FINAL_DIR)
        for name, digest in catalog['counter_bodies_sha256'].items():
            gate()
            response, _ = evidenced_http(att, W.ORIGINS['fixture'] + '/admin/src/' + name)
            assert response.status == 200 and G.sha256_bytes(response.body) == digest
        pg = record['containers'][W.Q3_POSTGRES]['cid']
        count, _ = command('whole-schedule-grants', ['docker', 'exec', pg, 'psql', '-X', '-qAt', '-v',
                           'ON_ERROR_STOP=1', '-U', 'openfoundry', '-d', 'connectors', '-c',
                           'BEGIN READ ONLY; SELECT count(*) FROM sync_schedule_grants; COMMIT;'])
        assert count.strip() == '0'
        inventory, _ = command('whole-owned-inventory', ['docker', 'ps', '-a', '--filter', 'label=of-p26-owner=p26-http',
                               '--filter', 'label=of-p26-stage=p2-6', '--format', '{{.Names}}'])
        assert set(inventory.splitlines()) == {name for name in before_facts if name.startswith('p26')}
        text, guard_ref = command('whole-typed-postguard', ['python3', '-I', '-B', str(U.GUARD), release_file])
        guard = json.loads(text.strip().splitlines()[-1])
        assert guard['ok'] is True
        gate()
        completion_counts = classify_step_receipts(att.dir)
        composite = {'schema': 'p26-connector-image-readonly-composite-v1', 'ok': True,
                     'original_execution_outer_rc': 1, 'original_32_step_receipts_zero': True,
                     'original_receipt_counts': origin['original_receipt_counts'],
                     'completion_receipt_counts': completion_counts, 'new_up_executions': 0,
                     'source_sha256': source, 'current': CURRENT,
                     'original_authority_and_receipts': origin,
                     'completion_root_sha256': root_sha, 'health_status': health.status,
                     'other10_fullfacts_unchanged': True, 'oldfixture_counters_unchanged': True,
                     'whole_grants_count': 0, 'owned_inventory_exact': True,
                     'typed_postguard': guard, 'typed_postguard_receipt': guard_ref}
        composite_path = output / 'composite-proof.json'
        U.exclusive_json(composite_path, composite)
        candidate = U.candidate_record_with_origins(record, 'connector', CURRENT, composite_path,
                                                  {'identity': after_ref, 'env_names': names_ref})
        candidate['acceptance_status'] = 'UNREVIEWED-COMPOSITE-CANDIDATE'
        candidate['scope'] = 'Readonly completion succeeded; original image execution rc1 retained, requires Root acceptance.'
        U.exclusive_json(output / 'candidate-deployment-record.json', candidate)
        att.finish({'ok': True, 'original_execution_outer_rc': 1, 'completion_ok': True,
                    'new_up_executions': 0, 'current': CURRENT, 'source_sha256': source,
                    'composite_proof': ref(composite_path), 'typed_postguard': guard,
                    'original_receipt_counts': origin['original_receipt_counts'],
                    'completion_receipt_counts': completion_counts,
                    'other10_fullfacts_unchanged': True, 'whole_grants_count': 0})
        print(att.dir)
        return 0
    except Exception as exc:
        return terminal_error(att, exc, [])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--release-file', required=True)
    args = parser.parse_args()
    try:
        rc = run(args.release_file)
    except Exception as exc:
        print('Connector readonly completion refused (' + type(exc).__name__ + ')', file=sys.stderr)
        rc = 1
    sys.exit(rc)
