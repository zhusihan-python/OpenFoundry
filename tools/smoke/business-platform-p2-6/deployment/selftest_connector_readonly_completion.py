#!/usr/bin/env python3
"""Captured-original regression and fake-I/O completion; no runtime calls."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26 / 'deployment'))
import connector_image_readonly_completion as C
U = C.U
spec = importlib.util.spec_from_file_location('bridge_env_pure', P26 / 'regressions/adapters/w10_image_transition.py')
B = importlib.util.module_from_spec(spec)
spec.loader.exec_module(B)


def main(output):
    output.mkdir(parents=True, exist_ok=False)
    ready = P26 / 'results/w6/S2-readiness/connector-completion-ready-attempt1'
    authority = json.loads((ready / 'original-authority-candidate.json').read_bytes())
    origin = C.validate_origin(authority)
    assert origin['original_outer_rc'] == 1 and origin['original_32_step_receipts_zero'] is True
    assert origin['original_receipt_counts'] == {'step_receipts_zero': 32, 'unix_child_rc0': 23, 'http_status200_transport0': 9}
    failure = C.read_pin(authority['failure_summary'])
    inner = Path(failure['inner'])
    before = (inner / '26-target-before-env_names.raw').read_text()
    after = (inner / '32-target-after-env_names.raw').read_text()
    assert before != after and set(U.env_names(before)) == set(U.env_names(after))
    cases = ['original-32-zero-receipts-23Unix9HTTP-and-outer1-9-old-tool-blobs-validated', 'captured-before26-after32-permutation-accepted']
    for normalizer in [U.env_names, B.env_names]:
        assert set(normalizer(before)) == set(normalizer(after))
        for label, raw in [('duplicate', 'A\nA\n'), ('internal-empty', 'A\n\nB\n'),
                           ('illegal', 'A\nBAD-NAME\n'), ('empty', '\n\n')]:
            try:
                normalizer(raw)
            except (AssertionError, ValueError):
                pass
            else:
                raise AssertionError(label + ' accepted')
        assert set(normalizer('A\nB\n')) != set(normalizer('A\n'))
        assert set(normalizer('A\nB\n')) != set(normalizer('A\nB\nC\n'))
        cases.append(normalizer.__module__ + '-duplicate-internalempty-illegal-empty-add-delete-negative-controls')
    for key in ['original_root_release', 'failure_summary', 'used_tools_snapshot']:
        wrong = copy.deepcopy(authority)
        wrong[key]['sha256'] = '0' * 64
        try:
            C.validate_origin(wrong)
        except AssertionError:
            pass
        else:
            raise AssertionError('wrong binding accepted ' + key)
        cases.append(key + '-SHA-drift-rejected')
    record_path = ready / 'deployment-expectation-record.json'
    source = origin['source_sha256']
    r = {'root_accepted': True, 'released_for': [C.PHASE], 'stage': 'runtime-image-completion',
         'suite': 'p26-runtime', 'candidate_source': source, 'deployment_record': str(record_path),
         'deployment_record_sha256': C.G.sha256_file(record_path),
         'completion_tools_sha256': {rel: C.G.sha256_file(P26 / rel) for rel in C.TOOLS},
         'connector_readonly_completion': copy.deepcopy(authority)}
    r['connector_readonly_completion']['output_dir'] = str(P26 / 'results/w6/deployment' / (output.parent.name + '-synthetic-completion'))
    release = output / 'synthetic-root.json'
    release.write_text(json.dumps(r, indent=2) + '\n')
    captured_facts = {}
    for path in inner.glob('*-before-*.raw'):
        if 13 <= int(path.name.split('-', 1)[0]) <= 23:
            fact = json.loads(path.read_bytes())
            captured_facts[fact['cid']] = fact
    captured_facts[C.CURRENT['cid']] = origin['after']
    calls = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        if argv[:2] == ['docker', 'inspect']:
            raw = after if argv[-1] == U.ENV_NAMES else json.dumps(captured_facts[argv[2]])
        elif argv[:3] == ['docker', 'ps', '-a']:
            raw = '\n'.join(sorted(f['name'].lstrip('/') for f in captured_facts.values() if f['name'].startswith('/p26')))
        elif argv[:2] == ['docker', 'exec']:
            assert argv[-1] == 'BEGIN READ ONLY; SELECT count(*) FROM sync_schedule_grants; COMMIT;'
            raw = '0\n'
        else:
            assert argv == ['python3', '-I', '-B', str(U.GUARD), str(release)]
            raw = json.dumps({'ok': True, 'controlled_count': 9, 'cp_count': 8, 'synthetic': True})
        return SimpleNamespace(stdout=raw, stderr='', returncode=0)

    def fake_http(url, **kwargs):
        return SimpleNamespace(status=200, body=b'{}', headers={}, error=None)

    counters = {'counter_bodies_sha256': {'synthetic': C.G.sha256_bytes(b'{}')}}
    with patch.object(C.G, 'load_release', return_value=r), patch.object(C.subprocess, 'run', side_effect=fake_run), \
         patch.object(C.W, 'http_call', side_effect=fake_http), patch.object(C.W, 'load_baseline', return_value=(None, counters, None)):
        rc = C.run(str(release))
    assert rc == 0
    proof = json.loads((Path(r['connector_readonly_completion']['output_dir']) / 'composite-proof.json').read_bytes())
    assert proof['original_execution_outer_rc'] == 1 and proof['new_up_executions'] == 0
    assert all(not set(argv).intersection({'up', 'restart', 'stop', 'rm', 'start', 'kill'}) for argv in calls)
    assert (Path(failure['outer']) / 'rc').read_text().strip() == '1'
    cases.append('full-completion-fake-IO-all-reads-retains-original-rc1-no-up')
    result = {'pure_only': True, 'actual_Docker_HTTP_SQL_calls': 0, 'original_rc': rc,
              'passed': len(cases), 'cases': cases, 'fake_subprocess_calls': len(calls),
              'tools_sha256': r['completion_tools_sha256']}
    (output / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': len(cases), 'original_rc': rc, 'actual_calls': 0, 'output': str(output)}))


if __name__ == '__main__':
    main(Path(sys.argv[1]).resolve())
