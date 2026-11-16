#!/usr/bin/env python3
"""Pure safe-input tests. No subprocess, Docker, HTTP, SQL or private reads."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('subject', HERE / 'final_substrate_counter_readonly.py')
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)


def main(plan_path, output_dir):
    output = Path(output_dir)
    output.mkdir(exist_ok=False)
    plan_path = Path(plan_path)
    release = {'plan_path': str(plan_path), 'plan_sha256': M.sha(plan_path),
               'source_sha256': 'ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'}
    plan, record, expected, counters = M.load_plan(release)
    passed = []
    for name, pin in record['containers'].items():
        M.validate_snapshot(pin, expected[name], expected[name], name == M.W.Q3_CONNECTOR)
    passed.append('positive-all5-current-safe-config-complete-original-receipts')

    def reject(label, fn):
        try:
            fn()
        except (ValueError, KeyError, OSError):
            passed.append(label)
        else:
            raise AssertionError('accepted invalid input: ' + label)

    pg = M.W.Q3_POSTGRES
    ds = M.W.Q3_DATASET
    bad = copy.deepcopy(expected[pg])
    bad['identity'] = bad['identity'].replace(record['containers'][pg]['started_at'], '2026-11-15T00:00:00Z')
    reject('PGboot-drift', lambda: M.validate_snapshot(record['containers'][pg], expected[pg], bad))
    badmount = copy.deepcopy(expected[ds])
    values = json.loads(badmount['mounts'])
    values[0]['RW'] = not values[0]['RW']
    badmount['mounts'] = json.dumps(values)
    # A newly computed recording SHA must not legitimize changed semantics.
    assert hashlib.sha256(badmount['mounts'].encode()).hexdigest() != hashlib.sha256(expected[ds]['mounts'].encode()).hexdigest()
    reject('rehashed-mount-field-drift', lambda: M.validate_snapshot(record['containers'][ds], expected[ds], badmount))
    extra = copy.deepcopy(expected[ds])
    mounts = json.loads(extra['mounts']); mounts[0]['unrecognized_new_field'] = 'extra'
    extra['mounts'] = json.dumps(mounts)
    reject('extra-mount-field-not-dropped', lambda: M.validate_snapshot(record['containers'][ds], expected[ds], extra))
    dupe = copy.deepcopy(expected[ds]); mounts = json.loads(dupe['mounts']); mounts.append(mounts[0]); dupe['mounts'] = json.dumps(mounts)
    reject('duplicate-mount-destination', lambda: M.validate_snapshot(record['containers'][ds], expected[ds], dupe))
    badports = copy.deepcopy(expected[pg]); ports = M.ports(badports['ports']); ports[1]['5432/tcp'] = [{'HostPort': '5432'}]; badports['ports'] = '\n'.join(json.dumps(v) for v in ports)
    reject('full-port-binding-extra-field', lambda: M.validate_snapshot(record['containers'][pg], expected[pg], badports))
    badenv = copy.deepcopy(expected[pg]); badenv['env_names'] += '\nNEW_NAME\n'
    reject('environment-name-addition', lambda: M.validate_snapshot(record['containers'][pg], expected[pg], badenv))
    for name, body in counters.items():
        M.validate_counter(body, SimpleNamespace(status=200, error=None, body=body))
    passed.append('positive-nine-complete-counter-bodies')
    body = next(iter(counters.values()))
    reject('rehashed-counter-body-drift', lambda: M.validate_counter(body, SimpleNamespace(status=200, error=None, body=body + b' ')))
    reject('HTTP500', lambda: M.validate_counter(body, SimpleNamespace(status=500, error=None, body=body)))
    reject('HTTPtransport-failure', lambda: M.validate_counter(body, SimpleNamespace(status=None, error='offline', body=b'')))

    def plan_case(label, mutate):
        value = copy.deepcopy(plan); mutate(value)
        path = output / (label + '.json'); path.write_text(json.dumps(value))
        r = dict(release, plan_path=str(path), plan_sha256=M.sha(path))
        reject(label, lambda: M.load_plan(r))

    plan_case('rehashed-Source-extra', lambda v: v['counters'].append(copy.deepcopy(v['counters'][0])))
    plan_case('rehashed-Source-missing', lambda v: v['counters'].pop())
    plan_case('rehashed-Source-unknown', lambda v: v['counters'][0].update(source_name='unknown'))
    plan_case('rehashed-container-duplicate', lambda v: v['containers'].__setitem__(0, copy.deepcopy(v['containers'][1])))
    plan_case('rehashed-container-missing', lambda v: v['containers'].pop())
    plan_case('missing-original-raw', lambda v: v['containers'][0]['receipts']['mounts'].pop('raw'))
    result = {'synthetic_pure_only': True, 'actual_calls': 0, 'passed': passed, 'count': len(passed)}
    (output / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
