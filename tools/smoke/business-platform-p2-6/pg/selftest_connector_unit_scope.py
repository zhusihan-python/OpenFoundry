#!/usr/bin/env python3
"""Exercise actual unit command assembly with fake I/O; no Docker calls."""
import argparse
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
from pg import p26_pg_runner as R


def main(output):
    output.mkdir(parents=True, exist_ok=False)
    calls = []
    source = 'a' * 64

    def fake_runner(argv, **kwargs):
        calls.append(argv)
        if argv[:3] == ['docker', 'volume', 'inspect']:
            return 0, 'synthetic volume\n'
        if argv[:3] == ['docker', 'image', 'inspect']:
            return 0, 'sha256:' + R.GO_IMAGE_IDS_RECORDED['golang:1.25'] + ' golang:1.25.14\n'
        assert argv[:2] == ['docker', 'run']
        return 0, 'go version go1.25.14 linux/arm64\nok synthetic/cmd/service 0.001s\n'

    args = argparse.Namespace(service='connector', expect_source_sha=source)
    with patch.dict(os.environ, {'P26_PG_EV_DIR': str(output / 'synthetic-inner')}), patch.object(R.G, 'candidate_hash', return_value=source):
        rc = R.cmd_run_unit(args, fake_runner, {'released_for': ['unit-connector']}, True)
    unit = [argv for argv in calls if argv[:2] == ['docker', 'run']]
    assert rc == 0 and len(unit) == 1
    argv = unit[0]
    assert argv[-1] == 'go version && go vet ./... && go test -count=1 ./...'
    assert argv[argv.index('-w') + 1] == '/src/services/connector-management-service'
    assert argv[argv.index('--network') + 1] == 'none'
    assert './internal/...' not in argv[-1] and '-tags' not in argv[-1]
    assert R.UNIT_COHORTS['dataset']['pkgs'] == './services/dataset-versioning-service/...'
    assert R.UNIT_COHORTS['pipeline']['pkgs'] == './services/pipeline-build-service/...'
    result = {'pure_only': True, 'actual_subprocess_calls': 0, 'passed': 5,
              'cases': ['full-module-vet-and-test', 'connector-module-cwd',
                        'offline-network-none', 'no-internal-only-or-integration-tags',
                        'other-two-service-scopes-unchanged'],
              'synthetic_command': argv,
              'runner_sha256': R.G.sha256_file(P26 / 'pg/p26_pg_runner.py')}
    (output / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': 5, 'actual_subprocess_calls': 0, 'output': str(output)}))


if __name__ == '__main__':
    main(Path(sys.argv[1]).resolve())
