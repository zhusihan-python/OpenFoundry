#!/usr/bin/env python3
"""Verify the joint pilot's source identity and automatic gate conclusions."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys

# Changing this reviewed pilot baseline requires human review. Do not derive it
# from the target branch, HEAD, or an initial push's all-zero before SHA.
PILOT_BASE = '8d8543437da7f14b168b3667f1eb784892c629fc'
REQUIRED_JOBS = frozenset({'preflight', 'go', 'frontend', 'excel', 'architecture', 'policy', 'contracts', 'docs'})
REQUIRED_FILES = (
    '.github/workflows/openfoundry-go.yml',
    '.github/workflows/ci-frontend.yml',
    '.github/workflows/dataset-excel-acceptance.yml',
    '.github/workflows/architecture-pilot.yml',
    '.github/workflows/verification-policy.yml',
    'tools/smoke/dataset-excel/run.sh',
)


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


def validate_base(base, head):
    if not isinstance(base, str) or not re.fullmatch(r'[0-9a-f]{40}', base) or base == '0' * 40:
        raise ValueError('lint baseline must be a nonzero full commit SHA')
    if base == head:
        raise ValueError('lint baseline must not equal HEAD')
    subprocess.run(['git', 'merge-base', '--is-ancestor', base, head], check=True)


def preflight(expected_sha, base):
    head = git('rev-parse', 'HEAD')
    if head != expected_sha:
        raise ValueError('checkout does not match GITHUB_SHA')
    if base != PILOT_BASE:
        raise ValueError('pilot baseline differs from the reviewed fixed baseline')
    validate_base(base, head)
    missing = [name for name in REQUIRED_FILES if not Path(name).is_file()]
    if missing:
        raise ValueError('incomplete combined tree: ' + ', '.join(missing))
    return {'source_sha': head, 'source_tree': git('rev-parse', 'HEAD^{tree}'),
            'lint_base': base, 'human_approval': 'not-claimed'}


def aggregate(needs):
    if not isinstance(needs, dict) or set(needs) != REQUIRED_JOBS:
        raise ValueError('aggregate must receive every required automatic job exactly once')
    failed = {name: job.get('result') if isinstance(job, dict) else None
              for name, job in needs.items()
              if not isinstance(job, dict) or job.get('result') != 'success'}
    if failed:
        raise ValueError('automatic gates did not all succeed: ' + json.dumps(failed, sort_keys=True))
    return {'automatic_gates': 'success', 'human_approval': 'not-claimed',
            'note': 'Human review remains required; no approval is synthesized by CI.'}


def main():
    if len(sys.argv) == 2 and sys.argv[1] == 'preflight':
        result = preflight(os.environ['GITHUB_SHA'], os.environ['LINT_BASE'])
    elif len(sys.argv) == 2 and sys.argv[1] == 'aggregate':
        result = aggregate(json.loads(os.environ['NEEDS_JSON']))
    else:
        raise ValueError('usage: joint_verification.py preflight|aggregate')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, subprocess.CalledProcessError) as exc:
        raise SystemExit(f'joint verification: {exc}')
