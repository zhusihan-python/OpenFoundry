#!/usr/bin/env python3
"""Run the pinned linter against an explicit branch base, or audit everything."""
import os
from pathlib import Path
import subprocess
import sys

# GitHub event "before" shas are all zeros when a branch is first pushed.
# Falling back to origin/main there would silently hide committed issues
# when origin/main already equals HEAD (e.g. the first push to main), so an
# all-zero base audits the full tree instead.
ALL_ZERO_SHAS = frozenset(('0' * 40, '0' * 64))


def main():
    version = (Path(__file__).resolve().parents[2] / '.golangci-lint-version').read_text().strip().lstrip('v')
    actual = subprocess.check_output(['golangci-lint', 'version', '--short'], text=True).strip().lstrip('v')
    if actual != version:
        raise SystemExit(f'Expected golangci-lint {version}, got {actual}; run make tools')
    args = ['golangci-lint', 'run', '--timeout=5m']
    ref = os.environ.get('BASE_REV', '').strip()
    full = os.environ.get('LINT_FULL') == '1' or ref in ALL_ZERO_SHAS
    if not full:
        if not ref:
            ref = 'origin/main'
        base = subprocess.check_output(['git', 'merge-base', 'HEAD', ref], text=True).strip()
        if not base:
            raise SystemExit(f'git merge-base HEAD {ref} produced no revision; refusing to guess a base')
        args.append(f'--new-from-rev={base}')
    raise SystemExit(subprocess.call(args))


if __name__ == '__main__':
    try:
        main()
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f'Cannot establish lint gate: {exc}', file=sys.stderr)
        raise SystemExit(2)
