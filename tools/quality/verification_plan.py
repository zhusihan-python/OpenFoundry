#!/usr/bin/env python3
"""Print required verification categories; never claim they have executed."""
import json
import re
import subprocess
import sys

ALL = {'go-unit', 'go-integration', 'contracts', 'frontend', 'real-excel', 'infra-review', 'docs-links', 'human-review'}

def is_frontend_validation_entrypoint(path):
    return (path.startswith('apps/web/scripts/')
            or path == 'apps/web/package.json'
            or re.fullmatch(r'apps/web/(?:playwright|vite|vitest)(?:\.[^/]+)?\.config\.[cm]?[jt]s', path) is not None)

def plan(paths):
    checks = set()
    for path in paths:
        if path.startswith(('docs/',)) or path in {'README.md', 'CHANGELOG.md'}:
            checks.add('docs-links')
            if path.startswith('docs/agent-automation/'):
                checks.add('human-review')
        elif path.startswith('apps/web/'):
            checks.add('frontend')
            if 'dataset' in path or any(x in path for x in ['lib/api/', 'lib/auth/', 'vite.', 'playwright.', 'package.json']):
                checks.add('real-excel')
        elif path.startswith('services/'):
            checks.update({'go-unit', 'go-integration', 'contracts'})
            if path.startswith(('services/dataset-versioning-service/', 'services/edge-gateway-service/')):
                checks.add('real-excel')
        elif path.startswith('infra/'):
            checks.update({'infra-review', 'human-review'})
        else:
            checks.update(ALL)
        if is_frontend_validation_entrypoint(path):
            checks.add('human-review')
        if any(x in path for x in ['auth', 'identity', '/migrations/', 'proto/', 'CODEOWNERS', 'AGENTS.md', 'CLAUDE.md', 'CONTRIBUTING.md', 'quality/', '.github/', 'eslint', 'lint-baseline']):
            checks.add('human-review')
    return sorted(checks)

def _git(*args):
    try:
        return subprocess.check_output(['git', *args])
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f'verification_plan: git {" ".join(args)} failed with exit {exc.returncode}; '
                         f'BASE_REF {sys.argv[1]!r} is invalid or has no merge base with HEAD')

def main():
    if len(sys.argv) != 2:
        raise SystemExit('Usage: python3 tools/quality/verification_plan.py BASE_REF')
    base = _git('merge-base', 'HEAD', sys.argv[1]).decode().strip()
    if not base:
        raise SystemExit(f'verification_plan: no merge base between HEAD and {sys.argv[1]!r}')
    changed = _git('diff', '--name-only', '--no-renames', '-z', base).decode().split('\0')
    changed += _git('ls-files', '--others', '--exclude-standard', '-z').decode().split('\0')
    paths = sorted(set(filter(None, changed)))
    print(json.dumps({'base_sha': base, 'paths': paths, 'required_categories': plan(paths), 'execution_status': 'not-run'}, indent=2))

if __name__ == '__main__':
    main()
