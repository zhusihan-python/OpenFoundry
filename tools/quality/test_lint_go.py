"""Negative control using the real pinned linter in a disposable git repo."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

runner = Path(__file__).with_name('lint_go.py').resolve()
with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    def run(*cmd, **kwargs):
        return subprocess.run(cmd, cwd=root, check=True, capture_output=True, text=True, **kwargs)
    run('git', 'init', '-q')
    run('git', 'config', 'user.name', 'Quality gate fixture')
    run('git', 'config', 'user.email', 'fixture@example.invalid')
    (root/'go.mod').write_text('module fixture\n\ngo 1.25.0\n')
    (root/'.golangci.yml').write_text('version: "2"\nlinters:\n  default: none\n  enable: [errcheck]\n')
    good = 'package fixture\nimport "os"\nfunc Change() error { return os.Chdir(".") }\n'
    (root/'fixture.go').write_text(good)
    run('git', 'add', '.')
    run('git', 'commit', '-qm', 'baseline')
    base = run('git', 'rev-parse', 'HEAD').stdout.strip()
    env = dict(os.environ, BASE_REV=base)
    env.pop('LINT_FULL', None)
    run('python3', str(runner), env=env)
    (root/'fixture.go').write_text('package fixture\nimport "os"\nfunc Change() { os.Chdir(".") }\n')
    run('git', 'add', '.')
    run('git', 'commit', '-qm', 'committed regression')
    bad = subprocess.run(['python3', str(runner)], cwd=root, env=env, capture_output=True, text=True)
    assert bad.returncode == 1 and 'errcheck' in bad.stdout + bad.stderr, bad.stdout + bad.stderr
    (root/'fixture.go').write_text(good)
    run('python3', str(runner), env=env)
    missing = subprocess.run(['python3', str(runner)], cwd=root, env=dict(env, BASE_REV='missing-ref'), capture_output=True)
    assert missing.returncode != 0
    # All-zero BASE_REV (CI "before" on a first push) means no usable base:
    # the runner must audit everything, so the committed violation at HEAD
    # (restored to a clean checkout) fails instead of hiding behind a
    # degenerate merge-base.
    run('git', 'checkout', '--', 'fixture.go')
    zeros = dict(env, BASE_REV='0'*40)
    zero_bad = subprocess.run(['python3', str(runner)], cwd=root, env=zeros, capture_output=True, text=True)
    assert zero_bad.returncode == 1 and 'errcheck' in zero_bad.stdout + zero_bad.stderr, zero_bad.stdout + zero_bad.stderr
    (root/'fixture.go').write_text(good)
    run('python3', str(runner), env=zeros)
    # A stale linter binary is refused before anything is linted.
    with tempfile.TemporaryDirectory() as stale_dir:
        stale_dir = Path(stale_dir)
        (stale_dir/'.golangci-lint-version').write_text('v0.0.0')
        (stale_dir/'tools'/'quality').mkdir(parents=True)
        shutil.copy(runner, stale_dir/'tools'/'quality'/'lint_go.py')
        stale = subprocess.run(['python3', str(stale_dir/'tools'/'quality'/'lint_go.py')],
                               cwd=root, env=env, capture_output=True, text=True)
    assert stale.returncode != 0 and 'Expected golangci-lint 0.0.0' in stale.stderr, stale.stdout + stale.stderr
print('PASS: clean baseline, committed regression rejected, fix accepted, missing base rejected, '
      'all-zero base forces a full audit, stale binary rejected')
