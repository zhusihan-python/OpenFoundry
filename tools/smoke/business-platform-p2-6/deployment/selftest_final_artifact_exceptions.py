#!/usr/bin/env python3
"""Fake-I/O failure evidence tests. Never calls Docker, HTTP, or SQL."""
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('artifact_exception_tests', HERE / 'final_old_artifact_readonly.py')
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)
PRIVATE = 'fake-private-value-for-redaction-test'
SIG = 'fake-query-signature-for-redaction-test'
URL = 'http://127.0.0.1:17078/v1/_internal/local-fs/test?sig=' + SIG
MESSAGE = 'failure ' + PRIVATE + ' url=' + URL


def run(root):
    root.mkdir(parents=True, exist_ok=False)
    results = []
    def check(name, kind, error=None, response=None, child=None, expected_rc=None, child_rc=None):
        att = M.W.Attempt(root, name)
        att.add_secret(PRIVATE)
        def fake(*args, **kwargs):
            if error:
                raise error
            return response if kind == 'http' else child
        observed = None
        try:
            if kind == 'http':
                M.evidenced_http(att, URL, transport=fake)
            else:
                M.evidenced_pg(att, ['docker', 'exec', 'pure-fake-cid', 'psql', '-c', 'SELECT 1'], runner=fake)
        except Exception as exc:
            observed = exc
        rcfiles = list(att.dir.glob('*.rc'))
        assert len(rcfiles) == 1 and rcfiles[0].read_text().strip() == str(expected_rc)
        assert not any(f.read_text().strip() == 'pending' for f in rcfiles)
        meta = json.loads(next(att.dir.glob('*.meta.json')).read_text())
        assert meta['original_child_rc'] == child_rc
        if kind == 'http':
            assert meta['transport_outcome'] == int(expected_rc)
            assert meta['status'] == (None if error else response.status)
        if error:
            assert observed is error, 'primary exception was replaced'
        if observed:
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                assert M.terminal_error(att, observed, []) == 1
            emitted = stdout.getvalue() + stderr.getvalue()
            assert 'Traceback' not in emitted and PRIVATE not in emitted and SIG not in emitted
            assert MESSAGE not in emitted
        for f in att.dir.iterdir():
            if f.is_file():
                text = f.read_text()
                assert PRIVATE not in text and SIG not in text, name + ': leaked private material'
        results.append({'case': name, 'pass': True, 'recorded_rc': expected_rc, 'original_child_rc': child_rc})
    check('http-bodycap', 'http', error=M.W.PreflightRefusal(MESSAGE), expected_rc=1)
    check('http-socket-timeout', 'http', error=socket.timeout(MESSAGE), expected_rc=1)
    check('http-oserror', 'http', error=OSError(MESSAGE), expected_rc=1)
    check('http-transport-returned-error', 'http', response=M.W.HttpResponse(None, {}, b'', URL, False, error=MESSAGE), expected_rc=1)
    check('http-404-is-transport-success', 'http', response=M.W.HttpResponse(404, {}, MESSAGE.encode(), URL, False), expected_rc=0)
    check('http-302-location-projection', 'http', response=M.W.HttpResponse(302, {'Location': URL}, b'', URL, False), expected_rc=0)
    timeout = subprocess.TimeoutExpired(['pure-fake-child'], 30, output=(MESSAGE+' partialstdout').encode(), stderr=(MESSAGE+' partialstderr').encode())
    check('pg-timeout-with-partial-output', 'pg', error=timeout, expected_rc='unavailable')
    check('pg-oserror', 'pg', error=OSError(MESSAGE), expected_rc='unavailable')
    check('pg-exception-known-child-code', 'pg', error=subprocess.CalledProcessError(17, ['pure-fake-child'], output=MESSAGE, stderr=MESSAGE), expected_rc=17, child_rc=17)
    check('pg-real-nonzero-returncode', 'pg', child=subprocess.CompletedProcess([], 23, MESSAGE, MESSAGE), expected_rc=23, child_rc=23)
    check('pg-success-returncode', 'pg', child=subprocess.CompletedProcess([], 0, '1\n', ''), expected_rc=0, child_rc=0)
    # A secondary terminal-write failure must not replace or expose primary.
    stderr = io.StringIO()
    class Broken:
        def finish(self, payload):
            assert payload['error'].startswith('TimeoutError:')
            raise OSError(MESSAGE)
    with redirect_stderr(stderr):
        assert M.terminal_error(Broken(), TimeoutError(MESSAGE), []) == 1
    assert PRIVATE not in stderr.getvalue() and SIG not in stderr.getvalue() and 'Traceback' not in stderr.getvalue()
    results.append({'case': 'terminal-secondary-write-failure-preserves-primary-type', 'pass': True})
    (root / 'RESULT.json').write_text(json.dumps({'pure_only': True, 'actual_resource_calls': 0, 'passed': len(results), 'cases': results, 'wrapper_sha256': hashlib.sha256((HERE/'final_old_artifact_readonly.py').read_bytes()).hexdigest()}, indent=2)+'\n')
    print(root)
    print(str(len(results)) + ' fake-I/O exception cases passed; actual resource calls=0')


if __name__ == '__main__':
    run(Path(sys.argv[1]).resolve())
