import hashlib
import gzip
import http.client
import importlib.util
import json
import pathlib
import tempfile
import threading
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

spec = importlib.util.spec_from_file_location('fault_proxy', pathlib.Path(__file__).with_name('fault_proxy.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class FaultProxyTest(unittest.TestCase):
    def test_restart_preserves_existing_raw_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            first = module.Proxy(("127.0.0.1",0),"http://unused",directory)
            first.record({"action":"old"},b"original")
            first.server_close()
            second = module.Proxy(("127.0.0.1",0),"http://unused",directory)
            second.record({"action":"new"},b"new")
            second.server_close()
            self.assertEqual((pathlib.Path(directory)/"000001.request.bin").read_bytes(),b"original")
            self.assertEqual((pathlib.Path(directory)/"000002.request.bin").read_bytes(),b"new")

    def test_committed_ack_loss_replay_and_hold(self):
        self.exercise(False)

    def test_gzip_committed_ack_loss_and_replay(self):
        self.exercise(True)

    def exercise(self, compressed):
        calls = []
        committed = {}

        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                calls.append((self.path, body, self.headers.get('Authorization'), self.headers.get('Accept-Encoding')))
                key = json.loads(body)['idempotency_key']
                committed.setdefault(key, '00000000-0000-0000-0000-000000000001')
                ack = json.dumps({'transaction': {'status': 'COMMITTED', 'id': committed[key]}}).encode()
                if compressed:
                    ack = gzip.compress(ack)
                self.send_response(200)
                if compressed:
                    self.send_header('Content-Encoding', 'gzip')
                self.send_header('Content-Length', str(len(ack)))
                self.end_headers()
                self.wfile.write(ack)

            def do_GET(self):
                calls.append((self.path, b'', self.headers.get('Authorization')))
                self.send_response(200)
                self.send_header('Content-Length', '2')
                self.end_headers()
                self.wfile.write(b'[]')

        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        with tempfile.TemporaryDirectory() as directory:
            proxy = module.Proxy(('127.0.0.1', 0), f'http://127.0.0.1:{upstream.server_port}', directory, 3)
            threading.Thread(target=proxy.serve_forever, daemon=True).start()
            base = f'http://127.0.0.1:{proxy.server_port}'

            def control(action, payload):
                request = urllib.request.Request(base + '/__faults/' + action,
                                                 data=json.dumps(payload).encode(), method='POST')
                with urllib.request.urlopen(request, timeout=3) as response:
                    self.assertEqual(response.status, 200)

            path = '/v1/datasets/one/outputs:commit'
            body = b'{ "idempotency_key": "stable", "data": [1,2] }'
            control('drop-next', {'method': 'POST', 'path': path, 'key': 'stable', 'hold_replay': compressed})
            connection = http.client.HTTPConnection('127.0.0.1', proxy.server_port, timeout=3)
            connection.request('POST', path, body, {'Authorization': 'Bearer private-test-token', 'Accept-Encoding': 'gzip'})
            with self.assertRaises(http.client.RemoteDisconnected):
                connection.getresponse()
            connection.close()
            self.assertEqual(len(committed), 1)  # Upstream success predates client failure.
            release_thread = None
            if compressed:
                def release_replay():
                    import time
                    deadline = time.monotonic()+2
                    while 'hold_started' not in (pathlib.Path(directory)/'events.jsonl').read_text():
                        if time.monotonic()>deadline: raise AssertionError('replay did not hold')
                        time.sleep(.01)
                    self.assertEqual(len(calls),1)
                    control('release',{})
                release_thread=threading.Thread(target=release_replay)
                release_thread.start()
            request = urllib.request.Request(base + path, data=body,
                                             headers={'Authorization': 'Bearer private-test-token', 'Accept-Encoding': 'gzip'})
            with urllib.request.urlopen(request, timeout=3) as response:
                raw = response.read()
                if compressed:
                    self.assertEqual(response.headers.get('Content-Encoding'), 'gzip')
                    raw = gzip.decompress(raw)
                self.assertEqual(json.loads(raw)['transaction']['id'], committed['stable'])
            if release_thread: release_thread.join(3)
            self.assertEqual(len(committed), 1)
            self.assertEqual(calls[:2], [(path, body, 'Bearer private-test-token', 'gzip')] * 2)
            events = [json.loads(line) for line in (pathlib.Path(directory) / 'events.jsonl').read_text().splitlines()]
            self.assertEqual([event['action'] for event in events if event['action'] in ('ack_dropped','forwarded')], ['ack_dropped', 'forwarded'])
            self.assertEqual(events[0]['request_sha256'], hashlib.sha256(body).hexdigest())
            self.assertEqual((pathlib.Path(directory) / events[0]['request_file']).read_bytes(), body)
            self.assertNotIn('private-test-token', (pathlib.Path(directory) / 'events.jsonl').read_text())
            control('hold-next', {'method': 'GET', 'path': '/rows'})
            finished = threading.Event()
            def fetch():
                with urllib.request.urlopen(base + '/rows', timeout=3) as response:
                    self.assertEqual(response.read(), b'[]')
                finished.set()
            thread = threading.Thread(target=fetch)
            thread.start()
            # Wait for deterministic evidence rather than assuming a timing sleep.
            import time
            deadline = time.monotonic() + 2
            while 'hold_started' not in (pathlib.Path(directory) / 'events.jsonl').read_text():
                if time.monotonic() > deadline:
                    self.fail('hold did not start')
                finished.wait(0.01)
            self.assertFalse(finished.is_set())
            self.assertFalse(any(call[0] == '/rows' for call in calls))
            control('release', {})
            thread.join(3)
            self.assertTrue(finished.is_set())
            proxy.shutdown()
            proxy.server_close()
        upstream.shutdown()
        upstream.server_close()


if __name__ == '__main__':
    unittest.main()
