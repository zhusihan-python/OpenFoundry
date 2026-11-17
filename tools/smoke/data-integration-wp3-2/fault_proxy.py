#!/usr/bin/env python3
"""Test-only DVS reverse proxy: confirmed commit ACK loss and bounded read holds."""
import argparse
import hashlib
import gzip
import http.client
import json
import pathlib
import socket
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOP = {'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
       'te', 'trailer', 'transfer-encoding', 'upgrade', 'host', 'content-length'}


class Proxy(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, upstream, evidence, timeout=30):
        super().__init__(address, Handler)
        self.upstream = upstream.rstrip('/')
        self.evidence = pathlib.Path(evidence)
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.timeout_seconds = timeout
        self.lock = threading.Lock()
        self.drop = None
        self.hold = None
        self.release = threading.Event()
        self.sequence = max((int(p.name.split(".",1)[0]) for p in self.evidence.glob("*.request.bin") if p.name.split(".",1)[0].isdigit()), default=0)
        event_log = self.evidence / "events.jsonl"
        if event_log.exists():
            for line in event_log.read_text().splitlines():
                try:
                    self.sequence = max(self.sequence, int(json.loads(line).get("sequence",0)))
                except (ValueError, TypeError):
                    pass
        self.opener = urllib.request.build_opener(NoRedirect)

    def record(self, entry, body=None):
        with self.lock:
            self.sequence += 1
            entry = dict(entry, sequence=self.sequence, time=time.time())
            if body is not None:
                name = f'{self.sequence:06d}.request.bin'
                with (self.evidence / name).open("xb") as raw:
                    raw.write(body)
                entry['request_file'] = name
            with (self.evidence / 'events.jsonl').open('a') as out:
                out.write(json.dumps(entry, sort_keys=True) + '\n')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def matches(rule, method, path, key):
    return rule is not None and rule['method'] == method and rule['path'] == path and (
        not rule.get('key') or rule['key'] == key)


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass  # Never emit request headers or bearer credentials.

    def setup(self):
        super().setup()
        self.connection.settimeout(self.server.timeout_seconds)

    def reply(self, status, body=b'{}', headers=None):
        self.send_response(status)
        for key, value in (headers or {}).items():
            if key.lower() not in HOP:
                self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_request(self):
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if length < 0 or length > 64 * 1024 * 1024:
                return self.reply(413)
            body = self.rfile.read(length)
            if len(body) != length:
                return self.reply(400)
            if self.path.startswith('/__faults/'):
                return self.control(body)
            try:
                key = json.loads(body).get('idempotency_key', '') if body else ''
            except (ValueError, AttributeError):
                key = ''
            event = {'method': self.command, 'path': self.path,
                     'key': key, 'request_sha256': hashlib.sha256(body).hexdigest()}
            with self.server.lock:
                hold = matches(self.server.hold, self.command, self.path, key)
                if hold:
                    self.server.hold = None
            if hold:
                self.server.record(dict(event, action='hold_started'))
                released = self.server.release.wait(self.server.timeout_seconds)
                self.server.record(dict(event, action='hold_released' if released else 'hold_timeout'))
                if not released:
                    return self.reply(504)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
            request = urllib.request.Request(self.server.upstream + self.path, data=body if length else None,
                                             headers=headers, method=self.command)
            try:
                response = self.server.opener.open(request, timeout=self.server.timeout_seconds)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                raw = response.read()
                status = response.status
                response_headers = dict(response.headers.items())
            event['upstream_status'] = status
            try:
                encoding = response_headers.get("Content-Encoding", response_headers.get("content-encoding", "")).lower()
                ack_bytes = gzip.decompress(raw) if encoding == "gzip" else raw
                ack = json.loads(ack_bytes)
                txn = ack.get('transaction', {})
                committed = 200 <= status < 300 and txn.get('status') == 'COMMITTED' and bool(txn.get('id'))
            except (ValueError, AttributeError):
                txn, committed = {}, False
            with self.server.lock:
                rule_hold_replay = bool(self.server.drop and self.server.drop.get("hold_replay"))
                drop = committed and matches(self.server.drop, self.command, self.path, key)
                if drop:
                    self.server.drop = None
                    if rule_hold_replay:
                        self.server.release.clear()
                        self.server.hold = {"method": self.command, "path": self.path, "key": key}
            if committed:
                event['committed_transaction_id'] = txn['id']
            event['action'] = 'ack_dropped' if drop else 'forwarded'
            self.server.record(event, body)
            if drop:
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.reply(status, raw, response_headers)
        except (OSError, ValueError, http.client.HTTPException):
            self.server.record({'method': self.command, 'path': self.path, 'action': 'transport_error'})
            self.close_connection = True
            try:
                self.reply(502)
            except OSError:
                pass

    def control(self, body):
        if self.command != 'POST':
            return self.reply(405)
        if self.path == '/__faults/release':
            self.server.release.set()
            return self.reply(200)
        if self.path not in ('/__faults/drop-next', '/__faults/hold-next'):
            return self.reply(404)
        rule = json.loads(body)
        if not isinstance(rule, dict) or not str(rule.get('path', '')).startswith('/'):
            return self.reply(400)
        rule = {k: rule[k] for k in ('method', 'path', 'key', 'hold_replay') if k in rule}
        rule['method'] = str(rule.get('method', 'POST')).upper()
        with self.server.lock:
            if self.path.endswith('drop-next'):
                if self.server.drop is not None:
                    return self.reply(409)
                self.server.drop = rule
            else:
                if self.server.hold is not None:
                    return self.reply(409)
                self.server.release.clear()
                self.server.hold = rule
        self.reply(200)

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = handle_request


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upstream', required=True)
    parser.add_argument('--listen', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--evidence-dir', required=True)
    parser.add_argument('--timeout', type=float, default=30)
    args = parser.parse_args()
    if not 0 < args.timeout <= 300:
        parser.error('timeout must be >0 and <=300 seconds')
    Proxy((args.listen, args.port), args.upstream, args.evidence_dir, args.timeout).serve_forever()
