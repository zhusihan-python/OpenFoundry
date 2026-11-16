#!/usr/bin/env python3
"""Read-only final quiet facts for the exact owned HTTP fixture source union."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ENGINE_SHA = 'de6eb2bcbc6be34b57f6f7ab4da3be535fec9b140c2646bcbfcc9214395259a1'
BASES = {'fixture': 'http://127.0.0.1:17092', 'proxy_admin': 'http://127.0.0.1:17093'}


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def canonical(value): return json.dumps(value, indent=2) + '\n'
def engine():
    path = HERE / 'smoke_p25.py'
    if sha(path) != ENGINE_SHA: raise ValueError('frozen engine mismatch')
    spec = importlib.util.spec_from_file_location('p25_quiet_engine', path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


def source_union(handles_paths, m, resolve=Path):
    inputs, sources = [], set()
    for p in handles_paths:
        p = str(Path(p).absolute())
        data = json.loads(resolve(p).read_text())
        inputs.append({'path': p, 'sha256': sha(resolve(p))})
        for h in data['handles']:
            if h.get('ok') is True and h.get('kind') == 'source':
                src = h.get('config', {}).get('sync', {}).get('query_params', {}).get('src')
                if not m._is_uuidish(h.get('ref')) or src not in m.SRC_WINDOWS:
                    raise ValueError('source handle missing actual UUID / owned fixture src')
                sources.add(src)
    if not sources: raise ValueError('empty owned source set')
    return inputs, sorted(sources)


def collect(handles_paths, root, api_factory=None, released=False):
    if api_factory is None and (not released or os.environ.get("P25_CONFIRM") != "yes"):
        raise ValueError("readonly IO not released")
    m = engine()
    inputs, sources = source_union(handles_paths, m)
    root = Path(root).resolve(); root.mkdir(parents=True, exist_ok=False)
    rec = m.AttemptRecorder(str(root), 'final-quiet')
    api = (api_factory(m, rec) if api_factory else
           m.HttpApi(BASES, os.environ.get('OPENFOUNDRY_SMOKE_JWT', ''), rec, 'final-quiet'))
    quiet = m.P25Engine(lambda *a, **kw: api, {}, rec).final_quiet(api, sources)
    records = sorted(Path(rec.dir).glob('final-quiet-attempt-*.json'))
    for p in records:
        d = json.loads(p.read_text())
        if d.get('base') == 'fixture':
            body = json.loads(d.get('resp_body', '{}'))
            if body.get('name') != d.get('path', '').rsplit('/', 1)[-1]:
                raise ValueError('fixture body.name differs from requested owned source')
    proof = {'schema': 'p25-owned-quiet-proof-v1', 'ok': quiet['ok'],
        'handles_inputs': inputs, 'source_names': sources, 'bases': BASES,
        'engine_sha256': ENGINE_SHA, 'quiet': quiet,
        'raw_records': [{'path': str(p), 'sha256': sha(p)} for p in records]}
    (root / 'proof.json').write_text(canonical(proof))
    return proof, root / 'proof.json'


def verify_quiet_proof(path, expected_handles, resolve_path=None):
    """Pure replay only. resolve_path maps sealed original paths into offline bundle."""
    resolve = resolve_path or Path
    m = engine()
    proof = json.loads(resolve(str(path)).read_text())
    if proof.get('schema') != 'p25-owned-quiet-proof-v1' or proof.get('bases') != BASES or proof.get('engine_sha256') != ENGINE_SHA:
        raise ValueError('quiet proof contract mismatch')
    inputs, sources = source_union(expected_handles, m, resolve)
    if proof.get('handles_inputs') != inputs or proof.get('source_names') != sources:
        raise ValueError('quiet proof source/handle union mismatch')
    rows = {}
    for member in proof.get('raw_records', []):
        p = resolve(member['path'])
        if sha(p) != member['sha256']: raise ValueError('quiet raw SHA mismatch')
        d = json.loads(p.read_text()); raw = d.get('resp_body', '').encode()
        if d.get('method') != 'GET' or d.get('status') != 200 or d.get('resp_sha256') != hashlib.sha256(raw).hexdigest() or d.get('resp_bytes') != len(raw):
            raise ValueError('quiet HTTP raw status/body hash mismatch')
        key = (d.get('base'), d.get('path'))
        if key in rows: raise ValueError('duplicate quiet raw request')
        body = json.loads(raw)
        if d.get('base') == 'fixture' and body.get('name') != d.get('path', '').rsplit('/', 1)[-1]:
            raise ValueError('fixture raw body.name differs from exact requested source')
        rows[key] = body
    expected = {('proxy_admin', BASES['proxy_admin'] + '/admin/stats')} | {
        ('fixture', BASES['fixture'] + '/admin/src/' + src) for src in sources}
    if set(rows) != expected: raise ValueError('quiet raw request set not exact')
    class Replay:
        def call(self, base, method, path, **kw):
            if method != 'GET': raise ValueError('quiet replay mutation forbidden')
            return {'status': 200, 'json': rows[(base, BASES[base] + path)]}
    api = Replay()
    quiet = m.P25Engine(None, {}, None).final_quiet(api, sources)
    if quiet != proof.get('quiet') or quiet['ok'] is not True or proof.get('ok') is not True:
        raise ValueError('quiet adjudication mismatch/failure')
    return quiet


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--handles', action='append', required=True)
    p.add_argument('--evidence-root', required=True)
    p.add_argument('--released', action='store_true')
    a = p.parse_args(argv)
    if not a.released or os.environ.get('P25_CONFIRM') != 'yes':
        print('REFUSED: --released and P25_CONFIRM=yes required', file=sys.stderr); return 3
    try:
        proof, path = collect(a.handles, a.evidence_root, released=a.released)
        print(json.dumps({'proof_file': str(path), 'ok': proof['ok']}))
        return 0 if proof['ok'] else 1
    except Exception as exc:
        print('quiet proof failed: ' + type(exc).__name__, file=sys.stderr); return 1


if __name__ == '__main__': raise SystemExit(main())
