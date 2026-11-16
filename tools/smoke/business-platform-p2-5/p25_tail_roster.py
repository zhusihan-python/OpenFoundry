#!/usr/bin/env python3
"""Runtime HTTP-v1 semantic declarations; pure exact dual-roster replay.

Reuses root-reviewed HTTP pure loader/adjudicator. Archive separately
checks global full-27 rows. Future UUIDs are never prerequisites.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile

HERE = Path(__file__).resolve().parent
HTTP_SHA256 = 'de6eb2bcbc6be34b57f6f7ab4da3be535fec9b140c2646bcbfcc9214395259a1'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def http_pure():
    path = HERE / 'smoke_p25.py'
    if sha(path) != HTTP_SHA256:
        raise ValueError('HTTP engine differs from root-reviewed semantic contract')
    spec = importlib.util.spec_from_file_location('p25_tail_http_pure', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def declare(spec, known_bindings=None, provenance=None):
    if not isinstance(spec, dict) or (known_bindings is not None and not isinstance(known_bindings, dict)):
        raise ValueError('spec/known_bindings must be objects')
    return {'schema': 'p25-tail-roster-v1', 'ok': True, 'spec': spec,
            'declared_at': datetime.now(timezone.utc).isoformat(),
            'declaration_sha256': canonical_sha(spec),
            'known_bindings': known_bindings or {}, 'provenance': provenance or {}}


def default_declaration(h10_file, failed_summary):
    engine = http_pure()
    h10 = json.loads(Path(h10_file).read_text())
    known = h10.get('known_bindings')
    if not isinstance(known, dict) or 'h10-held' not in known or h10.get('semantics') != 'p25-crash-held-prepublish':
        raise ValueError('h10 file must contain actual known bindings and prepublish semantics')
    if any(not isinstance(ids, dict) or set(ids) != {'def_id','source_id','run_id'} or not all(engine._is_uuidish(v) for v in ids.values()) for ids in known.values()):
        raise ValueError('h10 known bindings must be exact actual UUID triples')
    known = dict(known)
    baseline = json.loads(Path(failed_summary).read_text())
    rows = baseline.get('final_owner_roster')
    if not isinstance(rows, list) or not rows or 'h13' not in baseline.get('failed', []):
        raise ValueError('failedhealthy baseline must retain actual h13 failure owners')
    hp = Path(baseline['attempts']['handles_file'])
    handles = json.loads(hp.read_text())['handles']
    h13defs = {h['ref'] for h in handles if h.get('ok') is True and h.get('kind') == 'def' and h.get('scenario') == 'h13'}
    h13sources = {h['ref'] for h in handles if h.get('ok') is True and h.get('kind') == 'source' and h.get('scenario') == 'h13'}
    if len({r.get('def_id') for r in rows}) != len(rows):
        raise ValueError('duplicate failedhealthy owner def')
    proofs = []
    owners = []
    existing_rows = h10.get('retained_owner_rows')
    if not isinstance(existing_rows, list) or len(existing_rows) != len(rows):
        raise ValueError('h10 retained owner origins differ from failedhealthy baseline')
    existing = {r.get('def_tag'): r for r in existing_rows}
    expected_tags = {'failedhealthy-h13-' + r['def_id'] for r in rows}
    if set(known) != expected_tags | {'h10-held'} or set(existing) != expected_tags or len(existing) != len(existing_rows):
        raise ValueError('h10 known owner set missing/extra or origin tags differ')
    for row in sorted(rows, key=lambda x: x['def_id']):
        ids = {k: row.get(k) for k in ('def_id', 'source_id', 'run_id')}
        if row.get('run_status') != 'failed' or row.get('blocked_reason') != 'requires_reconciliation' or not all(engine._is_uuidish(v) for v in ids.values()):
            raise ValueError('failedhealthy baseline owner state/parents differ')
        if ids['def_id'] not in h13defs or ids['source_id'] not in h13sources:
            raise ValueError('owner is not an actual failedhealthy h13 handle')
        hits = []
        for raw in Path(baseline['attempts']['evidence_dir']).glob('h13-attempt-*.json'):
            response = json.loads(raw.read_text())
            try:
                run = json.loads(response.get('resp_body', ''))
            except (ValueError, TypeError):
                continue
            if isinstance(run, dict) and run.get('id') == ids['run_id'] and run.get('sync_def_id') == ids['def_id'] and run.get('status') == 'failed' and run.get('error') == 'dataset-versioning commit_output responded with status 403' and run.get('publish_entered_at') is not None and response.get('method') == 'GET' and response.get('status') == 200:
                hits.append({'path': str(raw.resolve()), 'sha256': sha(raw)})
        if not hits:
            raise ValueError('failedhealthy owner lacks actual postpublish-403 Run GET proof')
        proofs.extend(hits)
        tag = 'failedhealthy-h13-' + row['def_id']
        actual_origin = existing[tag]
        expected_origin = {'def_tag':tag,'run_ref':'terminal-failed',
                           'blocked_reason':'requires_reconciliation','origin':'failedhealthy',
                           'source_summary':str(Path(failed_summary).resolve()),
                           'source_summary_sha256':sha(failed_summary)}
        if known[tag] != ids or actual_origin != expected_origin:
            raise ValueError('h10 existing owner identity/origin differs from exact failedhealthy proof')
        owners.append(dict(actual_origin))
    owners.append({'def_tag': 'h10-held', 'run_ref': 'same',
                   'blocked_reason': 'requires_reconciliation', 'origin': 'h10-prepublish-crash'})
    for mode in ('no_txn_200', 'not_committed', 'wrong_target', 'http_500', 'drop_after_verify'):
        owners.append({'def_tag': 'h14-drop' if mode == 'drop_after_verify' else 'h11-' + mode,
                       'mode': mode, 'run_ref': 'terminal-failed',
                       'blocked_reason': 'requires_reconciliation', 'origin': 'intentional-tail'})
    spec = {'owner_rows': owners, 'run_rows': [{'def_tag': 'h10-held',
            'source_tag': 'h10-held-source', 'semantics': 'p25-crash-held-prepublish', 'count': 1}],
            'produced_by': 'p25-tail'}
    return declare(spec, known, {'h10_binding_file': str(Path(h10_file).resolve()),
        'h10_binding_sha256': sha(h10_file), 'failed_healthy_summary': str(Path(failed_summary).resolve()),
        'failed_healthy_summary_sha256': sha(failed_summary),
        'failed_healthy_handles': str(hp), 'failed_healthy_handles_sha256': sha(hp),
        'failed_healthy_owner_run_proofs': proofs, 'http_engine_sha256': HTTP_SHA256})


def verify(declaration_path, summary_path, bindings_path=None):
    engine = http_pure()
    declaration, error = engine.load_semantic_declaration(str(declaration_path))
    if error:
        return {'ok': False, 'reasons': [error]}
    summary = json.loads(Path(summary_path).read_text())
    if not isinstance(summary, dict) or summary.get('phase') != 'tail' or summary.get('failed') != []:
        return {'ok': False, 'reasons': ['tail summary missing/failed/wrong phase']}
    problems = []
    if summary.get('quiet', {}).get('ok') is not True or summary.get('roster_problems'):
        problems.append('tail finally quiet/parent collection failed')
    owners, runs = summary.get('final_owner_roster'), summary.get('final_run_roster')
    if not isinstance(owners, list) or not isinstance(runs, list):
        return {'ok': False, 'reasons': ['both actual final rosters are mandatory']}
    modes = {(r.get('def_id'), r.get('run_id')): r.get('mode') for r in summary.get('owner_roster', [])}
    owners = [dict(o, mode=modes.get((o.get('def_id'), o.get('run_id')))) for o in owners]
    verdict = engine.adjudicate_tail(declaration, owners, runs)
    problems.extend(verdict['problems'])
    path = Path(bindings_path or summary.get('tail_bindings_file', ''))
    binding = json.loads(path.read_text())
    if binding.get('schema') != 'p25-tail-bindings-v1' or binding.get('problems') != []:
        problems.append('actual binding schema/problems invalid')
    if binding.get('declaration_sha256') != declaration['declaration_sha256']:
        problems.append('binding declaration SHA differs')
    if Path(binding.get('declaration', '')).resolve() != Path(declaration_path).resolve():
        problems.append('binding declaration path differs')
    if binding.get('summary_sha256') != sha(summary_path):
        problems.append('binding does not bind exact final summary bytes')
    if binding.get('bindings_sha256') != canonical_sha(binding.get('bindings')):
        problems.append('binding identities SHA differs')
    if binding.get('bindings') != verdict['bindings'] or summary.get('tail_bindings') != verdict['bindings']:
        problems.append('actual identities differ from exact re-adjudication')
    saved = summary.get('tail_adjudication', {})
    if saved.get('ok') is not True or saved.get('problems') != [] or saved.get('declaration_sha256') != declaration['declaration_sha256']:
        problems.append('summary verdict does not bind same declaration')
    return {'schema': 'p25-tail-runtime-verdict-v1', 'ok': not problems, 'all_ok': not problems,
            'reasons': problems, 'declaration_sha256': declaration['declaration_sha256'],
            'bindings_sha256': binding.get('bindings_sha256'), 'summary_sha256': sha(summary_path),
            'owners': len(owners), 'runs': len(runs), 'bindings': verdict['bindings']}


def actual_owner_roster(declaration_path, summary_path, bindings_path=None):
    verdict = verify(declaration_path, summary_path, bindings_path)
    if verdict.get('ok') is not True:
        raise ValueError('actual binding verification failed: ' + str(verdict.get('reasons')))
    declaration = json.loads(Path(declaration_path).read_text())
    return [{**verdict['bindings'][row['def_tag']], 'tag':row['def_tag'],
             'run_ref':row['run_ref'],'blocked_reason':row['blocked_reason'],
             'origin':row.get('origin','intentional-tail')}
            for row in declaration['spec']['owner_rows']]


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest='mode', required=True)
    p = commands.add_parser('declare')
    for flag in ('spec', 'known-bindings', 'h10-binding', 'failed-healthy-summary'):
        p.add_argument('--' + flag)
    p.add_argument('--out', required=True)
    p = commands.add_parser('owner-roster')
    for flag in ('declaration', 'summary'):
        p.add_argument('--' + flag, required=True)
    p.add_argument('--bindings')
    p.add_argument('--out', required=True)
    p = commands.add_parser('verify')
    for flag in ('declaration', 'summary'):
        p.add_argument('--' + flag, required=True)
    p.add_argument('--bindings')
    p.add_argument('--out', required=True)
    args = parser.parse_args()
    try:
        if args.mode == 'declare':
            if args.spec:
                doc = declare(json.loads(Path(args.spec).read_text()),
                    json.loads(Path(args.known_bindings).read_text()) if args.known_bindings else {})
            else:
                if not args.h10_binding or not args.failed_healthy_summary:
                    raise ValueError('declare requires spec or actual h10 + failedhealthy inputs')
                doc = default_declaration(args.h10_binding, args.failed_healthy_summary)
            text = json.dumps(doc, indent=2) + '\n'
            with tempfile.TemporaryDirectory() as td:
                candidate = Path(td) / 'declaration.json'; candidate.write_text(text)
                _, error = http_pure().load_semantic_declaration(str(candidate))
                if error:
                    raise ValueError(error)
            with Path(args.out).open('x') as output:
                output.write(text)
        elif args.mode == 'verify':
            doc = verify(args.declaration, args.summary, args.bindings)
            Path(args.out).write_text(json.dumps(doc, indent=2) + '\n')
        else:
            rows = actual_owner_roster(args.declaration, args.summary, args.bindings)
            Path(args.out).write_text(json.dumps(rows, indent=2) + '\n')
            doc = {'ok': True}
        print(json.dumps({'ok': doc.get('ok'), 'out': args.out}))
        return 0 if doc.get('ok') is True else 1
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print('tail roster refused: ' + str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
