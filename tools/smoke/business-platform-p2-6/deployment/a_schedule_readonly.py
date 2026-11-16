#!/usr/bin/env python3
"""Bounded read-only schedule exclusion gate; never disarms or updates a Def."""
import json
import sys
from pathlib import Path
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import p26_gates as G
from preflight import p26_current_preflight as W


def protected_ids(release):
    typed_path = Path(release['typed_roster'])
    assert G.sha256_file(typed_path) == release['typed_roster_sha256']
    typed = json.loads(typed_path.read_bytes())
    assert typed['schema'] == 'p26-w10-typed-v1'
    _, catalog, derived = W.load_baseline(G.P25_FINAL_DIR)
    views = W.public_view_baselines(derived, catalog)
    ids = {v['def_id'] for v in views} | {e['def_id'] for e in typed['runs']}
    assert ids and all(G.UUID_RE.fullmatch(i) for i in ids)
    assert {v['def_id'] for v in typed['public_views']} == {e['def_id'] for e in typed['runs']}
    return sorted(ids)


def argv(pg_cid, ids):
    assert len(pg_cid) == 64 and all(c in '0123456789abcdef' for c in pg_cid)
    assert ids == sorted(set(ids)) and ids and all(G.UUID_RE.fullmatch(i) for i in ids)
    literals = ','.join("'"+i+"'::uuid" for i in ids)
    sql = ("BEGIN READ ONLY; SET LOCAL statement_timeout = '5s'; SET LOCAL lock_timeout = '1s'; "
           "SELECT json_build_object('protected_defs', COALESCE((SELECT json_agg(row_to_json(t) ORDER BY t.id) FROM "
           "(SELECT id::text, schedule_enabled, schedule_next_fire_at FROM batch_sync_defs WHERE id IN ("+literals+")) t), '[]'::json), "
           "'global_enabled', COALESCE((SELECT json_agg(id::text ORDER BY id) FROM batch_sync_defs WHERE schedule_enabled), '[]'::json), "
           "'global_due_count', (SELECT count(*) FROM batch_sync_defs WHERE schedule_enabled AND schedule_next_fire_at IS NOT NULL AND schedule_next_fire_at <= now()), "
           "'global_grants_count', (SELECT count(*) FROM sync_schedule_grants), "
           "'nonterminal_runs', COALESCE((SELECT json_agg(row_to_json(t) ORDER BY t.id) FROM "
           "(SELECT id::text, sync_def_id::text, control_mode, recovery_protocol, status FROM sync_runs WHERE status IN ('pending','running')) t), '[]'::json)); COMMIT;")
    return ['docker', 'exec', pg_cid, 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'openfoundry', '-d', 'connectors', '-c', sql]


def validate(raw, ids):
    data = json.loads(raw)
    assert set(data) == {'protected_defs', 'global_enabled', 'global_due_count', 'global_grants_count', 'nonterminal_runs'}
    rows = data['protected_defs']
    assert len(rows) == len(ids) and {r['id'] for r in rows} == set(ids)
    for row in rows:
        assert set(row) == {'id', 'schedule_enabled', 'schedule_next_fire_at'}
        assert row['schedule_enabled'] is False and row['schedule_next_fire_at'] is None
    assert data['global_enabled'] == [] and data['global_due_count'] == data['global_grants_count'] == 0
    # Whole immutable nonterminal rosters are adjudicated independently by H17ab.
    # Retain this unfiltered inventory as original RO facts for Root review.
    runs = data['nonterminal_runs']
    assert isinstance(runs, list) and len({r['id'] for r in runs}) == len(runs)
    for row in runs:
        assert set(row) == {'id', 'sync_def_id', 'control_mode', 'recovery_protocol', 'status'}
        assert G.UUID_RE.fullmatch(row['id']) and G.UUID_RE.fullmatch(row['sync_def_id'])
        assert row['status'] in ('pending', 'running')
    return data


def check(release, command, tag):
    ids = protected_ids(release)
    assert G.sha256_file(release['deployment_record']) == release['deployment_record_sha256']
    rec = json.loads(Path(release['deployment_record']).read_bytes())
    cmd = argv(rec['containers'][W.Q3_POSTGRES]['cid'], ids)
    raw, prefix = command('schedule-exclusion-'+tag, cmd)
    data = validate(raw, ids)
    return {'protected_ids': ids, 'original_receipt_prefix': prefix, 'facts': data}
