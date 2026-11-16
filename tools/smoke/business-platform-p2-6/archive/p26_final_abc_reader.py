"""Independent final ABC originals. No producer imports, private reads or live IO.

p: kind/epoch/canonical_root_label/audit (relative), outer (relative),
launch_rc (relative), creation_proof/current_main_record (absolute SHA refs).
The original report contains HTTP/artifact semantic assertions, not full archived
HTTP response bodies. C historical SQL has FIVE fields; never thirteen/full27.
"""
import hashlib
import importlib.util
import json
import re
from datetime import datetime
from pathlib import Path

CREATION_SHA = '464edc4e80c3439d2e350556876237b0d2db94ad177ef9ba0efe8ef35946b812'
CASES = ('heartbeat_progress', 'lease_loss_fencing', 'terminal_commit_fault')


def need(ok, why):
    if not ok:
        raise ValueError('final ABC: ' + why)


def mod(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def semantics(report, targets):
    need(report['partial'] is False and report['official'] is True
         and report['counts'] == {'pass': 3, 'fail': 0}
         and set(report['results']) == set(report['evidence']) == set(CASES)
         and all(v['status'] == 'pass' for v in report['results'].values()), 'exact original three mandatory cases')
    a, b, c = (report['evidence'][k] for k in CASES)
    need(set(a) == {'heartbeat_advanced', 'elapsed_at_advance_s', 'attempt_generation_stable', 'owner_vs_requester', 'elapsed_s'}
         and a['heartbeat_advanced'] is True and a['attempt_generation_stable'] is True
         and a['owner_vs_requester'] == {'owner_is_source_owner': True, 'requester_is_admin': True}
         and type(a['elapsed_at_advance_s']) in (int, float) and 0 < a['elapsed_at_advance_s'] < 25
         and a['elapsed_s'] >= a['elapsed_at_advance_s'], 'A real in-flight heartbeat and stable attempt/generation')
    need(set(b) == {'expiry_injected', 'http_status', 'lease_lost_marker', 'pages_before_inject', 'pages_after_wait', 'target_files', 'db_status'}
         and b['expiry_injected'] is True and b['http_status'] == 409 and b['lease_lost_marker'] is True
         and b['pages_before_inject'] == b['pages_after_wait'] == 2 and b['target_files'] == 0
         and b['db_status'] == 'running', 'B injected expiry/409/no follow-on pages/no files')
    need(set(c) == {'files_before', 'http_status', 'fault_marker_present', 'db_status', 'lease_claimed', 'attempt', 'generation', 'files_after', 'logical_path', 'txn_bound', 'txn_status', 'lineage_run_match', 'artifact_sha256_matches_txn', 'fact', 'db_status_after_unblock'}
         and c['files_before'] == 0 and c['files_after'] == 1 and c['http_status'] == 503
         and c['fault_marker_present'] is True and c['lease_claimed'] is True
         and type(c['attempt']) is type(c['generation']) is int and c['attempt'] == c['generation'] == 1
         and c['db_status'] == c['db_status_after_unblock'] == 'running'
         and c['logical_path'] == 'sync-' + targets['C']['run_id'] + '.ndjson'
         and c['txn_bound'] is True and c['txn_status'] == 'COMMITTED'
         and c['lineage_run_match'] is True and c['artifact_sha256_matches_txn'] is True
         and 'committed-but-unconfirmed' in c['fact'], 'C injected503/committed sameRun artifact/retained running')


def abc(r, p, S):
    h = mod('p26_h10_actual_reader')
    need(p['kind'] == 'final-ABC3-actual-component' and p['epoch'] == S, 'kind/epoch')
    a = r.doc(p['audit'])
    mod('p26_final_actual_readers').exact_members(r, p, a, 487)
    need(a['schema'] == 'p26-S2-final-ABC3-original-audit-v1' and a['source_before_after_original_rc'] == [0, 0]
         and a['original_outer_child_suite_rc'] == [0, 0, 0], 'actual three Unix layers/source checks')
    r.rc(p['launch_rc']); outer = p['outer']; r.rc(outer + '/child/rc')
    terminal = r.doc(outer + '/RESULT.json')
    need(terminal['ok'] is True and terminal['suite'] == 'p21' and terminal['stage'] == 'p21-ABC-final'
         and terminal['candidate_source'] == S and terminal['child_original_rc'] == 0 and terminal['error'] is None, 'outer original terminal')
    for side in ('source-before', 'source-after'):
        r.rc(outer + '/' + side + '/rc')
        need(S in r.text(outer + '/' + side + '/stdout.log'), 'original source before/after')
    tools = r.doc(outer + '/tools-before.json')
    need(tools == r.doc(outer + '/tools-after.json'), 'all imported tool pins stable')
    for path, digest in tools.items():
        relative = outer + '/tools-before/' + path
        need(relative.removeprefix('tools/smoke/business-platform-p2-6/') in a['files'], 'tool snapshot absent from exact487 closure')
        need(hashlib.sha256(r.read(relative)).hexdigest() == digest, 'historical tool bytes')
    report = json.loads(h.ref(r, p, a['report'])); adapter = json.loads(h.ref(r, p, a['adapter']))
    semantics(report, {v['case']: v for v in a['new_BC_targets']})
    need(terminal['links']['adapter'] == {k: a['adapter'][k] for k in ('path', 'sha256')}
         and terminal['links']['original_report']['sha256'] == a['report']['sha256'], 'original outer report/adapter linkage')
    need(adapter['suite'] == 'p21' and adapter['suite_rc'] == 0 and adapter['raised'] is None
         and adapter['abort_reason'] is None and adapter['cleanup_ok'] is True
         and adapter['pending_holders'] == 0 and adapter['pending_sql_cleanup'] == []
         and adapter['guard_calls'] == adapter['expected_guard_calls'] == 0 and adapter['count_ok'] is True
         and adapter['release_sha256'] == terminal['release_sha256'], 'actual adapter original rc/cleanup/phase')
    suite = 'regressions/vendor/tree/business-platform-p2-1/smoke_p21.py'
    need(adapter['suite_sha256'] == tools[suite], 'actual suite source pin')
    stdout = r.text(outer + '/child/stdout.log')
    need(re.findall(r'^\[p21\] PASS (\w+)', stdout, re.M) == list(CASES)
         and 'OFFICIAL pass=3 fail=0' in stdout and '[p21] FAIL' not in stdout, 'original three-case stdout')
    argv = r.doc(outer + '/child/cmd.json')
    need(argv[argv.index('--suite') + 1] == 'p21' and argv[argv.index('--stage') + 1] == 'p21-ABC-final'
         and argv[argv.index('--only') + 1] == 'A,B,C', 'official exact full ABC invocation')
    need(p['creation_proof']['sha256'] == CREATION_SHA and p['creation_proof'] == {k: a['actual_creators'][k] for k in ('path', 'sha256')}, 'fixed actual nine-creator proof')
    creators = json.loads(h.ref(r, p, p['creation_proof']))
    need(creators['source_adapter'] == a['adapter'] and creators['source_report'] == a['report']
         and creators['receipts'] == adapter['actual_owned_receipts'] and len(creators['receipts']) == 9, 'nine actual original creation receipts')
    targets = {v['case']: v for v in creators['targets']}
    need(set(targets) == {'B', 'C'} and creators['targets'] == a['new_BC_targets'], 'exact new BC target links')
    receipts = creators['receipts']
    for index in range(3):
        src, ds, definition = receipts[3 * index:3 * index + 3]
        need([v['kind'] for v in (src, ds, definition)] == ['source', 'dataset', 'def']
             and all(v['method'] == 'POST' and v['status'] == 201 for v in (src, ds, definition)), 'actual three source/dataset/Def creator chains')
        source_row, dataset_row, def_row = [v['safe_actual_creation'] for v in (src, ds, definition)]
        need(def_row['source_id'] == source_row['id'] and def_row['output_dataset_id'] == dataset_row['id']
             and len({(v['actor'], v['tenant']) for v in (source_row, dataset_row, def_row)}) == 1, 'created source/Def/output actor tenant parent binding')
        if index:
            target = targets['B' if index == 1 else 'C']
            need((target['def_id'], target['source_id'], target['output_dataset_id'], target['actor'], target['tenant'], target['source_name'])
                 == (def_row['id'], source_row['id'], dataset_row['id'], def_row['actor'], def_row['tenant'], source_row['name']), 'BC actual creation target exact binding')
    semantics(report, targets)
    need(a['case_results'] == report['results'] and a['case_evidence'] == report['evidence'], 'audit vs original report full semantics')
    main = json.loads(h.ref(r, p, p['current_main_record']))
    need(main['source_sha256'] == S and set(main['containers']) == {'openfoundry-platform-p1-q3v9-' + n + '-1' for n in ('connector', 'dataset', 'pipeline', 'postgres', 'fixture')}, 'current main source/exact5')
    inspections = {q['argv'][2]: q for q in adapter['initial_guard']['records'] if q.get('argv', [])[:2] == ['docker', 'inspect']}
    need(adapter['initial_guard']['ok'] is True and set(main['containers']) <= set(inspections), 'original full initial identity guard')
    for name, pin in main['containers'].items():
        q = inspections[name]
        need(q['rc'] == 0 and q['stdout'].strip().splitlines() == [pin['cid'], pin['image'], pin['started_at'], 'true'], 'actual current main boot identity')
    steps = {}
    for path in a['files']:
        if '/suite-commands-' not in path or not path.endswith('.cmd.json'):
            continue
        stem = 'tools/smoke/business-platform-p2-6/' + path.removesuffix('.cmd.json')
        command = r.doc(stem + '.cmd.json'); meta = r.doc(stem + '.meta.json'); raw = r.read(stem + '.raw')
        r.rc(stem + '.rc'); steps[Path(stem).name] = (command, meta, raw)
    need(len(steps) == 66, 'all66 original creator/SQL/freeze/cleanup operations')
    created = [json.loads(raw) for name, (cmd, meta, raw) in sorted(steps.items()) if name.endswith('-actual-http-owned-create')]
    need(created == creators['receipts'], 'nine original creator raw projections exact')
    for name, (cmd, meta, raw) in steps.items():
        if name.endswith('-actual-http-owned-create'):
            row = json.loads(raw)
            need(cmd == {k: row[k] for k in ('method', 'path')} and meta['http_status'] == row['status'] == 201
                 and meta['original_rc'] is None, 'creation HTTP original status, no invented Unix child')
    # Original delegated commands are bound to their complete cmd/raw/rc capture.
    delegated = [(cmd, meta, raw) for name, (cmd, meta, raw) in sorted(steps.items()) if name.endswith('-delegate')]
    need(len(delegated) == len(adapter['commands']) == 46, 'all46 actual delegated SQL commands')
    for (cmd, meta, raw), recorded in zip(delegated, adapter['commands']):
        need(cmd == {'original': recorded['original_argv'], 'replacement': recorded['replacement_argv']}
             and recorded['rc'] == 0 and raw.endswith(b'\n')
             and hashlib.sha256(raw[:-1]).hexdigest() == recorded['stdout_sha256']
             and hashlib.sha256(meta['stderr'].encode()).hexdigest() == recorded['stderr_sha256'], 'delegated SQL original raw chain')
    # B is the only historical full27 frozen row. C remains the original5-field observation.
    B = targets['B']; C = targets['C']; frozen = a['frozen_B']; b27 = frozen['safe_projection_27']
    need(set(b27) == set(mod('p26_h10_anchor_reader').SAFE27)
         and adapter['runtime_frozen_B'] == {B['run_id']: frozen}
         and b27['id'] == B['run_id'] and b27['sync_def_id'] == B['def_id'] and b27['source_id'] == B['source_id']
         and b27['output_dataset_id'] == B['output_dataset_id'] and b27['status'] == 'running'
         and b27['gate_entered'] == b27['cancel_requested'] == b27['has_hash'] == b27['has_version'] == 'false'
         and b27['has_holder'] == b27['has_token'] == 'true' and b27['attempt'] == b27['generation'] == '1'
         and b27['finished_at'] == '' and b27['source_name'] == B['source_name'] and b27['fixture_src'] == B['fixture_src']
         and all(b27[k] == B['actor'] for k in ('owner_id', 'run_owner_id', 'requested_by'))
         and all(b27[k] == B['tenant'] for k in ('org_id', 'run_org_id', 'def_org_id', 'conn_org_id')), 'B original frozen full27 target')
    for n in (47, 54, 59):
        cmd, _, raw = steps[f'{n:02d}-B-safe27']; need(B['run_id'] in cmd[-1] and h.rows(raw) == [b27], 'B all three original full27 immutable')
    for n in (48, 55, 60):
        cmd, _, raw = steps[f'{n:02d}-B-DBclock-lease']; value = json.loads(next(line for line in raw.splitlines() if line.startswith(b'{')))
        need(B['run_id'] in cmd[-1] and type(value['expired']) is bool and value == a['B_actual_DBclock_protocol'] == {'id': B['run_id'], 'expired': True, 'recovery_protocol': None}, 'B actual DB-clock expiry/NULL protocol')
    def sql_entries(run):
        return [(cmd['replacement'][-3] if cmd['replacement'][-1] == 'COMMIT;' else cmd['replacement'][-1], raw.decode().strip())
                for cmd, _, raw in delegated if run in ' '.join(cmd['replacement'])]
    # A's original heartbeat observations are SQL text; retain source-private
    # holder/token VALUES absent, comparing only the original five safe fields.
    Aqueries = [(cmd['replacement'], raw.decode().strip()) for cmd, _, raw in delegated
                if 'f8f0a68e-c46f-46f1-8ac2-48df719d4f8b' in ' '.join(cmd['replacement'])]
    states = [raw.split('|') for cmd, raw in Aqueries if 'SELECT status,' in ' '.join(cmd)]
    need(len(states) >= 2 and all(len(v) == 5 for v in states)
         and states[0][0:2] == ['running', 't'] and states[-1][0] == 'succeeded'
         and all(v[4] == 't' for v in states), 'A original running to succeeded claimed lease')
    dt = lambda value: datetime.fromisoformat(value)
    need(any(v[0] == 'running' and dt(v[2]) > dt(states[0][2]) for v in states)
         and any(raw == 't' and 'SELECT (heartbeat_at >' in ' '.join(cmd) for cmd, raw in Aqueries), 'A actual in-flight heartbeat SQL advance')
    queries_B = sql_entries(B['run_id']); queries_C = sql_entries(C['run_id'])
    need(any(q.startswith('UPDATE sync_runs SET lease_expires_at') and "INTERVAL '5 seconds'" in q for q, raw in queries_B), 'B exact expiry injection original')
    crows = [raw.split('|') for q, raw in queries_C if q.startswith('SELECT status,')]
    need(len(crows) == 2 and crows[0] == crows[1] and len(crows[0]) == 5
         and crows[0][0:2] == ['running', 't'] and crows[0][4] == 't', 'C original five-field running/holder/token after unblock')
    all_sql = [' '.join(cmd['replacement']) for cmd, _, _ in delegated]
    need(any('CREATE OR REPLACE FUNCTION p21_c_terminal_fault()' in q and C['def_id'] in q and 'p21 injected terminal-state fault' in q for q in all_sql)
         and any('CREATE TRIGGER p21_c_terminal_fault_trg' in q for q in all_sql)
         and any('DROP TRIGGER IF EXISTS p21_c_terminal_fault_trg' in q for q in all_sql)
         and any('DROP FUNCTION IF EXISTS p21_c_terminal_fault()' in q for q in all_sql), 'C actual scoped fault injection and both cleanup originals')
    for n in (53, 58):
        need(steps[f'{n:02d}-temporary-object-absent'][2].decode().strip() == '0\npublic', 'actual temporary SQL object absence')
    for n in (56, 61):
        need(steps[f'{n:02d}-owned-inflight-zero'][2].decode().strip() == '0', 'actual owned inflight zero')
    return {'kind': p['kind'], 'named_pass': 3, 'cases': list(CASES), 'actual_creation_receipts': 9,
            'new_Def_Source_Dataset': [3, 3, 3], 'B_original_frozen_fields': 27,
            'C_historical_SQL_fields': 5, 'C_original_full27_claimed': False,
            'original_HTTP_artifact_semantics': 'original suite assertions/report; no invented full HTTP body originals',
            'original_outer_child_suite_rc': [0, 0, 0], 'actual_SQL_cleanup': True,
            'full_P26': False, 'actual_package': False}
