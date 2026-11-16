#!/usr/bin/env python3
"""Targeted pure closure checks; original saved PG reads and injected SQL only."""
import argparse
import hashlib
import importlib.util
import json
import tempfile
import sys
sys.dont_write_bytecode = True
import unittest
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent

def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + '.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

class Closure(unittest.TestCase):
    def test_pg_actual_saved_three_stages(self):
        f = load('finalize_p25')
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            b = HERE / 'pg-candidate-bindings-r17.json'
            (d / b.name).write_bytes(b.read_bytes())
            reasons, imports = f.v_pg(d, {})
            self.assertEqual(reasons, [])
            self.assertEqual(len(imports), 18)
            obj = json.loads(b.read_text())
            obj['stages'].append(obj['stages'][0])
            (d / b.name).write_text(json.dumps(obj))
            self.assertTrue(f.v_pg(d, {})[0])

    def test_persist_replays_fullstate_and_rejects_rehashed_substitution(self):
        c = load('collect_persist_state_p25')
        frozen = c.sealed_baseline(HERE)
        sqls = c.table_sqls(frozen)
        data = dict(frozen['content_gates']['owned_fullstate'], cp_owners=[])
        sql_to_name = {sql: key for key, sql in sqls.items()}
        def run(argv, timeout):
            return 0, json.dumps(data[sql_to_name[argv[-1]]]) + '\n', '', 'ok'
        with tempfile.TemporaryDirectory() as td:
            args = argparse.Namespace(env_dir='/private/tmp/openfoundry-platform-p1-q3v9', out_dir=td)
            self.assertEqual(c.collect_body(args, run), 0)
            d = Path(td) / 'attempt-1'
            self.assertEqual(c.verify_attempt(d)['problems'], [])
            # Replace one source while retaining all 17 ids and recomputing ledger.
            data['defs'][0]['source_id'] = data['defs'][1]['source_id']
            for suffix in ('.txt', '.json'):
                (d / ('defs' + suffix)).write_text(json.dumps(data['defs']) + '\n')
            mp = d / 'persist-manifest.json'
            man = json.loads(mp.read_text())
            for rel in ('defs.txt', 'defs.json'):
                man['files'][rel]['sha256'] = c._sha(d / rel)
            mp.write_text(json.dumps(man))
            self.assertTrue(any('full rows' in p for p in c.verify_attempt(d)['problems']))

    def test_seal_rejects_changed_selected_rc(self):
        f = load('finalize_p25')
        with tempfile.TemporaryDirectory() as td:
            d = Path(td); ev = d / 'ev'; ev.mkdir(); tools = d / 'tools'; tools.mkdir()
            (tools / 'tool.py').write_text('pass\n'); (ev / 'rc').write_text('0\n')
            with patch.object(f, 'CANONICAL_STAGES', {}), patch.object(f, 'load_selected_index', return_value=({}, [])):
                self.assertTrue(f.prepare(ev, tools, [])['final_ok'])
            (ev / 'rc').write_text('1\n')
            report = f.seal(ev, d / 'meta')
            self.assertFalse(report['ok'])
            self.assertTrue(any('EV bytes/set' in p for p in report['problems']))
            self.assertFalse((ev / 'hash-manifest.json').exists())

    def test_seal_hash_last_and_external_meta(self):
        f = load('finalize_p25')
        with tempfile.TemporaryDirectory() as td:
            d = Path(td); ev = d / 'ev'; ev.mkdir(); tools = d / 'tools'; tools.mkdir()
            (tools / 'tool.py').write_text('pass\n'); (ev / 'rc').write_text('0\n')
            with patch.object(f, 'CANONICAL_STAGES', {}), patch.object(f, 'load_selected_index', return_value=({}, [])):
                self.assertTrue(f.prepare(ev, tools, [])['final_ok'])
            self.assertFalse(f.seal(ev, ev / 'bad-meta')['ok'])
            self.assertFalse((ev / 'bad-meta').exists())
            result = f.seal(ev, d / 'meta')
            self.assertTrue(result['ok'], result)
            self.assertTrue(f.reverify(ev, d / 'meta')['ok'])

    def test_source_and_frozen_archive(self):
        f = load('finalize_p25')
        self.assertEqual(f.source_tree_hash(f.REPO_ROOT), f.PG_GREEN_SOURCE)
        self.assertEqual(f.old_archive_reverify()[0], [])

    def test_http_actual_registry_gates_and_raw(self):
        import ast
        f = load('finalize_p25')
        specs = ast.literal_eval(f._scenario_ast(HERE / 'smoke_p25.py'))
        with tempfile.TemporaryDirectory() as td:
            ev = Path(td); evidence = ev / 'records'; evidence.mkdir()
            cases = []
            for spec in specs:
                if spec['phase'] != 'healthy' or spec['id'] == 'h10':
                    continue
                case = {'id': spec['id'], 'phase': 'healthy', 'failed': False,
                        'gates': [{'gate': g, 'ok': True} for g in spec['gates']]}
                cases.append(case)
                (evidence / (spec['id'] + '-attempt-0001.json')).write_text(json.dumps(case))
            obj = {'phase': 'healthy', 'failed': [], 'quiet': {'ok': True},
                   'scenarios': cases, 'attempts': {'evidence_dir': str(evidence)}}
            p = ev / 'window-result.json'; p.write_text(json.dumps(obj))
            self.assertEqual(f.v_window(ev, {'ev': ev})[0], [])
            cases[0]['gates'] = [{'gate': 'arbitrary', 'ok': True}]
            p.write_text(json.dumps(obj))
            self.assertTrue(f.v_window(ev, {'ev': ev})[0])

    def test_tail_dual_exact_bindings_and_real_reason(self):
        f = load('verify_postfacts_p25')
        ids = {'def_id': '00000000-0000-0000-0000-000000000001',
               'source_id': '00000000-0000-0000-0000-000000000002',
               'run_id': '00000000-0000-0000-0000-000000000003', 'kind': 'owner'}
        spec = {'run_rows': [], 'owner_rows': [{'def_tag': 'negative', 'run_ref': 'terminal-failed',
                                             'blocked_reason': 'requires_reconciliation'}]}
        decl = {'schema': 'p25-tail-roster-v1', 'ok': True, 'spec': spec,
                'declaration_sha256': hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()}
        owner = {'sync_def_id': ids['def_id'], 'source_id': ids['source_id'],
                 'run_id': ids['run_id'], 'run_status': 'failed', 'blocked_reason': 'requires_reconciliation'}
        self.assertEqual(f._tail_gate(decl, [], [owner], {'negative': ids}, set()), [])
        self.assertTrue(f._tail_gate(decl, [], [owner], None, set()))
        self.assertTrue(f._tail_gate(decl, [], [owner, owner], {'negative': ids}, set()))
        owner['blocked_reason'] = 'awaiting_p26_resolution'
        self.assertTrue(f._tail_gate(decl, [], [owner], {'negative': ids}, set()))
        spec['owner_rows'][0].update(run_ref='same', blocked_reason='')
        decl['declaration_sha256'] = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
        owner.update(run_status='running', blocked_reason='')
        self.assertEqual(f._tail_gate(decl, [], [owner], {'negative': ids}, set()), [])

    def test_postfacts_canonical_raw_replay_and_missing_command(self):
        fixtures = load('selftest_p25_archive')
        f = load('verify_postfacts_p25')
        with tempfile.TemporaryDirectory() as td:
            attempt, _ = fixtures._green_postfacts(Path(td), verify=False)
            self.assertEqual(f.replay_facts_from_attempt(attempt)['_reasons'], [])
            # Even a rehashed dynamic ledger cannot omit the original command.
            cmd = attempt / '01-controlled-rows.rows.json.cmd'; cmd.unlink()
            sp = attempt / 'attempt-summary.json'; obj = json.loads(sp.read_text())
            obj['artifacts'].pop(cmd.name); sp.write_text(json.dumps(obj))
            self.assertTrue(f.replay_facts_from_attempt(attempt)['_reasons'])

    def test_bundle_relocates_real_imports_and_rejects_missing_semantics(self):
        f, sa, offline = load('finalize_p25'), load('standalone_p25'), load('offline_verify_p25')
        fixtures = load('selftest_p25_archive')
        with tempfile.TemporaryDirectory() as td:
            d = Path(td); ev = d / 'ev'; ev.mkdir(); selections = {}
            for stage in f.CANONICAL_STAGES:
                obj = {'ok': True}
                if stage == 'source-freeze':
                    obj.update(removed=[], added=[], changed=[])
                p = fixtures.make_stage(ev, stage, obj)
                selections[stage] = {'attempt_dir': str(p.relative_to(ev)), 'attempt': 1}
                if stage == 'pg-isolated':
                    (p / 'pg-candidate-bindings-r17.json').write_bytes((HERE / 'pg-candidate-bindings-r17.json').read_bytes())
            (ev / 'selected-index.json').write_text(json.dumps({'schema': 'p25-selected-index-v1', 'selections': selections}))
            for name in ("01-source-delta.json", "14-source-delta-final.json"):
                (ev / name).write_text(json.dumps({"removed": [], "changed": [], "added": []}))
            (ev / 'nested').mkdir(); (ev / 'nested/opaque.pyc').write_bytes(b'opaque')
            for control in ("bundle-hash-manifest.json", "standalone-meta.json"):
                (ev / "nested" / control).write_text("{}\n")
            meta = sa.build_bundle(ev, HERE, d / 'bundle')
            self.assertIn('ev/nested/opaque.pyc', meta['files'])
            result = offline.verify_bundle(d / 'bundle')
            status = result['semantic_stage_status']
            for key in ('unit', 'pg-isolated', 'build', 'preflight', 'source-freeze'):
                self.assertEqual(status.get(key), 'ok', result['problems'])
            self.assertFalse(result['ok'])
            self.assertEqual(status['window'], 'refused')
            # Omitting a source file together with its meta entry is detected
            # by the independently enumerated source file set.
            ev_meta = {'files': {k[3:]: v for k, v in meta['files'].items() if k.startswith('ev/')}}
            del ev_meta['files']['nested/opaque.pyc']
            self.assertFalse(sa.replay_compare(ev, d / 'bundle/ev', ev_meta)['ok'])

    def test_final_bundle_keeps_sealed_ev_and_refuses_missing_semantics(self):
        f = load('finalize_p25')
        with tempfile.TemporaryDirectory() as td:
            d = Path(td); ev = d / 'ev'; ev.mkdir()
            (ev / 'rc').write_text('0\n')
            with patch.object(f, 'CANONICAL_STAGES', {}), patch.object(f, 'load_selected_index', return_value=({}, [])):
                self.assertTrue(f.prepare(ev, HERE, [])['final_ok'])
            self.assertTrue(f.seal(ev, d / 'meta')['ok'])
            before = f._snapshot(ev)
            result = f.package_final(ev, HERE, d / 'meta')
            self.assertFalse(result['ok'])  # No selected semantic stages in this negative.
            self.assertTrue(result['ev_unchanged'])
            self.assertEqual(before, f._snapshot(ev))
            bundle = Path(result['bundle'])
            meta = json.loads((bundle / 'standalone-meta.json').read_text())
            self.assertEqual(meta['scope'], 'sealed-final')
            self.assertIn('ev/hash-manifest.json', meta['files'])
            self.assertEqual(set(meta['ev_source_files']), {str(p.relative_to(ev)) for p in ev.rglob('*') if p.is_file()})
            self.assertEqual({str(p.relative_to(bundle)) for p in bundle.rglob("*") if p.is_file()}, set(json.loads((bundle / "bundle-hash-manifest.json").read_text())["files"]) | {"bundle-hash-manifest.json"})
            self.assertEqual((d / 'meta/final-bundle-attempt-1.rc').read_text(), '1\n')

    def test_identity_catalog_rederives_history_and_rejects_unknown_global(self):
        f, c = load('verify_postfacts_p25'), load('collect_postfacts_p25')
        frozen = c.sealed_baseline(HERE)
        did, sid = '00000000-0000-0000-0000-000000000010', '00000000-0000-0000-0000-000000000011'
        with tempfile.TemporaryDirectory() as td:
            ev = Path(td); attempt = ev / 'postfacts'; attempt.mkdir()
            hp, response = ev / 'handles.json', ev / 'create.json'
            hp.write_text(json.dumps({'handles': [{'kind': 'def', 'ref': did, 'schedule_cron': '* * * * * *'}], 'count': 1}))
            response.write_text(json.dumps({'method': 'POST', 'path': 'http://127.0.0.1:17094/api/v1/data-connection/syncs', 'status': 201,
                                            'resp_body': json.dumps({'id': did, 'source_id': sid, 'schedule_cron': '* * * * * *'})}))
            row = {'id': did, 'source_id': sid, 'cron': '* * * * * *', 'enabled': 'false', 'cursor_null': 'true'}
            rp = ev / 'catalog-read.stdout'; rp.write_text(json.dumps([row]) + '\n')
            cmd, rc, err = ev / 'catalog-read.cmd', ev / 'catalog-read.rc', ev / 'catalog-read.stderr'
            cmd.write_text(' '.join(c.psql_argv(Path(c.ENV_DIR_DEFAULT), c.identity_def_sql([did]))) + '\n'); rc.write_text('0\n'); err.write_text('')
            digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
            inputs = [{'path': str(hp), 'sha256': digest(hp), 'kind': 'p25-handles'},
                      {'path': str(response), 'sha256': digest(response), 'kind': 'p25-def-create-response'},
                      {'path': str(rp), 'sha256': digest(rp), 'kind': 'catalog-def-read',
                       'cmd_path': str(cmd), 'cmd_sha256': digest(cmd), 'rc_path': str(rc), 'rc_sha256': digest(rc),
                       'stderr_path': str(err), 'stderr_sha256': digest(err)}]
            cat = {'schema': 'p25-owned-identity-catalog-v1', 'inputs': inputs,
                   'defs': [{'def_id': did, 'source_id': sid, 'cron': row['cron'], 'derived_from': [str(hp), str(response)]}]}
            cp = ev / 'owned-identity-catalog.json'; cp.write_text(json.dumps(cat))
            cg = frozen['content_gates']; wanted = [ {k: r[k] for k in ('id', 'source_id', 'cron', 'enabled', 'cursor_null')}
                                                   for r in cg['owned_fullstate']['defs'] if r['id'] in cg['global_catalog']['nonempty_cron_ids']] + [row]
            gp = attempt / '05d-global-schedules.rows.json.stdout'; gp.write_text(json.dumps(wanted) + '\n')
            (attempt / '05c-global-nonempty-cron.ids.json.stdout').write_text(json.dumps([{'id': r['id']} for r in wanted]) + '\n')
            self.assertEqual(f.verify_identity_catalog(cp, attempt, frozen), [])
            gp.write_text(json.dumps(wanted + [dict(row, id='00000000-0000-0000-0000-000000000099')]) + '\n')
            self.assertTrue(any('unfiltered global' in p for p in f.verify_identity_catalog(cp, attempt, frozen)))
            gp.write_text(json.dumps(wanted) + '\n')
            cat['inputs'] = inputs[1:]; cp.write_text(json.dumps(cat))
            self.assertTrue(any('omitted/mistyped' in p for p in f.verify_identity_catalog(cp, attempt, frozen)))

    def test_final_images_replay_original_raw_and_strict_counts(self):
        f = load('finalize_p25')
        b = json.loads((HERE / 'build-candidate-bindings-r18.json').read_text())
        with tempfile.TemporaryDirectory() as td:
            ev = Path(td); rp = ev / 'final-running.json'; rd = ev / 'final-running.json.raw'; rd.mkdir()
            services = {}
            for i, name in enumerate(('connector', 'dataset', 'pipeline', 'connector2', 'connector3', 'dataset-proxy', 'p25fixture', 'p25-proxy', 'p25connector')):
                present = name in ('connector', 'dataset', 'pipeline')
                ps = ('%064x' % (i + 1)) + '\n' if present else ''
                (rd / (name + '.ps-aq.raw')).write_text(ps); (rd / (name + '.ps-aq.rc')).write_text('0\n')
                row = {'ps_rc': 0, 'ps_raw': ps, 'containers': 1 if present else 0, 'running': present}
                if present:
                    row.update(inspect_rc=0, image=b['service_images'][name]['image_id'], started_at='2026-11-14T16:19:08.000000000Z')
                    (rd / (name + '.inspect.raw')).write_text(row['started_at'] + '|' + row['image'] + '|true\n')
                    (rd / (name + '.inspect.rc')).write_text('0\n')
                services[name] = row
            obj = {'schema': 'p25-running-evidence-v1', 'project': 'openfoundry-platform-p1-q3v9',
                   'env_dir': '/private/tmp/openfoundry-platform-p1-q3v9', 'services': services}
            rp.write_text(json.dumps(obj))
            self.assertEqual(f.v_final_images(ev, {'ev': ev})[0], [])
            raw = rd / 'connector.inspect.raw'; raw.write_text(services['connector']['started_at'] + '|sha256:' + '0' * 64 + '|true\n')
            self.assertTrue(f.v_final_images(ev, {'ev': ev})[0])
            raw.write_text(services['connector']['started_at'] + '|' + services['connector']['image'] + '|true\n')
            services['connector']['containers'] = True; rp.write_text(json.dumps(obj))
            self.assertTrue(f.v_final_images(ev, {'ev': ev})[0])

    def test_h10_original_command_rc_is_not_a_wrapper_boolean(self):
        import shlex
        f = load('finalize_p25')
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); step = root / '01-kill'; step.mkdir()
            argv = ['docker', 'kill', '-s', 'KILL', 'a' * 64]
            (step / 'argv.json').write_text(json.dumps(argv)); (step / 'cmd.txt').write_text(shlex.join(argv) + '\n')
            (step / 'stdout.raw').write_text('a' * 64 + '\n'); (step / 'stderr.raw').write_text(''); (step / 'rc').write_text('0\n')
            rec = {'cmd': shlex.join(argv), 'rc': 0, 'cmd_file': str(step / 'cmd.txt'), 'cmd_sha256': f.sha(step / 'cmd.txt'),
                   'argv_file': str(step / 'argv.json'), 'artifact': str(step / 'stdout.raw'), 'artifact_sha256': f.sha(step / 'stdout.raw'),
                   'stderr_artifact': str(step / 'stderr.raw'), 'stderr_sha256': f.sha(step / 'stderr.raw'),
                   'rc_file': str(step / 'rc'), 'rc_sha256': f.sha(step / 'rc')}
            (step / 'record.json').write_text(json.dumps(rec))
            self.assertEqual(f._v_h10_command_records({'commands': [rec]}, root), [])
            (step / 'rc').write_text('1\n')
            self.assertTrue(f._v_h10_command_records({'commands': [rec]}, root))

    def test_failed_prepare_controls_capability_new_prepare_final_bundle(self):
        f, sa = load('finalize_p25'), load('standalone_p25')
        fixtures = load('selftest_p25_archive')
        with tempfile.TemporaryDirectory() as td:
            d = Path(td); ev = d / 'ev'; ev.mkdir(); selected = {}
            for stage in f.CANONICAL_STAGES:
                step = fixtures.make_stage(ev, stage, {'ok': True})
                selected[stage] = {'attempt_dir': str(step.relative_to(ev)), 'attempt': 1}
            (ev / 'selected-index.json').write_text(json.dumps({'schema': 'p25-selected-index-v1', 'selections': selected}))
            # Real failed prepare leaves its failed manifest; older control
            # receipts also survive, exactly as in the preserved working EV.
            self.assertFalse(f.prepare(ev, HERE, [])['final_ok'])
            (ev / 'tools-sha.json').write_text('{"historical":true}\n')
            (ev / 'prepare-lock.json').write_text('{"historical":true}\n')
            nested = ev / 'evidence/manifest.json'; nested.parent.mkdir(); nested.write_text('{"immutable":true}\n')
            capability = d / 'ev-standalone'; meta = sa.build_bundle(ev, HERE, capability)
            # Isolate lifecycle bookkeeping here. The final offline verifier
            # still executes real semantic validators and correctly refuses
            # the deliberately missing business facts in this negative EV.
            with patch.object(f, 'CANONICAL_STAGES', {}):
                self.assertTrue(f.prepare(ev, HERE, [])['final_ok'])
            self.assertEqual(f.capability_predecessor_replay(ev, capability, meta), [])
            self.assertTrue(f.seal(ev, d / 'meta')['ok'])
            result = f.package_final(ev, HERE, d / 'meta')
            self.assertFalse(result['ok'])
            self.assertTrue(result['ev_unchanged'])
            final_bundle = Path(result['bundle'])
            final_meta = json.loads((final_bundle / 'standalone-meta.json').read_text())
            for name in ('manifest.json', 'tools-sha.json', 'prepare-lock.json', 'hash-manifest.json'):
                self.assertEqual(final_meta['files']['ev/' + name]['original'], f.sha(ev / name))
            offline = json.loads((d / 'meta/final-bundle-attempt-1.stdout').read_text())
            self.assertFalse(any('root seal' in p or 'source tool' in p or 'predecessor input drift' in p for p in offline['problems']))
            nested.write_text('{"tampered":true}\n')
            self.assertTrue(any('evidence/manifest.json' in p for p in f.capability_predecessor_replay(ev, capability, meta)))

    @unittest.skipUnless((HERE / 'results/2026-11-14-p25-final/h10/attempt1/result.json').is_file(), 'requires the preserved actual h10 attempt')
    def test_actual_h10_raw_baseline_replay_and_coordinated_generation_tamper(self):
        import shutil
        f = load('finalize_p25'); engine = f.loadmod_sibling('smoke_p25.py')
        ev = HERE / 'results/2026-11-14-p25-final'; original = ev / 'h10/attempt1'
        report = json.loads((original / 'result.json').read_text())
        self.assertEqual(f._v_h10_baseline(report, {'ev': ev}, engine), [])
        with tempfile.TemporaryDirectory() as td:
            copy = Path(td) / 'actual-h10'; shutil.copytree(original, copy)
            native = Path
            def rebound(*parts):
                p = native(*parts)
                try: return copy / p.relative_to(original)
                except ValueError: return p
            # Only copied h10 files change; accepted original full-window raw
            # history remains the independent protected-field anchor.
            with patch.object(f, 'Path', rebound):
                self.assertEqual(f._v_h10_baseline(report, {'ev': ev}, engine), [])
                owner = json.loads((ev / 'window-aggregate.json').read_text())['final_owner_roster'][0]
                changed = 0
                for p in (copy / 'prepare').rglob('h10-prepare-attempt-*.json'):
                    obj = json.loads(p.read_text())
                    body = obj.get('resp_body')
                    if not isinstance(body, str): continue
                    parsed = json.loads(body)
                    rows = parsed if isinstance(parsed, list) else parsed.get('runs', []) if isinstance(parsed, dict) else []
                    hit = [r for r in rows if isinstance(r, dict) and r.get('id') == owner['run_id']]
                    if not hit: continue
                    for row in hit:
                        old = row['generation']; row['generation'] = str(int(old) + 1) if isinstance(old, str) else old + 1
                    encoded = json.dumps(parsed)
                    obj.update(resp_body=encoded, resp_sha256=hashlib.sha256(encoded.encode()).hexdigest(), resp_bytes=len(encoded.encode()))
                    p.write_text(json.dumps(obj)); changed += 1
                self.assertGreater(changed, 0)
                for name, sha_key in (('baseline.json', 'baseline_sha256'), ('baseline-after.json', 'baseline_after_sha256')):
                    p = copy / name; facts = json.loads(p.read_text())
                    for row in facts['snapshot'][owner['def_id']]['runs']:
                        if row['id'] == owner['run_id']:
                            old = row['generation']; row['generation'] = str(int(old) + 1) if isinstance(old, str) else old + 1
                    for entry in facts['raw_records']:
                        entry['sha256'] = f.sha(rebound(entry['path']))
                    p.write_text(json.dumps(facts)); report[sha_key] = f.sha(p)
                kp = copy / 'tail-known-binding.json'; known = json.loads(kp.read_text())
                known.update(baseline_sha256=report['baseline_sha256'], baseline_after_sha256=report['baseline_after_sha256'])
                kp.write_text(json.dumps(known))
                problems = f._v_h10_baseline(report, {'ev': ev}, engine)
                self.assertTrue(any('original failedhealthy raw history' in p for p in problems), problems)

    def test_h14_typed_branch_must_call_refusing_raw_verifier(self):
        import types
        f = load('verify_postfacts_p25')
        with tempfile.TemporaryDirectory() as td:
            ev = Path(td); sp, bp = ev / 'tail-aggregate.json', ev / 'tail-selected-bindings.json'
            sp.write_text(json.dumps({'schema': 'p25-tail-aggregate-v1', 'phase': 'tail', 'failed': [], 'quiet': {'ok': True},
                                      'tail_bindings_file': str(bp), 'scenarios': []}))
            bp.write_text('{}')
            calls = []
            def refuse(path, resolve_path=None):
                calls.append(str(path)); self.assertEqual(resolve_path(str(bp)), bp)
                return {'ok': False, 'problems': ['raw ACK proof refused']}
            with patch.object(f, 'loadmod', return_value=types.SimpleNamespace(verify_supplement=refuse)):
                result = f.load_verified_tail(sp, bp, logical_path_class=Path)
            self.assertEqual(calls, [str(sp)])
            self.assertTrue(any('raw replay refused' in p for p in result['problems']))
            # Wrong schema cannot fall through as an arbitrary normal summary.
            obj = json.loads(sp.read_text()); obj['schema'] = 'arbitrary-ok-wrapper'; sp.write_text(json.dumps(obj))
            self.assertTrue(f.load_verified_tail(sp, bp)['problems'])

    def test_original_tail_cannot_relabel_failed_gates_and_rebind_hash(self):
        f = load('verify_postfacts_p25')
        with tempfile.TemporaryDirectory() as td:
            ev = Path(td); records = ev / 'records'; records.mkdir()
            bp, sp = ev / 'original-bindings.json', ev / 'tail-result.json'; bp.write_text('{}')
            cases = [{'id': 'h11', 'failed': False, 'gates': [{'gate': 'G_ACK_STRICT', 'ok': True}]},
                     {'id': 'h14', 'failed': True, 'gates': [{'gate': 'G_ACK_STRICT', 'ok': False}]}]
            for case in cases:
                (records / (case['id'] + '-attempt-0001.json')).write_text(json.dumps(case))
            obj = {'phase': 'tail', 'failed': ['h14'], 'quiet': {'ok': True}, 'scenarios': cases,
                   'attempts': {'evidence_dir': str(records)}, 'tail_bindings_file': str(bp)}
            sp.write_text(json.dumps(obj)); self.assertTrue(f.load_verified_tail(sp, bp)['problems'])
            obj['failed'] = []; cases[1]['failed'] = False; cases[1]['gates'][0]['ok'] = True
            sp.write_text(json.dumps(obj)); bp.write_text(json.dumps({'summary_sha256': hashlib.sha256(sp.read_bytes()).hexdigest()}))
            self.assertTrue(any('scenario recorder' in p for p in f.load_verified_tail(sp, bp)['problems']))

    @unittest.skipUnless((HERE / 'results/2026-11-14-p25-final/tail-aggregate.json').is_file(), 'requires preserved actual h14 supplement')
    def test_actual_h14_typed_tail_isolated_copy_replay(self):
        import builtins
        import io
        import pathlib
        import shutil
        ev = HERE / 'results/2026-11-14-p25-final'
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); copied_ev, tools = root / 'ev', root / 'tools'; copied_ev.mkdir(); tools.mkdir()
            pending = [ev / name for name in ('tail-aggregate.json', 'tail-selected-bindings.json', 'h14-supplement-proof.json', 'tail-declaration.json')]
            seen = set()
            def discover(obj, key=None):
                if isinstance(obj, dict):
                    for k, value in obj.items(): discover(value, k)
                elif isinstance(obj, list):
                    for value in obj: discover(value, key)
                elif isinstance(obj, str) and obj.startswith(str(ev) + '/'):
                    candidate = Path(obj)
                    if candidate.is_file(): pending.append(candidate)
                    elif candidate.is_dir() and key in ('evidence_dir', 'original_step', 'step_dir'):
                        pending.extend(p for p in candidate.rglob('*') if p.is_file())
            while pending:
                p = pending.pop()
                if p in seen: continue
                seen.add(p); raw = p.read_bytes(); target = copied_ev / p.relative_to(ev)
                target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(raw)
                try: discover(json.loads(raw))
                except (ValueError, UnicodeDecodeError): pass
            for p in HERE.glob('*.py'): shutil.copyfile(p, tools / p.name)
            native, native_io, native_open = Path, io.open, builtins.open
            def rebound(*parts):
                p = native(*parts)
                for origin, dest in ((ev, copied_ev), (HERE, tools)):
                    try: return dest / p.relative_to(origin)
                    except ValueError: pass
                return p
            class CopyPath:
                def __new__(cls, *parts): return rebound(*parts)
            def reject_original(fn):
                def guarded(path, *args, **kwargs):
                    if isinstance(path, (str, bytes)) or hasattr(path, '__fspath__'):
                        candidate = native(path)
                        for origin in (ev, HERE):
                            if candidate.is_relative_to(origin): raise AssertionError('original read forbidden: ' + str(candidate))
                    return fn(path, *args, **kwargs)
                return guarded
            with patch.object(pathlib, 'Path', CopyPath), patch.object(io, 'open', reject_original(native_io)), patch.object(builtins, 'open', reject_original(native_open)):
                spec = importlib.util.spec_from_file_location('h14_copied_finalizer', tools / 'finalize_p25.py')
                f = importlib.util.module_from_spec(spec); spec.loader.exec_module(f)
                reasons, _ = f.v_tail(copied_ev, {'ev': copied_ev, 'logical_path_class': native})
                self.assertEqual(reasons, [])
                # Rebinding the new summary SHA cannot authorize an unrelated
                # change to the otherwise passing h11 case.
                ap, bp = copied_ev / 'tail-aggregate.json', copied_ev / 'tail-selected-bindings.json'
                doc = json.loads(ap.read_text()); doc['scenarios'][0]['gates'][0]['detail'] = 'forged unrelated replacement'
                ap.write_text(json.dumps(doc)); binding = json.loads(bp.read_text())
                binding['summary_sha256'] = hashlib.sha256(ap.read_bytes()).hexdigest(); bp.write_text(json.dumps(binding))
                refused, _ = f.v_tail(copied_ev, {'ev': copied_ev, 'logical_path_class': native})
                self.assertTrue(any('typed h14 supplement raw replay refused' in r for r in refused), refused)
            self.assertGreater(len(seen), 10)

    def test_persist_actual_blank_empty_set_still_refuses_explicit_array_passes(self):
        c = load('collect_persist_state_p25')
        frozen = c.sealed_baseline(HERE); sqls = c.table_sqls(frozen)
        self.assertTrue(sqls['grants'].startswith("SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) FROM"))
        actual = HERE / 'results/2026-11-14-p25-final/persist-attempts/attempt-1/grants.txt'
        old_wire = actual.read_text() if actual.is_file() else 'SET\nSET\n\n'
        self.assertEqual(old_wire, 'SET\nSET\n\n')
        data = dict(frozen['content_gates']['owned_fullstate'], cp_owners=[])
        names = {sql: name for name, sql in sqls.items()}
        for label, wire, expected_rc in (('old-blank', old_wire, 1), ('explicit-array', 'SET\nSET\n[]\n', 0)):
            with self.subTest(wire=label), tempfile.TemporaryDirectory() as td:
                def run(argv, timeout):
                    name = names[argv[-1]]
                    return 0, wire if name == 'grants' else 'SET\nSET\n' + json.dumps(data[name]) + '\n', '', 'ok'
                args = argparse.Namespace(env_dir=c.ENV_DIR_DEFAULT, out_dir=td)
                self.assertEqual(c.collect_body(args, run), expected_rc)
                attempt = Path(td) / 'attempt-1'
                manifest = json.loads((attempt / 'persist-manifest.json').read_text())
                if expected_rc:
                    self.assertFalse(manifest['ok'])
                    self.assertTrue(any('grants: projection last line not JSON' in p for p in manifest['failures']))
                    self.assertFalse((attempt / 'grants.json').exists())
                else:
                    self.assertTrue(manifest['ok'])
                    self.assertEqual(json.loads((attempt / 'grants.json').read_text()), [])
                    self.assertEqual(c.verify_attempt(attempt)['problems'], [])

if __name__ == '__main__':
    unittest.main()
