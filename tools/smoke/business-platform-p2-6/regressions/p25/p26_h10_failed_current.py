#!/usr/bin/env python3
"""One Root-released, fixed-target failed-h10 CURRENT observation; GET only."""
import argparse
import json
import sys
from pathlib import Path

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
P26 = HERE.parents[1]
sys.path.insert(0, str(HERE.parent / 'adapters'))
import w10_guard_shim as H

SOURCE = 'ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
STAGE = 'p25-h10-failed-current'
IDENTITY = {'actor': '1c0e238a-3657-4978-98dc-d71a342c3fa1', 'tenant': 'a792b91e-dab5-4739-9344-2848c1e83487'}
HELD = {'def_id': '29edee53-dab7-4ece-80b8-88272188ed5e', 'source_id': 'c1006127-0706-473b-91e8-b4f41c2cc069', 'run_id': '06d34d47-260d-4c6c-b6e2-f11db906fc5f'}
DATASET = 'ecdade13-60ab-42a9-b839-642bdd0d8d69'
RECORD_SHA = 'dc6a5cd409a83364239d420b551f8fa9dad9bbb34138dc42878348b5b5a53e26'
TYPED_SHA = '74ab26829dbc107e95f9d4cd013311e75c23fd17ce25d4a2695813e85678cc29'
W10 = P26 / 'results/w10'
FAILED = W10 / 'S2-seven-stage-ready-attempt1/p25-h10-runtime-actual'
INPUTS = {
    str(W10 / 'S2-P25-h10-outer-launch-attempt1/FAILED-ORIGINAL-AUDIT.json'): '9dc1e319015a6d88e2492cb99fa0887b88a5238ddd30840b279b8432bb43fe72',
    str(FAILED / 'h10/result.json'): '650f9667b97b33f3c13e92867ed91628797e9f4ac73296daab7214245dda1dfa',
    str(FAILED / 'runtime-20261115T150446Z-9045be/RESULT.json'): 'b52f1fc3e2122e604c1847855f10b9c8fffbb81334f431afc224d40319aa073b',
    str(FAILED / 'h10/prepare/p25-h10-prepare-20261115T150449/handles.json'): '1dd3da399f7258dd7f8034e90fd8a05fa7b413bfb5f771a5991a4cadec096860',
}
TOOLS = [Path(__file__), Path(H.__file__), Path(H.W.__file__), Path(H.G.__file__)]


def tool_pins():
    return {str(p.resolve()): H.sha(p) for p in TOOLS}


def validate(release_path, output, source_hash=None):
    r = H.validate_release(release_path, stage=STAGE, suite='p25')
    if Path(release_path).stat().st_mode & 0o777 != 0o600:
        raise ValueError('Root private release mode must be 0600')
    if r.get('candidate_source') != SOURCE or (source_hash or H.G.candidate_hash)(H.G.OPENFOUNDRY) != SOURCE:
        raise ValueError('CURRENT exact frozen S required')
    if r.get('readonly_tools') != tool_pins() or r.get('readonly_inputs') != INPUTS:
        raise ValueError('CURRENT exact tools/failed inputs required')
    for p, want in INPUTS.items():
        if H.sha(p) != want:
            raise ValueError('failed original drift')
    for field, want in [('deployment_record', RECORD_SHA), ('typed_roster', TYPED_SHA)]:
        if r.get(field + '_sha256') != want or H.sha(r[field]) != want:
            raise ValueError('CURRENT runtime15/typedN drift')
    if r.get('readonly_identity') != IDENTITY or r.get('readonly_held') != HELD:
        raise ValueError('CURRENT actor/failed targets mismatch')
    out = Path(output).resolve()
    if str(out) != r.get('readonly_output') or not out.is_relative_to(W10.resolve()):
        raise ValueError('CURRENT exact NEW output required')
    if r.get('evidence_root') != str(out / 'whole-guard'):
        raise ValueError('CURRENT exact whole-guard output required')
    if r.get('image_transition') is not None or r.get('main_boot_transition') is not None:
        raise ValueError('CURRENT permits no deployment transition')
    return r


def routes():
    did, rid = HELD['def_id'], HELD['run_id']
    root = 'http://127.0.0.1:17100/api/v1/data-connection/syncs/' + did
    return [('def', root), ('run', root + '/runs/' + rid), ('cp', root + '/checkpoint'),
            ('runs', root + '/runs'), ('fixture', 'http://127.0.0.1:17103/admin/src/p25h13'),
            ('proxy', 'http://127.0.0.1:17104/admin/stats')]


def verdict(bodies):
    d, run, cp, rows, f, p = [bodies[k] for k in ('def', 'run', 'cp', 'runs', 'fixture', 'proxy')]
    if d.get('id') != HELD['def_id'] or d.get('source_id') != HELD['source_id'] or d.get('output_dataset_id') != DATASET:
        raise ValueError('failed actual Def parent drift')
    if d.get('org_id') != IDENTITY['tenant']:
        raise ValueError('failed actual Def tenant drift')
    if run.get('id') != HELD['run_id'] or run.get('sync_def_id') != HELD['def_id']:
        raise ValueError('failed actual Run identity drift')
    if any(run.get(k) != v for k, v in {'org_id': IDENTITY['tenant'], 'requested_by': IDENTITY['actor'],
                                       'owner_id': IDENTITY['actor'], 'control_mode': 'controlled'}.items()):
        raise ValueError('failed actual Run actor/tenant/protocol drift')
    rows = rows.get('runs') if isinstance(rows, dict) else rows
    if not isinstance(rows, list) or len(rows) != 1 or rows[0].get('id') != HELD['run_id'] or rows[0].get('sync_def_id') != HELD['def_id'] or rows[0].get('status') != run.get('status'):
        raise ValueError('failed Def full known Run list mismatch')
    if any(rows[0].get(k) != run[k] for k in ('org_id', 'requested_by', 'owner_id', 'control_mode')):
        raise ValueError('failed Run list identity/protocol drift')
    # Public CP has no Def ID; the immutable GET URL is its target binding.
    if cp.get('mode') != 'cursor' or type(cp.get('confirmed_epoch')) is not int or cp['confirmed_epoch'] < 0 or (cp.get('owner') is not None and not isinstance(cp['owner'], dict)):
        raise ValueError('failed CP response shape mismatch')
    for body, keys in [(f, ('inventory_started', 'inventory_finished', 'active')),
                       (p, ('commit_started', 'commit_finished', 'commit_inflight'))]:
        if any(type(body.get(k)) is not int or body[k] < 0 for k in keys) or body[keys[0]] - body[keys[1]] != body[keys[2]]:
            raise ValueError('invalid actual lifecycle counters')
    if f.get('name') != 'p25h13':
        raise ValueError('wrong fixture source')
    owner = cp.get('owner') or {}
    converged = (run.get('status') in ('succeeded', 'failed', 'aborted') and not owner.get('run_id')
                 and f['active'] == 0 and p['commit_inflight'] == 0)
    return {'ok': converged, 'run_status': run.get('status'), 'cp_owner': owner,
            'fixture': f, 'proxy': p, 'full_known_run_count': len(rows)}


def execute(release_path, output, transport=None, guard=None):
    r = validate(release_path, output)
    release_sha = H.sha(release_path)
    out = Path(output).resolve()
    out.mkdir(parents=True, exist_ok=False)
    before_tools = tool_pins()
    (out / 'tools-before.json').write_text(json.dumps(before_tools, indent=2) + '\n')
    # The original full old+typedN/current15 guard is read-only; its raw IO is retained.
    old = (guard or H.check)(release_path)
    att = H.W.Attempt(out, 'current-get')
    secret = H.W.read_jwt_secret()
    jwt = H.W.sign_testing_jwt(secret, IDENTITY['actor'], IDENTITY['tenant'])
    att.add_secret(secret); att.add_secret(jwt)
    bodies = {}
    try:
        for name, url in routes():
            if validate(release_path, output) != r or H.sha(release_path) != release_sha:
                raise ValueError('CURRENT release changed')
            step = att.step_begin('GET-' + name, {'method': 'GET', 'url': url})
            response = (transport or H.W.http_call)(url, auth=jwt if name in ('def', 'run', 'cp', 'runs') else None)
            att.step_finish('GET-' + name, step, response.body, 'unavailable',
                            {'original_rc': None, 'http_status': response.status, 'error': response.error})
            if response.status != 200 or response.error is not None:
                raise ValueError('CURRENT GET failed: ' + name)
            bodies[name] = json.loads(response.body)
        observed = verdict(bodies)
        result = {'schema': 'p26-h10-failed-current-v1', **observed, 'failed_original_unchanged': INPUTS,
                  'root_release_sha256': release_sha, 'creators': json.loads(Path(list(INPUTS)[2]).read_text())['created_safe'],
                  'initial_whole_guard': old, 'bodies': bodies}
        if validate(release_path, output) != r or before_tools != tool_pins():
            raise ValueError('CURRENT tools/source/inputs changed')
        (out / 'tools-after.json').write_text(json.dumps(tool_pins(), indent=2) + '\n')
        att.finish(result)
        print(json.dumps({'ok': result['ok'], 'result': str(att.dir / 'RESULT.json')}))
        return 0 if result['ok'] else 1
    except BaseException as exc:
        att.finish({'ok': False, 'error': type(exc).__name__ + ': ' + str(exc), 'initial_whole_guard': old})
        raise


def main():
    p = argparse.ArgumentParser(); p.add_argument('--release-file', required=True); p.add_argument('--output', required=True)
    a = p.parse_args(); return execute(a.release_file, a.output)


if __name__ == '__main__':
    try: sys.exit(main())
    except (ValueError, KeyError, OSError) as e:
        print('CURRENT refused: ' + str(e), file=sys.stderr); sys.exit(2)
