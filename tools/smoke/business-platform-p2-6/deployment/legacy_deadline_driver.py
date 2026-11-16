#!/usr/bin/env python3
"""NEW A-only deadline/restore driver. No mutation without exact Root binding."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid
from urllib import request

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
from common import p26_gates as G
from preflight import p26_current_preflight as W
HERE = Path(__file__).resolve().parent
KEYS = ['CONNECTOR_SYNC_ASYNC_WORKERS', 'CONNECTOR_SYNC_ASYNC_QUEUE',
        'CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS', 'CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS']
NAME = 'p26connectora'
IMAGE = 'sha256:f23f69161c6f86f7402a18dbd14e24fdcad581ecbd9968e7b964ef1baf11df34'
ENV4_FORMAT = '{{range .Config.Env}}{{$p := split . "="}}{{if or (eq (index $p 0) "CONNECTOR_SYNC_ASYNC_WORKERS") (eq (index $p 0) "CONNECTOR_SYNC_ASYNC_QUEUE") (eq (index $p 0) "CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS") (eq (index $p 0) "CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS")}}{{println .}}{{end}}{{end}}'
LABELS = {'of-p26-owner': 'p26-http', 'of-p26-stage': 'p2-6', 'of-p26-role': 'connector_a', 'com.docker.compose.project': 'openfoundry-p26-legacy', 'com.docker.compose.service': NAME}


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


S2_SOURCE = 'ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
S2_IMAGE = 'sha256:ea8244f03a3b07e72b77bd7539e13ae1d39cf91bf90a26d01675f008c64685d6'
S2_OVERRIDE = 'legacy-stage/override-S2-imageonly.yaml'


def expected_image(release):
    if release['candidate_source'] == S2_SOURCE:
        assert release['expected_candidate_images']['connector'] == S2_IMAGE
        return S2_IMAGE
    # Historical S receipts retain their original image contract.
    assert release['candidate_source'] == 'b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c'
    return IMAGE


def compose_prefix(release, action):
    assert action in ('up-deadline:7', 'restore')
    image = expected_image(release)
    files = ['legacy-stage/compose.yaml']
    if release['candidate_source'] == S2_SOURCE:
        files.append(S2_OVERRIDE)
    if action == 'up-deadline:7':
        files.append('legacy-stage/deadline7.yaml')
    # Both J actions must bind every file before any child can run.
    for rel in set(files) | {'legacy-stage/deadline7.yaml'}:
        assert release['driver_config_shas'][rel] == sha(HERE / rel)
    if release['candidate_source'] == S2_SOURCE:
        override = json.loads((HERE / S2_OVERRIDE).read_text())
        assert override == {'services': {n: {'image': image} for n in ('p26connectorc', NAME, 'p26connectorb')}}
    return ['docker', 'compose'] + [part for rel in files for part in ('-f', str(HERE / rel))]


def persist_transition_intent(path, intent):
    """Must finish before the recreate subprocess can be called."""
    with path.open('x') as stream:
        stream.write(json.dumps(intent, indent=2) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    return {'path': str(path), 'sha256': sha(path)}


def validate_before(actual, expected, config, action):
    assert actual['name'] == '/' + NAME and actual['running'] is True
    assert all(actual[k] == expected[k] for k in ['cid', 'image', 'started_at'])
    assert all(actual['labels'].get(k) == v for k, v in LABELS.items())
    assert config == dict(zip(KEYS, [1, 64, 300 if action == 'up-deadline:7' else 7, 300]))


def logical_current(actual):
    """Logical driver identity excludes Docker's leading name slash."""
    assert actual['name'] == '/' + NAME
    return {'name': NAME, **{k:actual[k] for k in ('cid','image','started_at')}}


def failure_restore_authority(release, root_anchor):
    """A failed transition is evidence, never automatic restore authority."""
    assert 'legacy-deadline-failure-restore' in release['released_for']
    binding = release['driver_failure_restore']
    def read_pinned(ref):
        path = Path(ref['path'])
        assert path.resolve().is_relative_to((P26 / 'results/w6/deployment').resolve())
        assert not path.is_symlink() and sha(path) == ref['sha256']
        return json.loads(path.read_text())
    failure = read_pinned(binding['manifest'])
    assert failure['schema'] == 'p26-w10-driver-failure-v1'
    assert failure['release_id'] == binding['origin_release_id']
    intent = read_pinned(failure['transition_intent'])
    assert intent['schema'] == 'p26-w10-driver-transition-intent-v1'
    assert intent['release_id'] == failure['release_id']
    assert intent['root_anchor'] == root_anchor and intent['source_S'] == release['candidate_source']
    assert intent['action'] in ('up-deadline:7', 'restore')
    base = compose_prefix(release, intent['action'])
    assert intent['recreate_argv'] == base + ['--project-name', 'openfoundry-p26-legacy', 'up', '-d', '--no-deps', '--no-build', '--pull=never', '--force-recreate', NAME]
    proof = read_pinned(binding['target_proof'])
    assert proof['transition_intent_sha256'] == failure['transition_intent']['sha256']
    current, config = proof['actual_inspect'], proof['actual_config']
    assert current['name'] == '/' + NAME and current['image'] == expected_image(release)
    assert all(current['labels'].get(k) == v for k, v in LABELS.items())
    assert type(current['running']) is bool
    assert re.fullmatch('[a-f0-9]{64}', current['cid'])
    assert current['cid'] in set(failure['observed_candidate_cids']) | {intent['previous']['cid']}
    assert set(config) == set(KEYS) and all(type(v) is int for v in config.values())
    selected = {k: current[k] for k in ('cid', 'image', 'started_at')}
    selected['name'] = NAME
    return selected, current, config, {'manifest': binding['manifest'], 'target_proof': binding['target_proof'], 'origin_release_id': failure['release_id']}


def load_predecessor(release, anchor, adapter_file=None):
    """Verify an I start receipt anchored to Root's initial A, filesystem only.

    The JSON and each receipt file are read once. Hashes describe those exact
    parsed bytes, rather than trusting a self-reported digest. Production's
    writer is the fixed NEW facade; adapter_file injection is pure-test only.
    """
    adapter = Path(adapter_file or P26 / 'regressions/adapters/w10_suite_adapter.py')
    writer_sha = sha(adapter)
    assert writer_sha == release['driver_predecessor_adapter_sha256'], 'predecessor writer differs from Root pin'
    path = Path(release['driver_predecessor_file'])
    evidence_root = Path(release['evidence_root']).resolve()
    allowed = (P26 / 'results/w10').resolve()
    assert evidence_root.is_relative_to(allowed)
    assert path.is_absolute() and not path.is_symlink() and path.resolve().is_relative_to(evidence_root)
    raw = path.read_bytes()
    proof = json.loads(raw)
    assert proof['schema'] == 'p26-w10-compose-predecessor-v1'
    assert proof['release_id'] == release['release_id']
    assert proof['adapter_tool_sha256'] == writer_sha
    assert proof['previous'] == anchor, 'predecessor does not start at Root initial A'
    current = proof['current']
    assert set(current) == {'name', 'cid', 'image', 'started_at'}
    assert all(current[k] == anchor[k] for k in ['name', 'cid', 'image'])
    assert anchor['name'] == NAME and anchor['image'] == expected_image(release)
    boot_re = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z'
    assert re.fullmatch(boot_re, current['started_at'])
    assert current['started_at'] != anchor['started_at'], 'I start must change boot'
    service_map = {'connector': 'p26connectorc', 'connector2': NAME, 'connector3': 'p26connectorb'}
    assert release['service_map'] == service_map
    assert release['aux_project'] == 'openfoundry-p26-legacy'
    expected = ['docker', 'compose', '-f', release['aux_compose'], '--project-name', release['aux_project'], 'start', 'p26connectorc', NAME, 'p26connectorb']
    transition = proof['actual_transition']
    assert transition['argv'] == expected and transition['original_rc'] == 0
    hashes = {}
    def receipt(prefix):
        prefix = Path(prefix)
        assert prefix.is_absolute() and prefix.resolve().is_relative_to(evidence_root)
        data = {}
        for suffix in ['.cmd.json', '.rc', '.raw', '.meta.json']:
            f = Path(str(prefix) + suffix)
            assert not f.is_symlink() and f.resolve().is_relative_to(evidence_root)
            b = f.read_bytes()
            hashes[str(f)] = hashlib.sha256(b).hexdigest()
            data[suffix] = b.decode()
        assert data['.rc'].strip() == '0', 'original transition/inspect rc nonzero'
        assert isinstance(json.loads(data['.meta.json'])['stderr'], str)
        return json.loads(data['.cmd.json']), data['.raw']
    command, _ = receipt(transition['evidence_step'])
    assert command['replacement'] == expected
    env = Path(release['legacy_env_dir'])
    original = ['docker', 'compose', '-f', str(env / 'compose.yaml')]
    tail = ['--project-name', 'openfoundry-platform-p1-q3v9', 'start', 'connector', 'connector2', 'connector3']
    assert command['original'] in [original + tail, original + ['-f', str(env / 'compose-p22-connector2.yaml')] + tail]
    after_cmd, after_raw = receipt(transition['actual_after_step'])
    assert after_cmd == ['docker', 'inspect', NAME, '--format', '{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}']
    assert after_raw.strip().splitlines() == [current['cid'], current['image'], current['started_at'], 'true']
    return current, {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest(), 'writer_sha256': writer_sha, 'root_anchor': anchor, 'actual_transition': transition, 'receipt_sha256': hashes}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('action', choices=['up-deadline:7', 'restore'])
    ap.add_argument('--release-file', required=True)
    args = ap.parse_args()
    r = json.loads(Path(args.release_file).read_text())
    assert r['root_accepted'] and r['schema'] == 'p26-w10-release-v1'
    expiry = datetime.fromisoformat(r['expires_utc'].replace('Z', '+00:00'))
    assert expiry.tzinfo is not None and datetime.now(timezone.utc) < expiry
    assert 'legacy-deadline-driver' in r['released_for'] or (args.action == 'restore' and 'legacy-deadline-failure-restore' in r['released_for'])
    binding = r['driver_commands'][args.action]
    assert Path(binding['tool']).resolve() == Path(__file__).resolve()
    assert binding['tool_sha256'] == sha(__file__)
    assert binding['argv'] == ['python3', '-I', '-B', str(Path(__file__).resolve()), args.action, '--release-file', args.release_file]
    assert G.candidate_hash(G.OPENFOUNDRY) == r['candidate_source']
    image = expected_image(r)
    compose = compose_prefix(r, args.action)
    # Root pins the first A; restore follows only this driver's previous,
    # release-bound, hashed receipt. No container-name based adoption.
    state = Path(r['driver_chain_file'])
    rec = json.loads(Path(r['deployment_record']).read_text())
    assert sha(r['deployment_record']) == r['deployment_record_sha256']
    previous = dict(rec['containers'][NAME], name=NAME)
    previous = {k: previous[k] for k in ['name', 'cid', 'image', 'started_at']}
    root_anchor = dict(previous)
    predecessor = None
    failure_authority = None
    if args.action == 'restore' and 'driver_failure_restore' in r:
        previous, failure_actual, failure_config, failure_authority = failure_restore_authority(r, root_anchor)
    elif args.action == 'restore':
        prior = json.loads(state.read_text())
        assert prior['release_id'] == r['release_id'] and prior['action'] == 'up-deadline:7'
        assert sha(prior['evidence']['path']) == prior['evidence']['sha256']
        proof = json.loads(Path(prior['evidence']['path']).read_text())
        assert all(proof[k] == prior[k] for k in ['release_id', 'action', 'previous', 'current', 'current_config'])
        previous = prior['current']
    else:
        assert not state.exists(), 'driver chain collision; preserve'
        previous, predecessor = load_predecessor(r, previous)
    out = P26 / 'results/w6/deployment' / ('legacy-driver-' + G.now_utc() + '-' + uuid.uuid4().hex[:6])
    out.mkdir(parents=True, exist_ok=False)
    def save(name, obj):
        (out / name).write_text(json.dumps(obj, indent=2) + '\n')
    def run(name, argv):
        assert json.loads(Path(args.release_file).read_text()) == r
        assert datetime.now(timezone.utc) < expiry
        assert compose_prefix(r, args.action) == compose
        save(name + '.cmd-before.json', argv)
        try:
            cp = subprocess.run(argv, capture_output=True, timeout=120)
        except Exception as exc:
            (out / (name + '.raw')).write_text(type(exc).__name__ + '\n')
            (out / (name + '.rc')).write_text('unavailable\n')
            save(name + '.meta.json', {'original_child_rc': None, 'failure_kind': type(exc).__name__})
            raise RuntimeError(name + ' spawn/timeout; original child rc unavailable') from None
        (out / (name + '.stdout')).write_bytes(cp.stdout)
        (out / (name + '.stderr')).write_bytes(cp.stderr)
        (out / (name + '.raw')).write_bytes(cp.stdout + cp.stderr)
        (out / (name + '.rc')).write_text(str(cp.returncode) + '\n')
        assert cp.returncode == 0, name + ' failed original rc=' + str(cp.returncode)
        return cp.stdout.decode()
    def facts(label, cid, require_running=True):
        assert re.fullmatch('[a-f0-9]{64}', cid)
        fmt = '{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"labels":{{json .Config.Labels}}}'
        f = json.loads(run(label, ['docker', 'inspect', cid, '--format', fmt]))
        assert f['name'] == '/' + NAME and f['image'] == image
        if require_running is not None:
            assert f['running'] is require_running
        for k, v in LABELS.items():
            assert f['labels'][k] == v
        return f
    intent_ref = None
    observed_candidates = []
    try:
        before = facts('before', previous['cid'], failure_actual['running'] if failure_authority else True)
        assert all(before[k] == previous[k] for k in ['cid', 'image', 'started_at'])
        raw_before_cfg = run('before-actual-env4', ['docker', 'inspect', previous['cid'], '--format', ENV4_FORMAT])
        before_cfg = {k: int(v) for k, v in (ln.split('=', 1) for ln in raw_before_cfg.splitlines() if ln)}
        if failure_authority:
            assert before == failure_actual and before_cfg == failure_config
        else:
            validate_before(before, previous, before_cfg, args.action)
        _, _, old = W.load_baseline(G.P25_FINAL_DIR)
        oldids = [str(uuid.UUID(x['run_id'])) for x in old['protected9']]
        sql = "BEGIN READ ONLY; SELECT count(*) FROM sync_runs WHERE control_mode='controlled' AND status IN ('pending','running') AND id NOT IN (" + ','.join("'" + i + "'::uuid" for i in oldids) + '); COMMIT;'
        pg = r['w7_pg_observer']
        assert re.fullmatch('[a-f0-9]{64}', pg['cid']) and pg['user'] == 'openfoundry' and pg['database'] == 'connectors'
        count = run('zero-new-active', ['docker', 'exec', pg['cid'], 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'openfoundry', '-d', 'connectors', '-c', sql])
        assert count.strip() == '0', 'NEW controlled task active; preserve'
        envdir = Path(r['legacy_env_dir'])
        assert envdir == Path('/private/tmp/openfoundry-p26-legacy-env')
        # The original suite reads this logical-name, nonsecret fragment.
        logical = envdir / 'compose-p22-deadline.yaml'
        logical.write_text('services:\n  connector2:\n    environment:\n      CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS: "7"\n      CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS: "300"\n')
        base = list(compose)
        base += ['--project-name', 'openfoundry-p26-legacy']
        recreate = base + ['up', '-d', '--no-deps', '--no-build', '--pull=never', '--force-recreate', NAME]
        intent_path = out / 'transition-intent.json'
        intent = {'schema': 'p26-w10-driver-transition-intent-v1', 'release_id': r['release_id'], 'action': args.action, 'root_anchor': root_anchor, 'previous': previous, 'actual_before': before, 'before_config': before_cfg, 'source_S': r['candidate_source'], 'recreate_argv': recreate, 'compose_predecessor': predecessor, 'failure_restore_authority': failure_authority}
        intent_ref = persist_transition_intent(intent_path, intent)
        run('recreate-only-A', recreate)
        cid = run('new-A-cid', base + ['ps', '-q', NAME]).strip()
        assert re.fullmatch('[a-f0-9]{64}', cid)
        observed_candidates = [cid]
        save('candidate-CID-before-checks.json', {'transition_intent': intent_ref, 'unverified_candidate_cid': cid})
        current = facts('after', cid)
        save('candidate-identity-before-config.json', {'transition_intent': intent_ref, 'actual_inspect': current})
        assert current['cid'] != previous['cid'] and current['started_at'] != previous['started_at']
        # Whitelist extraction occurs in Docker template; secret env values
        # never enter this Python process or its command evidence.
        raw = run('actual-env4', ['docker', 'inspect', cid, '--format', ENV4_FORMAT])
        cfg = {k: int(v) for k, v in (ln.split('=', 1) for ln in raw.splitlines() if ln)}
        save('restore-target-proof.json', {'transition_intent_sha256': intent_ref['sha256'], 'actual_inspect': current, 'actual_config': cfg})
        expected = dict(zip(KEYS, [1, 64, 7 if args.action == 'up-deadline:7' else 300, 300]))
        assert cfg == expected
        for attempt in range(1, 11):
            sid = 'health-' + str(attempt)
            save(sid + '.cmd-before.json', {'method': 'GET', 'url': 'http://127.0.0.1:17097/healthz'})
            try:
                with request.urlopen('http://127.0.0.1:17097/healthz', timeout=5) as response:
                    (out / (sid + '.raw')).write_bytes(response.read(1048576))
                    save(sid + '.meta.json', {'status': response.status})
                    assert response.status == 200
                (out / (sid + '.rc')).write_text('0\n')
                break
            except Exception as exc:
                (out / (sid + '.raw')).write_text(type(exc).__name__ + '\n')
                (out / (sid + '.rc')).write_text('1\n')
                if attempt == 10:
                    raise
                time.sleep(1)
        assert G.candidate_hash(G.OPENFOUNDRY) == r['candidate_source']
        safe = {'schema': 'p26-w10-driver-proof-v1', 'release_id': r['release_id'], 'action': args.action, 'previous': previous, 'current': logical_current(current), 'current_config': cfg, 'safe_labels': current['labels'], 'actual_inspect': current}
        if predecessor:
            safe['compose_predecessor'] = predecessor
            safe['before_config'] = before_cfg
        safe['transition_intent'] = intent_ref
        if failure_authority:
            safe['failure_restore_authority'] = failure_authority
        proof = out / 'proof.json'; save('proof.json', safe)
        result = {k: safe[k] for k in ['release_id', 'action', 'previous', 'current', 'current_config']}
        result.update(schema='p26-w10-driver-result-v1', evidence={'path': str(proof), 'sha256': sha(proof)})
        # Write state only after all original evidence and checks succeed.
        state.write_text(json.dumps(result, indent=2) + '\n')
        save('RESULT.json', result)
        print(json.dumps(result, separators=(',', ':')))
    except Exception as exc:
        if intent_ref:
            # Discovery is read-only and UNTRUSTED. It cannot authorize any
            # restore: Root must pin the resulting full-CID proof separately.
            try:
                raw = run('failure-candidate-list', compose + ['--project-name', 'openfoundry-p26-legacy', 'ps', '-a', '-q', NAME])
                candidates = raw.splitlines()
                assert all(re.fullmatch('[a-f0-9]{64}', c) for c in candidates)
                observed_candidates = sorted(set(observed_candidates + candidates))
                if len(candidates) == 1:
                    observed = facts('failure-candidate-identity', candidates[0], None)
                    raw_cfg = run('failure-candidate-config', ['docker','inspect', candidates[0], '--format', ENV4_FORMAT])
                    config = {k:int(v) for k,v in (ln.split('=',1) for ln in raw_cfg.splitlines() if ln)}
                    save('failure-restore-target-proof.json', {'transition_intent_sha256':intent_ref['sha256'],'actual_inspect':observed,'actual_config':config})
            except Exception:
                pass
        save('FAILURE.json', {'schema':'p26-w10-driver-failure-v1','release_id':r['release_id'],'transition_intent':intent_ref,'observed_candidate_cids':observed_candidates,'error_type':type(exc).__name__,'cleanup_executed':False,'restore_authority':'NONE: separate Root failure-restore phase + manifest/targetproof hashes required'})
        print('deadline driver failed ('+type(exc).__name__+'); evidence='+str(out),file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    try: rc = main()
    except Exception as exc:
        print('deadline preflight refused ('+type(exc).__name__+')',file=sys.stderr);rc=1
    sys.exit(rc)
