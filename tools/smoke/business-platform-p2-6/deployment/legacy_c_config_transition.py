#!/usr/bin/env python3
"""Four enumerated S2 C-only config transitions; never a main or other aux."""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
sys.path.insert(0, str(P26 / 'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
from final_old_artifact_readonly import evidenced_http, terminal_error

MAIN_OVERRIDE = P26 / 'deployment/override-S2-main-imageonly.yaml'
LEGACY_BASE = P26 / 'deployment/legacy-stage/compose.yaml'
LEGACY_OVERRIDE = P26 / 'deployment/legacy-stage/override-S2-imageonly.yaml'
GUARD = P26 / 'regressions/adapters/w10_guard_shim.py'
NET = G.Q3_PROJECT + '_p1net'
SERVICES = ['dataset', 'connector', 'p26connectorc', 'p26connectora', 'p26connectorb']
ROLES = {n: 'connector_' + n[-1] for n in SERVICES if n.startswith('p26')}
PORTS = {'dataset': 17078, 'connector': 17088, 'p26connectorc': 17100,
         'p26connectora': 17097, 'p26connectorb': 17098}
IDENTITY = '{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}'
ENV_NAMES = '{{range .Config.Env}}{{println (index (split . "=") 0)}}{{end}}'
FACTS = '{"cid":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"config_image":{{json .Config.Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"restart_count":{{json .RestartCount}},"labels":{{json .Config.Labels}},"networks":{{json .NetworkSettings.Networks}},"ports":{{json .NetworkSettings.Ports}},"mounts":{{json .Mounts}}}'
AUX_CONFIG = {'HOST': '0.0.0.0', 'PORT': '50088',
              'ALLOWED_EGRESS_HOSTS': 'fixture,p26fixture,p25fixture',
              'ALLOW_PRIVATE_NETWORK_EGRESS': 'true',
              'OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS': '0',
              'OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS': '0',
              'OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED': 'false',
              'CONNECTOR_SYNC_ASYNC_WORKERS': '1',
              'CONNECTOR_SYNC_ASYNC_QUEUE': '64',
              'CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS': '300',
              'CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS': '300'}
ACTIONS=('p24-enable','p24-restore','p25-enter','p25-restore')
TOOLS = {'deployment/legacy_c_config_transition.py','deployment/final_old_artifact_readonly.py',
         'common/p26_gates.py','preflight/p26_current_preflight.py','regressions/adapters/w10_guard_shim.py',
         'deployment/legacy-stage/compose.yaml',*[('deployment/legacy-stage/complete-C-'+a+'.json') for a in ACTIONS]}


def canonical_mounts(mounts):
    assert isinstance(mounts, list)
    assert all(isinstance(m, dict) and isinstance(m.get('Destination'), str) and m['Destination'] for m in mounts)
    result = {m['Destination']: m for m in mounts}
    assert len(result) == len(mounts)
    return result


def canonical_facts(fact):
    return dict(fact, mounts=canonical_mounts(fact['mounts']))


def name_for(service):
    assert service in SERVICES
    return G.Q3_PROJECT + '-' + service + '-1' if service in ['dataset', 'connector'] else service


def compose_prefix(action):
    assert action in ACTIONS
    return ['docker','compose','-f',str(LEGACY_BASE),'-f',str(P26/'deployment/legacy-stage'/('complete-C-'+action+'.json')),'--project-name','openfoundry-p26-legacy']


def upgrade_argv(action):
    return compose_prefix(action)+['up','-d','--no-deps','--no-build','--pull=never','--force-recreate','p26connectorc']


def config_for(service):
    want = dict(AUX_CONFIG)
    want['DATASET_SERVICE_URL'] = 'http://p26p22proxy:8091' if service == 'p26connectorb' else 'http://dataset:50078'
    return want


def env_format(keys):
    clauses = ' '.join('(eq (index $p 0) "' + key + '")' for key in keys)
    return '{{range .Config.Env}}{{$p := split . "="}}{{if or ' + clauses + '}}{{println .}}{{end}}{{end}}'


def validate_fact(service, fact, image, pin=None):
    assert re.fullmatch('[a-f0-9]{64}', fact['cid'])
    assert fact['name'] == '/' + name_for(service) and fact['running'] is True
    assert fact['image'] == fact['config_image'] == image and set(fact['networks']) == {NET}
    assert fact['restart_count'] == 0
    assert re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z', fact['started_at'])
    if pin:
        assert all(fact[k] == pin[k] for k in ['cid', 'image', 'started_at'])
    project = G.Q3_PROJECT if service in ['dataset', 'connector'] else 'openfoundry-p26-legacy'
    assert fact['labels']['com.docker.compose.project'] == project
    assert fact['labels']['com.docker.compose.service'] == service
    if service in ROLES:
        for key, val in {'of-p26-owner': 'p26-http', 'of-p26-stage': 'p2-6', 'of-p26-role': ROLES[service]}.items():
            assert fact['labels'].get(key) == val


def env_names(raw):
    names = raw.rstrip('\n').splitlines()
    assert names and all(re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', name) for name in names)
    assert len(names) == len(set(names)), 'duplicate environment NAME'
    return names


def validate_replacement(service, before, after, expected_image, env_before, env_after):
    validate_fact(service, after, expected_image)
    assert before['cid'] != after['cid'] and before['started_at'] != after['started_at']
    assert canonical_mounts(before['mounts']) == canonical_mounts(after['mounts']) and before['ports'] == after['ports']
    assert set(before['networks']) == set(after['networks']) == {NET}
    assert before['networks'][NET]['NetworkID'] == after['networks'][NET]['NetworkID']
    before_names, after_names = env_names(env_before), env_names(env_after)
    assert set(before_names) == set(after_names)
    if service == 'connector':
        assert not set(after_names).intersection(W.LOOP_ENV_NAMES)


def exclusive_json(path, value):
    with path.open('x') as stream:
        stream.write(json.dumps(value, indent=2) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def candidate_record_with_origins(record, service, current, proof_path, after_refs):
    candidate_record = copy.deepcopy(record)
    name = name_for(service)
    candidate_record['containers'][name].update(current)
    origins = candidate_record.setdefault('origins', {})
    prior = copy.deepcopy(origins.get(name))
    ref = lambda p: {'path': str(Path(p).resolve()), 'sha256': G.sha256_file(p)}
    origins[name] = {'pin_source': 'this exact authorized image transition; requires Root independent acceptance',
                     'transition_proof': ref(proof_path),
                     'actual_after_receipts': [ref(str(prefix) + suffix) for prefix in after_refs.values()
                                               for suffix in ['.cmd.json', '.raw', '.rc', '.meta.json']],
                     'priorOrigin': prior}
    return candidate_record


def run(action, release_file):
    assert action in ACTIONS
    service="p26connectorc"
    release_path = Path(release_file).resolve()
    r = G.load_release(str(release_path))
    phase = 's2-C-config-' + action
    assert r and r.get('root_accepted') is True and phase in r['released_for']
    authority = r['image_transition']
    assert authority['transition_kind']=='config' and authority['config_action']==action
    assert authority['service'] == service and authority['target_name'] == name_for(service)
    assert authority['executor_tool'] == str(Path(__file__).resolve())
    assert authority['executor_tool_sha256'] == G.sha256_file(__file__)
    assert r['stage'] == 'runtime-config-transition' and r['suite'] == 'p26-runtime'
    assert authority['approved_argv'] == upgrade_argv(action)
    source = r['candidate_source']
    assert G.candidate_hash(G.OPENFOUNDRY) == source == 'ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
    assert set(r['image_upgrade_tools_sha256']) == TOOLS
    for rel, sha in r['image_upgrade_tools_sha256'].items():
        assert G.sha256_file(P26 / rel) == sha
    assert G.sha256_file(r['deployment_record']) == r['deployment_record_sha256']
    rec = json.loads(Path(r['deployment_record']).read_bytes())
    assert rec['source_sha256'] == source
    old = rec['containers'][name_for(service)]
    candidate = r['expected_candidate_images']['dataset' if service == 'dataset' else 'connector']
    assert candidate == old['image'] == 'sha256:ea8244f03a3b07e72b77bd7539e13ae1d39cf91bf90a26d01675f008c64685d6'
    assert authority['candidate_image'] == candidate
    config_files = {str(LEGACY_BASE), str(P26/'deployment/legacy-stage'/('complete-C-'+action+'.json'))}
    assert set(authority['config_shas']) == config_files
    for path, sha in authority['config_shas'].items():
        assert G.sha256_file(path) == sha
    if service in ROLES:
        assert G.sha256_file('/private/tmp/openfoundry-p26-aux.env') == authority['aux_env_sha256']
    assert W.Q3_ENV_FILE.is_file() and not W.Q3_ENV_FILE.is_symlink() and W.Q3_ENV_FILE.stat().st_mode & 0o777 == 0o600
    # No trusted post-projection is fabricated locally: an independently pinned
    # guard extension must validate the transition against this original release.
    extension = P26 / 'regressions/adapters/w10_image_transition.py'
    assert G.sha256_file(extension) == authority['guard_extension_sha256']
    import importlib.util
    spec = importlib.util.spec_from_file_location('pinned_image_transition_contract', extension)
    bridge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bridge)
    assert FACTS == bridge.IDENTITY_JSON and ENV_NAMES == bridge.ENVNAMES
    assert env_format(config_for('p26connectora')) == bridge.AUX_CONFIG_FORMAT
    config_pair=bridge.configuration_pair(action)
    assert (authority['before_safe_config'],authority['after_safe_config'])==config_pair
    assert json.loads((P26/'deployment/legacy-stage'/('complete-C-'+action+'.json')).read_bytes())=={'services':{service:{'image':candidate,'environment':config_pair[1]}}}
    output = Path(authority['output_dir']).resolve()
    assert output.is_relative_to(P26 / 'results/w6/deployment')
    output.mkdir(parents=True, exist_ok=False)
    proof_path = Path(authority['proof_file']).resolve()
    assert proof_path == output / 'image-transition.json'
    att = W.Attempt(output, 'commands')
    for path in [W.Q3_ENV_FILE, Path('/private/tmp/openfoundry-p26-aux.env')]:
        assert path.is_file() and not path.is_symlink() and path.stat().st_mode & 0o777 == 0o600
        for line in path.read_text().splitlines():
            if '=' in line and not line.startswith('#'):
                att.add_secret(line.split('=', 1)[1].strip().strip('\"').strip("'"))
    original_release_sha = G.sha256_file(release_path)

    def gate():
        assert G.sha256_file(release_path) == original_release_sha and G.load_release(str(release_path)) == r
        assert G.candidate_hash(G.OPENFOUNDRY) == source
        for rel, sha in r['image_upgrade_tools_sha256'].items():
            assert G.sha256_file(P26 / rel) == sha
        assert G.sha256_file(extension) == authority['guard_extension_sha256']
        for path, sha in authority['config_shas'].items():
            assert G.sha256_file(path) == sha
        if service in ROLES:
            assert G.sha256_file('/private/tmp/openfoundry-p26-aux.env') == authority['aux_env_sha256']

    def command(tag, argv):
        gate()
        prefix = att.step_begin(tag, argv)
        try:
            cp = subprocess.run(argv, capture_output=True, text=True, timeout=180)
        except Exception as exc:
            att.step_finish(tag, prefix, type(exc).__name__, 'unavailable', {'original_child_rc': None}, kind='argv')
            raise RuntimeError(tag + ' timeout/spawn; child rc unavailable') from None
        att.step_finish(tag, prefix, cp.stdout, cp.returncode, {'stderr': cp.stderr, 'original_child_rc': cp.returncode}, kind='argv')
        assert cp.returncode == 0, tag + ' original rc=' + str(cp.returncode)
        return cp.stdout, prefix

    def inspect(tag, cid):
        return json.loads(command(tag, ['docker', 'inspect', cid, '--format', FACTS])[0])

    def target_inspect(tag, cid):
        refs = {}
        values = {}
        for key, fmt in [('identity', IDENTITY), ('facts', FACTS), ('env_names', ENV_NAMES)]:
            values[key], refs[key] = command(tag + '-' + key, ['docker', 'inspect', cid, '--format', fmt])
        fact = json.loads(values['facts'])
        assert values['identity'].strip().splitlines() == [fact['cid'], fact['image'], fact['started_at'], 'true']
        if service in ROLES:
            text, refs['safe_config'] = command(tag + '-safe-config', ['docker', 'inspect', cid, '--format', env_format(config_for(service))])
            pairs = [line.split('=', 1) for line in text.rstrip('\n').splitlines()]
            assert len(pairs) == len(dict(pairs)) == len(config_for(service))
            assert dict(pairs) == config_pair[0 if tag=='target-before' else 1]
        return fact, values, refs

    def guard(tag, projected=False):
        argv = ['python3', '-I', '-B', str(GUARD), str(release_path)]
        if projected:
            argv += ['--deployment-projection', str(proof_path)]
        text, receipt = command('typed-guard-' + tag, argv)
        result = json.loads(text.strip().splitlines()[-1])
        assert result['ok'] is True
        return {'result': result, 'receipt_prefix': receipt}

    def counters(tag):
        _, catalog, _ = W.load_baseline(G.P25_FINAL_DIR)
        for name, want in catalog['counter_bodies_sha256'].items():
            gate()
            response, _ = evidenced_http(att, W.ORIGINS['fixture'] + '/admin/src/' + name)
            assert response.status == 200 and G.sha256_bytes(response.body) == want

    def owned_inventory(tag, pins):
        names, _ = command('whole-owned-container-names-' + tag, ['docker', 'ps', '-a', '--filter',
                           'label=of-p26-owner=p26-http', '--filter', 'label=of-p26-stage=p2-6', '--format', '{{.Names}}'])
        expected = {name for name in pins if name.startswith('p26')}
        assert set(names.splitlines()) == expected and len(names.splitlines()) == len(expected)

    def grants(tag):
        pg = rec['containers'][W.Q3_POSTGRES]['cid']
        text, _ = command('whole-schedule-grants-' + tag, ['docker', 'exec', pg, 'psql', '-X', '-qAt',
                          '-v', 'ON_ERROR_STOP=1', '-U', 'openfoundry', '-d', 'connectors', '-c',
                          'BEGIN READ ONLY; SELECT count(*) FROM sync_schedule_grants; COMMIT;'])
        assert text.strip() == '0'

    created = None
    try:
        guard_before = guard('before')
        counters('before')
        required = {W.Q3_DATASET, W.Q3_CONNECTOR, W.Q3_PIPELINE, W.Q3_POSTGRES, W.Q3_FIXTURE,
                    'p26connectora', 'p26connectorb', 'p26connectorc', 'p26p22proxy', 'p26fixture', 'p26proxy', 'p26connectora2', 'p26connectorb2'}
        if action.startswith('p25-'):required.update({'p26p25fixture','p26p25proxy'})
        pins = r['image_upgrade_preserve_containers']
        assert required.issubset(pins)
        owned_inventory('before', pins)
        grants('before')
        for name, pin in rec['containers'].items():
            assert all(pins[name][key] == pin[key] for key in ['cid', 'image', 'started_at'])
        protected = {}
        for name, pin in pins.items():
            actual = inspect('before-' + name, pin['cid'])
            assert actual['name'] == '/' + name and all(actual[k] == pin[k] for k in ['cid', 'image', 'started_at'])
            assert actual['running'] is True
            protected[name] = actual
        before, bvalues, brefs = target_inspect('target-before', old['cid'])
        validate_fact(service, before, old['image'], old)
        assert command('cached-candidate', ['docker', 'image', 'inspect', candidate, '--format', '{{.Id}}'])[0].strip() == candidate
        exclusive_json(output / 'command-intent.json', {'release_id': r['release_id'], 'source_sha256': source,
                       'service': service, 'previous': {k: old[k] for k in ['cid', 'image', 'started_at']}, 'candidate_image': candidate, 'transition_kind':'config','config_action':action,
                       'upgrade_argv': upgrade_argv(action), 'before': brefs})
        _, upref = command('upgrade-only-' + service, upgrade_argv(action))
        cid_text, cidref = command('resolve-command-created-cid', compose_prefix(action) + ['ps', '-q', service])
        cid = cid_text.strip()
        assert re.fullmatch('[a-f0-9]{64}', cid) and cid != old['cid']
        created = {'cid': cid, 'verification': 'pending'}
        exclusive_json(output / 'immediate-created-cid.json', created)
        after, avalues, arefs = target_inspect('target-after', cid)
        validate_replacement(service, before, after, candidate, bvalues['env_names'], avalues['env_names'])
        current = {k: after[k] for k in ['cid', 'image', 'started_at']}
        receipts = {'before_identity': brefs['facts'], 'before_env_names': brefs['env_names'],
                    'command': upref, 'discovery': cidref,
                    'after_identity': arefs['facts'], 'after_env_names': arefs['env_names']}
        if service in ROLES:
            receipts.update(before_aux_config=brefs['safe_config'], after_aux_config=arefs['safe_config'])
        proof = {'schema': 'p26-image-transition-proof-v1', 'transition_kind':'config','config_action':action,'release_id': r['release_id'],
                 'root_release_sha256': original_release_sha,
                 'executor_tool_sha256': G.sha256_file(__file__),
                 'deployment_record_sha256': r['deployment_record_sha256'],
                 'service': service, 'previous': {k: old[k] for k in ['cid', 'image', 'started_at']}, 'current': current,
                 'receipts': receipts}
        exclusive_json(proof_path, proof)
        created = after
        for attempt in range(1, 11):
            gate()
            try:
                response, _ = evidenced_http(att, 'http://127.0.0.1:' + str(PORTS[service]) + '/healthz')
                if response.status == 200:
                    break
            except Exception:
                if attempt == 10:
                    raise RuntimeError('health transport not ready') from None
            if attempt == 10:
                raise RuntimeError('health not200')
            time.sleep(1)
        for name, pin in protected.items():
            if name != name_for(service):
                assert canonical_facts(inspect('after-' + name, pin['cid'])) == canonical_facts(pin)
        counters('after')
        owned_inventory('after', pins)
        grants('after')
        guard_after = guard('after', projected=True)
        candidate_record = candidate_record_with_origins(rec, service, current, proof_path, arefs)
        exclusive_json(output / 'candidate-deployment-record.json', candidate_record)
        att.finish({'ok': True, 'source_sha256': source, 'service': service, 'current': current,
                    'transition_proof': str(proof_path), 'guard_before': guard_before, 'guard_after': guard_after,
                    'all_other_container_facts_unchanged': True, 'oldfixture_counters_unchanged': True,
                    'typed_preserved': True, 'candidate_record_requires_Root_independent_acceptance': True})
        print(att.dir)
        return 0
    except Exception as exc:
        exclusive_json(output / 'PARTIAL.json', {'ok': False, 'known_created': created,
                       'auto_retry_rollback_cleanup': False, 'Root_review_required': True})
        return terminal_error(att, exc, [])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', choices=ACTIONS, required=True)
    parser.add_argument('--release-file', required=True)
    args = parser.parse_args()
    try:
        rc = run(args.action, args.release_file)
    except Exception as exc:
        print('single image upgrade refused (' + type(exc).__name__ + ')', file=sys.stderr)
        rc = 1
    sys.exit(rc)
