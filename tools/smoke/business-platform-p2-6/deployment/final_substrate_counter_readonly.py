#!/usr/bin/env python3
"""Narrow final RO proof: main5 safe config/boots and nine fixed old counters.

No SQL, mutations, environment values, discovery, or historical executables.
The only subprocess argv is exact docker inspect with four safe projections.
This complements H.check and the independent final-owned-state collector.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
sys.path.insert(0, str(P26 / 'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
from final_old_artifact_readonly import evidenced_http, terminal_error

PHASE = 'final-substrate-counter-readonly'
FORMATS = {
    'identity': '{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}',
    'mounts': '{{json .Mounts}}',
    'ports': '{{json .NetworkSettings.Ports}}\n{{json .HostConfig.PortBindings}}',
    'env_names': '{{range .Config.Env}}{{println (index (split . "=") 0)}}{{end}}',
}
SOURCES = frozenset({
    'p21a-b6ec49ab', 'p21b-1020a565', 'p21b-528455c2',
    'p21b-96351e3b', 'p21b-b6ec49ab', 'p21c-1020a565',
    'p21c-528455c2', 'p21c-96351e3b', 'p21c-b6ec49ab',
})
TOOLS = {'deployment/final_substrate_counter_readonly.py',
         'deployment/final_old_artifact_readonly.py',
         'common/p26_gates.py', 'preflight/p26_current_preflight.py'}


def need(ok, why):
    if not ok:
        raise ValueError(why)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_ref(ref):
    need(set(ref) == {'path', 'sha256'}, 'exact public evidence ref required')
    path = Path(ref['path']).resolve()
    need(path.is_relative_to((P26 / 'results').resolve()), 'public results ref required')
    need(sha(path) == ref['sha256'], 'original input hash drift')
    return path.read_bytes()


def receipt(refs, argv):
    need(set(refs) == {'cmd', 'raw', 'rc', 'meta'}, 'all four original receipt files required')
    raw = {key: read_ref(value) for key, value in refs.items()}
    cmd = json.loads(raw['cmd'])
    if isinstance(cmd, dict) and set(cmd) == {'argv'}:
        cmd = cmd['argv']
    meta = json.loads(raw['meta'])
    need(cmd == argv and raw['rc'].strip() == b'0', 'original exact argv/rc mismatch')
    need(meta.get('original_child_rc', meta.get('original_rc')) == 0,
         'original Unix rc required')
    return raw['raw'].decode()


def mounts(raw):
    value = json.loads(raw)
    need(isinstance(value, list), 'complete mount list required')
    need(all(isinstance(v, dict) and isinstance(v.get('Destination'), str)
             and v['Destination'] for v in value), 'invalid mount destination')
    keyed = {v['Destination']: v for v in value}
    need(len(keyed) == len(value), 'duplicate mount destination')
    return keyed  # Keep EVERY field. Only list ordering is canonicalized.


def ports(raw):
    lines = raw.strip().splitlines()
    need(len(lines) == 2, 'complete two-part ports projection required')
    value = [json.loads(line) for line in lines]
    need(all(isinstance(v, dict) for v in value), 'complete port dictionaries required')
    return value


def env_names(raw):
    names = raw.strip().splitlines()
    need(names and len(names) == len(set(names)), 'unique nonempty env names required')
    need(all(re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', n) for n in names), 'env values forbidden')
    return set(names)


def validate_snapshot(pin, expected, actual, connector=False):
    need(set(expected) == set(actual) == set(FORMATS), 'exact safe projection set required')
    identity = [pin['cid'], pin['image'], pin['started_at'], 'true']
    need(expected['identity'].strip().splitlines() == identity, 'original boot/identity mismatch')
    need(actual['identity'].strip().splitlines() == identity, 'current boot/identity drift')
    need(mounts(actual['mounts']) == mounts(expected['mounts']), 'complete mounts drift')
    need(ports(actual['ports']) == ports(expected['ports']), 'complete ports drift')
    need(env_names(actual['env_names']) == env_names(expected['env_names']), 'env names drift')
    if connector:
        need(not env_names(actual['env_names']).intersection(W.LOOP_ENV_NAMES), 'main loops must stay off')


def validate_counter(expected, response):
    need(response.status == 200 and response.error is None, 'counter HTTP must succeed with200')
    need(response.body == expected, 'complete old counter body drift')


def load_plan(release):
    plan = json.loads(read_ref({'path': release['plan_path'], 'sha256': release['plan_sha256']}))
    need(plan['schema'] == 'p26-final-substrate-counter-plan-v1', 'plan schema mismatch')
    need(plan['source_sha256'] == release['source_sha256'], 'plan source mismatch')
    record = json.loads(read_ref(plan['deployment_record']))
    need(record['source_sha256'] == release['source_sha256'], 'record source mismatch')
    need(set(record['containers']) == set(G.Q3_CONTAINERS), 'exact main5 required')
    entries = plan['containers']
    need(len(entries) == 5 and {e['name'] for e in entries} == set(G.Q3_CONTAINERS),
         'duplicate/missing/unknown main container')
    expected = {}
    for entry in entries:
        name = entry['name']; pin = record['containers'][name]
        need(set(entry['receipts']) == set(FORMATS), 'complete original config receipts required')
        expected[name] = {key: receipt(entry['receipts'][key],
            ['docker', 'inspect', pin['cid'], '--format', fmt]) for key, fmt in FORMATS.items()}
        validate_snapshot(pin, expected[name], expected[name], name == W.Q3_CONNECTOR)
    sources = plan['counters']
    need(len(sources) == 9 and {e['source_name'] for e in sources} == SOURCES,
         'duplicate/missing/unknown old source name')
    counters = {}
    for entry in sources:
        name = entry['source_name']; refs = entry['receipt']
        need(set(refs) == {'cmd', 'raw', 'rc', 'meta'}, 'all original counter files required')
        evidence = {k: read_ref(v) for k, v in refs.items()}
        cmd = json.loads(evidence['cmd']); meta = json.loads(evidence['meta'])
        need(cmd.get('method') == 'GET' and cmd.get('url') == W.ORIGINS['fixture'] + '/admin/src/' + name,
             'original fixed counter GET required')
        need(cmd.get('auth_attached') is False and evidence['rc'].strip() == b'0', 'anonymous original counter required')
        need(meta.get('status') == 200 and meta.get('transport_outcome') == 0
             and meta.get('original_child_rc') is None, 'original HTTP rc semantics required')
        # W.Attempt appends one recording newline. Recover only the exact
        # metadata-sized original prefix, and verify its independent body SHA.
        # The immutable raw-file SHA remains separately verified by read_ref.
        size = meta['bytes']
        need(type(size) is int and size >= 0, 'original byte length required')
        raw = evidence['raw']
        need(len(raw) >= size and raw[size:] in (b'', b'\n'), 'unexpected raw recording suffix')
        body = raw[:size]
        need(len(body) == size and hashlib.sha256(body).hexdigest() == meta['body_sha256'],
             'complete original counter body/length required')
        counters[name] = body
    return plan, record, expected, counters


def run(release_file, runner=None, transport=None):
    original_release = Path(release_file).read_bytes()
    r = json.loads(original_release)
    need(r['schema'] == 'p26-final-substrate-counter-release-v1' and r['root_accepted'] is True
         and r['phase'] == PHASE, 'explicit Root release required')
    need(set(r['tools_sha256']) == TOOLS, 'exact NEW helper dependency pins required')
    def gate():
        need(Path(release_file).read_bytes() == original_release, 'Root release changed')
        need(datetime.now(timezone.utc) < datetime.fromisoformat(r['expires_utc'].replace('Z', '+00:00')),
             'Root release expired')
        need(G.candidate_hash(G.OPENFOUNDRY) == r['source_sha256'], 'source changed')
        for rel, want in r['tools_sha256'].items():
            need(sha(P26 / rel) == want, 'NEW helper changed')
        need(sha(r['plan_path']) == r['plan_sha256'], 'plan changed')
    gate()
    plan, record, expected, counters = load_plan(r)
    root = Path(r['output_root']).resolve()
    need(root.is_relative_to((P26 / 'results/w6/deployment').resolve()), 'W6 output required')
    att = W.Attempt(root, 'final-substrate-counter-readonly')
    runner = runner or subprocess.run
    observations = {}
    try:
        for side in ['before', 'after']:
            observations[side] = {}
            for name in sorted(record['containers']):
                pin = record['containers'][name]; actual = {}
                for key, fmt in FORMATS.items():
                    gate(); argv = ['docker', 'inspect', pin['cid'], '--format', fmt]
                    prefix = att.step_begin(side + '-' + name + '-' + key, argv)
                    try:
                        cp = runner(argv, capture_output=True, text=True, timeout=120)
                    except Exception as exc:
                        att.step_finish('inspect', prefix, type(exc).__name__, 'unavailable',
                            {'original_child_rc': None, 'failure_kind': 'spawn-or-timeout'}, kind='argv')
                        raise ValueError('inspect spawn/timeout') from None
                    att.step_finish('inspect', prefix, cp.stdout, cp.returncode,
                        {'stderr': cp.stderr, 'original_child_rc': cp.returncode}, kind='argv')
                    need(cp.returncode == 0, 'inspect original rc nonzero')
                    actual[key] = cp.stdout
                validate_snapshot(pin, expected[name], actual, name == W.Q3_CONNECTOR)
                observations[side][name] = actual
            if side == 'before':
                for name in sorted(SOURCES):
                    gate()
                    response, _ = evidenced_http(att, W.ORIGINS['fixture'] + '/admin/src/' + name,
                                                  transport=transport)
                    validate_counter(counters[name], response)
        gate()
        # Revalidate complete original inputs at the end, without live reads.
        load_plan(r)
        att.finish({'schema': 'p26-final-substrate-counter-proof-v1', 'ok': True,
            'source_sha256': r['source_sha256'], 'plan': {'path': r['plan_path'], 'sha256': r['plan_sha256']},
            'main5_safe_config_before_after': observations, 'old_counter_names': sorted(SOURCES),
            'old_counter_sha256': {n: hashlib.sha256(b).hexdigest() for n, b in counters.items()},
            'PG_oldfixture_boots_unchanged': True, 'no_env_values_read': True,
            'scope': 'identity/boot/running + full Mounts + both port dictionaries + complete env NAMES; fixed9 counter bytes. No SQL/H whole-row/creation/health/network claim.',
            'mutation_commands': 0})
        print(att.dir)
        return 0
    except Exception as exc:
        return terminal_error(att, exc, [])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--release-file', required=True)
    args = parser.parse_args()
    try:
        result = run(args.release_file)
    except Exception as exc:
        print('final substrate counter preflight refused (' + type(exc).__name__ + ')', file=sys.stderr)
        result = 1
    sys.exit(result)
