#!/usr/bin/env python3
"""Pure filesystem contract tests; no Docker, HTTP, SQL, or real containers."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from unittest.mock import patch

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('deadline_contract', HERE / 'legacy_deadline_driver.py')
D = importlib.util.module_from_spec(spec)
spec.loader.exec_module(D)


def run(out):
    out.mkdir(parents=True, exist_ok=False)
    results = []
    def case(name, change=None, actual_change=None, expected_ok=False):
        folder = out / name
        root = folder / 'mock-p26'
        ev = root / 'results/w10/this-invocation'
        ev.mkdir(parents=True)
        writer = root / 'regressions/adapters/w10_suite_adapter.py'
        writer.parent.mkdir(parents=True)
        writer.write_text('# pure fixture for Root-pinned writer\n')
        anchor = {'name': D.NAME, 'cid': 'a' * 64, 'image': D.IMAGE, 'started_at': '2026-11-15T01:00:00.000000000Z'}
        current = dict(anchor, started_at='2026-11-15T02:00:00.000000000Z')
        r = {'release_id': 'pure-I-J-invocation', 'driver_predecessor_adapter_sha256': D.sha(writer),
             'driver_predecessor_file': str(ev / 'predecessor.json'), 'evidence_root': str(ev),
             'aux_compose': str(root / 'deployment/legacy-stage/compose.yaml'),
             'aux_project': 'openfoundry-p26-legacy', 'legacy_env_dir': '/private/tmp/openfoundry-p26-legacy-env',
             'service_map': {'connector': 'p26connectorc', 'connector2': D.NAME, 'connector3': 'p26connectorb'}}
        command = ['docker', 'compose', '-f', r['aux_compose'], '--project-name', r['aux_project'], 'start', 'p26connectorc', D.NAME, 'p26connectorb']
        original = ['docker', 'compose', '-f', r['legacy_env_dir'] + '/compose.yaml', '--project-name', 'openfoundry-platform-p1-q3v9', 'start', 'connector', 'connector2', 'connector3']
        def receipt(prefix, cmd, raw):
            for suffix, data in {'.cmd.json': json.dumps(cmd), '.raw': raw, '.rc': '0\n', '.meta.json': json.dumps({'stderr': '', 'kind': 'call', 'utc': '20261115T020001Z'})}.items():
                Path(str(prefix) + suffix).write_text(data)
        start, after = ev / '01-start', ev / '02-after'
        receipt(start, {'original': original, 'replacement': command}, '')
        receipt(after, ['docker', 'inspect', D.NAME, '--format', '{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}'], '\n'.join([current['cid'], current['image'], current['started_at'], 'true']) + '\n')
        proof = {'schema': 'p26-w10-compose-predecessor-v1', 'release_id': r['release_id'],
                 'adapter_tool_sha256': D.sha(writer), 'previous': anchor, 'current': current,
                 'actual_transition': {'argv': command, 'original_rc': 0, 'evidence_step': str(start), 'actual_after_step': str(after)}}
        # JSON objects must not share mutable lists/dicts with release/anchor.
        proof = copy.deepcopy(proof)
        if change:
            change(r, proof, writer, ev, start, after)
        Path(r['driver_predecessor_file']).write_text(json.dumps(proof))
        accepted = False
        try:
            with patch.object(D, 'P26', root), patch.object(D.subprocess, 'run', side_effect=AssertionError('forbidden process')):
                chosen, evidence = D.load_predecessor(r, anchor, writer)
                assert evidence['sha256'] == D.sha(r['driver_predecessor_file'])
                assert len(evidence['receipt_sha256']) == 8
                actual = dict(chosen, name='/' + D.NAME, running=True, labels=dict(D.LABELS))
                cfg = dict(zip(D.KEYS, [1, 64, 300, 300]))
                if actual_change:
                    actual_change(actual, cfg)
                D.validate_before(actual, chosen, cfg, 'up-deadline:7')
                accepted = True
        except (AssertionError, ValueError, KeyError, FileNotFoundError):
            pass
        assert accepted == expected_ok, name + ': unexpected contract verdict'
        results.append({'case': name, 'expected_accept': expected_ok, 'actual_accept': accepted, 'pass': True})
    case('valid-I-sameCID-newboot', expected_ok=True)
    case('wrong-release', lambda r,p,*a: p.update(release_id='other-invocation'))
    case('wrong-writer-rootpin', lambda r,p,*a: r.update(driver_predecessor_adapter_sha256='0'*64))
    case('changed-writer-bytes', lambda r,p,w,*a: w.write_text('changed writer'))
    case('wrong-proof-writerhash', lambda r,p,*a: p.update(adapter_tool_sha256='1'*64))
    case('wrong-root-anchor', lambda r,p,*a: p['previous'].update(cid='b'*64))
    case('newCID-not-I-restart', lambda r,p,*a: p['current'].update(cid='b'*64))
    case('unchanged-boot', lambda r,p,*a: p['current'].update(started_at=p['previous']['started_at']))
    case('wrong-start-target', lambda r,p,*a: p['actual_transition']['argv'].__setitem__(-1,'postgres'))
    case('nonzero-reported-rc', lambda r,p,*a: p['actual_transition'].update(original_rc=1))
    case('nonzero-original-receipt', lambda r,p,w,e,s,a: Path(str(s)+'.rc').write_text('1\n'))
    case('tampered-replacement-receipt', lambda r,p,w,e,s,a: Path(str(s)+'.cmd.json').write_text(json.dumps({'original':[],'replacement':[]})))
    case('tampered-after-boot', lambda r,p,w,e,s,a: Path(str(a)+'.raw').write_text('a'*64+'\n'+D.IMAGE+'\n'+p['previous']['started_at']+'\ntrue\n'))
    case('missing-raw', lambda r,p,w,e,s,a: Path(str(a)+'.raw').unlink())
    case('receipt-outside-invocation', lambda r,p,w,e,s,a: p['actual_transition'].update(evidence_step=str(e.parent/'not-this-invocation')))
    case('actual-CID-drift', actual_change=lambda a,c: a.update(cid='c'*64))
    case('actual-boot-drift', actual_change=lambda a,c: a.update(started_at='2026-11-15T03:00:00Z'))
    case('actual-owner-drift', actual_change=lambda a,c: a['labels'].update({'of-p26-owner':'other'}))
    case('actual-compose-project-drift', actual_change=lambda a,c: a['labels'].update({'com.docker.compose.project':'q3'}))
    case('actual-TTL-not-300', actual_change=lambda a,c: c.update(CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS=7))
    (out / 'RESULT.json').write_text(json.dumps({'pure_only': True, 'live_calls': 0, 'cases': results, 'passed': len(results), 'driver_sha256': D.sha(HERE/'legacy_deadline_driver.py')}, indent=2)+'\n')
    print(out)
    print(str(len(results)) + ' pure contract cases passed; live calls=0')


if __name__ == '__main__':
    run(Path(sys.argv[1]).resolve())
