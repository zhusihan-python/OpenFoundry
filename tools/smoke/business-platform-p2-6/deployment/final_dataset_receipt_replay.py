#!/usr/bin/env python3
"""CODE proposal: exact formal NEW snapshot receipt replay after Dataset restart.

baseline and replay are separately Root-released. Private publish_body is read
twice into process memory only; immutable SHA/length bind the two reads. The
helper never restarts anything, creates tasks, or persists private material.
"""
import argparse
import base64
import hashlib
import json
import re
from pathlib import Path
import subprocess
import sys
from urllib import request, error

sys.dont_write_bytecode = True
P26 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(P26))
sys.path.insert(0, str(P26 / 'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
from final_old_artifact_readonly import evidenced_http, terminal_error

RUN = '1c757fac-7a33-4586-9ae7-567cac66abab'
DEF = 'c808bb76-0ae9-4ed7-9940-cc9dab74b973'
SOURCE_ID = 'fa248de0-7642-4e89-ac45-a3af8b5cac47'
DATASET = '55665582-a89c-4f7a-8be9-886136e79cb2'
ORG = 'a792b91e-dab5-4739-9344-2848c1e83487'
ACTOR = '1c0e238a-3657-4978-98dc-d71a342c3fa1'
TXN = '51ae5250-a914-40a6-8f8a-7a4b150e09b3'
KEY = 'of-p26-pub-v1:' + RUN
BODY_SHA = '2deb1a96fe8577f17b6263499570f83236caf631cd8793c76f5caa24fb7aaa4e'
DOWNLOAD_SHA = '742c7c5a0660cf4d226821d982173bce8342cdb551b03075118e8040c73329d9'
CASE_DIR = P26 / 'results/w7/attempts/dropack_auto_retry_exact_replay-20261115T070904Z'
SELECTION_FILES = [CASE_DIR / 'RESULT.json', CASE_DIR / '018-08-run-poll.raw']
PRIVATE_SQL = f"""SELECT json_build_object('body_base64', encode(pi.publish_body,'base64'),
 'request_fingerprint',pi.request_fingerprint,'body_bytes',octet_length(pi.publish_body))
FROM sync_run_publish_intents pi
JOIN sync_runs r ON r.id=pi.run_id AND r.sync_def_id=pi.sync_def_id
JOIN batch_sync_defs d ON d.id=r.sync_def_id AND d.source_id=pi.source_id
JOIN connections c ON c.id=d.source_id
WHERE pi.run_id='{RUN}'::uuid AND pi.sync_def_id='{DEF}'::uuid
 AND pi.source_id='{SOURCE_ID}'::uuid AND pi.output_dataset_id='{DATASET}'::uuid
 AND d.output_dataset_id=pi.output_dataset_id
 AND pi.org_id='{ORG}'::uuid AND r.org_id=pi.org_id AND d.org_id=pi.org_id AND c.org_id=pi.org_id
 AND c.owner_id='{ACTOR}'::uuid AND r.owner_id=c.owner_id AND r.requested_by=c.owner_id
 AND r.recovery_source_id=pi.source_id AND r.recovery_output_dataset_id=pi.output_dataset_id
 AND r.recovery_protocol='p26-v1' AND pi.recovery_protocol=r.recovery_protocol
 AND r.status='succeeded' AND r.control_mode='controlled' AND r.publish_entered_at IS NOT NULL
 AND COALESCE(r.cp_mode,'')='' AND pi.cp_mode='' AND pi.completed_checkpoint IS NULL
 AND pi.recovery_state='finalized' AND pi.resolved_at IS NOT NULL
 AND pi.txn_id='{TXN}'::uuid AND r.dataset_version_id::text=pi.txn_id::text
 AND pi.publish_key='{KEY}' AND pi.branch='main' AND pi.request_fingerprint='{BODY_SHA}';"""


def digest(data):
    return hashlib.sha256(data).hexdigest()


def private_material(att, argv, runner=None):
    """Deliberate PRIVATE projection: full stdout/stderr NEVER archived."""
    begin = att.step_begin('private-intent-select', argv)
    try:
        cp = (runner or subprocess.run)(argv, capture_output=True, timeout=30)
    except Exception as exc:
        out = getattr(exc, 'stdout', None) or b''
        err = getattr(exc, 'stderr', None) or b''
        if isinstance(out, str): out = out.encode()
        if isinstance(err, str): err = err.encode()
        code = getattr(exc, 'returncode', None)
        code = code if type(code) is int else None
        att.step_finish('private-intent-select', begin, 'PRIVATE stdout/stderr omitted by projection policy', 'unavailable' if code is None else code,
                        {'original_child_rc': code, 'failure_kind': type(exc).__name__, 'stdout_sha256': digest(out), 'stdout_bytes': len(out), 'stderr_sha256': digest(err), 'stderr_bytes': len(err)}, kind='private-projection')
        raise RuntimeError('private SELECT failed; projection evidence retained') from None
    metadata = {'original_child_rc': cp.returncode, 'stdout_sha256': digest(cp.stdout), 'stdout_bytes': len(cp.stdout), 'stderr_sha256': digest(cp.stderr), 'stderr_bytes': len(cp.stderr), 'projection_policy': 'PRIVATE body/candidate/inline/auth never written; command + original rc + hashes/lengths only'}
    att.step_finish('private-intent-select', begin, 'PRIVATE stdout/stderr omitted by projection policy', cp.returncode, metadata, kind='private-projection')
    assert cp.returncode == 0, 'private SELECT original rc nonzero'
    # Exact unique row from PK run_id, trusted joins and all frozen predicates.
    assert len(cp.stdout.splitlines()) == 1, 'private SELECT must return exactly one row'
    obj = json.loads(cp.stdout)
    body = base64.b64decode(''.join(obj['body_base64'].split()), validate=True)
    assert 2 <= len(body) <= 67108864 and obj['body_bytes'] == len(body)
    assert digest(body) == obj['request_fingerprint'] == BODY_SHA
    doc = json.loads(body)
    assert doc['idempotency_key'] == KEY and doc['branch'] == 'main' and doc['create_if_missing'] is False
    att.record('private-material-hash', {'selection': RUN}, 'PRIVATE publish_body omitted; SHA/length verified', 0, {'body_sha256': digest(body), 'body_bytes': len(body)}, kind='private-projection')
    return body


def frozen_source(release):
    source = release.get('candidate_source', release.get('frozen_source_sha256'))
    assert isinstance(source, str) and re.fullmatch('[0-9a-f]{64}', source) and int(source, 16) != 0, 'Root full frozen source required'
    if 'frozen_source_sha256' in release:
        assert release['frozen_source_sha256'] == source, 'Root frozen source fields differ'
    return source


def snapshot_checkpoint(public_get):
    """The selected ordinary snapshot has no incremental checkpoint/epoch."""
    url = W.ORIGINS['connector'] + '/api/v1/data-connection/syncs/' + DEF + '/checkpoint'
    cp, _ = public_get(url)
    assert cp.status == 404, 'ordinary snapshot checkpoint must remain uninitialized'
    body = json.loads(cp.body)
    assert body == {'error': 'incremental checkpoint not initialized for this sync definition'}
    return {'status': cp.status, 'body': body, 'body_sha256': digest(cp.body)}


def run(mode, release_file):
    phase = 'final-receipt-replay-baseline' if mode == 'baseline' else 'final-receipt-replay-post'
    release = G.load_release(release_file)
    assert release and phase in release['released_for']
    source = frozen_source(release)
    assert G.candidate_hash(G.OPENFOUNDRY) == source
    for rel in ['deployment/final_dataset_receipt_replay.py', 'deployment/final_old_artifact_readonly.py', 'preflight/p26_current_preflight.py']:
        assert G.sha256_file(P26 / rel) == release['final_receipt_tool_shas'][rel]
    for f in SELECTION_FILES:
        assert G.sha256_file(f) == release['final_receipt_selection_shas'][str(f.relative_to(P26))]
    formal = json.loads(SELECTION_FILES[0].read_text())
    assert formal['outcome'] == 'pass' and formal['run_id'] == RUN and formal['def_id'] == DEF and formal['dataset_id'] == DATASET and formal['body_sha256'] == BODY_SHA
    assert {'kind': 'source', 'id': SOURCE_ID, 'src': 'p26snap1'} in formal['owned_roster']
    att = W.Attempt(P26 / 'results/w6/deployment', 'final-receipt-' + mode)
    def released():
        assert G.load_release(release_file) == release
    def cmd(name, argv):
        released()
        before = att.step_begin(name, argv)
        try:
            cp = subprocess.run(argv, capture_output=True, text=True, timeout=30)
        except Exception as exc:
            att.step_finish(name, before, type(exc).__name__, 'unavailable', {'original_child_rc': None}, kind='argv')
            raise RuntimeError('safe identity read failed') from None
        att.step_finish(name, before, cp.stdout, cp.returncode, {'stderr': cp.stderr, 'original_child_rc': cp.returncode}, kind='argv')
        assert cp.returncode == 0
        return cp.stdout.strip()
    def identity():
        lines = cmd('dataset-identity', ['docker','inspect', W.Q3_DATASET,'--format','{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}']).splitlines()
        assert len(lines) == 4 and lines[3] == 'true'
        return dict(zip(['cid','image','started_at'],lines[:3]))
    try:
        # Load existing private writer only into process memory; never shell-source.
        private = Path('/private/tmp/openfoundry-p26-http-smoke.env')
        assert private.stat().st_mode & 0o777 == 0o600 and not private.is_symlink()
        values = dict(ln.split('=',1) for ln in private.read_text().splitlines() if ln and '=' in ln)
        bearer = values['P26_W7_JWT_WRITER']
        assert bearer.startswith('Bearer ')
        token = bearer[7:]
        att.add_secret(token)
        claims = json.loads(base64.urlsafe_b64decode(token.split('.')[1] + '==='))
        assert claims['sub'] == ACTOR and claims['org_id'] == ORG
        dvs = W.ORIGINS['dataset'] + '/api/v1/datasets/' + DATASET
        def get(url, auth=True):
            released()
            response, location = evidenced_http(att, url, token if auth else None)
            return response, location
        def snapshot():
            paths = {'receipt': dvs+'/outputs/commits/'+KEY, 'branch': dvs+'/branches/main', 'versions': dvs+'/versions?limit=500', 'files': dvs+'/files', 'run':W.ORIGINS['connector']+'/api/v1/data-connection/syncs/'+DEF+'/runs/'+RUN}
            docs, raw_receipt = {}, None
            for k,url in paths.items():
                response,_ = get(url)
                assert response.status == 200
                docs[k] = json.loads(response.body)
                if k == 'receipt': raw_receipt = response.body
            assert docs['receipt']['request_fingerprint'] == BODY_SHA and docs['receipt']['idempotency_key'] == KEY
            assert docs['receipt']['transaction']['id'] == TXN and docs['branch']['head_transaction_id'] == TXN
            transaction = docs['receipt']['transaction']
            assert transaction['status'] == 'COMMITTED' and docs['receipt']['branch'] == 'main'
            assert transaction['metadata']['content_hash'] == DOWNLOAD_SHA
            assert transaction['metadata']['lineage'] == {'source_id':SOURCE_ID,'sync_def_id':DEF,'sync_run_id':RUN}
            assert len(docs['versions']['data']) == 1 and not docs['versions'].get('has_more')
            assert docs['versions']['data'][0]['transaction_id'] == TXN
            assert len(docs['files']['files']) == 1
            assert docs['run']['id'] == RUN and docs['run']['status'] == 'succeeded' and docs['run']['recovery']['state'] == 'finalized'
            assert docs['run']['org_id'] == ORG and docs['run']['owner_id'] == ACTOR and docs['run']['requested_by'] == ACTOR
            assert docs['run']['sync_def_id'] == DEF and docs['run']['recovery']['protocol'] == 'p26-v1' and docs['run']['recovery']['transaction_id'] == TXN
            fid = docs['files']['files'][0]['id']
            response,location = get(dvs+'/files/'+fid+'/download')
            assert response.status == 302
            response,_ = get(W.validate_download_redirect(location), False)
            assert response.status == 200 and digest(response.body) == DOWNLOAD_SHA
            checkpoint = snapshot_checkpoint(get)
            return {'docs':docs,'receipt_bytes_sha256':digest(raw_receipt),'download_sha256':digest(response.body),'download_bytes':len(response.body),'checkpoint':checkpoint}
        current = identity()
        before = snapshot()
        pg = release['w7_pg_observer']
        assert pg['user'] == 'openfoundry' and pg['database'] == 'connectors'
        assert len(pg['cid']) == 64 and all(c in '0123456789abcdef' for c in pg['cid'])
        released()
        argv = ['docker','exec',pg['cid'],'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','openfoundry','-d','connectors','-c','BEGIN READ ONLY;','-c',PRIVATE_SQL,'-c','COMMIT;']
        body = private_material(att, argv)
        if mode == 'baseline':
            assert G.candidate_hash(G.OPENFOUNDRY) == source
            result = {'ok':True,'selection_run':RUN,'dataset_identity':current,'public_snapshot':before,'body_sha256':digest(body),'body_bytes':len(body),'source_S':source}
            att.finish(result)
            del body
            print(att.dir)
            return 0
        base = release['final_receipt_baseline']
        assert G.sha256_file(base['path']) == base['sha256']
        baseline = json.loads(Path(base['path']).read_text())
        assert baseline['ok'] and baseline['selection_run'] == RUN and baseline['source_S'] == source
        old = baseline['dataset_identity']
        assert current == release['final_receipt_dataset_after']
        assert current['cid'] == old['cid'] and current['image'] == old['image'] and current['started_at'] != old['started_at']
        assert before == baseline['public_snapshot'] and digest(body) == baseline['body_sha256'] and len(body) == baseline['body_bytes']
        released()
        receipt = att.step_begin('exact-body-replay-POST', {'method':'POST','url':dvs+'/outputs:commit','private_body_sha256':digest(body),'private_body_bytes':len(body),'request_body_recorded':False})
        # Single direct POST, no retry, no proxy, no new body serialization.
        req = request.Request(dvs+'/outputs:commit', data=body, method='POST', headers={'Authorization':bearer,'Content-Type':'application/json'})
        try:
            opener = request.build_opener(request.ProxyHandler({}), W._NoRedirectHandler)
            try:
                with opener.open(req, timeout=30) as response:
                    status, ack = response.status, response.read(W.HTTP_MAX_BODY+1)
            except error.HTTPError as exc:
                status, ack = exc.code, exc.read(W.HTTP_MAX_BODY+1)
            att.step_finish('exact-body-replay-POST',receipt,ack,0,{'HTTP_status':status,'transport_outcome':0,'original_child_rc':None,'response_sha256':digest(ack)},kind='http')
        except Exception as exc:
            att.step_finish('exact-body-replay-POST',receipt,type(exc).__name__,1,{'transport_outcome':1,'original_child_rc':None},kind='http')
            raise RuntimeError('exact replay transport failed; not retried') from None
        assert status == 200 and len(ack) <= W.HTTP_MAX_BODY and digest(ack) == before['receipt_bytes_sha256']
        del body, req
        after = snapshot()
        assert after == before and identity() == current and G.candidate_hash(G.OPENFOUNDRY) == source
        att.finish({'ok':True,'selection_run':RUN,'before':before,'after':after,'dataset_identity':current,'body_sha256':BODY_SHA,'body_bytes':baseline['body_bytes'],'POST_status':status,'POST_response_sha256':digest(ack),'source_S':source})
        print(att.dir)
        return 0
    except Exception as exc:
        return terminal_error(att, exc, [])


if __name__ == '__main__':
    ap=argparse.ArgumentParser();ap.add_argument('mode',choices=['baseline','replay']);ap.add_argument('--release-file',required=True);args=ap.parse_args()
    try: rc=run(args.mode,args.release_file)
    except Exception as exc:
        print('receipt replay preflight failed ('+type(exc).__name__+')',file=sys.stderr);rc=1
    sys.exit(rc)
