#!/usr/bin/env python3
"""Real isolated PBS crash windows. Prepare is harmless; execute needs exclusive PBS slot.

A test trigger waits on a test-session advisory lock. Before: INSERT waits
inside admission, so dispatch/build are not visible. After: execution's first
executing UPDATE waits while the admitted dispatch and deterministic Build are
already committed. We observe the waiting PG lock, force-delete the actual PBS
Pod, release the barrier, and verify redelivery/recovery and the real product.
No production source or fault endpoint is used.
"""
import argparse
import hashlib
import json
import subprocess
import time
from canonical_acceptance import K, ROOT, command, forward, http, pg, inject, save
from failure_acceptance import prepare, await_state

KEYS = {'before': 'runtime-admission-crash-before-v1',
        'after': 'runtime-admission-crash-after-v1'}


def sql(statement):
    return command(K + ['exec', 'ofwp32-pg-0', '--', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'ofwp32', '-d', 'ofwp32', '-Atc', statement], text=True)


def wait_for(probe, description, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = probe()
        if value:
            return value
        time.sleep(.25)
    raise TimeoutError(description)


def snapshot(event):
    eid = event['event_id']
    return pg("SELECT json_build_object('dispatches',(SELECT count(*) FROM scheduling.dispatches WHERE event_id='" + eid +
              "'),'builds',(SELECT count(*) FROM builds WHERE id IN (SELECT build_id FROM scheduling.dispatches WHERE event_id='" + eid +
              "')),'receipts',(SELECT count(*) FROM scheduling.publish_receipts WHERE dispatch_event_id='" + eid +
              "'),'states',(SELECT coalesce(json_agg(state),'[]'::json) FROM scheduling.dispatches WHERE event_id='" + eid + "'));")


def prepare_cases():
    token = (ROOT / 'org-token.txt').read_text().strip()
    seed = json.loads((ROOT / 'canonical-seed.json').read_text())
    with forward('ofwp32-pbs', 50081) as pbs:
        for key in KEYS.values():
            if (ROOT / (key + '-event.ndjson')).exists():
                raise RuntimeError('Refusing to replace existing event fixture: ' + key)
            prepare(pbs, token, seed, key)
    print('PREPARED only: no events injected, barriers installed, or Pods changed')


def execute(window):
    key = KEYS[window]
    event = json.loads((ROOT / (key + '-event.ndjson')).read_text())
    if snapshot(event)['dispatches']:
        raise RuntimeError('Case already admitted; use a fresh explicit case key')
    suffix = hashlib.sha256(key.encode()).hexdigest()[:12]
    name = 'ofwp32_crash_' + suffix
    lock_id = int(suffix[:7], 16)
    app = name + '_holder'
    holder = None
    token = (ROOT / 'org-token.txt').read_text().strip()
    seed = json.loads((ROOT / 'canonical-seed.json').read_text())
    prefix = 'admission-crash-' + window
    log = open(ROOT / (prefix + '-holder.log'), 'ab')
    try:
        # Only this fixture's key can hit the trigger. All other service work
        # sees an immediate RETURN NEW, though Pod crash still requires exclusivity.
        condition = "NEW.schedule_key='" + key + "'"
        operation = 'INSERT' if window == 'before' else 'UPDATE'
        if window == 'after':
            condition += " AND NEW.state='executing' AND OLD.state IS DISTINCT FROM NEW.state"
        sql("CREATE FUNCTION scheduling." + name + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF " + condition +
            " THEN PERFORM pg_advisory_xact_lock(" + str(lock_id) + "); END IF; RETURN NEW; END $$; " +
            "CREATE TRIGGER " + name + " BEFORE " + operation + " ON scheduling.dispatches FOR EACH ROW EXECUTE FUNCTION scheduling." + name + "()")
        holder = subprocess.Popen(K + ['exec', 'ofwp32-pg-0', '--', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'ofwp32', '-d', 'ofwp32', '-Atc',
                                      "SET application_name='" + app + "'; SELECT pg_advisory_lock(" + str(lock_id) + "); SELECT pg_sleep(240);"], stdout=log, stderr=log)
        wait_for(lambda: pg("SELECT count(*)::int FROM pg_locks WHERE locktype='advisory' AND objid=" + str(lock_id) + " AND granted;"), 'holder lock')
        pod = json.loads(command(K + ['get', 'pods', '-l', 'app=ofwp32-pbs', '-o', 'json'], text=True))
        if len(pod['items']) != 1:
            raise RuntimeError('Expected exactly one PBS Pod: ' + str([item['metadata']['name'] for item in pod['items']]))
        victim = pod['items'][0]['metadata']['name']
        uid = pod['items'][0]['metadata']['uid']
        save(prefix + '-victim.json', {'pod': victim, 'uid': uid})
        with forward('ofwp32-dvs', 50078) as dvs:
            path = '/v1/datasets/' + seed['output']['id']
            before = http(dvs, path + '/versions', token, evidence=prefix + '-product')
            input_rows = http(dvs, '/v1/datasets/' + seed['input']['id'] + '/files/rows?branch=main&mode=preview&limit=10', token, evidence=prefix + '-product')
            save(prefix + '-input-rows.json', input_rows)
            save(prefix + '-versions-before.json', before)
            inject((ROOT / (key + '-event.ndjson')).read_bytes(), prefix + '-producer')
            waiting = wait_for(lambda: pg("SELECT coalesce(json_agg(json_build_object('pid',a.pid,'state',a.state,'wait_event',a.wait_event,'query',a.query)), '[]'::json) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory' AND l.objid=" + str(lock_id) + " AND NOT l.granted;"), 'PBS waiting inside target transaction')
            save(prefix + '-barrier-wait.json', waiting)
            at_barrier = snapshot(event)
            save(prefix + '-barrier-snapshot.json', at_barrier)
            expected = 0 if window == 'before' else 1
            assert at_barrier['dispatches'] == expected and at_barrier['builds'] == expected, at_barrier
            result = subprocess.run(K + ['delete', 'pod', victim, '--grace-period=0', '--force', '--wait=true'], capture_output=True, text=True)
            save(prefix + '-kill.json', {'rc': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr, 'time': time.time()})
            assert result.returncode == 0
            sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='" + app + "'")
            command(K + ['rollout', 'status', 'deployment/ofwp32-pbs', '--timeout=120s'], text=True)
            replacement = json.loads(command(K + ['get', 'pods', '-l', 'app=ofwp32-pbs', '-o', 'json'], text=True))
            save(prefix + '-replacement.json', [{'name': p['metadata']['name'], 'uid': p['metadata']['uid']} for p in replacement['items']])
            assert all(p['metadata']['uid'] != uid for p in replacement['items'])
            dispatch = await_state(event, {'succeeded'}, timeout=190)
            save(prefix + '-final-dispatch.json', dispatch)
            final = snapshot(event)
            save(prefix + '-final-snapshot.json', final)
            assert final['dispatches'] == final['builds'] == final['receipts'] == 1, final
            versions = http(dvs, path + '/versions', token, evidence=prefix + '-product')
            rows = http(dvs, path + '/files/rows?branch=main&mode=preview&limit=10', token, evidence=prefix + '-product')
            save(prefix + '-versions-after.json', versions)
            save(prefix + '-rows-after.json', rows)
            assert rows['rows'] == input_rows['rows'], (input_rows, rows)
            def size(value):
                if isinstance(value, list): return len(value)
                for field in ('versions', 'items', 'data'):
                    if isinstance(value.get(field), list): return len(value[field])
                raise AssertionError('Unrecognized versions response')
            assert size(versions) == size(before) + 1, (before, versions)
        save(prefix + '-result.json', {'status': 'PASS', 'key': key, 'event_id': event['event_id'], 'window': window})
        print('PASS', prefix, event['event_id'])
    finally:
        try:
            sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='" + app + "'")
            sql('DROP TRIGGER IF EXISTS ' + name + ' ON scheduling.dispatches; DROP FUNCTION IF EXISTS scheduling.' + name + '()')
        finally:
            if holder is not None:
                try: holder.wait(timeout=5)
                except subprocess.TimeoutExpired: holder.terminate(); holder.wait(timeout=5)
            log.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'before', 'after'))
    parser.add_argument('--exclusive-pbs-slot', action='store_true')
    args = parser.parse_args()
    ROOT.mkdir(parents=True, exist_ok=True)
    if args.action == 'prepare': prepare_cases()
    elif not args.exclusive_pbs_slot: parser.error('Execution needs runtime-coordinated --exclusive-pbs-slot')
    else: execute(args.action)
