#!/usr/bin/env python3
"""V8 real Kafka/PG crash windows; run only in the assigned exclusive PBS/Kafka slot.
Never delete/scale the ephemeral Kafka Pod. Pause its exact CRI process from
its kind ancestor, with unconditional CONT and identity checks.
"""
import argparse
import ipaddress
import json
import re
import subprocess
import time
import uuid
from canonical_acceptance import K, ROOT, command, forward, http, pg, inject, save, drain
from failure_acceptance import prepare, await_state
from admission_crash_acceptance import sql, wait_for

TOPIC = 'pipeline.builds.v1'
KAFKA = 'ofwp32-kafka-0'
GROUP = 'ofwp32-runtime-v1'


def kafka(tool, args, **kwargs):
    return command(K + ['exec', KAFKA, '--', '/opt/kafka/bin/' + tool] + args, **kwargs)


def scale_pbs(count):
    command(K + ['scale', 'deployment/ofwp32-pbs', '--replicas=' + str(count)], text=True)
    if count:
        command(K + ['rollout', 'status', 'deployment/ofwp32-pbs', '--timeout=120s'], text=True)
    else:
        command(K + ['wait', '--for=delete', 'pod', '-l', 'app=ofwp32-pbs', '--timeout=90s'], text=True)


def crash_pbs(prefix):
    pods=json.loads(command(K+['get','pods','-l','app=ofwp32-pbs','-o','json'],text=True))
    victims=[{'name':p['metadata']['name'],'uid':p['metadata']['uid']} for p in pods['items']]
    command(K+['scale','deployment/ofwp32-pbs','--replicas=0'],text=True)
    results=[]
    for victim in victims:
        result=subprocess.run(K+['delete','pod',victim['name'],'--force','--grace-period=0','--ignore-not-found=true'],capture_output=True,text=True)
        results.append(dict(victim,rc=result.returncode,stdout=result.stdout,stderr=result.stderr))
        assert result.returncode==0
    save(prefix+'-pbs-crash.json',results)
    command(K+['wait','--for=delete','pod','-l','app=ofwp32-pbs','--timeout=90s'],text=True)


def broker_identity():
    pod = json.loads(command(K + ['get', 'pod', KAFKA, '-o', 'json'], text=True))
    status = pod['status']['containerStatuses'][0]
    cid = status['containerID'].split('://', 1)[1]
    node = pod['spec']['nodeName']
    info = json.loads(command(['docker', 'exec', node, 'crictl', 'inspect', cid], text=True))
    assert info['status']['labels']['io.kubernetes.pod.namespace'] == 'ofwp32', info['status']['labels']
    assert info['status']['labels']['io.kubernetes.pod.name'] == KAFKA, info['status']['labels']
    pid = int(info['info']['pid'])
    cmdline = command(['docker', 'exec', node, 'cat', '/proc/' + str(pid) + '/cmdline'], text=True)
    assert 'kafka.Kafka' in cmdline, cmdline
    return {'uid': pod['metadata']['uid'], 'restart_count': status['restartCount'], 'container_id': cid, 'node': node, 'pid': pid}


def signal_broker(identity, signal):
    # The PID is validated from this exact live Kafka container, not a process-name search.
    current = broker_identity()
    assert current == identity, (identity, current)
    command(['docker', 'exec', identity['node'], 'kill', '-' + signal, str(identity['pid'])], text=True)
    if signal == 'STOP':
        state = command(['docker', 'exec', identity['node'], 'cat', '/proc/' + str(identity['pid']) + '/status'], text=True)
        assert re.search(r'^State:\s+T', state, re.MULTILINE), state
        return state


def offsets():
    return {'end_offsets': kafka('kafka-get-offsets.sh', ['--bootstrap-server', 'localhost:9092', '--topic', TOPIC], text=True),
            'consumer_offsets': kafka('kafka-consumer-groups.sh', ['--bootstrap-server', 'localhost:9092', '--group', GROUP, '--describe'], text=True)}


def poison(offset):
    return pg("SELECT coalesce(json_agg(row_to_json(p)),'[]'::json) FROM scheduling.poison_messages p WHERE topic='" + TOPIC + "' AND partition_id=0 AND partition_offset=" + str(offset) + ';')


def dlq_records(offset, prefix):
    result = subprocess.run(K + ['exec', KAFKA, '--', '/opt/kafka/bin/kafka-console-consumer.sh', '--bootstrap-server', 'localhost:9092', '--topic', '__dlq.' + TOPIC,
                                 '--from-beginning', '--timeout-ms', '6000', '--property', 'print.headers=true', '--property', 'print.offset=true'], capture_output=True, text=True)
    save(prefix + '-dlq-consumer.json', {'rc': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
    records = []
    for line in result.stdout.splitlines():
        payload = line.rsplit('\t', 1)[-1]
        try: body = json.loads(payload)
        except ValueError: continue
        if body.get('topic') == TOPIC and body.get('partition') == 0 and body.get('offset') == offset:
            records.append({'wire': line, 'body': body})
    return records


def group_committed(raw):
    rows = [line.split() for line in raw.splitlines() if line.startswith(GROUP + ' ')]
    row = next(row for row in rows if row[1] == TOPIC and row[2] == '0')
    return int(row[3])


def main(window, run_id):
    prefix = 'poison-' + window + '-' + run_id
    marker = '{invalid-json-' + prefix + '-' + uuid.uuid4().hex
    key = 'runtime-v8-' + window + '-' + uuid.uuid4().hex[:8]
    token = (ROOT / 'org-token.txt').read_text().strip()
    seed = json.loads((ROOT / 'canonical-seed.json').read_text())
    identity = broker_identity()
    save(prefix + '-broker-before.json', identity)
    with forward('ofwp32-pbs', 50081) as pbs:
        event = prepare(pbs, token, seed, key)
    initial = offsets()
    # The fixture must use one partition, guaranteeing poison and healthy event ordering.
    assert len(initial['end_offsets'].strip().splitlines()) == 1, initial
    save(prefix + '-offsets-before.json', initial)
    lock_id = 80480001 if window == 'tx-failure' else 80480002
    name = 'ofwp32_poison_' + window.replace('-', '_')
    app = name + '_holder'
    stopped = False
    network_rule = None
    holder = None
    log = open(ROOT / (prefix + '-holder.log'), 'ab')
    try:
        scale_pbs(0)
        offset = int(offsets()['end_offsets'].strip().split(':')[-1])
        inject((marker + '\n').encode(), prefix + '-producer')
        assert int(offsets()['end_offsets'].strip().split(':')[-1]) == offset + 1, 'Concurrent producer polluted fixture offset'
        save(prefix + '-identity.json', {'topic': TOPIC, 'partition': 0, 'offset': offset, 'raw_message': marker})
        sql('CREATE SEQUENCE scheduling.' + name + '_seq')
        first_failure = "IF nextval('scheduling." + name + "_seq')=1 THEN RAISE EXCEPTION 'ofwp32 deliberate DLQ transaction failure'; END IF; " if window == 'tx-failure' else ''
        sql("CREATE FUNCTION scheduling." + name + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.topic='" + TOPIC + "' AND NEW.partition_id=0 AND NEW.partition_offset=" + str(offset) + " AND NEW.state='dlq_enqueued' THEN " + first_failure + "PERFORM pg_advisory_xact_lock(" + str(lock_id) + "); END IF; RETURN NEW; END $$; CREATE TRIGGER " + name + " BEFORE UPDATE ON scheduling.poison_messages FOR EACH ROW EXECUTE FUNCTION scheduling." + name + '()')
        holder = subprocess.Popen(K + ['exec', 'ofwp32-pg-0', '--', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'ofwp32', '-d', 'ofwp32', '-Atc', "SET application_name='" + app + "'; SELECT pg_advisory_lock(" + str(lock_id) + '); SELECT pg_sleep(240);'], stdout=log, stderr=log)
        wait_for(lambda: pg("SELECT count(*)::int FROM pg_locks WHERE locktype='advisory' AND objid=" + str(lock_id) + ' AND granted;'), 'poison holder')
        scale_pbs(1)
        waiting = wait_for(lambda: pg("SELECT coalesce(json_agg(json_build_object('pid',a.pid,'wait_event',a.wait_event,'query',a.query)),'[]'::json) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory' AND l.objid=" + str(lock_id) + ' AND NOT l.granted;'), 'DLQ transaction barrier', 80)
        save(prefix + '-barrier.json', waiting)
        before = poison(offset)
        save(prefix + '-pre-commit-poison.json', before)
        assert len(before) == 1 and before[0]['attempts'] == 5 and before[0]['state'] == 'counting', before
        save(prefix + '-offsets-at-barrier.json', offsets())
        assert group_committed(offsets()['consumer_offsets']) <= offset
        if window == 'tx-failure':
            sequence = pg('SELECT last_value FROM scheduling.' + name + '_seq;')
            save(prefix + '-failed-tx-count.json', sequence)
            assert sequence >= 2, sequence
            assert dlq_records(offset, prefix + '-before') == []
            # The first DLQ transaction demonstrably failed. Kill while the retry
            # still cannot commit; the fresh service must use the spent budget.
            crash_pbs(prefix)
        else:
            pbs_pods=json.loads(command(K+['get','pods','-l','app=ofwp32-pbs','-o','json'],text=True))
            assert len(pbs_pods['items'])==1
            pbs_pod=pbs_pods['items'][0]
            pbs_ip=pbs_pod['status']['podIP']
            pbs_node=pbs_pod['spec']['nodeName']
            pbs_cid=pbs_pod['status']['containerStatuses'][0]['containerID'].split('://',1)[1]
            pbs_cri=json.loads(command(['docker','exec',pbs_node,'crictl','inspect',pbs_cid],text=True))
            pbs_pid=int(pbs_cri['info']['pid'])
            broker_pod=json.loads(command(K+['get','pod',KAFKA,'-o','json'],text=True))
            broker_ip=broker_pod['status']['podIP']
            ipaddress.ip_address(pbs_ip);ipaddress.ip_address(broker_ip)
            stopped = True  # Set before signalling so finally always attempts CONT.
            stop_state=signal_broker(identity, 'STOP')
            netns=command(['docker','exec',identity['node'],'readlink','/proc/'+str(identity['pid'])+'/ns/net'],text=True)
            node_netns=command(['docker','exec',identity['node'],'readlink','/proc/1/ns/net'],text=True)
            assert netns != node_netns,(netns,node_netns)
            network_rule=['-s',pbs_ip,'-d',broker_ip,'-p','tcp','--dport','9092','-m','comment','--comment',name+'_'+run_id,'-j','DROP']
            ipt=['docker','exec',identity['node'],'nsenter','-t',str(identity['pid']),'-n','iptables','-w','5']
            command(ipt+['-I','INPUT','1']+network_rule,text=True)
            save(prefix+'-network-rule-installed.json',{'netns':netns,'node_netns':node_netns,'rule':network_rule,'rules':command(ipt+['-nvL','INPUT'],text=True)})
            save(prefix + '-broker-stopped.json', {'identity': identity, 'time': time.time(), 'proc_status':stop_state})
            sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='" + app + "'")
            committed = wait_for(lambda: (lambda rows: rows if rows and rows[0]['state'] == 'dlq_enqueued' else None)(poison(offset)), 'DLQ committed with Kafka unable to ACK')
            save(prefix + '-committed-before-crash.json', committed)
            assert committed[0]['attempts'] == 5
            crash_pbs(prefix)
            # Ensure the exact original PBS task is dead before removing its
            # flow rule; no client may retransmit a queued CommitRequest later.
            current=json.loads(command(['docker','exec',pbs_node,'crictl','inspect',pbs_cid],text=True))
            if current['status']['state']=='CONTAINER_RUNNING':
                assert int(current['info']['pid'])==pbs_pid
                command(['docker','exec',pbs_node,'kill','-KILL',str(pbs_pid)],text=True)
            wait_for(lambda: json.loads(command(['docker','exec',pbs_node,'crictl','inspect',pbs_cid],text=True))['status']['state'] != 'CONTAINER_RUNNING','old PBS task terminated')
            counters=command(ipt+['-x','-nvL','INPUT'],text=True)
            hitline=next(line for line in counters.splitlines() if name+'_'+run_id in line)
            assert int(hitline.split()[0])>0,counters
            save(prefix+'-ack-flow-drop.json',{'pbs_ip':pbs_ip,'kafka_ip':broker_ip,'counters':counters,'retained_rules':command(ipt+['-S','INPUT'],text=True),'old_pbs_task_dead':True})
        sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='" + app + "'")
        sql('DROP TRIGGER IF EXISTS ' + name + ' ON scheduling.poison_messages; DROP FUNCTION IF EXISTS scheduling.' + name + '()')
        if stopped:
            signal_broker(identity, 'CONT')
            save(prefix+'-broker-resumed.json',{'identity':broker_identity(),'proc_status':command(['docker','exec',identity['node'],'cat','/proc/'+str(identity['pid'])+'/status'],text=True),'time':time.time()})
            stopped = False
        resumed_offsets=wait_for(lambda: offsets(), 'broker resumed')
        save(prefix+'-offsets-before-pbs-restart.json',resumed_offsets)
        assert group_committed(resumed_offsets['consumer_offsets']) <= offset, resumed_offsets
        assert int(resumed_offsets['end_offsets'].strip().split(':')[-1]) == offset+1, resumed_offsets
        assert broker_identity() == identity
        scale_pbs(1)
        if network_rule is not None:
            newpods=json.loads(command(K+['get','pods','-l','app=ofwp32-pbs','-o','json'],text=True))
            assert all(p['status']['podIP'] != pbs_ip for p in newpods['items']),newpods
            save(prefix+'-new-pbs-network.json',[{'uid':p['metadata']['uid'],'ip':p['status']['podIP']} for p in newpods['items']])
        drain()
        stable = poison(offset)
        save(prefix + '-post-restart-poison.json', stable)
        expected_id = str(uuid.uuid5(uuid.NAMESPACE_OID, 'scheduling_dlq\x1f' + TOPIC + '\x1f0\x1f' + str(offset)))
        assert stable[0]['attempts'] == 5 and stable[0]['state'] == 'dlq_enqueued' and stable[0]['dlq_event_id'] == expected_id, stable
        records = wait_for(lambda: dlq_records(offset, prefix + '-after'), 'real Kafka DLQ event', 45)
        assert len(records) == 1 and expected_id in records[0]['wire'], records
        assert records[0]['body']['raw_message'] == marker
        with forward('ofwp32-dvs', 50078) as dvs:
            path = '/v1/datasets/' + seed['output']['id']
            versions_before = http(dvs, path + '/versions', token, evidence=prefix + '-healthy-product')
            inject((ROOT / (key + '-event.ndjson')).read_bytes(), prefix + '-healthy-producer')
            healthy = await_state(event, {'succeeded'}, 100)
            save(prefix + '-healthy-dispatch.json', healthy)
            versions_after = http(dvs, path + '/versions', token, evidence=prefix + '-healthy-product')
            rows = http(dvs, path + '/files/rows?branch=main&mode=preview&limit=10', token, evidence=prefix + '-healthy-product')
            source = http(dvs, '/v1/datasets/' + seed['input']['id'] + '/files/rows?branch=main&mode=preview&limit=10', token, evidence=prefix + '-healthy-product')
            assert len(versions_after['data']) == len(versions_before['data']) + 1 and rows['rows'] == source['rows']
            count = pg("SELECT count(*)::int FROM builds WHERE id='" + healthy['build_id'] + "';")
            assert count == 1
        drain()
        # A second restart after ACK exercises durable state without another budget
        # unit or logical DLQ publication; the specific pending-ACK window above is
        # independently evidenced, not inferred from this ordinary restart.
        scale_pbs(0); scale_pbs(1); drain()
        assert poison(offset) == stable
        final_records = dlq_records(offset, prefix + '-final')
        assert len(final_records) == 1 and expected_id in final_records[0]['wire']
        final_offsets = offsets()
        save(prefix + '-offsets-final.json', final_offsets)
        assert int(final_offsets['end_offsets'].strip().split(':')[-1]) >= offset + 2
        assert group_committed(final_offsets['consumer_offsets']) > offset
        assert broker_identity() == identity
        save(prefix+'-broker-after.json',broker_identity())
        if network_rule is not None:
            save(prefix+'-network-final-counters.json',command(ipt+['-nvL','INPUT'],text=True))
        save(prefix + '-result.json', {'status': 'PASS', 'window': window, 'offset': offset, 'dlq_event_id': expected_id, 'attempts': 5, 'healthy_event_id': event['event_id']})
        print('PASS', prefix, offset, expected_id)
    finally:
        try:
            if network_rule is not None:
                command(ipt+['-D','INPUT']+network_rule,text=True)
                save(prefix+'-network-cleanup.json',{'rules_after':command(ipt+['-S','INPUT'],text=True)})
                network_rule=None
        finally:
            if stopped:
                signal_broker(identity, 'CONT')
        try:
            sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='" + app + "'")
            sql('DROP TRIGGER IF EXISTS ' + name + ' ON scheduling.poison_messages; DROP FUNCTION IF EXISTS scheduling.' + name + '(); DROP SEQUENCE IF EXISTS scheduling.' + name + '_seq')
        finally:
            scale_pbs(1)
            if holder:
                try: holder.wait(timeout=5)
                except subprocess.TimeoutExpired: holder.terminate(); holder.wait(timeout=5)
            log.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('window', choices=('tx-failure', 'pending-ack'))
    parser.add_argument('--exclusive-pbs-kafka-slot', action='store_true')
    parser.add_argument('--run-id',default=time.strftime('%Y%m%d%H%M%S'))
    args = parser.parse_args()
    if not args.exclusive_pbs_kafka_slot: parser.error('Coordinate explicit exclusive PBS/Kafka slot first')
    ROOT.mkdir(parents=True, exist_ok=True)
    main(args.window,args.run_id)
