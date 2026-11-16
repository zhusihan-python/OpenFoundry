#!/usr/bin/env python3
"""Pure aggregation of the preserved failed full window and exact three fresh repairs."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import sys

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('p25_owned_quiet', HERE / 'collect_http_quiet_p25.py')
q = importlib.util.module_from_spec(spec); spec.loader.exec_module(q)


def digest(raw): return hashlib.sha256(raw).hexdigest()
def absolute(path): return str(Path(path).absolute())
def raw_json(value): return (json.dumps(value, indent=2) + '\n').encode()
def read(path, resolve): return json.loads(resolve(str(path)).read_text())
def link(path, resolve): return {'path': str(path), 'sha256': q.sha(resolve(str(path)))}


def step_proof(directory, resolve, expected_rc):
    directory = Path(directory)
    command_names = [name for name in ('cmd.txt','cmd.json') if resolve(str(directory/name)).is_file()]
    if len(command_names) != 1: raise ValueError('need one exact captured command artifact')
    command_name = command_names[0]
    members = {name: link(str(directory / name), resolve) for name in (command_name,'stdout.log','stderr.log','rc')}
    if resolve(str(directory / 'rc')).read_text().strip() != str(expected_rc):
        raise ValueError('source original rc mismatch: ' + str(directory))
    command = resolve(str(directory / command_name)).read_text().strip()
    if command_name == 'cmd.json':
        argv = json.loads(command)
        if not isinstance(argv,list) or not all(isinstance(x,str) for x in argv):
            raise ValueError('captured cmd.json must contain exact argv strings')
    else:
        if command.startswith('CMD: '): command = command[5:]
        argv = shlex.split(command)
    stdout = read(str(directory / 'stdout.log'), resolve)
    return members, argv, stdout


def flag(argv, name):
    if argv.count(name) != 1 or argv.index(name) + 1 >= len(argv):
        raise ValueError('missing/duplicate source command argument ' + name)
    return argv[argv.index(name)+1]


def source(summary_path, step_dir, resolve, rc, case=None):
    doc = read(summary_path, resolve)
    members, argv, stdout = step_proof(step_dir, resolve, rc)
    if flag(argv, '--phase') != 'healthy' or absolute(flag(argv, '--result-file')) != summary_path or '--released' not in argv:
        raise ValueError('source command not bound to actual healthy result')
    if case:
        if flag(argv, '--only') != case: raise ValueError('replacement command case mismatch')
    elif '--only' in argv:
        raise ValueError('original must be full window')
    if absolute(stdout.get('result_file', '')) != summary_path or stdout.get('failed') != doc.get('failed'):
        raise ValueError('raw stdout not bound to summary')
    if doc.get('phase') != 'healthy' or doc.get('released') is not True:
        raise ValueError('source summary phase/release invalid')
    evidence = doc['attempts']['evidence_dir']; handles = doc['attempts']['handles_file']
    if absolute(str(Path(evidence) / 'handles.json')) != absolute(handles):
        raise ValueError('source handles outside actual recorder directory')
    # A sealed recorder scenario-result, not a hand-edited summary alone.
    records = []
    for p in resolve(evidence).glob('*-attempt-*.json'):
        try: data = json.loads(p.read_text())
        except ValueError: continue
        records.append((p.name, data))
    cases, scenario_records = {}, {}
    for row in doc.get('scenarios', []):
        sid = row.get('id')
        if sid in cases: raise ValueError('duplicate case in source summary')
        candidates = [name for name,data in records if data == row and name.startswith(str(sid) + '-attempt-')]
        if len(candidates) != 1: raise ValueError('actual scenario recorder result missing/ambiguous: ' + str(sid))
        cases[sid] = row
        scenario_records[sid] = link(str(Path(evidence) / candidates[0]), resolve)
    handles_doc = read(handles, resolve)
    if handles_doc.get('count') != len(handles_doc.get('handles', [])):
        raise ValueError('source handles count mismatch')
    descriptor = {'summary_path': summary_path, 'summary_sha256': q.sha(resolve(summary_path)),
        'step_dir': step_dir, 'raw_members': members, 'original_rc': rc,
        'handles_file': handles, 'handles_sha256': q.sha(resolve(handles)),
        'scenario_records': scenario_records,
        'evidence_records': [link(str(Path(evidence) / name), resolve) for name,_ in sorted(records)]}
    return doc, cases, handles_doc['handles'], descriptor, records


def good_case(row, spec):
    gates = row.get('gates')
    if row.get('failed') is not False or row.get('phase') != 'healthy' or not isinstance(gates, list):
        raise ValueError('selected case is not a real PASS: ' + str(row.get('id')))
    if row['id'] != 'h5' and not gates: raise ValueError('selected case gates empty')
    # Root-reviewed actual emitted labels; all semantic assertion content is retained.
    mandatory = {'h2': {'G_SNAPSHOT_ONLY'}, 'h3': {'G_EMPTY_BATCH','G_LEDGER'},
                 'h13': {'G_CAS_ATOMIC','G_OWNER_DURABLE'}}.get(row['id'], set(spec['gates']))
    if any(not isinstance(g, dict) or g.get('ok') is not True or g.get('gate') not in mandatory for g in gates):
        raise ValueError('selected case has failed/unknown gates')
    if not mandatory <= {g['gate'] for g in gates}:
        raise ValueError('selected case missing mandatory registry gate names: ' + row['id'])


def build(arguments, resolve_path=None):
    """Pure recomputation. Returns aggregate data and exact union handles bytes."""
    resolve = resolve_path or Path
    m = q.engine()
    specs = {s['id']: s for s in m.SCENARIOS if s['phase'] == 'healthy' and s['id'] != 'h10'}
    original, old_cases, old_handles, old_proof, _ = source(arguments['original_summary'], arguments['original_step'], resolve, 1)
    replacements = arguments['replacements']
    tags = [r['case'] for r in replacements]
    if (set(old_cases) != set(specs) or original.get('failed') != ['h7','h13','h15']
        or set(tags) != set(original['failed']) or len(tags) != 3 or tags[-1] != 'h13'):
        raise ValueError('exact original full case set / failed replacement set / final h13 required')
    selected = {sid: row for sid,row in old_cases.items() if sid not in original['failed']}
    sources = {'original': old_proof}; case_sources = {sid: dict({k:v for k,v in old_proof.items() if k not in ('evidence_records','scenario_records')}, source_tag='original', scenario_record=old_proof['scenario_records'][sid]) for sid in selected}
    all_handles = [('original', old_handles, old_proof)]
    last_doc = last_records = last_handles = None
    for rep in replacements:
        sid = rep['case']
        doc, rows, handles, proof, records = source(rep['summary'], rep['step'], resolve, 0, sid)
        if set(rows) != {sid} or doc.get('failed') != [] or doc.get('quiet', {}).get('ok') is not True:
            raise ValueError('replacement must be actual single-case successful CLI')
        good_case(rows[sid], specs[sid])
        selected[sid] = rows[sid]; sources[sid] = proof
        case_sources[sid] = dict({k:v for k,v in proof.items() if k not in ('evidence_records','scenario_records')}, source_tag=sid, scenario_record=proof['scenario_records'][sid])
        all_handles.append((sid, handles, proof))
        if sid == 'h13': last_doc, last_records, last_handles = doc, records, handles
    for sid,row in selected.items(): good_case(row, specs[sid])
    if set(selected) != set(specs): raise ValueError('chosen healthy roster not exact')
    # Preserve every origin SHA even when an imported handle repeats a real object.
    merged, indices = [], {}
    for origin, rows, proof in all_handles:
        for i,h in enumerate(rows):
            if not isinstance(h, dict): raise ValueError('malformed actual handle')
            identity = (h.get('kind'), h.get('ref')) if h.get('ref') else (origin, i)
            provenance = {'source': origin, 'summary_path': proof['summary_path'],
                'summary_sha256': proof['summary_sha256'], 'handles_file': proof['handles_file'],
                'handles_sha256': proof['handles_sha256'], 'handle_index': i}
            if identity in indices: merged[indices[identity]]['origins'].append(provenance)
            else:
                indices[identity] = len(merged); merged.append(dict(h, origins=[provenance]))
    union = {'handles': merged, 'count': len(merged)}
    owned_defs = {h['ref'] for h in merged if h.get('kind') in ('def','h10-def') and h.get('ok')}
    last_defs = {h['ref'] for h in last_handles if h.get('kind') in ('def','h10-def') and h.get('ok')}
    if last_defs != owned_defs: raise ValueError('final h13 did not import all actual owned defs')
    roster_candidates = [data for _,data in last_records if isinstance(data, dict) and
        set(data) == {'owner_roster','run_roster','defs','problems'}]
    if len(roster_candidates) != 1: raise ValueError('last live final roster recorder missing')
    roster = roster_candidates[0]
    if (set(roster['defs']) != owned_defs or len(roster['defs']) != len(owned_defs) or roster['problems']
        or roster['owner_roster'] != last_doc.get('final_owner_roster')
        or roster['run_roster'] != last_doc.get('final_run_roster')):
        raise ValueError('last raw finally roster/summary/scope mismatch')
    def owner_key(o): return (o.get('def_id'),o.get('source_id'),o.get('run_id'),o.get('run_status'),o.get('blocked_reason'))
    baseline = original.get('final_owner_roster', [])
    if (len(baseline) != 2 or len({owner_key(o) for o in baseline}) != 2
        or any(o.get('run_status') != 'failed' or o.get('blocked_reason') != 'requires_reconciliation' for o in baseline)
        or {owner_key(o) for o in roster['owner_roster']} != {owner_key(o) for o in baseline}
        or len(roster['owner_roster']) != 2 or original.get('final_run_roster') != [] or roster['run_roster'] != []):
        raise ValueError('retained failedhealthy exact baseline changed / unknown extra owner or run')
    # Prove actual parents/status from the last h13 HTTP bodies, not UUID shape alone.
    bodies = []
    for _,data in last_records:
        if isinstance(data, dict) and data.get('status') == 200 and isinstance(data.get('resp_body'), str):
            try: bodies.append(json.loads(data['resp_body']))
            except ValueError: pass
    for owner in roster['owner_roster']:
        if not any(isinstance(b,dict) and b.get('id') == owner['def_id'] and b.get('source_id') == owner['source_id'] for b in bodies):
            raise ValueError('retained owner actual source parent missing')
        runs = [b for b in bodies if isinstance(b,list) and any(isinstance(r,dict) and r.get('id') == owner['run_id'] for r in b)]
        if not any(any(r.get('id') == owner['run_id'] and r.get('sync_def_id') == owner['def_id'] and r.get('status') == 'failed' for r in rows) for rows in runs):
            raise ValueError('retained owner actual terminal run parent missing')
    for owner in roster['owner_roster']:
        cps = [data for _,data in last_records if isinstance(data,dict) and data.get('status') == 200
               and data.get('path','').endswith('/syncs/' + owner['def_id'] + '/checkpoint')]
        if not any((json.loads(data.get('resp_body','{}')).get('owner') or {}).get('run_id') == owner['run_id']
                   and (json.loads(data.get('resp_body','{}')).get('owner') or {}).get('run_status') == 'failed'
                   and (json.loads(data.get('resp_body','{}')).get('owner') or {}).get('blocked_reason') == 'requires_reconciliation'
                   for data in cps):
            raise ValueError('retained owner public CP raw state missing')
    handles_inputs = [old_proof['handles_file']] + [sources[r['case']]['handles_file'] for r in replacements]
    quiet = q.verify_quiet_proof(arguments['final_quiet_proof'], handles_inputs, resolve)
    quiet_members, quiet_argv, quiet_stdout = step_proof(arguments['final_quiet_step'], resolve, 0)
    if '--released' not in quiet_argv or absolute(quiet_stdout.get('proof_file','')) != arguments['final_quiet_proof'] or quiet_stdout.get('ok') is not True:
        raise ValueError('actual final quiet CLI result not bound')
    cli_handles = [quiet_argv[i+1] for i,arg in enumerate(quiet_argv[:-1]) if arg == '--handles']
    if list(map(absolute, cli_handles)) != list(map(absolute, handles_inputs)):
        raise ValueError('final quiet command handles union mismatch')
    ledger_spec=importlib.util.spec_from_file_location('p25_ledger_supplement', HERE/'verify_h13_ledger_p25.py')
    ledger_module=importlib.util.module_from_spec(ledger_spec);ledger_spec.loader.exec_module(ledger_module)
    ledger=ledger_module.verify_ledger(arguments['h13_ledger_step'], sources['h13']['summary_path'], resolve)
    handles_bytes = raw_json(union)
    aggregate = {'schema':'p25-healthy-aggregate-v1','ok':True,'phase':'healthy','released':True,
        'failed':[],'scenarios':[selected[s['id']] for s in m.SCENARIOS if s['id'] in selected],
        'case_sources':case_sources,'sources':sources,'source_arguments':arguments,
        'quiet':quiet,'final_owner_roster':roster['owner_roster'],'final_run_roster':roster['run_roster'],
        'retained_owner_origins':[dict(o, origin='failedhealthy', source_summary=old_proof['summary_path'],
                                      source_summary_sha256=old_proof['summary_sha256']) for o in baseline],
        'h13_ledger_supplement':ledger,'final_quiet_proof':link(arguments['final_quiet_proof'],resolve),'final_quiet_raw_members':quiet_members,
        'attempts':{'handles_file':arguments['handles_output'],'handles_sha256':digest(handles_bytes)},
        'original_failure_preserved':{'summary_path':old_proof['summary_path'], 'summary_sha256':old_proof['summary_sha256'],
                                      'original_rc':1,'failed':original['failed']}}
    return aggregate, handles_bytes


def verify_aggregate(path, resolve_path=None):
    """Pure verifier API for Archive/offline: returns ok/problems, never live IO."""
    resolve = resolve_path or Path
    try:
        doc = read(path, resolve)
        if doc.get('schema') != 'p25-healthy-aggregate-v1': raise ValueError('aggregate schema invalid')
        rebuilt, handles = build(doc['source_arguments'], resolve)
        if rebuilt != doc: raise ValueError('aggregate differs from independently rebuilt sources')
        if resolve(doc['attempts']['handles_file']).read_bytes() != handles:
            raise ValueError('union handles differ from actual source union')
        return {'ok': True, 'problems': []}
    except Exception as exc:
        return {'ok': False, 'problems': [str(exc)]}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--original-summary',required=True);p.add_argument('--original-step',required=True)
    p.add_argument('--replacement',nargs=3,action='append',metavar=('CASE','SUMMARY','STEP'),required=True)
    p.add_argument('--final-quiet-proof',required=True);p.add_argument('--final-quiet-step',required=True)
    p.add_argument('--h13-ledger-step',required=True)
    p.add_argument('--output',required=True);p.add_argument('--handles-output',required=True)
    a=p.parse_args(argv)
    arguments={'original_summary':absolute(a.original_summary),'original_step':absolute(a.original_step),
        'replacements':[{'case':tag,'summary':absolute(summary),'step':absolute(step)} for tag,summary,step in a.replacement],
        'final_quiet_proof':absolute(a.final_quiet_proof),'final_quiet_step':absolute(a.final_quiet_step),
        'handles_output':absolute(a.handles_output),'h13_ledger_step':absolute(a.h13_ledger_step)}
    try:
        if Path(a.output).exists() or Path(a.handles_output).exists(): raise ValueError('refuse to overwrite existing outputs')
        doc,handles=build(arguments)
        Path(a.handles_output).write_bytes(handles);Path(a.output).write_bytes(raw_json(doc))
        verdict=verify_aggregate(absolute(a.output))
        print(json.dumps({'aggregate_file':absolute(a.output),**verdict}));return 0 if verdict['ok'] else 1
    except Exception as exc:
        print(json.dumps({'ok':False,'problems':[str(exc)]}));return 1


if __name__=='__main__':raise SystemExit(main())
