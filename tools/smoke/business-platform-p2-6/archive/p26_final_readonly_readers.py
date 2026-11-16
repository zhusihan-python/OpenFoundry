"""Independent final RO readers. Never import or execute a producer."""
import hashlib
import importlib.util
import json
from pathlib import Path
import re


def owned(r, p, S):
    spec = importlib.util.spec_from_file_location('h10_refs', Path(__file__).with_name('p26_h10_actual_reader.py'))
    h = importlib.util.module_from_spec(spec); spec.loader.exec_module(h)
    def need(ok, why):
        if not ok: raise ValueError(why)
    def doc(ref): return json.loads(h.ref(r, p, ref))
    a = r.doc(p['audit'])
    need(p['kind'] == 'final-owned-state-actual-component' and p['epoch'] == S, 'owned component epoch/kind')
    need(a['schema'] == 'p26-final-owned-state-original-audit-v1' and a['source_sha256'] == S and len(a['files']) == 27, 'owned actual audit schema/source/exact27')
    for path, pin in a['files'].items(): h.ref(r, p, dict(pin, path=path))
    release = doc({'path': a['release']['snapshot'], 'sha256': a['release']['sha256']})
    need(release['schema'] == 'p26-final-owned-state-release-v1' and release['phase'] == 'p26-final-owned-state-readonly' and release['root_accepted'] is True and release['source_sha256'] == S, 'owned actual Root phase')
    need(release['collector_sha256'] == a['collector_sha256'] == '447d90b30ab9d1ec0c2a05b4700049d793348023715f50de3cfca6412bb49168', 'owned original producer pin')
    r.rc(p['outer'] + '/rc'); launch = r.doc(p['outer'] + '/cmd.json')
    need(launch[:3] == ['python3', '-I', '-B'] and launch[-2:] == ['--release-file', a['release']['original_path']], 'owned original launch')
    r.read(p['outer'] + '/stdout.log'); r.read(p['outer'] + '/stderr.log')
    plan = doc(a['plan']); result = doc(a['result']); state = doc(a['current_state']); origin = doc(a['origin_ledger'])
    need(a['plan'] == {'path': release['plan_path'], 'sha256': release['plan_sha256']} and result['plan'] == origin['plan'] == a['plan'], 'owned original plan binding')
    need(plan['schema'] == 'p26-final-owned-state-plan-v1' and plan['source_sha256'] == S and plan['after_ABC_cutoff_complete'] is True, 'owned actual afterABC plan')
    need(plan['ABC_creation_proof'] == a['ABC_creation_proof'] == result['ABC_creation_proof'] == origin['ABC_creation_proof'], 'owned original ABC binding')
    doc(plan['ABC_creation_proof'])
    entries, sources = plan['entries'], plan['source_entries']
    defs = {e['def_id']: e for e in entries}; srcs = {e['source_id']: e for e in sources}
    need(len(defs) == len(entries) == 310 and len(srcs) == len(sources) == 294 and len(set(plan['protected_exclusions'])) == 19 and not set(defs) & set(plan['protected_exclusions']), 'owned exact310/294 excludes19')
    need({e['source_id'] for e in entries} <= set(srcs), 'owned independent Source inventory')
    standalone = sorted(set(srcs) - {e['source_id'] for e in entries})
    need(standalone == ['544b1d4a-0574-44a9-8bbc-95e4aada1d94'], 'owned orphan Source retained')
    # Register all complete historical creation references for standalone closure.
    stack = [entries, sources]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            if {'path', 'sha256'} <= value.keys(): h.ref(r, p, value)
            stack.extend(value.values())
        elif isinstance(value, list): stack.extend(value)
    base = str(Path(a['result']['path']).parent)
    need({Path(path).name for path in a['files'] if str(Path(path).parent)==base and path.endswith('.cmd.json')} == {'PG-before.cmd.json','CURRENT-final-owned-state-SELECT.cmd.json','PG-after.cmd.json'}, 'owned only three original commands')
    def command(name):
        stem = h.local(p, base + '/' + name)
        cmd = r.doc(stem + '.cmd.json'); meta = r.doc(stem + '.meta.json')
        r.rc(stem + '.rc'); raw = r.read(stem + '.stdout'); err = r.read(stem + '.stderr')
        need(meta == {'kind': 'Unix-child', 'original_child_rc': 0} and err == b'', 'owned original Unix0 metadata/stderr')
        need(cmd == a['inner_original_commands'][name]['argv'], 'owned audit/raw argv mismatch')
        return cmd, raw
    pg = {'cid': '604060baaa88095d64f00ae055f60a6bf1cdc48011db650f2498d506979967e8', 'image': 'sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139', 'started_at': '2026-11-13T09:52:38.670741805Z'}
    need(release['pg_pin'] == pg, 'owned old PG pins')
    fmt = '{"cid":{{json .Id}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}}}'
    for name in ['PG-before', 'PG-after']:
        cmd, raw = command(name)
        need(cmd == ['docker', 'inspect', pg['cid'], '--format', fmt] and json.loads(raw) == dict(pg, running=True), 'owned actual oldPG unchanged')
    dids = ','.join("('" + e + "'::uuid)" for e in sorted(defs))
    sids = ','.join("('" + e + "'::uuid)" for e in sorted(srcs))
    query = "WITH known_defs(requested_id) AS (VALUES " + dids + "), known_sources(requested_id) AS (VALUES " + sids + ") SELECT json_build_object('definitions',(SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT k.requested_id::text,d.id::text AS def_id,d.org_id::text,d.source_id::text,d.output_dataset_id::text,c.id::text AS conn_id,c.org_id::text AS conn_org_id,c.owner_id::text AS conn_owner_id,c.name AS source_name,c.connector_type,d.schedule_enabled,d.schedule_next_fire_at::text FROM known_defs k LEFT JOIN batch_sync_defs d ON d.id=k.requested_id LEFT JOIN connections c ON c.id=d.source_id ORDER BY k.requested_id) t),'sources',(SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT k.requested_id::text,c.id::text AS source_id,c.owner_id::text,c.org_id::text,c.name AS source_name,c.connector_type FROM known_sources k LEFT JOIN connections c ON c.id=k.requested_id ORDER BY k.requested_id) t),'grants_count',(SELECT count(*) FROM sync_schedule_grants));"
    cmd, raw = command('CURRENT-final-owned-state-SELECT')
    need(cmd == ['docker', 'exec', pg['cid'], 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'openfoundry', '-d', 'connectors', '-c', 'BEGIN READ ONLY;', '-c', query, '-c', 'COMMIT;'], 'owned exact fixed readonly SQL/all requested IDs')
    rows = [json.loads(line) for line in raw.decode().splitlines() if line.startswith('{')]
    need(len(rows) == 1 and set(rows[0]) == {'definitions', 'sources', 'grants_count'} and type(rows[0]['grants_count']) is int and rows[0]['grants_count'] == 0, 'owned original whole grants0')
    actual = rows[0]; fields = {'requested_id','def_id','org_id','source_id','output_dataset_id','conn_id','conn_org_id','conn_owner_id','source_name','connector_type','schedule_enabled','schedule_next_fire_at'}
    need(origin['original_SELECT']=={'path':base+'/CURRENT-final-owned-state-SELECT.stdout','sha256':hashlib.sha256(raw).hexdigest(),'original_rc':0,'rc_file':base+'/CURRENT-final-owned-state-SELECT.rc'} and origin['current_state']==a['current_state'], 'owned original SELECT/derived ledger binding')
    drows = {v['requested_id']: v for v in actual['definitions']}; srows = {v['requested_id']: v for v in actual['sources']}
    need(len(drows) == len(actual['definitions']) == 310 and set(drows) == set(defs) and len(srows) == len(actual['sources']) == 294 and set(srows) == set(srcs), 'owned original exact requested sets/no duplicates')
    for did, e in defs.items():
        row = drows[did]
        need(set(row) == fields and (row['def_id'],row['source_id'],row['conn_id'],row['output_dataset_id'],row['org_id'],row['conn_org_id'],row['conn_owner_id']) == (did,e['source_id'],e['source_id'],e['output_dataset_id'],e['expected_tenant'],e['expected_tenant'],e['expected_actor']) and row['schedule_enabled'] is False and row['schedule_next_fire_at'] is None, 'owned actual full parent/identity/schedules')
    for sid, e in srcs.items():
        row = srows[sid]
        need(set(row) == {'requested_id','source_id','owner_id','org_id','source_name','connector_type'} and (row['source_id'],row['owner_id'],row['org_id']) == (sid,e['expected_actor'],e['expected_tenant']), 'owned actual independent Source identity')
    for row in drows.values(): need((row['conn_owner_id'],row['conn_org_id']) == (srows[row['source_id']]['owner_id'],srows[row['source_id']]['org_id']), 'owned JOIN/independent Source agreement')
    need(state['definitions'] == drows and state['sources'] == srows and state['grants_count'] == 0, 'owned derived state equals original SQL')
    for key, expected in [('definitions',defs),('sources',srcs)]:
        need(set(origin[key]) == set(expected), 'owned full origin ledger coverage')
        for ident, e in expected.items(): need(origin[key][ident] == {'historical_basis':e,'current_present':True,'current_pointer':'/'+key+'/'+ident,'absence_does_not_assert_original_DELETE':True}, 'owned original per-object historical ledger')
    need(result['schema'] == 'p26-final-owned-state-proof-v1' and result['source_sha256'] == S and result['current_state'] == a['current_state'] and result['origin_ledger'] == a['origin_ledger'] and result['counts'] == {'Def':{'requested':310,'present':310,'absent':0},'Source':{'requested':294,'present':294,'absent':0}} and result['standalone_known_Source_ids'] == standalone, 'owned result rebuilt from originals')
    for side in ['before','after']: need(r.doc(h.local(p, base+'/source-'+side+'.json')) == {'source':S}, 'owned source gate original')
    return {'kind':p['kind'],'safe_original_members':27,'original_Unix_rcs':[0,0,0],'Def_present':310,'Source_present':294,'absent':0,'standalone_Source_ids':standalone,'whole_grants_count':0,'present_Def_schedules_false_nextNULL':True,'source_name_type_invariance_claimed':False,'full_Source_config_invariance_claimed':False,'HTTP_calls':0,'business_mutations':0}


def substrate(r, p, S):
    spec = importlib.util.spec_from_file_location('h10_refs', Path(__file__).with_name('p26_h10_actual_reader.py'))
    h = importlib.util.module_from_spec(spec); spec.loader.exec_module(h)
    def need(ok, why):
        if not ok: raise ValueError(why)
    def doc(ref): return json.loads(h.ref(r, p, ref))
    def pathdoc(path): return r.doc(h.local(p,path))
    def command(base):
        stem = h.local(p,base); cmd=r.doc(stem+'.cmd.json'); meta=r.doc(stem+'.meta.json'); raw=r.read(stem+'.raw'); r.rc(stem+'.rc')
        if isinstance(cmd,list): need(meta.get('original_child_rc')==0 and meta.get('stderr','')=='','substrate original Unix0')
        else: need(meta.get('original_child_rc') is None and meta['transport_outcome']==0 and meta['status']==200,'substrate original HTTP200 transport0 UnixNone')
        return cmd,meta,raw
    def body(meta,raw):
        size=meta['bytes']; need(type(size) is int and size>=0 and raw[size:] in (b'',b'\n') and len(raw)>=size,'substrate raw/body capture length')
        value=raw[:size]; need(hashlib.sha256(value).hexdigest()==meta['body_sha256'],'substrate raw/body SHA'); return value
    a=r.doc(p['audit']); need(p['kind']=='final-substrate-counter-actual-component' and p['epoch']==S and a['schema']=='p26-final-substrate-counter-actual-original-audit-v1' and len(a['files'])==207,'substrate actual kind/epoch/exact207')
    for path,pin in a['files'].items(): h.ref(r,p,dict(pin,path=path))
    outer=p['outer']; r.rc(outer+'/original-child.rc'); release=r.doc(outer+'/Root-release.snapshot.json'); launch=r.doc(outer+'/command.json'); outerresult=r.doc(outer+'/RESULT.json')
    need(release['schema']=='p26-final-substrate-counter-release-v1' and release['phase']=='final-substrate-counter-readonly' and release['root_accepted'] is True and release['source_sha256']==S and launch[:3]==['python3','-I','-B'] and launch[-2]=='--release-file' and Path(launch[-1]).name=='openfoundry-p26-S2-final-substrate-counter-readonly-release.json','substrate exact Root phase/launch')
    need(release['tools_sha256']['deployment/final_substrate_counter_readonly.py']=='13b70cbc1328b95ea08a173d5d3f809109cdf4617cf3dbc9343fb75d619d4c76','substrate actual producer pin')
    for rel,digest in release['tools_sha256'].items(): h.ref(r,p,{'path':p['canonical_root_label']+'/tools/smoke/business-platform-p2-6/'+rel,'sha256':digest})
    plan=doc({'path':release['plan_path'],'sha256':release['plan_sha256']}); record=doc(plan['deployment_record'])
    need(plan['schema']=='p26-final-substrate-counter-plan-v1' and plan['source_sha256']==record['source_sha256']==S,'substrate source plan')
    names={'openfoundry-platform-p1-q3v9-'+s+'-1' for s in ['dataset','pipeline','connector','postgres','fixture']}; need(set(record['containers'])==names and len(plan['containers'])==5 and {e['name'] for e in plan['containers']}==names,'substrate exact main5')
    fmts={'identity':'{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}','mounts':'{{json .Mounts}}','ports':'{{json .NetworkSettings.Ports}}\n{{json .HostConfig.PortBindings}}','env_names':'{{range .Config.Env}}{{println (index (split . "=") 0)}}{{end}}'}
    def original(refs,argv,http=False):
        need(set(refs)=={'cmd','raw','rc','meta'},'substrate all four original receipt refs'); v={k:h.ref(r,p,e) for k,e in refs.items()}; cmd=json.loads(v['cmd']); meta=json.loads(v['meta']); need(v['rc'].strip()==b'0','substrate origin rc0')
        if not http:
            if isinstance(cmd,dict) and set(cmd)=={'argv'}: cmd=cmd['argv']
            need(cmd==argv and meta.get('original_child_rc',meta.get('original_rc'))==0,'substrate original exact inspect/Unix0'); return v['raw'].decode()
        need(cmd==argv and meta['original_child_rc'] is None and meta['transport_outcome']==0 and meta['status']==200,'substrate original fixed counterHTTP'); return body(meta,v['raw'])
    expected={}
    for e in plan['containers']:
        name=e['name']; pin=record['containers'][name]; need(set(e['receipts'])==set(fmts),'substrate complete safeconfig origins')
        expected[name]={key:original(e['receipts'][key],['docker','inspect',pin['cid'],'--format',fmt]) for key,fmt in fmts.items()}
    sources={'p21a-b6ec49ab','p21b-1020a565','p21b-528455c2','p21b-96351e3b','p21b-b6ec49ab','p21c-1020a565','p21c-528455c2','p21c-96351e3b','p21c-b6ec49ab'}
    need(len(plan['counters'])==9 and {e['source_name'] for e in plan['counters']}==sources,'substrate exact9 oldsource names')
    counters={}
    for e in plan['counters']:
        cmd=json.loads(h.ref(r,p,e['receipt']['cmd'])); need(cmd['method']=='GET' and cmd['auth_attached'] is False and cmd['url']=='http://127.0.0.1:17090/admin/src/'+e['source_name'],'substrate original fixed anonymous URL')
        counters[e['source_name']]=original(e['receipt'],cmd,True)
    need(outerresult['original_child_rc']==0 and len(outerresult['inner_output'])==1,'substrate original outer rc/inner')
    inner=outerresult['inner_output'][0]; result=pathdoc(inner+'/RESULT.json'); actual={'before':{},'after':{}}; actualhttp={}; unix=0
    for path in sorted(a['files']):
        if str(Path(path).parent)!=inner or not path.endswith('.cmd.json'): continue
        stem=path.removesuffix('.cmd.json'); cmd,meta,raw=command(stem)
        if isinstance(cmd,list):
            need(len(cmd)==5 and cmd[:2]==['docker','inspect'] and cmd[3]=='--format','substrate only exact inspect permitted')
            match=[(n,k) for n,pin in record['containers'].items() for k,fmt in fmts.items() if cmd==['docker','inspect',pin['cid'],'--format',fmt]]; need(len(match)==1,'substrate exact pinned target/projection')
            n,k=match[0]; label=Path(stem).name; side='before' if '-before-' in label else 'after' if '-after-' in label else None; need(side is not None and k not in actual[side].setdefault(n,{}),'substrate unique before/after projection'); actual[side][n][k]=raw.decode().removesuffix('\n'); unix+=1
        else:
            need(cmd['method']=='GET' and cmd['auth_attached'] is False and cmd['url'].startswith('http://127.0.0.1:17090/admin/src/'),'substrate only anonymous fixed GET')
            n=cmd['url'].rsplit('/',1)[-1]; need(n in sources and n not in actualhttp,'substrate duplicate/unknown counter'); actualhttp[n]=body(meta,raw); need(actualhttp[n]==counters[n],'substrate actual oldcounter byte drift')
    need(unix==40 and set(actualhttp)==sources,'substrate actual40 Unix0/nineHTTP')
    def canonical(raw):
        values=json.loads(raw); need(isinstance(values,list) and all(isinstance(v,dict) and v.get('Destination') for v in values),'substrate complete mounts'); keyed={v['Destination']:v for v in values}; need(len(keyed)==len(values),'substrate duplicate mount destination'); return keyed
    for side in ['before','after']:
        need(set(actual[side])==names,'substrate eachmain5 beforeafter')
        for n,obs in actual[side].items():
            pin=record['containers'][n]; exp=expected[n]; need(set(obs)==set(fmts),'substrate complete four safe projections')
            need(obs['identity'].strip().splitlines()==exp['identity'].strip().splitlines()==[pin['cid'],pin['image'],pin['started_at'],'true'],'substrate exactactual identity/boot/running')
            need(canonical(obs['mounts'])==canonical(exp['mounts']),'substrate every mount dictionary field unchanged')
            need([json.loads(v) for v in obs['ports'].strip().splitlines()]==[json.loads(v) for v in exp['ports'].strip().splitlines()] and len(obs['ports'].strip().splitlines())==2,'substrate complete two port dictionaries')
            on=obs['env_names'].strip().splitlines(); en=exp['env_names'].strip().splitlines(); need(len(on)==len(set(on)) and set(on)==set(en) and all(re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',v) for v in on),'substrate complete envNAMES only')
            need(result['main5_safe_config_before_after'][side][n].get('identity')==obs['identity'] and result['main5_safe_config_before_after'][side][n]==obs,'substrate result projection equals original raw capture')
    need(record['containers']['openfoundry-platform-p1-q3v9-postgres-1']['started_at']=='2026-11-13T09:52:38.670741805Z' and record['containers']['openfoundry-platform-p1-q3v9-fixture-1']['started_at']=='2026-11-13T18:29:14.176785055Z','substrate oldPG/fixture boot')
    need(result['schema']=='p26-final-substrate-counter-proof-v1' and result['source_sha256']==S and result['old_counter_names']==sorted(sources) and result['old_counter_sha256']=={n:hashlib.sha256(v).hexdigest() for n,v in counters.items()},'substrate result rebuilt from originals')
    before=r.doc(outer+'/before.json')['file_sha256']; need(before==r.doc(outer+'/after.json')['file_sha256'],'substrate source/code/input unchanged')
    for path,digest in before.items(): h.ref(r,p,{'path':path,'sha256':digest}) if path.startswith(p['canonical_root_label']+'/') else need(path==launch[-1] and hashlib.sha256(r.read(outer+'/Root-release.snapshot.json')).hexdigest()==digest,'substrate Rootsnapshot original hash')
    return {'kind':p['kind'],'safe_original_members':207,'original_Unix0_count':40,'HTTP200_transport0_UnixNone_count':9,'original_safe_input_refs':117,'full_main5_safeconfig_before_after':True,'old_counter_bodies_exact':True,'health_network_invariance_claimed':False,'SQL_calls':0,'business_mutations':0}
