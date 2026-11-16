"""Specific final original readers. No subprocess, credential reads, or tool imports.

These interfaces are strict until real Root-released final originals exist.
The caller supplies immutable whole input contracts, never acceptance booleans.
"""
import hashlib
import json
import os
import re


def need(ok, message):
    if not ok: raise ValueError(message)


def local(p, absolute):
    prefix=p['canonical_root_label']+'/'
    need(absolute.startswith(prefix),'original receipt outside canonical workspace')
    return absolute[len(prefix):]


def closed(r,p,source):
    need(p['epoch']==source,'final source epoch')
    r.rc(p['outer']+'/'+p.get('outer_rc_file','rc'))
    for rel in p['outer_inputs']:r.read(rel)
    need(p['source_checks'] and len(set(p['source_checks']))==len(p['source_checks'])>=2,'final actual source before/after originals required')
    for rel in p['source_checks']:
        text=r.text(rel);need(source in text and ('CANDIDATE_VERIFY=OK' in text or 'source' in text.lower()),'final original source check')
    need(p['historical_tools'],'final original used tools absent')
    for rel,digest in p['historical_tools'].items():need(hashlib.sha256(r.read(rel)).hexdigest()==digest,'final historical tool changed')
    directory=p['directory'];actual={n[:-3] for n in os.listdir(r.path(directory)) if n.endswith('.rc') and not n.endswith('.gate.rc')}
    gates={n[:-8] for n in os.listdir(r.path(directory)) if n.endswith('.gate.rc')}
    need(gates==set(p.get('gates',{})),'final exact gate set')
    for name,expected in p.get('gates',{}).items():
        r.rc(directory+'/'+name+'.gate.rc');need(r.doc(directory+'/'+name+'.gate.json')==expected and expected['ok'] is True,'final original gate false/changed')
    need(actual==set(p['steps']) and actual,'final exact original step closure')
    for name,expected in p['steps'].items():
        base=directory+'/'+name;r.rc(base+'.rc',expected.get('rc',0))
        need(r.doc(base+'.cmd.json')==expected['command'],'final exact original command')
        need(r.doc(base+'.meta.json')==expected['meta'],'final exact original metadata')
        need(hashlib.sha256(r.read(base+'.raw')).hexdigest()==expected['raw_sha256'],'final original raw changed')
    return r.doc(directory+'/RESULT.json')


def argv(r,base):
    r.rc(base+'.rc');command=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json')
    need(isinstance(command,list) and meta['kind'] in ('argv','call') and meta.get('original_child_rc',0)==0,'original Unix step kind')
    return command,r.text(base+'.raw')


def rows(r,base):
    command,raw=argv(r,base)
    values=[json.loads(line) for line in raw.splitlines() if line.startswith('[') or line=='null']
    need(len(values)==1,'original SQL exact one JSON rows value')
    return command,values[0] or []


def exact(expected,actual,key):
    need(all(isinstance(x,dict) and key in x for x in expected+actual),'whole row schema')
    a={x[key]:x for x in expected};b={x[key]:x for x in actual}
    need(len(a)==len(expected) and len(b)==len(actual) and a==b,'whole rows missing/extra/duplicate/changed '+key)


def typed_whole(r,p,source):
    closed(r,p,source)
    old=p['old_runs'];cp=p['old_cp'];typed=p['typed_roster']
    need(len(old)==9 and len(cp)==8 and typed['schema']=='p26-w10-typed-v1','full old9/cp8 and typed schema')
    new=[e['safe_projection_27'] for e in typed['runs']]
    need(len({x['id'] for x in old+new})==len(old+new),'old/new overlap or duplicate')
    need(all(len(x)==27 for x in old+new),'full safe27 required')
    whole=p['observations'];need({'controlled','cp','parents','typed','protocols','grants'}.issubset(whole),'final typed full observations missing')
    cmd,actual=rows(r,whole['controlled'])
    sql=cmd[-1];need("r.control_mode = 'controlled' AND r.status IN ('pending','running')" in sql and 'r.id IN (' not in sql,'whole controlled predicate filtered')
    expected=old+[x for x in new if x['control_mode']=='controlled' and x['status'] in ('pending','running')]
    exact(expected,actual,'id')
    _,actual=rows(r,whole['cp']);exact(cp+[e['cp_owner_row_full'] for e in typed['runs'] if e.get('cp_owner_row_full')],actual,'run_id')
    _,actual=rows(r,whole['typed']);exact(new,actual,'id')
    _,actual=rows(r,whole['protocols']);exact([{'id':e['run_id'],'recovery_protocol':None if e['protocol']=='legacy-null' else e['protocol']} for e in typed['runs']],actual,'id')
    parents=p['old_parents']+[e['parent'] for e in typed['runs']]
    # A Def may have several explicitly typed Runs, but its complete parent agrees.
    indexed={}
    for x in parents:
        need(x['def_id'] not in indexed or indexed[x['def_id']]==x,'conflicting typed Def parent');indexed[x['def_id']]=x
    _,actual=rows(r,whole['parents']);exact(list(indexed.values()),actual,'def_id')
    legacy=[x for x in new if x['control_mode']=='legacy']
    if legacy:
        cmd,actual=rows(r,whole['legacy']);need("r.control_mode = 'legacy' AND r.status IN ('pending','running')" in cmd[-1] and 'r.id IN (' not in cmd[-1],'whole legacy filtered')
        exact(typed['legacy_whole_baseline_27']+legacy,actual,'id')
    _,grants=rows(r,whole['grants']);need(grants==[],'whole grants nonzero')
    need(set(p['public_views'])==set(indexed),'complete old+typed public Def union')
    for did,view in p['public_views'].items():
        for kind in ('checkpoint','runs'):
            base=view[kind]['base'];r.rc(base+'.rc');cmd=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json');raw=r.read(base+'.raw')
            need(cmd['method']=='GET' and cmd['url'].endswith('/'+did+'/'+kind),'public Def endpoint binding')
            need(meta['status']==view[kind]['status'] and json.loads(raw)==view[kind]['body'],'whole original public CP/Run changed')
    # Connector config/default-loop facts must be independent originals, all main services.
    need(set(p['main_default_config'])=={'dataset','pipeline','connector'},'base3 default loops evidence')
    for service,ref in p['main_default_config'].items():
        _,raw=argv(r,ref['base']);need(raw==ref['expected_raw'],'base3 actual default config changed')
        need(all(not re.search(r'=([1-9][0-9]*|true)$',line,re.I) for line in raw.splitlines() if ('_INTERVAL' in line or 'AUTO_RECOVERY_ENABLED' in line)),'base3 default loops nonzero')
    return {'kind':'final-typed-whole','old_runs':9,'old_cp':8,'typed_runs':len(new),'unknown_extras_rejected':True}


def restart(r,p,source):
    closed(r,p,source);need(len(p['transitions'])==3,'main restart exact3')
    need([x['service'] for x in p['transitions']]==['dataset','pipeline','connector'],'main restart actual ordered services')
    for item in p['transitions']:
        doc=r.doc(item['ledger']);need(doc['schema']=='p26-main-boot-transition-v1' and doc['service']==item['service'],'restart ledger service/schema')
        old,new=doc['previous'],doc['current'];need(old['cid']==new['cid'] and old['image']==new['image'] and old['started_at']!=new['started_at'],'restart CID/image/boot')
        command,_=argv(r,local(p,doc['restart_step']))
        need(command in (['docker','restart',old['cid']],['docker','restart','--time','120',old['cid']],['docker','restart','--timeout','120',old['cid']]),'exact restart argv')
        for side,pin in (('before',old),('after',new)):
            _,raw=argv(r,local(p,doc[side]['identity']));need(raw.strip().splitlines()==[pin['cid'],pin['image'],pin['started_at'],'true'],'restart original identity differs')
        for key in ('mounts','ports','env_names'):
            _,a=argv(r,local(p,doc['before'][key]));_,b=argv(r,local(p,doc['after'][key]))
            if key=='env_names':
                a=[x for x in a.splitlines() if x];b=[x for x in b.splitlines() if x]
                need(len(a)==len(set(a)) and len(b)==len(set(b)) and set(a)==set(b),'restart env unique set')
            else:need(a==b,'restart original mounts/ports changed')
    need(set(p['fixed_identities'])=={'postgres','fixture'},'fixed PG+old fixture mandatory')
    for name,ref in p['fixed_identities'].items():
        _,a=argv(r,ref['before']);need(a.strip().splitlines()[-1]=='true','fixed original running')
        need(len(ref['after'])==3,'fixed original observation after each restart')
        for base in ref['after']:need(argv(r,base)[1]==a,'PG/oldfixture changed across main restart')
    need(p['fixture_counter_pairs'],'old fixture counters absent')
    for a,b in p['fixture_counter_pairs']:need(r.read(a)==r.read(b),'old fixture counter changed')
    typed_whole(r,p['guard_before'],source);typed_whole(r,p['guard_after'],source)
    return {'kind':'final-restart','same_CID_image_new_boot':3,'fixed_PG_fixture':True}


def cleanup(r,p,source):
    result=closed(r,p,source);quiesce(r,p['quiesce'],source);ledger=r.doc(p['aux_ledger'])
    need(ledger['schema']=='p26-final-owned-aux-v1' and ledger['source_sha256']==source,'cleanup original ledger source')
    targets=ledger['containers'];need({'p26connectora','p26connectorb','p26connectorc'}.issubset(targets),'cleanup must include ABC')
    need(set(p['targets'])==set(targets),'cleanup exact Root-owned target set')
    removed=[]
    for name,pin in targets.items():
        need(pin['ownership']['of-p26-owner']=='p26-http' and pin['ownership']['of-p26-stage']=='p2-6' and 'of-p26-role' in pin['ownership'],'cleanup ownership')
        refs=p['targets'][name]
        _,raw=argv(r,refs['before']);actual=json.loads(raw)
        need(actual['name']=='/'+name and all(actual[k]==pin[k] for k in ('cid','image','started_at','running')),'cleanup exact original identity')
        need(all(actual['labels'].get(k)==v for k,v in pin['ownership'].items()),'cleanup actual labels')
        command,_=argv(r,refs['remove']);need(command==['docker','rm',pin['cid']],'cleanup exact rm CID')
        if pin['running']:
            command,_=argv(r,refs['stop']);need(command==['docker','stop','--time','120',pin['cid']],'cleanup exact stop CID')
        removed.append(pin['cid'])
    command,raw=argv(r,p['inventory_after']);need(command==['docker','ps','-a','--no-trunc','--format','{{.ID}}'] and not set(raw.splitlines()).intersection(removed),'removed aux remains')
    need(len(p['main_identities'])==5,'main5 full preserved identities')
    for name,ref in p['main_identities'].items():
        _,a=argv(r,ref['before']);_,b=argv(r,ref['after']);need(a==b and json.loads(a)['running'] is True,'cleanup main5 changed')
        need(json.loads(a)['cid'] not in removed,'cleanup removed main CID')
    typed_whole(r,p['guard_before'],source);typed_whole(r,p['guard_after'],source)
    need(result['removed']==r.doc(p['directory']+'/removed-ledger.json'),'cleanup result/actual removed ledger')
    return {'kind':'final-cleanup','removed_exact_owned':len(targets),'main5_preserved':True}


def receipt(r,p,source):
    baseline=closed(r,p['baseline'],source);post=closed(r,p['post'],source)
    need(baseline['source_S']==post['source_S']==source and baseline['selection_run']==post['selection_run']==p['run_id'],'receipt actual source/selection')
    before=baseline['public_snapshot'];need(before==post['before']==post['after'],'receipt full before/post public observations changed')
    old,new=baseline['dataset_identity'],post['dataset_identity']
    need(old['cid']==new['cid'] and old['image']==new['image'] and old['started_at']!=new['started_at'],'receipt Dataset sameCID/image after restart')
    need(baseline['body_sha256']==post['body_sha256']==p['body_sha256'] and baseline['body_bytes']==post['body_bytes']>0,'private body SHA/length differs')
    docs=before['docs'];rec=docs['receipt'];txn=rec['transaction'];metadata=txn['metadata'];lineage=metadata['lineage']
    need(rec['idempotency_key']=='of-p26-pub-v1:'+p['run_id'] and rec['request_fingerprint']==p['body_sha256'],'receipt Run key/fingerprint')
    need(txn['status']=='COMMITTED' and txn['id']==p['txn_id'] and txn['dataset_id']==p['dataset_id'] and rec['branch']=='main','receipt transaction/Dataset')
    need(lineage=={'source_id':p['source_id'],'sync_def_id':p['def_id'],'sync_run_id':p['run_id']},'receipt lineage')
    need(docs['branch']['head_transaction_id']==p['txn_id'] and len(docs['versions']['data'])==1 and docs['versions']['data'][0]['transaction_id']==p['txn_id'] and not docs['versions'].get('has_more'),'receipt version/head changed')
    need(len(docs['files']['files'])==len(rec['files'])==1,'receipt exact one file')
    run=docs['run'];need(run['id']==p['run_id'] and run['status']=='succeeded' and run['dataset_version_id']==p['txn_id'] and run['recovery']['state']=='finalized' and run['content_hash']==metadata['content_hash'],'receipt full Run binding')
    # All three public snapshots must bind to the original GET response bytes,
    # not only the RESULT aggregate. Original harness appends exactly one LF.
    need(set(p['snapshots'])=={'baseline','pre_replay','post_replay'},'receipt exact3 historical snapshots')
    for name,refs in p['snapshots'].items():
        need(set(refs)==set(docs)|{'download'},'receipt full public observation scope')
        for key in docs:
            base=refs[key];r.rc(base+'.rc');cmd=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json');raw=r.read(base+'.raw');body=raw[:-1]
            need(cmd['method']=='GET' and meta['kind']=='http' and meta['status']==200 and meta['transport_outcome']==0 and meta['original_child_rc'] is None,'receipt GET original status/kind')
            need(meta['scrub_hits']==0 and len(body)==meta['bytes'] and hashlib.sha256(body).hexdigest()==meta['body_sha256'] and json.loads(body)==docs[key],'receipt original public bytes/body hash')
        base=refs['download'];r.rc(base+'.rc');meta=r.doc(base+'.meta.json');body=r.read(base+'.raw')[:-1]
        digest=hashlib.sha256(body).hexdigest()
        need(meta['status']==200 and meta['transport_outcome']==0 and meta['body_sha256']==digest==before['download_sha256']==metadata['content_hash'],'receipt actual downloaded content hash')
        need(len(body)==meta['bytes']==before['download_bytes']==metadata['bytes'] and len(body.splitlines())==metadata['rows'],'receipt original bytes/rows')
    for ref in p['private_selects']:
        base=ref;r.rc(base+'.rc');meta=r.doc(base+'.meta.json');cmd=r.doc(base+'.cmd.json');raw=r.text(base+'.raw')
        need(meta['kind']=='private-projection' and meta['original_child_rc']==0 and meta['stdout_bytes']>0 and len(meta['stdout_sha256'])==64,'private original SELECT projection')
        need('publish_body' in json.dumps(cmd) and p['run_id'] in json.dumps(cmd) and raw=='PRIVATE stdout/stderr omitted by projection policy\n','private SELECT projection policy')
    need(len(p['private_selects'])==2,'private SELECT baseline+post exact2')
    base=p['post_step'];r.rc(base+'.rc');cmd=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json');ack=r.read(base+'.raw')[:-1]
    need(cmd['method']=='POST' and cmd['url'].endswith('/'+p['dataset_id']+'/outputs:commit') and cmd['request_body_recorded'] is False and cmd['private_body_sha256']==p['body_sha256'] and cmd['private_body_bytes']==post['body_bytes'],'receipt exact private body POST binding')
    need(meta['kind']=='http' and meta['HTTP_status']==post['POST_status']==200 and meta['transport_outcome']==0 and meta['original_child_rc'] is None,'receipt POST original status')
    need(hashlib.sha256(ack).hexdigest()==meta['response_sha256']==post['POST_response_sha256']==before['receipt_bytes_sha256'] and json.loads(ack)==rec,'receipt exact original response bytes')
    need(p['checkpoint_pairs'],'ordinary snapshot CP404 original observations required')
    for a,b in p['checkpoint_pairs']:
        for base in (a,b):
            r.rc(base+'.rc');cmd=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json')
            need(cmd['method']=='GET' and cmd['url'].endswith('/'+p['def_id']+'/checkpoint') and meta['status']==404 and meta['transport_outcome']==0 and meta['original_child_rc'] is None,'ordinary snapshot CP404 binding')
        need(r.read(a+'.raw')==r.read(b+'.raw'),'ordinary snapshot CP404 changed')
        need(before['checkpoint']['status']==404 and json.loads(r.read(a+'.raw'))==before['checkpoint']['body'] and hashlib.sha256(r.read(a+'.raw')[:-1]).hexdigest()==before['checkpoint']['body_sha256'],'snapshot CP404 RESULT/original body linkage')
    return {'kind':'final-receipt','same_original_body_sha256':p['body_sha256'],'exact_original_ACK_replay':True,'versions':1,'scope':'ordinary snapshot DVS/Run/CP404 cross-restart replay; incremental epoch and Source-no-reread supplied separately by S2HTTP/PG'}


def secret(r,p,source):
    # v2 is an explicit registration change, never a path-based actual-value exemption.
    version=p.get('contract_version');v2='p26-scan1-payload-members-v2-private20-publicfixture2'
    need(version==v2,'new final secret proof requires v2 protected20; v1 remains historical data only')
    metadata_cache={}
    def metadata_read(ref):
        need(isinstance(ref,dict) and set(ref)=={'path','sha256','size'} and isinstance(ref['path'],str)
             and isinstance(ref['sha256'],str) and re.fullmatch('[a-f0-9]{64}',ref['sha256']) and type(ref['size']) is int,'exact public metadata/source ref')
        label=p['canonical_root_label'];path=ref['path'];path=path[len(label)+1:] if path.startswith(label+'/') else path
        need(path.startswith('tools/smoke/business-platform-p2-6/') and not path.startswith('/') and '\\' not in path
             and all(v not in ('','.','..') for v in path.split('/'))
             and not any(v.lower().startswith('.env') or v.lower().endswith(('.env','.body')) or '.env.' in v.lower() for v in path.split('/')),'private inputs are lexical metadata, never opened by proof reader')
        need(path.startswith('tools/smoke/business-platform-p2-6/results/') or path=='tools/smoke/business-platform-p2-6/w6/selftest_p26_pg_runner.py','only explicit result evidence or known public creator source')
        if path not in metadata_cache:metadata_cache[path]=r.read(path)
        raw=metadata_cache[path];need(hashlib.sha256(raw).hexdigest()==ref['sha256'] and len(raw)==ref['size'],'registration review source/metadata pin')
        return raw
    def validate_publicfixture_review(read, ref, source):
        """Metadata-only exact22 -> protected20 + two proven public inputs."""
        import ast,re
        digest=lambda raw:hashlib.sha256(raw).hexdigest()
        def doc(pin):
            need(isinstance(pin,dict) and isinstance(pin.get('path'),str) and pin['path'].endswith('.json'),'JSON metadata role before read')
            return json.loads(read(pin))
        review=doc(ref);removed={'candidate-17','candidate-19'}
        need(review['schema']=='p26-registered-input-public-fixture-classification-v1' and review['root_accepted'] is True
             and review['active_S2']==source and review['scope']=='exact-candidate17-and19-public-selftest-fixtures-only','independent Root public-fixture registration review')
        need(review['classification']=='public-selftest-fixed-fixture-nonsecret' and set(review['classified_ids'])==removed
             and len(review['classified_ids'])==2 and review['other20_actual_value_hits_remain_fatal'] is True
             and review['values_value_hashes_lengths_not_persisted'] is True and review['original_env_files_preserved_local'] is True
             and review['original22_and_scan1_failure3490_preserved'] is True and review['candidate18_retained_private'] is True,'exacttwo classification; old failures and allother private inputs preserved')
        old,new=doc(review['original_registry22']),doc(review['revised_private_registry20']);original=old['registered_inputs'];revised=new['registered_inputs']
        need(old['schema']=='p26-secret-input-registrations-CANDIDATE-v1' and old['source_sha256']==source
             and old['actual_values_read'] is False and old['private_paths_stat_read_hashed_now'] is False
             and old['registered_inputs_sha256']==digest(json.dumps(original,sort_keys=True,separators=(',',':')).encode()),'original22 metadata source/count/canonical digest, not current values')
        fields={'id','path','key','role','projection'}
        need(isinstance(original,list) and len(original)==22 and all(set(e)==fields and all(isinstance(v,str) and v for v in e.values()) for e in original),'original22 complete metadata')
        ids=[e['id'] for e in original];numbers=[int(re.fullmatch(r'candidate-(\d{1,2})',v).group(1)) if re.fullmatch(r'candidate-(\d{1,2})',v) else -1 for v in ids]
        need(len(set(ids))==22 and set(numbers)==set(range(1,23)),'original exact candidate1through22 identities')
        public=[e for e in original if e['id'] in removed];remaining=[e for e in original if e['id'] not in removed]
        need(len(public)==2 and review['public_fixture_inputs']==public and new['schema']=='p26-secret-input-registry20-v1'
             and new['root_accepted'] is True and new['source_sha256']==source and revised==remaining and len(revised)==20
             and type(new['original_registered_input_count']) is int and new['original_registered_input_count']==22
             and type(new['private_registered_input_count']) is int and new['private_registered_input_count']==20
             and type(new['public_fixture_input_count']) is int and new['public_fixture_input_count']==2
             and new['candidate18_retained_private'] is True and new['original_registry22']==review['original_registry22']
             and new['registered_inputs_sha256']==digest(json.dumps(revised,sort_keys=True,separators=(',',':')).encode()),'full22to20 metadata diff: no path/key/role/projection edits or extra exclusion')
        creator=doc(review['public_creator_evidence']);equality=review['signed_memory_boolean_equality_evidence'];evidence=doc(equality['report'])
        sourcebytes=read(equality['creator_source']);creator_sha=equality['creator_source']['sha256']
        need(creator_sha=='28b2237317beab52ae39d93663ce0d6af56a263655ded282623a4ea627773607'
             and creator['selftest_source']['sha256']==creator_sha and digest(sourcebytes)==creator_sha
             and type(equality['AST_fixture_line']) is int and equality['AST_fixture_line']==430,'public creator exact frozen source and AST line')
        tree=ast.parse(sourcebytes)
        def fixed_string(node):
            # Same bounded Constant/Add/Mult interpretation as the signed comparator.
            if isinstance(node,ast.Constant) and type(node.value) in (str,int):return node.value
            need(isinstance(node,ast.BinOp),'fixed public expression only')
            left,right=fixed_string(node.left),fixed_string(node.right)
            if isinstance(node.op,ast.Add):
                need(type(left)is str and type(right)is str,'literal Add strings only');result=left+right
            elif isinstance(node.op,ast.Mult):
                need(type(left)is str and type(right)is int and 0<=right<=4096,'bounded literal Mult only');result=left*right
            else:raise ValueError('not a fixed public string expression')
            need(len(result)<=32768,'bounded public expression output');return result
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and n.lineno==430 and isinstance(n.func,ast.Attribute) and n.func.attr=='write_text']
        need(len(calls)==1 and len(calls[0].args)==1 and any(isinstance(n,ast.ClassDef) and n.name=='Fake' for n in ast.walk(tree)),'public fixed writer and Fake mechanism source')
        public_text=fixed_string(calls[0].args[0]);need(type(public_text)is str and public_text.startswith('POSTGRES_PASSWORD=') and public_text.endswith('\n') and len(public_text.splitlines())==1,'fixed public fixture writer, not live random credentials');del public_text
        need(creator['exact_original_creation_command_recovered'] is False and creator['original_creation_time_claimed'] is False
             and creator['actual_env_read_stat_hash'] is False and len(creator['safe_corresponding_metadata'])==2
             and {v['candidate_id'] for v in creator['safe_corresponding_metadata']}==removed
             and all(v['env_contents_or_metadata_read'] is False and v['rel_equals_public_selftest_constructor_except_injected_phase_list'] is True and v['contains_runtime_CID_fields'] is False for v in creator['safe_corresponding_metadata']), 'creator evidence is public-source-plus-Fake, not invented creation provenance')
        expected_rows=[{'id':e['id'],'matches_public_fixture':True} for e in public]
        need(evidence['comparison_complete'] is True and evidence['rows']==expected_rows and all(type(v['matches_public_fixture']) is bool for v in evidence['rows'])
             and evidence['source_sha256']==creator_sha and type(evidence['source_line']) is int and evidence['source_line']==430
             and evidence['scope']=='current-public-test-fixture-value-equality-only; original-creation-commands-unproved; no-registry-change-or-credential-exemption'
             and review['required_memory_boolean_results']=={v:True for v in removed},'actual two true memory booleans bound to public creator')
        def actual_group(group,schema):
            report=doc(group['report']);audit=doc(group['audit'])
            need(group['original_rc']['path'].endswith('.rc') and read(group['original_rc']).strip()==b'0','original diagnostic Unixrc0')
            need(audit['schema']==schema and type(audit['original_child_rc']) is int and audit['original_child_rc']==0 and audit['private_values_or_value_hashes_persisted'] is False,'original memory-only diagnostic audit')
            base=group['audit']['path'].rsplit('/',1)[0];names={'RESULT.json','Root-release.snapshot.json','after.json','before.json','command.json','original-child.rc','stderr.raw','stdout.raw'}
            need(set(audit['files'])=={base+'/'+v for v in names}|{group['report']['path']},'exact original9 diagnostic metadata, never private inputs')
            raw={path:read({'path':path,**pin}) for path,pin in audit['files'].items()}
            need(audit['files'][group['report']['path']]['sha256']==group['report']['sha256'] and audit['files'][group['original_rc']['path']]['sha256']==group['original_rc']['sha256']
                 and raw[base+'/stderr.raw']==b'' and raw[base+'/original-child.rc'].strip()==b'0','report/rc original audit binding')
            before,after=doc({'path':base+'/before.json',**audit['files'][base+'/before.json']}),doc({'path':base+'/after.json',**audit['files'][base+'/after.json']})
            need({k:v for k,v in before.items() if k!='utc'}=={k:v for k,v in after.items() if k!='utc'},'diagnostic before/after metadata pins stable')
            return report,audit,raw,base,before
        _,ea,eraw,ebase,ebefore=actual_group(equality,'p26-two-input-public-fixture-comparison-original-audit-v1')
        need(ea['comparison_only_not_original_creation_provenance_or_credential_exemption'] is True,'equality is not automatic credential exemption')
        release=json.loads(eraw[ebase+'/Root-release.snapshot.json']);result=json.loads(eraw[ebase+'/RESULT.json'])
        need(release['schema']=='p26-two-input-public-fixture-comparison-release-v1' and release['root_accepted'] is True and release['source_sha256']==source
             and release['selected_ids']==[e['id'] for e in public] and release['selected_inputs']==public and release['creator']['sha256']==creator_sha and release['creator']['line']==430
             and release['registry']['sha256']==review['original_registry22']['sha256']
             and ebefore['release_sha256']==digest(eraw[ebase+'/Root-release.snapshot.json']) and ebefore['creator_line']==430
             and creator_sha in ebefore['input_code_metadata_pins'].values(),'actual Root selectedtwo phase and creator pins')
        need(type(result['original_child_rc']) is int and result['original_child_rc']==0 and result['before_after_pins_stable'] is True
             and result['rows']==expected_rows and result['source_sha256']==creator_sha and result['source_line']==430
             and result['registry_modified'] is False and result['other20_loaded'] is False and result['creator_executed'] is False,'actual equality result; no registry mutation or creator execution')
        attribution,aa,_,_,_=actual_group(review['original_attribution'],'p26-exact74-attribution-original-audit-v1')
        need(aa['attribution_only_not_official_scan1_retry'] is True and attribution['attribution_complete'] is True and attribution['secret_clean_claimed'] is False
             and type(attribution['official_scan1_original_rc']) is int and attribution['official_scan1_original_rc']==1 and attribution['registered_input_count']==22
             and attribution['summarytotal']==3490 and [v['id'] for v in attribution['rows']]==ids
             and all(type(v['occurrences']) is int and v['occurrences']==(1745 if v['id'] in removed else 0) and type(v['path_count']) is int and v['path_count']==(74 if v['id'] in removed else 0) for v in attribution['rows']), 'original22 failure3490 remains failure, exact2 attribution not a clean scan')
        return original,revised,review

    review_ref=p.get('publicfixture_registration_review')
    original22=revised20=review=None
    if version==v2:
        original22,revised20,review=validate_publicfixture_review(metadata_read,review_ref,source)
        need(p['registered_inputs']==revised20 and type(p['registered_value_count']) is int and p['registered_value_count']==20,'v2 exact protected20 metadata')
    need(p['epoch']==source,'secret scan source epoch')
    r.rc(p['rc']);command=r.doc(p['command']);need(isinstance(command,list) and '--values-stdin' in command and '--audit-json' in command,'actual-value stdin mode and all-byte audit flags')
    doc=r.doc(p['report']);need(doc['schema']=='p26-secret-allbyte-audit-v1' and doc['actual_values_stdin'] is True and doc['values_persisted'] is False,'secret original mode audit')
    need(type(doc['actual_value_count']) is int and doc['actual_value_count']==p['registered_value_count']>0
         and type(doc['actual_hit_count']) is int and doc['actual_hit_count']==0 and type(doc['suspect']) is int and doc['suspect']==0,'every protected actual-value hit is fatal, even in a public fixture')
    need(doc['scanned_members']==p['scanned_members'] and doc['scanned_members'],'secret scan exact whole members')
    for path,pin in doc['scanned_members'].items():
        need(not any(x.lower().startswith('.env') or x.lower().endswith('.env') or '.env.' in x.lower() for x in path.split('/')),'private env in scanned package')
        raw=r.member(path)
        need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['size'],'scanned member changed')
    expected=20
    need(type(p['registered_value_count']) is int and p['registered_value_count']==len(p['registered_inputs'])==expected,'explicit versioned registered input count')
    manifest=r.doc(p['payload_members_manifest']);need(manifest['schema']=='p26-secret-scan-members-v1' and manifest['source_sha256']==source and manifest['members']==doc['scanned_members'],'scan1 exact staged payload member manifest')
    need(p['terminal_scan2_and_seal_external'] is True,'final all-byte scan2/seal remain external to immutable package')
    need(all(f['classification']=='test-or-fixture-literal' and f['kind']!='actual-secret-value' for f in doc['findings']),'actual secret hit cannot become fixture disposition')
    for rel,digest in p['historical_tools'].items():need(hashlib.sha256(r.read(rel)).hexdigest()==digest,'scan original tool changed')
    need(p['historical_tools'],'actual scan original tool provenance missing')
    registry=r.doc(p['registered_inputs_metadata']);r.rc(p['provider_rc']);r.rc(p['scanner_rc'])
    need(registry['schema']=='p26-secret-input-registry-v1' and registry['source_sha256']==source and registry['phase']=='p26-final-secret-values-memory'
         and type(registry['provider_original_rc']) is int and registry['provider_original_rc']==0 and type(registry['scanner_original_rc']) is int and registry['scanner_original_rc']==0
         and registry['values_persisted'] is False,'original provider audit phase/source/rc')
    declared=[{k:v for k,v in e.items() if k!='value_count'} for e in registry['registered_inputs']]
    need(declared==p['registered_inputs'] and registry['actual_value_count']==sum(e['value_count'] for e in registry['registered_inputs'])==p['registered_value_count'],'actual provider exact Root path/key/role registry')
    need(all(type(e['value_count']) is int and e['value_count']==1 for e in registry['registered_inputs']),'provider missing/extra values')
    need(r.doc(p['command'])==r.doc(p['provider_command']),'provider recorded original scanner command')
    return {'kind':'final-actual-secret','contract_version':version,
            'scan1_payload_regular_members':len(doc['scanned_members']),'registered_values':doc['actual_value_count'],'actual_hits':0,
            'publicfixture_registration_review':review_ref if version==v2 else None,'public_fixture_inputs_removed':2 if version==v2 else 0,
            'all_protected_value_hits_remain_fatal':True,'formal_eligible':True,'original_creation_commands_proved':False,
            'terminal_scan2_and_seal_external':p.get('terminal_scan2_and_seal_external',False)}


def old_artifacts(r,p,source):
    result=closed(r,p,source)
    names={'p21_bc_projection','p21_bc_chain','p21_bc_fixture_counts','p21_bc_targets_live','p1_old_artifact_live','p1_old_buildjob_live'}
    expected=r.doc(p['sealed_baseline'])['steps'];expected=[x for x in expected if x['step'] in names]
    actual=result['steps'];need(len(expected)==len(actual)==6 and {x['step'] for x in actual}==names,'old artifact exact6 domains')
    need(actual==expected and all(x['ok'] is True for x in actual),'old full sealed artifact domains changed')
    need(result['source_S']==source,'old artifact actual source')
    # Every GET/SQL original is pinned by closed(); enforce dedicated body bytes
    # for the C target and both original P1 files, not a generic reachability gate.
    need(set(p['downloads'])=={'old_C','P1_file1','P1_file2'},'all three old downloads required')
    for name,ref in p['downloads'].items():
        base=ref['base'];meta=r.doc(base+'.meta.json');raw=r.read(base+'.raw');body=raw[:-1]
        need(meta['kind']=='http' and meta['status']==200 and meta['transport_outcome']==0 and meta['original_child_rc'] is None and meta['scrub_hits']==0,'old original download status')
        need(hashlib.sha256(body).hexdigest()==meta['body_sha256']==ref['sha256'] and len(body)==meta['bytes']==ref['bytes'],'old original download bytes')
    need(set(p['buildjobs'])=={'orgA','orgB','legacy_BUILD_ABORTED','cross_org'},'old BuildJob full scope')
    for name,ref in p['buildjobs'].items():
        base=ref['base'];meta=r.doc(base+'.meta.json');body=json.loads(r.read(base+'.raw'))
        need(meta['status']==(404 if name=='cross_org' else 200) and body==ref['body'],'old BuildJob full original body/status')
    return {'kind':'final-old-artifacts','sealed_domains':6,'downloads':3,'BuildJob_scopes':4}


def restart_receipt(r,p,source):
    if p.get('contract_version')=='p26-final-actual-components-v1':
        import importlib.util
        from pathlib import Path
        spec=importlib.util.spec_from_file_location('finalcomposition',Path(__file__).with_name('p26_final_composites.py'));fc=importlib.util.module_from_spec(spec);spec.loader.exec_module(fc);return fc.restart_receipt(r,p,source)
    need(set(p['proofs'])=={'restart','old_artifacts','receipt'},'complete restart+old readability+receipt scope')
    return {'kind':'final-restart-receipt','restart':restart(r,p['proofs']['restart'],source),'old_artifacts':old_artifacts(r,p['proofs']['old_artifacts'],source),'receipt':receipt(r,p['proofs']['receipt'],source)}


def quiesce(r,p,source):
    result=closed(r,p,source)
    need(result['schema']=='p26-final-quiesce-proof-v1' and result['source_sha256']==source,'quiesce original schema/source')
    need(set(p['ABC'])=={'p26connectora','p26connectorb','p26connectorc'},'quiesce exactABC')
    for name,refs in p['ABC'].items():
        command,before=argv(r,refs['before']);_,after=argv(r,refs['after']);a,b=json.loads(before),json.loads(after)
        need(a['name']==b['name']=='/'+name and a['running'] is True and b['running'] is False,'quiesce original running/stopped')
        need(all(a[k]==b[k] for k in ('cid','image','started_at','labels')),'quiesce original CID/image/boot/labels changed')
        need(a['labels']['of-p26-owner']=='p26-http' and a['labels']['of-p26-stage']=='p2-6' and a['labels']['of-p26-role']=='connector_'+name[-1],'quiesce ownership')
        command,_=argv(r,refs['stop']);need(command==['docker','stop','--time','120',a['cid']],'quiesce exact stopCID')
    defs=result['owned_definitions'];need(len({e['def_id'] for e in defs})==len(defs),'quiesce owned Def duplicates')
    protected={x['def_id'] for x in p['guard_before']['old_parents']}
    for entry in defs:
        need(entry['def_id'] not in protected,'quiesce attempted old definition')
        created={}
        for kind in ('definition_creation','source_creation'):
            ref=entry[kind];relative=local(p,ref['path']);raw=r.read(relative)
            need(hashlib.sha256(raw).hexdigest()==ref['sha256'],'quiesce actual creation original changed')
            doc=json.loads(raw[raw.index(b'{'):])
            for part in ref.get('json_pointer','').strip('/').split('/'):
                if part:doc=doc[int(part)] if isinstance(doc,list) else doc[part.replace('~1','/').replace('~0','~')]
            created[kind]=doc
        definition,origin=created['definition_creation'],created['source_creation']
        need((definition['id'],definition['source_id'],definition['output_dataset_id'],definition.get('org_id'))==(entry['def_id'],entry['source_id'],entry['output_dataset_id'],entry['org_id']),'quiesce original owned Def parent')
        need((origin['id'],origin['owner_id'],origin.get('org_id'))==(entry['source_id'],entry['actor_id'],entry['org_id']),'quiesce original owned Source actor')
    need(len(p['schedule_reads'])==len(p['grant_reads'])==2,'quiesce before/after readback required')
    for base in p['schedule_reads']:
        command,actual=rows(r,base);need({x['id'] for x in actual}=={x['def_id'] for x in defs} and len(actual)==len(defs),'quiesce full owned Def set')
        need(all(x['schedule_enabled'] is False and x['schedule_next_fire_at'] is None for x in actual),'quiesce schedule/next fire not disabled')
    for base in p['grant_reads']:
        command,actual=rows(r,base);need(actual==[] and 'sync_schedule_grants' in command[-3] and 'WHERE' not in command[-3].upper(),'quiesce grants not whole zero')
    need(set(p['definition_mutations'])=={x['def_id'] for x in defs},'quiesce exact owned definition mutations')
    for did,refs in p['definition_mutations'].items():
        need(set(refs)=={'DELETE','PATCH'},'quiesce disarm and schedulefalse')
        for method,base in refs.items():
            r.rc(base+'.rc');cmd=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json')
            need(cmd['method']==method and '/'+did in cmd['url'] and meta['kind']=='http' and meta['http_status']==200 and meta['transport_outcome']==0 and meta['original_child_rc'] is None,'quiesce actual owned mutation status')
            if method=='PATCH':need(cmd['body']=={'schedule_enabled':False},'quiesce exact schedulefalse patch')
    typed_whole(r,p['guard_before'],source);typed_whole(r,p['guard_after'],source)
    need(p['fixture_counter_pairs'],'quiesce old fixture counters required')
    for a,b in p['fixture_counter_pairs']:need(r.read(a)==r.read(b),'quiesce fixture counters changed')
    return {'kind':'final-quiesce','ABC_processes_stopped':3,'grants':0,'schedules_disabled':len(defs),'held_rows_preserved':True}
