"""Case-specific HTTP10 original readers. No runners or network operations."""
import hashlib
import json
import os
import re
from datetime import datetime


def need(ok,why):
    if not ok:raise ValueError(why)


def command_privacy(r, p, source):
    import importlib.util
    path = os.path.join(os.path.dirname(__file__), 'p26_final_history_review.py')
    spec = importlib.util.spec_from_file_location('private_command_reader', path)
    module = importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module, module.private_command_evidence(r, p, source)


def case(r,p,source):
    privacy, private = command_privacy(r, p, source);private_seen=set()
    d,o=p['inner'],p['outer'];r.rc(o+'/case.rc')
    for f in os.listdir(r.path(o)):
        if f.endswith(('.json','.raw','.cmd')):
            path=o+'/'+f
            if privacy.private_command_shape(r,p,source,path,private) is None:r.read(path)
            else:private_seen.add(path)
    before=r.doc(o+'/pins.json');outer_meta=r.doc(o+'/case.meta.json')
    after=outer_meta if p.get('epoch_schema')=='S2' else r.doc(o+'/candidate-after.json')
    need(outer_meta['original_unix_rc']==0,'HTTP original Unix case rc')
    need(before['actual_candidate_hash_before']==after['actual_candidate_hash_after']==source,'HTTP actual source drift')
    if p.get('epoch_schema')=='S2':
        need(before['source_S']==source and outer_meta['source_unchanged'] is True and outer_meta['tools_unchanged'] is True,'S2 HTTP source/tools before after')
        need(before['frozen_dependencies_sha256']==outer_meta['tool_shas_after'],'S2 HTTP original tool drift')
        need(len(outer_meta['inner_attempts'])==1 and outer_meta['inner_attempts'][0].endswith('/'+d),'S2 HTTP actual inner binding')
    need(before==p['pins'],'HTTP actual identity pins')
    ms={};ops=[];transients=[]
    for f in sorted(os.listdir(r.path(d))):
        if not f.endswith('.meta'):continue
        base=d+'/'+f[:-5];m=r.doc(base+'.meta');ms[f]=m
        raw=r.text(base+'.raw')
        if privacy.private_command_shape(r,p,source,base+'.cmd',private) is None:r.read(base+'.cmd')
        else:private_seen.add(base+'.cmd')
        if f.startswith('ops-'):
            r.rc(base+'.rc');need(m['original_rc']==0,'owned original child rc');ops.append(f)
            for suffix in ('.stdout.raw','.stderr.raw'):r.read(base+suffix)
        elif m.get('rc_kind')=='normalized_harness':
            status=m['original_http_status']
            if status is None:
                need('restart-health' in f and m['transport_exception'],'only bounded readiness transport transient allowed');transients.append(f);r.rc(base+'.rc',p['transient_rcs'][f]);continue
            need(type(status)is int and raw.startswith('status:'+str(status)+' bytes:'+str(m['response_bytes'])),'original HTTP/raw status')
            r.rc(base+'.rc',0 if 200<=status<300 else status)
            if 'safe_response' in m:need(json.loads(raw.split('\n',1)[1])==m['safe_response'],'safe raw/meta projection')
        else:r.rc(base+'.rc')
    for f,expected in p['owned_stdout_inputs'].items():need(r.text(d+'/'+f)==expected,'original owned safe SQL/identity observation changed '+f)
    need(len(ops)==p['owned_rc_count'],'owned exact original child count')
    def semantic(name):
        return re.sub(r'^\d+-','',name)
    def one(tag):
        selector=re.sub(r'^\d{3}-','',tag)
        xs=[m.get('safe_response',m) for f,m in ms.items() if (selector in semantic(f) if p.get('epoch_schema')=='S2' else tag in f) and not f.startswith('ops-')]
        if p.get('epoch_schema')=='S2' and 'run-poll' in selector:
            need(bool(xs),'missing semantic final poll '+selector);return xs[-1]
        need(len(xs)==1,'missing/duplicate '+tag);return xs[0]
    def alltag(tag):return [m.get('safe_response',m) for f,m in ms.items() if tag in f]
    # Root-reviewed field contracts are explicit projections, not RESULT verdicts.
    for f,expected in p['semantic_inputs'].items():
        actual=ms[f].get('safe_response',ms[f]);need(actual==expected,'case historical input changed '+f)
    for a,b in p['equal_whole_inputs']:need(one(a)==one(b),'whole historical row/CP/head changed')
    clocks=[]
    for f in ops:
        if 'lease-observe' in f:
            v=json.loads(r.text(d+'/'+f[:-5]+'.stdout.raw'));clocks.append(v)
    if ops:
        need(clocks and clocks[0]['lease_expired'] is False and clocks[-1]['lease_expired'] is True,'natural DB lease expiry absent')
        need(all(datetime.fromisoformat(a['db_clock'])<=datetime.fromisoformat(b['db_clock']) for a,b in zip(clocks,clocks[1:])),'DB clock regressed')
    # All output proofs are recomputed per download and its own receipt; ABA A/B stay distinct.
    downloads=[]
    for f,m in ms.items():
        if not m.get('download_artifact'):continue
        data=r.read(d+'/'+m['download_artifact']);sha=hashlib.sha256(data).hexdigest();rows=len(data.splitlines())
        need(sha==m['response_sha256'] and len(data)==m['response_bytes'],'download original bytes/hash')
        contract=p['downloads'][m['download_artifact']];need(contract=={'sha256':sha,'bytes':len(data),'rows':rows},'download own expected SHA/rows')
        # Prefix excludes numeric step ID; related receipt shares semantic label.
        label=f.split('-',1)[1].removesuffix('file-bytes.meta')
        receipts=[x['safe_response'] for n,x in ms.items() if label+'receipt-proof-receipt-get' in n]
        need(len(receipts)==1,'download corresponding original receipt missing')
        rec=receipts[0];tx=rec['transaction'];meta=tx['metadata'];run=meta['lineage']['sync_run_id']
        need(tx['status']=='COMMITTED' and meta['content_hash']==sha and meta['bytes']==len(data) and meta['rows']==rows,'download receipt bytes/rows/hash')
        need(rec['idempotency_key']=='of-p26-pub-v1:'+run,'receipt Run key')
        downloads.append({'file':m['download_artifact'],'sha256':sha,'rows':rows,'txn':tx['id']})
    need(set(p['downloads'])=={x['file'] for x in downloads},'download exact set')
    name=p['case']
    final_schema={
        'dropack_manual_owned_restart_lookup':('16-run-poll',2,1,1),
        'pregate_no_intent_owned_restart':('13-run-poll',2,1,0),
        'inflight_receipt404_samebody_replay':('14-run-poll',2,2,1),
        'twoinstances_exactly_one_claim':('10-run-poll',2,1,1),
        'budget_exhausted_then_lookup_finalize':('11-run-poll',2,3,1),
        'crossscope_permission_legacy_refusals':('11-run-poll',3,1,2)}
    if name in final_schema:
        tag,generation,send,query=final_schema[name]
        historical_prefix={'dropack_manual_owned_restart_lookup':'067-','pregate_no_intent_owned_restart':'063-','inflight_receipt404_samebody_replay':'027-','twoinstances_exactly_one_claim':'033-','budget_exhausted_then_lookup_finalize':'073-','crossscope_permission_legacy_refusals':'041-'}
        final=one(tag if p.get('epoch_schema')=='S2' else historical_prefix[name]+tag);recovery=final['recovery']
        need(final['status']=='succeeded' and final['generation']==generation and recovery['protocol']=='p26-v1' and recovery['state']=='finalized','HTTP final recovery state/generation')
        need((recovery['send_attempts'],recovery['query_attempts'])==(send,query),'HTTP final outward budgets')
        need(recovery['transaction_id']==final['dataset_version_id'],'HTTP final recovery version')
        logs=alltag('proxy-log-poll')[-1];commits=[x for x in logs if x.get('ack_lineage_run_id')==final['id'] and x.get('kind')=='commit' and not x.get('held_phase')]
        need(len(commits)==(2 if name=='inflight_receipt404_samebody_replay' else 1),'HTTP forwarded commit exact count')
        need(all(x['ack_txn_id']==final['dataset_version_id'] and x['ack_content_hash']==final['content_hash'] for x in commits),'HTTP proxy final receipt/content')
        if name=='inflight_receipt404_samebody_replay':
            need(sorted(x['upstream_status'] for x in commits)==[200,201] and len({x['request_body_sha256'] for x in commits})==1 and all(x['request_body_sha256']==x['ack_fingerprint'] for x in commits),'inflight samebody201replay/original200')

    if name=='pregate_no_intent_owned_restart':
        pre=one('07-pregate-proof' if p.get('epoch_schema')=='S2' else '014-07-pregate-proof');need(pre['no_intent'] and pre['gate_not_entered'] and pre['cp_owned_epoch_zero'] and pre['send_attempts']==pre['query_attempts']==0,'pregate original no gate/intent/outward')
        need(one('final-cp')['confirmed_epoch']==1 and 'owner' not in one('final-cp'),'pregate final CP')
    if name=='budget_exhausted_then_lookup_finalize':
        need(one('cp-held')['confirmed_epoch']==0 and one('cp-held')['owner']['run_id']==final['id'] and one('cp-after')['confirmed_epoch']==1 and 'owner' not in one('cp-after'),'budget CP0 owned to CP1released')
    if name=='twoinstances_exactly_one_claim':
        need(one('a-claimed-run-poll')['generation']==2,'A generation claim')
        statuses=[m.get('original_http_status') for f,m in ms.items() if 'b-busy' in f];need(statuses==[409],'B loser409')
    if name=='a_then_b_recover_a_head_unchanged':
        need(one('cp-after-b')['confirmed_epoch']==2,'ABA CP epoch')
        need(one('main-head-before')==one('main-head-after'),'ABA actual main B head')
    if name=='crossscope_permission_legacy_refusals':
        statuses=sorted(m['original_http_status'] for m in ms.values() if m.get('original_http_status',0) and m['original_http_status']>=400)
        need(statuses==[401,401,403,404,404,404,409,502],'crossscope exact negative statuses')
        need(one('09-run-poll')['status']=='running' and one('09-run-poll')['generation']==2 and one('11-run-poll')['generation']==3,'dependency502 running then gen3')
    if name=='inflight_receipt404_samebody_replay':
        need(all(m['original_http_status']==404 for f,m in ms.items() if 'inflight-' in f and '404-receipt' in f),'inflight original receipt404')
    # Nine historical code blobs plus wrapper are pinned separately from current archive tools.
    cp=r.doc(p['tools_checkpoint']);snap=os.path.dirname(p['tools_checkpoint'])
    if p.get('epoch_schema')=='S2':
        rows=cp['tools'];expected_tools={x['snapshot']:x['sha256'] for x in rows}
        need(len(rows)==11 and len(expected_tools)==11,'S2 exact11 historical HTTP tool blobs')
        toolmap={x['source']:x['sha256'] for x in rows if x['source']!='http/run_p26_s2_actual.py'}
        wrappers=[x['sha256'] for x in rows if x['source']=='http/run_p26_s2_actual.py']
        need(toolmap==before['frozen_dependencies_sha256'] and wrappers==[before['wrapper_sha256']],'S2 source wrapper checkpoint linkage')
    else:
        expected_tools={snap+'/'+rel:sha for rel,sha in cp['whole_file_sha256'].items()};expected_tools[snap+'/'+os.path.basename(cp['wrapper']['path'])]=cp['wrapper']['sha256']
        need(len(expected_tools)==10,'nine source tools + wrapper required')
    need(expected_tools==p['historical_tools'],'historical source/wrapper checkpoint bindings')
    for rel,sha in p['historical_tools'].items():need(hashlib.sha256(r.read(rel)).hexdigest()==sha,'historical HTTP tool blob changed')
    if p.get('current_supplement'):
        s=p['current_supplement'];sd=s['inner'];r.rc(s['outer']+'/proof.rc')
        need(hashlib.sha256(r.read(d+'/RESULT.json')).hexdigest()==s['original_result_sha256'],'current supplement original RESULT link')
        links=r.doc(sd+'/LINKS.json');original=r.doc(d+'/RESULT.json')
        need(links['frozen_S']==links['actual_source_hash_post_supplement']==source and links['original_case_result_sha256']==s['original_result_sha256'],'CURRENT original S linkage')
        need(links['dataset_id']==original['dataset_id'] and links['def_id']==original['def_id'] and links['run_id']==original['run_id'],'CURRENT owned tuple linkage')
        for f in os.listdir(r.path(sd)):
            if f.endswith(('.json','.meta','.rc','.raw','.cmd','.bin')):
                path=sd+'/'+f
                if privacy.private_command_shape(r,p,source,path,private) is None:r.read(path)
                else:private_seen.add(path)
        receipt_file=[f for f in os.listdir(r.path(sd)) if 'receipt-proof-receipt-get.meta' in f];need(len(receipt_file)==1,'CURRENT receipt missing')
        receipt=r.doc(sd+'/'+receipt_file[0])['safe_response'];tx=receipt['transaction']
        need(tx['dataset_id']==links['dataset_id'] and tx['metadata']['lineage']['sync_run_id']==links['run_id'] and tx['metadata']['lineage']['source_id']==links['source_id'] and tx['metadata']['lineage']['sync_def_id']==links['def_id'],'CURRENT Source/Def/Run/Dataset linkage')
        for f,expected in s['downloads'].items():
            data=r.read(sd+'/'+f);need(hashlib.sha256(data).hexdigest()==expected['sha256'] and len(data)==expected['bytes'] and len(data.splitlines())==expected['rows'],'CURRENT supplement download')
            need(tx['metadata']['content_hash']==expected['sha256'] and tx['metadata']['bytes']==expected['bytes'] and tx['metadata']['rows']==expected['rows'],'CURRENT receipt content linkage')
    need(one('cleanup-fixture-quiet')['quiet'] and one('cleanup-stats')['commit_inflight']==0 and one('cleanup-barrier-state')['barrier'] is None,'HTTP cleanup')
    return {'kind':'http-case','case':name,'outer_rc':0,'owned_original_rc0':len(ops),'downloads':downloads,'current_supplement_separate':bool(p.get('current_supplement')),'full_HTTP_complete':False,'private_command_bodies_not_read':len(private_seen),'private_command_body_bytes_verified_from_package':not bool(private_seen)}


def suite(r,p,source):
    expected={'healthy_snapshot_and_legacy','healthy_incremental_epoch_once','dropack_auto_retry_exact_replay','dropack_manual_owned_restart_lookup','pregate_no_intent_owned_restart','inflight_receipt404_samebody_replay','twoinstances_exactly_one_claim','budget_exhausted_then_lookup_finalize','a_then_b_recover_a_head_unchanged','crossscope_permission_legacy_refusals'}
    need(len(p['cases'])==10 and {c['case'] for c in p['cases']}==expected,'HTTP10 exact case roster')
    from importlib.util import spec_from_file_location,module_from_spec
    spec=spec_from_file_location('live',os.path.join(os.path.dirname(__file__),'p26_live_proofs.py'));live=module_from_spec(spec);spec.loader.exec_module(live)
    results=[]
    for c in p['cases']:
        if p.get('private_command_omission_addendum'):
            need(c.get('private_command_omission_addendum')==p['private_command_omission_addendum'] and c.get('canonical_root_label')==p['canonical_root_label'],'suite/case exact private-command proof linkage')
        if c['kind']=='http-first3':results.append(live.http_first3(r,c,source))
        else:results.append(case(r,c,source))
    completion=None
    if p.get('epoch_schema')=='S2':
        spec=spec_from_file_location('s2completion',os.path.join(os.path.dirname(__file__),'p26_s2_http_aggregate.py'));extra=module_from_spec(spec);spec.loader.exec_module(extra);completion=extra.complete(r,p,source)
    return {'kind':'http10','S2_completion':completion,'cases':results,'HTTP_stage_evidence_complete':True,'final_root_acceptance':False,'scope_limits':p['scope_limits']}


def crosscheck(root,contract,negative=False):
    import copy,importlib.util
    spec=importlib.util.spec_from_file_location('original',os.path.join(os.path.dirname(__file__),'p26_original_proofs.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    doc=json.load(open(contract));proof=doc['proof'];result,files=m.validate(root,proof,doc['source']);controls=[]
    if negative:
        original=m.Reader
        mutations=[('ABA-A-own-download','018-05-a-proof-file-bytes.download.bin',lambda b:b+b'bad'),('natural-DBclock-expiry','lease-observe.stdout.raw',lambda b:b.replace(b'"lease_expired" : true',b'"lease_expired" : false')),
                   ('owned-original-rc','lease-observe.rc',lambda b:b'1'),('CP-final-epoch','065-15-final-cp.meta',lambda b:b.replace(b'"confirmed_epoch": 1',b'"confirmed_epoch": 2')),
                   ('two-claim-whole-row','030-08c-b-no-write-run-poll.meta',lambda b:b.replace(b'"generation": 2',b'"generation": 3')),
                   ('cross-dependency-status','037-08b-receipt-dep-error-recover.meta',lambda b:b.replace(b'"original_http_status": 502',b'"original_http_status": 200')),
                   ('CURRENT-original-link','LINKS.json',lambda b:b.replace(b'0c58eab5',b'1c58eab5')),
                   ('historical-wrapper','run_remaining5_actual.py',lambda b:b+b'\n#changed')]
        for name,suffix,change in mutations:
            hit=[False]
            class Overlay(original):
                def read(self,rel):
                    raw=super().read(rel)
                    if rel.endswith(suffix):
                        changed=change(raw);hit[0]|=changed!=raw;return changed
                    return raw
            m.Reader=Overlay;p=copy.deepcopy(proof);p.pop('original_files',None)
            try:m.validate(root,p,doc['source']);refused=False
            except (ValueError,OSError):refused=True
            controls.append({'name':name,'mutated':hit[0],'refused':refused});m.Reader=original
        p=copy.deepcopy(proof);p.pop('original_files',None);p['cases'].pop()
        try:m.validate(root,p,doc['source']);refused=False
        except (ValueError,OSError):refused=True
        controls.append({'name':'missing-one-required-case','mutated':True,'refused':refused})
        need(all(x['mutated'] and x['refused'] for x in controls),'HTTP10 negative failed '+str(controls))
    return {'verdict':'HTTP10-ORIGINAL-STAGE-VERIFIED','final_root_acceptance':False,'result':result,'unique_original_files':len(files),'original_files':files,'negative_controls':controls}

if __name__=='__main__':
    import sys
    if len(sys.argv) not in (3,4):raise SystemExit('usage: p26_http10_proofs.py ROOT CONTRACT [--negative-controls]')
    try:print(json.dumps(crosscheck(sys.argv[1],sys.argv[2],len(sys.argv)==4),indent=2))
    except (ValueError,OSError,TypeError,KeyError) as e:print(json.dumps({'verdict':'REFUSED','problem':str(e)}));raise SystemExit(1)
