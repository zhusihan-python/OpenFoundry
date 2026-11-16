"""Explicit P26 copy ledger planner. Only fixture selftest copies in this phase.
Actual execution requires a bound Root final-phase document; current phase runs only pure tests.
"""
import argparse
import hashlib
import json
import os
import re
import stat
import tempfile
from pathlib import Path

S='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
P26='tools/smoke/business-platform-p2-6/'
TOOLS=('p26_archive_manifest.py','p26_archive_verifier.py','p26_original_proofs.py','p26_live_proofs.py','p26_http10_proofs.py','p26_secret_scan.py','p26_offline_probe.py','p26_legacy_proofs.py','p26_formal_core.py','p26_source402_allowlist.json','p26_s2_proofs.py','p26_runtime_proofs.py','p26_runtime_aggregate.py','p26_s2_http_aggregate.py','p26_final_proofs.py','p26_p25_proofs.py','p26_healthy_global_reader.py','p26_h9_history_proofs.py','p26_h9_drain_reader.py','p26_late_healthy_readers.py','p26_p25_supplement_readers.py','p26_failed_h10_history.py','p26_h10_current_reader.py','p26_h10_actual_reader.py','p26_h10_anchor_reader.py','p26_h10_freeze_reader.py','p26_p25_tail_reader.py','p26_h14_independent_reader.py','p26_p25_full_reader.py','p26_final_actual_readers.py','p26_final_restart_reader.py','p26_final_p1_reader.py','p26_collect_final_owned_state.py','p26_final_composites.py','p26_final_history_review.py','p26_final_readonly_readers.py','p26_final_artifact_receipt_reader.py','p26_final_bc_h_reader.py','p26_final_abc_reader.py','p26_final_cleanup_reader.py','p26_stage_scan_payload.py','p26_secret_values_memory.py','p26_package_assembler.py','p26_history_catalog_builder.py','p26_history_epoch.py')
SOURCE_ROOTS=('services/connector-management-service/','services/dataset-versioning-service/','services/pipeline-build-service/','libs/scheduling-cron/')
KINDS={'source','tool','evidence','history-actual-failed','history-other','root-acceptance','roster','readme'}

def need(ok,msg):
    if not ok:raise ValueError(msg)

def relative(value):
    need(isinstance(value,str) and re.fullmatch(r'[A-Za-z0-9._+-]+(?:/[A-Za-z0-9._+-]+)*',value) and not any(x in ('.','..','') for x in value.split('/')),'unsafe relative member')
    need(not any(x.lower().startswith('.env') or x.lower().endswith('.env') or '.env.' in x.lower() or x.lower().endswith('.body') for x in value.split('/')),'private env/raw body forbidden')
    return value

def regular(root,rel):
    relative(rel);p=Path(root)
    need(not p.is_symlink(),'origin root symlink')
    for part in rel.split('/'):
        p=p/part;need(not p.is_symlink(),'member symlink')
    need(stat.S_ISREG(p.stat().st_mode),'nonregular member')
    return p

def digest(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(1048576),b''):h.update(b)
    return h.hexdigest()

def validate_spec(spec):
    need(spec.get('format')=='p26-copy-ledger-v1','copy ledger version')
    need(spec.get('source_sha256')==S,'frozen source S')
    need(spec.get('history_roots_explicit') is True,'explicit failed/history roster required')
    allowed=load_local('p26_formal_core').source_allowlist(Path(__file__).with_name('p26_source402_allowlist.json'))
    frozen_pins=load_local('p26_formal_core').source_pins(Path(__file__).with_name('p26_source402_allowlist.json'))
    members=spec.get('members',[]);need(bool(members),'empty ledger')
    destinations=set();sources=set()
    for m in members:
        relative(m['path']);relative(m['destination']);need(m['kind'] in KINDS,'unknown member kind')
        need(m['origin'] in ('workspace','root-review'),'unknown origin')
        need(m['destination'] not in destinations,'duplicate/colliding destination');destinations.add(m['destination'])
        need(m['destination'] not in ('manifest.json','copy-ledger.json'),'bounded generated metadata destination reserved')
        need(type(m['size']) is int and m['size']>=0 and bool(re.fullmatch('[0-9a-f]{64}',m['sha256'])),'member size/hash malformed')
        if m['kind']=='source':
            need(m['origin']=='workspace' and (m['path'] in ('go.mod','go.sum') or m['path'] in allowed),'source origin outside source recipe')
            need({'sha256':m['sha256'],'size':m['size']}==frozen_pins[m['path']],'source per-file frozen SHA/size mismatch')
            need(m['destination']=='source/'+m['path'],'source destination framing');sources.add(m['path'])
        elif m['origin']=='workspace':need(m['path'].startswith(P26),'old tools/archives or workspace fallback forbidden')
        if m['kind']=='tool':need(m['destination'].startswith('tools/'),'tool destination')
    need(sources==allowed,'exact frozen source402 + go.mod/sum allowlist required')
    need(len([p for p in sources if p.endswith(('.go','.sql'))])==402 and {'go.mod','go.sum'}<=sources,'exact 402 GoSQL plus go.mod/sum required')
    need(all('tools/'+name in destinations for name in TOOLS),'standalone helper closure incomplete')
    need({'roster/p26_formal_roster.json','evidence/evidence.json','root-acceptance.json','README.md'}<=destinations,'required package inputs missing')
    need(spec.get('formal_required_paths') and set(spec['formal_required_paths'])<=destinations,'satisfier original closure incomplete')
    need(spec.get('history_required_paths') and set(spec['history_required_paths'])<=destinations,'failed actual history closure incomplete')
    return members

def plan(spec,origins):
    members=validate_spec(spec);seen={};total=0
    for m in members:
        need(m['origin'] in origins,'origin mapping missing')
        p=regular(origins[m['origin']],m['path']);need(p.stat().st_size==m['size'] and digest(p)==m['sha256'],'original hash/size changed: '+m['path'])
        seen[m['destination']]={'sha256':m['sha256'],'size':m['size'],'kind':m['kind']};total+=m['size']
    sources={m['path']:m for m in members if m['kind']=='source'}
    actual=set()
    for base in SOURCE_ROOTS:
        for d,dirs,names in os.walk(Path(origins['workspace'])/base,followlinks=False):
            need(not any((Path(d)/n).is_symlink() for n in dirs),'source directory symlink')
            for name in names:
                if name.endswith(('.go','.sql')) and (not base.startswith('libs/') or name.endswith('.go')):
                    actual.add(str((Path(d)/name).relative_to(origins['workspace'])))
    need(actual=={p for p in sources if p.endswith(('.go','.sql'))},'exact source tree membership changed')
    label=spec.get('canonical_root_label');need(isinstance(label,str) and label.startswith('/'),'source canonical LABEL required')
    blob=b'go.mod\n'+regular(origins['workspace'],'go.mod').read_bytes()+b'go.sum\n'+regular(origins['workspace'],'go.sum').read_bytes()+b'content-hashes\n'
    blob+=b''.join((sources[p]['sha256']+'  '+label+'/'+p+'\n').encode() for p in sorted(actual))
    need(hashlib.sha256(blob).hexdigest()==S,'source bytes/absolute LABEL recipe differs from S')
    payload=json.dumps(spec,sort_keys=True,separators=(',',':')).encode()
    return {'status':'PLAN-VERIFIED-NOT-COPIED','source_sha256':S,'files':len(members),'bytes':total,'copy_ledger_sha256':hashlib.sha256(payload).hexdigest(),'members':seen,'generated_metadata':['copy-ledger.json','manifest.json'],'actual_assembly_authorized':False,'pending':['Root final roster and original-proof interfaces','actual secret scan with private values only in memory','independent copy + denied original paths + no service dependencies','seal outside payload manifest']}

def copy_fixture_only(root,destination,members):
    """Pure-test copy primitive; CLI cannot call it with actual evidence in this phase."""
    need(not Path(destination).exists(),'never overwrite destination');Path(destination).mkdir()
    for m in members:
        src=regular(root,m['path']);need(digest(src)==m['sha256'] and src.stat().st_size==m['size'],'changed before copy')
        out=Path(destination)/relative(m['destination']);out.parent.mkdir(parents=True,exist_ok=True)
        with open(out,'xb') as f:f.write(src.read_bytes())
        need(digest(out)==m['sha256'],'copied-byte mismatch')

def load_local(name,directory=None):
    import importlib.util
    path=Path(directory or Path(__file__).parent)/(name+'.py')
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def authorize(release,spec,destination):
    need(load_local('p26_formal_core').ACTIVE_FINAL_SOURCE is not None,'S2 Root freeze pending; no old-S final publication')
    ledger_sha=hashlib.sha256(json.dumps(spec,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    need(release.get('phase')=='p26-final-assembly' and release.get('approved') is True,'Root final assembly phase required')
    need(release.get('source_sha256')==S and release.get('ledger_sha256')==ledger_sha,'Root phase ledger/source binding')
    need(release.get('destination')==str(Path(destination).absolute()),'Root phase destination binding')
    return ledger_sha


def bound_reader_factory(mapped,workspace,root_review=None):
    original=load_local('p26_original_proofs')
    class LedgerReader(original.Reader):
        def read(self,rel):
            need(rel in mapped,'original artifact absent from ledger destination: '+str(rel))
            member=mapped[rel]
            need(member['origin']=='workspace' and member['path']==rel,'original origin/path differs from packaged destination')
            data=super().read(rel);actual=self.files[rel]
            need(actual['sha256']==member['sha256'] and actual['size']==member['size'],'original artifact ledger hash/size mismatch: '+rel)
            return data
        def member(self,rel):
            # Only the secret payload reader uses destination aliases. Normal
            # original-proof read() retains exact origin/path equality above.
            need(rel in mapped,'scan member absent from exact copy ledger')
            m=mapped[rel];origin=workspace if m['origin']=='workspace' else root_review
            need(origin is not None,'scan member explicit origin unavailable')
            data=regular(origin,m['path']).read_bytes()
            actual={'sha256':hashlib.sha256(data).hexdigest(),'size':len(data)}
            need(actual=={k:m[k] for k in ('sha256','size')},'scan member signed byte mapping changed')
            self.files[rel]=actual;return data
    return lambda:LedgerReader(workspace)


def formal_preflight(spec,origins):
    mapped={m['destination']:m for m in validate_spec(spec)}
    def read(dest):
        m=mapped[dest];data=regular(origins[m['origin']],m['path']).read_bytes()
        need(hashlib.sha256(data).hexdigest()==m['sha256'] and len(data)==m['size'],'formal metadata ledger hash/size mismatch')
        return json.loads(data)
    roster=read('roster/p26_formal_roster.json');evidence=read('evidence/evidence.json');acceptance=read('root-acceptance.json')
    core=load_local('p26_formal_core')
    factory=bound_reader_factory(mapped,origins['workspace'],origins['root-review'])
    seen=core.validate_all(roster,evidence,acceptance,S,factory)
    need(set(seen)<=set(spec['formal_required_paths']),'formal original read closure absent from explicit required set')
    for name in TOOLS:
        member=mapped['tools/'+name]
        need(member['sha256']==digest(Path(__file__).parent/name),'reviewed current helper differs from ledger')


def assemble(spec,origins,destination,release):
    authorize(release,spec,destination);summary=plan(spec,origins);formal_preflight(spec,origins)
    dest=Path(destination).absolute();need(not dest.exists(),'never overwrite final destination')
    dest.mkdir(parents=False)
    # Failure leaves the new directory intact as honest failed history; never overwrite/resume it.
    for member in spec['members']:
        src=regular(origins[member['origin']],member['path']);need(digest(src)==member['sha256'] and src.stat().st_size==member['size'],'original changed after preflight')
        out=dest/relative(member['destination']);out.parent.mkdir(parents=True,exist_ok=True)
        with open(src,'rb') as f,open(out,'xb') as target:
            for chunk in iter(lambda:f.read(1048576),b''):target.write(chunk)
        need(digest(out)==member['sha256'],'copy hash changed')
    ledger={'format':'p26-copied-ledger-v1','input_ledger_sha256':summary['copy_ledger_sha256'],'members':summary['members'],'source_sha256':S}
    with open(dest/'copy-ledger.json','x') as f:json.dump(ledger,f,indent=2)
    manifest=load_local('p26_archive_manifest',dest/'tools');expected=[m['destination'] for m in spec['members']]+['copy-ledger.json']
    doc=manifest.build_manifest(str(dest),expected)
    with open(dest/'manifest.json','x') as f:json.dump(doc,f,indent=2)
    verifier=load_local('p26_archive_verifier',dest/'tools');verdict,problems,_=verifier.verify(str(dest),spec['canonical_root_label'])
    need(verdict=='VERIFIED-NOT-SEALED','copied package formal verification failed: '+str(problems))
    return {'status':'ASSEMBLED-VERIFIED-NOT-SEALED','destination':str(dest),'manifest':doc['manifest_sha256'],'file_count':doc['file_count'],'offline_independence':'NOT YET VERIFIED'}


def selftest():
    checks=[]
    def negative(name,fn):
        try:fn();ok=False
        except (ValueError,OSError):ok=True
        checks.append((name,ok))
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp)/'src';root.mkdir();(root/'a').write_bytes(b'fixture')
        m={'path':'a','destination':'x/a','sha256':digest(root/'a'),'size':7}
        copy_fixture_only(root,Path(tmp)/'out',[m]);checks.append(('fixture-exact-copy',digest(Path(tmp)/'out/x/a')==m['sha256']))
        negative('refuse-overwrite',lambda:copy_fixture_only(root,Path(tmp)/'out',[m]))
        negative('changed-before-copy',lambda:copy_fixture_only(root,Path(tmp)/'changed',[dict(m,sha256='0'*64)]))
        for rel in ('../escape','/absolute','nested/.env.private','nested/private.env','request.body'):
            negative('private-or-path-'+rel,lambda rel=rel:relative(rel))
        (root/'link').symlink_to(root/'a');negative('symlink',lambda:regular(root,'link'))
        negative('unreleased-phase',lambda:authorize({}, {},Path(tmp)/'never-created'))
        negative('phase-ledger-destination-mismatch',lambda:authorize({'approved':True,'phase':'p26-final-assembly','source_sha256':S,'ledger_sha256':'0'*64,'destination':'/wrong'}, {},Path(tmp)/'never-created'))
        negative('empty-ledger',lambda:validate_spec({'format':'p26-copy-ledger-v1','source_sha256':S,'history_roots_explicit':True,'members':[]}))
        negative('missing-final-contracts',lambda:validate_spec({'format':'p26-copy-ledger-v1','source_sha256':S,'members':[m]}))
    for name,ok in checks:print(('PASS ' if ok else 'FAIL ')+name)
    print(json.dumps({'controls':len(checks),'failed':sum(not ok for _,ok in checks)}));return int(any(not ok for _,ok in checks))

if __name__=='__main__':
    ap=argparse.ArgumentParser();sub=ap.add_subparsers(dest='command',required=True);sub.add_parser('selftest')
    for command in ('plan','assemble'):
        p=sub.add_parser(command);p.add_argument('ledger');p.add_argument('--workspace',required=True);p.add_argument('--root-review',required=True)
        if command=='assemble':p.add_argument('--destination',required=True);p.add_argument('--root-phase',required=True)
    args=ap.parse_args()
    if args.command=='selftest':raise SystemExit(selftest())
    try:
        spec=json.load(open(args.ledger));origins={'workspace':args.workspace,'root-review':args.root_review}
        out=plan(spec,origins) if args.command=='plan' else assemble(spec,origins,args.destination,json.load(open(args.root_phase)))
        print(json.dumps(out,indent=2))
    except (ValueError,OSError,KeyError,TypeError) as e:print(json.dumps({'status':'NOTREADY','problem':str(e)}));raise SystemExit(1)
