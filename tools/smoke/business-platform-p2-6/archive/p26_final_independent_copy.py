"""Root-pinned bounded copy and existing full-package offline probe.

No fixture backend is run here. Private input paths are SBPL deny literals;
their contents and filesystem metadata are never inspected by this producer.
"""
import argparse
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys

S = 'ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
PYTHON = '/opt/homebrew/Cellar/python@3.14/3.14.7/Frameworks/Python.framework/Versions/3.14/Resources/Python.app/Contents/MacOS/Python'
WORKSPACE = '/Volumes/ExternalSD/projects/OpenFoundry'
HANDOFF = '/Users/allen/.codex/visualizations/2026/11/13/01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs'
SCHEMA = 'p26-final-independent-copy-release-v1'


def need(ok):
    if not ok:
        raise ValueError('independent package copy refused')


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()


def pinned(path, ref):
    path = canonical(path)
    need(path.is_file() and path.stat().st_size == ref['size'] and sha(path) == ref['sha256'])
    return path


def canonical(path):
    path = Path(path)
    need(path.is_absolute() and '\n' not in str(path) and '\x00' not in str(path)
         and path == Path(os.path.abspath(path)))
    need(not any(p.is_symlink() for p in (path, *path.parents)))
    return path


def profile(config):
    roots = config['deny_roots']
    need(WORKSPACE in roots and HANDOFF in roots and '/Users/allen/.ssh' in roots
         and '/Users/allen/.aws' in roots and len(set(roots)) == len(roots))
    private = config['private_input_paths']
    need(private and len(set(private)) == len(private))
    for path in [*roots, *private]:
        # Lexical checks only: never stat/read private input files.
        need(isinstance(path, str) and path.startswith('/') and '\n' not in path
             and '\x00' not in path and os.path.normpath(path) == path)
    for forbidden in [*roots, *private]:
        need(not Path(config['package_copy']).is_relative_to(forbidden))
    q = json.dumps
    lines = ['(version 1)', '(allow default)']
    lines += ['(deny file-read* (subpath ' + q(path) + '))' for path in roots]
    lines += ['(deny file-read* (literal ' + q(path) + '))' for path in private]
    lines += ['(deny network*)', '(deny process-fork)',
              '(deny process-exec (require-not (literal ' + q(PYTHON) + ')))',
              '(deny file-write*)']
    return '\n'.join(lines) + '\n'


def registration_review20(config):
    # Validate public metadata only; private input paths remain SBPL deny literals.
    def need(ok, message='public registration review refused'):
        if not ok: raise ValueError(message)
    need(config.get('contract_version') == 'p26-scan1-payload-members-v2-private20-publicfixture2'
         and config.get('canonical_root_label') == WORKSPACE)
    metadata_cache = {}
    def read(ref):
        need(isinstance(ref,dict) and set(ref)=={'path','sha256','size'}
             and isinstance(ref['path'],str) and isinstance(ref['sha256'],str)
             and re.fullmatch('[a-f0-9]{64}',ref['sha256']) and type(ref['size']) is int)
        rel=ref['path'];rel=rel[len(WORKSPACE)+1:] if rel.startswith(WORKSPACE+'/') else rel
        need(not rel.startswith('/') and '\\' not in rel and all(v not in ('','.','..') for v in rel.split('/'))
             and not any(v.lower().startswith('.env') or v.lower().endswith(('.env','.body')) or '.env.' in v.lower() for v in rel.split('/')))
        need(rel.startswith('tools/smoke/business-platform-p2-6/results/')
             or rel=='tools/smoke/business-platform-p2-6/w6/selftest_p26_pg_runner.py')
        if rel not in metadata_cache:
            metadata_cache[rel]=canonical(Path(WORKSPACE)/rel).read_bytes()
        raw=metadata_cache[rel]
        need(len(raw)==ref['size'] and hashlib.sha256(raw).hexdigest()==ref['sha256'])
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
    original, revised, review = validate_publicfixture_review(read, config['publicfixture_registration_review'], S)
    need(config['registered_inputs_metadata'] == review['revised_private_registry20'])
    reg = json.loads(read(config['registered_inputs_metadata']))
    need(reg['registered_inputs'] == revised)
    need(config['private_input_paths'] == sorted({e['path'] for e in original}))
    return original, revised


def prepare(release):
    need(release['schema'] == SCHEMA and release['phase'] == 'p26-full-independent-copy-offline'
         and release['root_accepted'] is True and release['source_sha256'] == S)
    expiry = datetime.datetime.fromisoformat(release['expires_utc'].replace('Z', '+00:00'))
    need(expiry.tzinfo is not None and datetime.datetime.now(datetime.timezone.utc) < expiry)
    need(release['tool_sha256'] == sha(__file__))
    cfgref = release['config']; cfgpath = pinned(cfgref['path'], cfgref)
    config = json.loads(cfgpath.read_bytes())
    need(config['schema'] == 'p26-full-independent-copy-config-v1'
         and config['source_sha256'] == S and config['python_realpath'] == PYTHON
         and config['timeout_s'] == 1200)
    source = canonical(config['source_package']); base = canonical(config['exclusive_base'])
    copy = canonical(config['package_copy']); capture = canonical(config['capture_dir'])
    profilepath = canonical(config['profile_path'])
    need(re.fullmatch(re.escape(WORKSPACE) + r'/tools/smoke/business-platform-p2-6/results/w9/final-official-package-attempt[1-9][0-9]*/package', str(source))
         and re.fullmatch(r'/private/tmp/openfoundry-p26-final-independent-copy-attempt[1-9][0-9]*', str(base))
         and copy == base / 'package' and capture == base / 'capture'
         and profilepath == base / 'offline.sb' and not base.exists())
    original22, entries = registration_review20(config)
    probe = json.loads(pinned(config['probe_config_source']['path'], config['probe_config_source']).read_bytes())
    need(probe['package_copy'] == str(copy) and probe['canonical_root_label'] == WORKSPACE
         and config['probe_config_relative'] == 'offline-probe-config.json'
         and len(probe['deny_sentinels']) >= 2)
    for entry in probe['deny_sentinels']:
        sentinel = canonical(entry['path'])
        need(sentinel.name == '.p26-offline-read-deny-sentinel'
             and entry['existed_before_isolation'] is True
             and stat.S_ISREG(sentinel.stat().st_mode))
    need(any(Path(e['path']).is_relative_to(WORKSPACE) for e in probe['deny_sentinels'])
         and any(not Path(e['path']).is_relative_to(WORKSPACE) for e in probe['deny_sentinels']))
    for entry in probe['deny_sentinels']:
        need(any(Path(entry['path']).is_relative_to(root) for root in config['deny_roots']))
    support = Path(__file__).with_name('p26_archive_manifest.py')
    need(release['support_tool_shas'] == {'p26_archive_manifest.py': sha(support)})
    spec = importlib.util.spec_from_file_location('p26_copy_manifest', support)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    manifest = pinned(source / 'manifest.json', config['source_manifest'])
    before = module.verify_manifest(str(source), str(manifest))
    need(before['manifest_sha256'] == release['manifest_sha256'] == config['manifest_sha256']
         and before['file_count'] == release['file_count'] == config['file_count'])
    packaged_probe = source / config['probe_config_relative']
    need(packaged_probe.read_bytes() == Path(config['probe_config_source']['path']).read_bytes())
    return config, module, before, profile(config)


def run(release_file):
    release_file = canonical(release_file); raw = release_file.read_bytes()
    release = json.loads(raw)
    config, manifest, before, sbpl = prepare(release)
    source = Path(config['source_package']); base = Path(config['exclusive_base'])
    copy = Path(config['package_copy']); capture = Path(config['capture_dir'])
    base.mkdir(); copy.mkdir(); capture.mkdir()
    (capture / 'Root-release.snapshot.json').write_bytes(raw)
    (capture / 'config.snapshot.json').write_bytes(pinned(release['config']['path'], release['config']).read_bytes())
    (capture / 'producer-tool.snapshot.py').write_bytes(Path(__file__).read_bytes())
    rc = 1
    result = {'schema': 'p26-full-independent-offline-result-v1', 'ok': False,
              'source_sha256': S, 'manifest': before, 'fixture_backend_executed': False,
              'private_input_files_read_or_stat': False}
    try:
        for rel in manifest.walk_package(str(source)) + ['manifest.json']:
            need(release_file.read_bytes() == raw)
            src = source / rel; target = copy / rel
            need(not src.is_symlink() and src.is_file())
            target.parent.mkdir(parents=True, exist_ok=True)
            with src.open('rb') as inp, target.open('xb') as out:
                shutil.copyfileobj(inp, out, length=1048576)
        after = manifest.verify_manifest(str(copy), str(copy / 'manifest.json'))
        need(after == before and sha(copy / 'manifest.json') == config['source_manifest']['sha256'])
        profilepath = Path(config['profile_path']); profilepath.write_text(sbpl)
        argv = ['/usr/bin/sandbox-exec', '-f', str(profilepath), PYTHON, '-I', '-B',
                str(copy / 'tools/p26_offline_probe.py'), '--config',
                str(copy / config['probe_config_relative'])]
        (capture / 'command.json').write_text(json.dumps(argv, indent=2) + '\n')
        env = {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin', 'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONNOUSERSITE': '1'}
        need(release_file.read_bytes() == raw and sha(__file__) == release['tool_sha256'])
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        child = subprocess.run(argv, cwd=copy, env=env, close_fds=True,
                               capture_output=True, timeout=config['timeout_s'])
        ended = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (capture / 'stdout.raw').write_bytes(child.stdout)
        (capture / 'stderr.raw').write_bytes(child.stderr)
        (capture / 'original-child.rc').write_text(str(child.returncode) + '\n')
        (capture / 'child-meta.json').write_text(json.dumps({'started_utc': started, 'ended_utc': ended,
            'original_child_rc': child.returncode, 'cwd': str(copy), 'env_keys': sorted(env),
            'close_fds': True, 'timeout_s': config['timeout_s']}, indent=2) + '\n')
        need(child.returncode == 0)
        observed = json.loads(child.stdout)
        need(observed['status'] == 'ISOLATED-INDEPENDENT-COPY-VERIFIED-NOT-SEALED'
             and observed['source_sha256'] == S
             and observed['manifest']['manifest_sha256'] == before['manifest_sha256'])
        need(manifest.verify_manifest(str(source), str(source / 'manifest.json')) == before
             and manifest.verify_manifest(str(copy), str(copy / 'manifest.json')) == before)
        need(release_file.read_bytes() == raw and sha(__file__) == release['tool_sha256'])
        rc = 0
        result.update(ok=True, official_and_independent_manifest_equal=True, original_child_rc=0,
                      configured_deny_roots=config['deny_roots'],
                      configured_private_input_deny_literals=config['private_input_paths'],
                      registered_input_count=20,
                      publicfixture_registration_review=config['publicfixture_registration_review'],
                      original_registered_input_deny_count=22,
                      private_file_deny_literal_count=len(config['private_input_paths']),
                      actual_probes=observed['deny_probes'],
                      individual_private_input_read_probes_claimed=False,
                      profile_sha256=sha(profilepath))
    except subprocess.TimeoutExpired as exc:
        (capture / 'stdout.raw').write_bytes(exc.stdout or b'')
        (capture / 'stderr.raw').write_bytes(exc.stderr or b'')
        (capture / 'original-child.rc').write_text('unavailable\n')
        result.update(failure_kind='timeout', original_child_rc=None)
    except BaseException:
        result['failure_kind'] = 'copy-or-isolated-verification-refused'
    (capture / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    (capture / 'producer.rc').write_text(str(rc) + '\n')
    return rc


def main():
    parser = argparse.ArgumentParser(); parser.add_argument('--release-file', required=True)
    args = parser.parse_args()
    try:
        return run(args.release_file)
    except BaseException:
        print('independent copy refused; partial artifacts preserved', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.dont_write_bytecode = True
    sys.exit(main())
