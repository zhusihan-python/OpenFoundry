"""Read original P26 evidence only; no runners, subprocesses, services or credentials."""
import hashlib
import json
import os
import re
import stat

class ProofError(ValueError):
    pass

class Reader:
    def __init__(self, root):
        self.root = os.path.abspath(root)
        self.files = {}

    def path(self, rel):
        if not isinstance(rel, str) or rel.startswith('/') or '\\' in rel or any(x in ('', '.', '..') for x in rel.split('/')):
            raise ProofError('nonlocal proof path')
        p = self.root
        for part in rel.split('/'):
            p = os.path.join(p, part)
            if os.path.islink(p):
                raise ProofError('symlink proof path')
        return p

    def read(self, rel):
        p = self.path(rel)
        if not stat.S_ISREG(os.stat(p).st_mode):
            raise ProofError('nonregular proof file')
        data = open(p, 'rb').read()
        self.files[rel] = {'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data)}
        return data

    def member(self, rel):
        # Copied package: exact package-relative scan payload bytes.
        return self.read(rel)

    def text(self, rel):
        return self.read(rel).decode('utf-8')

    def doc(self, rel):
        return json.loads(self.text(rel))

    def rc(self, rel, expected=0):
        text = self.text(rel).strip()
        require(bool(re.fullmatch(r'[0-9]+', text)) and int(text) == expected, 'original rc mismatch: ' + rel)
        return int(text)

    def step(self, base, expected=0):
        self.rc(base + '.rc', expected)
        cmd = self.text(base + '.cmd')
        raw = self.text(base + '.raw')
        meta = self.doc(base + '.meta')
        require(meta.get('rc') == expected and bool(cmd.strip()) and meta.get('cmd') in cmd, 'original cmd/meta mismatch: ' + base)
        return raw, meta

    def unique(self, directory, prefix, suffix):
        p = self.path(directory)
        names = [x for x in os.listdir(p) if x.startswith(prefix) and x.endswith(suffix) and not (suffix == '.rc' and x.endswith('.gate.rc'))]
        require(len(names) == 1, 'missing/duplicate original step: ' + prefix)
        return directory + '/' + names[0][:-len(suffix)]


def require(condition, reason):
    if not condition:
        raise ProofError(reason)


def pg(r, proof, source):
    outer, inner = proof['outer'], proof['inner']
    r.rc(outer + '/outer.rc')
    cmd = r.text(outer + '/outer.cmd')
    stdout = r.text(outer + '/outer.stdout')
    r.text(outer + '/outer.stderr')
    result = r.doc(outer + '/RESULT.json')
    require(source in cmd and '--execute' in cmd and result.get('frozen_S') == source, 'PG outer source/execute')
    require(os.path.normpath(os.path.join(outer, result['inner_evidence_dir'])) == inner, 'PG outer/inner binding')
    require(inner.split('/')[-1] in stdout and 'original rc=0 wrapper rc=0' in stdout, 'PG outer stdout binding')
    b = inner + '/01-go-test'
    for suffix in ('.rc', '.wrapper.rc', '.source-gate.rc', '.version-gate.rc', '.names-gate.rc'):
        r.rc(b + suffix)
    r.rc(inner + '/00-precheck.rc'); r.rc(inner + '/00-precheck.gate.rc')
    for suffix in ('.cmd', '.raw', '.meta', '.gate.raw'):
        r.read(inner + '/00-precheck' + suffix)
    raw = r.text(b + '.raw'); cmd = r.text(b + '.cmd'); meta = r.doc(b + '.meta'); gates = r.doc(b + '.gates.json')
    names = proof['required_names']
    require(bool(names) and len(names) == len(set(names)), 'PG required names empty/duplicate')
    require(set(meta['required_names']) == set(names) == set(gates['names_gate']['per_name']), 'PG required-name set mismatch')
    for n in names:
        counts = [len(re.findall(pattern + re.escape(n) + r'(?:\s|$)', raw, re.M)) for pattern in (r'^=== RUN\s+', r'^--- PASS:\s+', r'^--- SKIP:\s+', r'^--- FAIL:\s+')]
        require(counts == [1, 1, 0, 0], 'PG original RUN/PASS/SKIP/FAIL mismatch: ' + n)
    require(not re.search(r'^--- (?:FAIL|SKIP):|^FAIL(?:\s|$)', raw, re.M), 'PG raw FAIL/SKIP')
    require(raw.splitlines()[0].startswith('go version ' + proof['expected_go'] + ' '), 'PG original Go version')
    require(meta.get('expected_go') == proof['expected_go'] and meta.get('cmd') == cmd.strip(), 'PG command/version meta binding')
    require('-count=1' in cmd and '-tags=integration' in cmd and '-v' in cmd and all(n in cmd for n in names), 'PG original test command')
    for k in ('source_before', 'source_after'):
        require(meta.get(k) == source, 'PG original source changed')
    require(gates['source_gate']['before'] == gates['source_gate']['after'] == source, 'PG source gate changed')
    require(gates.get('original_rc') == gates.get('wrapper_rc') == meta.get('rc') == meta.get('wrapper_rc') == 0, 'PG original rc/meta mismatch')
    require(all(gates.get(k) is True for k in ('expect_pin_ok','version_ok')) and gates['source_gate']['ok'] is True and gates['names_gate']['ok'] is True, 'PG gates false')
    return {'kind': 'pg', 'names': len(names), 'original_rc': 0, 'wrapper_rc': 0}


def unit(r, proof, source):
    d = proof['directory']
    for stem in ('00-verify-S-before', '01-env-check', '02-unit-dataset', '03-unit-pipeline', '04-unit-connector', '05-verify-S-after'):
        r.rc(d + '/' + stem + '.rc'); require(bool(r.text(d + '/' + stem + '.cmd').strip()), 'unit empty original command')
        r.text(d + '/' + stem + '.stderr')
    for stem in ('00-verify-S-before','05-verify-S-after'):
        raw = r.text(d + '/' + stem + '.stdout')
        require('CANDIDATE_SOURCE_SHA256=' + source in raw and 'CANDIDATE_VERIFY=OK' in raw, 'unit source before/after mismatch')
    r.text(d + '/01-env-check.stdout')
    for stem, svc, count, go in (('02-unit-dataset','dataset-versioning-service',8,'go1.26.3'),('03-unit-pipeline','pipeline-build-service',15,'go1.26.3'),('04-unit-connector','connector-management-service',39,'go1.25.14')):
        raw = r.text(d + '/' + stem + '.stdout'); cmd = r.text(d + '/' + stem + '.cmd')
        require(raw.splitlines()[0].startswith('go version ' + go + ' '), 'unit Go version mismatch')
        ok = re.findall(r'^ok\s+(\S+)', raw, re.M)
        require(len(ok) == len(set(ok)) == count and all('/services/' + svc + '/' in p for p in ok), 'unit package set mismatch')
        require(not re.search(r'^FAIL(?:\s|$)', raw, re.M) and 'go test' in cmd and './services/' + svc + '/...' in cmd, 'unit test scope/failure')
    return {'kind': 'unit', 'original_six_rc': [0]*6, 'packages': [8,15,39], 'optional': 'not verified'}


def build(r, proof, source):
    d, resume = proof['original'], proof['resume']
    result = r.doc(resume + '/RESUME-RESULT.json')
    outer = proof['original_outer']
    r.rc(outer + '/outer.rc', 2)
    r.text(outer + '/outer.stdout'); r.text(outer + '/outer.cmd'); r.text(outer + '/outer.stderr')
    original_result = r.doc(outer + '/RESULT.json')
    require(os.path.normpath(os.path.join(outer, original_result['inner_evidence_dir'])) == d and original_result['outer_rc'] == 2, 'build original outer/inner binding')
    r.rc(proof['resume_outer'] + '/outer.rc')
    require(resume.split('/')[-1] in r.text(proof['resume_outer'] + '/outer.stdout'), 'build resume outer/inner binding')
    r.text(proof['resume_outer'] + '/outer.cmd'); r.text(proof['resume_outer'] + '/outer.stderr')
    require(result['frozen_S'] == source, 'build source mismatch')
    for prefix in ('04-compile','05-create-base','06-cp','07-commit'):
        base = r.unique(d, prefix + '-', '.rc'); raw, meta = r.step(base)
        require(result['original_stage0_links'][prefix]['cmd_sha256'] == r.files[base + '.cmd']['sha256'], 'build original command hash mismatch')
        if prefix == '04-compile':
            require(result['binary_sha256'] in raw and meta.get('expect_source') == source and 'go build' in meta['cmd'], 'build original binary/source binding')
        if prefix == '07-commit':
            require(result['committed_full_id'] in raw, 'build original image binding')
    raw, meta = r.step(r.unique(d, '08-verify-new-tag-', '.rc'), 1)
    require('template' in raw, 'build initial failure not preserved')
    binary = r.doc(d + '/BINARY.json')
    require(binary['sha256'] == result['binary_sha256'], 'build binary hash mismatch')
    require(hashlib.sha256(r.read(d + '/out-b94e6b2f/service')).hexdigest() == binary['sha256'], 'build actual binary bytes mismatch')
    raws = {}
    for prefix in ('00-source-before','01-candidate-exact','02-old-runtime-exact','04-cleanup-precheck','05-rm-exact-cid','06-source-post'):
        raws[prefix], _ = r.step(r.unique(resume, prefix + '-', '.rc'))
    for prefix in ('00-source-before','06-source-post'):
        require(source in raws[prefix], 'build resume source original mismatch')
    require(result['committed_full_id'] in raws['01-candidate-exact'] and result['safe_config'] in raws['01-candidate-exact'], 'build candidate config mismatch')
    require(result['build_cid_cleaned'] in r.text(r.unique(resume, '05-rm-exact-cid-', '.rc') + '.cmd'), 'build cleanup CID mismatch')
    r.rc(r.unique(resume, '04-cleanup-precheck-', '.gate.rc') + '.gate.rc')
    require(result['ok'] is True and all(v is True for v in result['gates'].values()), 'build resume gates false')
    require(proof['image'] == result['committed_full_id'] and proof['binary_sha256'] == result['binary_sha256'], 'build Root candidate binding mismatch')
    return {'kind': 'build-composite', 'initial_verify_rc': 1, 'initial_outer_rc': 2, 'resume_rc': 0, 'binary_sha256': binary['sha256']}


def validate(root, proof, source, reader=None):
    r = reader if reader is not None else Reader(root)
    kind = proof.get('kind')
    fn = {'pg': pg, 'unit': unit, 'build-composite': build}.get(kind)
    if fn is None and kind in ('http-first3','h12','deployment-slice','w8-oldpre'):
        import importlib.util
        spec = importlib.util.spec_from_file_location('p26_live_proofs', os.path.join(os.path.dirname(__file__), 'p26_live_proofs.py'))
        live = importlib.util.module_from_spec(spec);spec.loader.exec_module(live)
        fn = {'http-first3':live.http_first3,'h12':live.h12,'deployment-slice':live.deployment,'w8-oldpre':live.oldpre}[kind]
    if fn is None and kind in ('http-case','http10'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('http10', os.path.join(os.path.dirname(__file__),'p26_http10_proofs.py'))
        http10=importlib.util.module_from_spec(spec);spec.loader.exec_module(http10)
        fn={'http-case':http10.case,'http10':http10.suite}[kind]
    if fn is None and kind in ('legacy-p1-slice','legacy-actual-stage'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('legacy',os.path.join(os.path.dirname(__file__),'p26_legacy_proofs.py'));legacy=importlib.util.module_from_spec(spec);spec.loader.exec_module(legacy);fn=legacy.p1_slice if kind=='legacy-p1-slice' else legacy.actual_stage
    if fn is None and kind in ('s2-pg','s2-unit','s2-build-original'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('s2',os.path.join(os.path.dirname(__file__),'p26_s2_proofs.py'));s2=importlib.util.module_from_spec(spec);spec.loader.exec_module(s2);fn={'s2-pg':s2.pg,'s2-unit':s2.unit,'s2-build-original':s2.build}[kind]
    if fn is None and kind=='s2-runtime-upgrade':
        import importlib.util
        spec=importlib.util.spec_from_file_location('runtime',os.path.join(os.path.dirname(__file__),'p26_runtime_proofs.py'));runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime);fn=runtime.upgrade
    if fn is None and kind in ('final-restart','final-receipt','final-cleanup','final-typed-whole','final-actual-secret','final-old-artifacts','final-restart-receipt','final-quiesce'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('final',os.path.join(os.path.dirname(__file__),'p26_final_proofs.py'));final=importlib.util.module_from_spec(spec);spec.loader.exec_module(final)
        fn={'final-restart':final.restart,'final-receipt':final.receipt,'final-cleanup':final.cleanup,'final-typed-whole':final.typed_whole,'final-actual-secret':final.secret,'final-old-artifacts':final.old_artifacts,'final-restart-receipt':final.restart_receipt,'final-quiesce':final.quiesce}[kind]
    if fn is None and kind=='s2-runtime-aggregate':
        import importlib.util
        spec=importlib.util.spec_from_file_location('runtimeaggregate',os.path.join(os.path.dirname(__file__),'p26_runtime_aggregate.py'));rt=importlib.util.module_from_spec(spec);spec.loader.exec_module(rt);fn=rt.aggregate
    if fn is None and kind in ('p25-failed-h9-history','p25-h9-current-quiet-only'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('h9history',os.path.join(os.path.dirname(__file__),'p26_h9_history_proofs.py'));h9=importlib.util.module_from_spec(spec);spec.loader.exec_module(h9);fn=h9.failed if kind=='p25-failed-h9-history' else h9.current
    if fn is None and kind in ('p25-h13-ledger-component','p25-global-quiet-component'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('supplement',os.path.join(os.path.dirname(__file__),'p26_p25_supplement_readers.py'));supp=importlib.util.module_from_spec(spec);spec.loader.exec_module(supp);fn=supp.ledger if kind=='p25-h13-ledger-component' else supp.quiet
    if fn is None and kind in ('p25-h10-natural-freeze-component','p25-h10-actual-complete'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('h10freeze',os.path.join(os.path.dirname(__file__),'p26_h10_freeze_reader.py'));hf=importlib.util.module_from_spec(spec);spec.loader.exec_module(hf);fn=hf.freeze if kind=='p25-h10-natural-freeze-component' else hf.h10_complete
    if fn is None and kind in ('final-owned-state-actual-component','final-substrate-counter-actual-component'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('readonlyactual',os.path.join(os.path.dirname(__file__),'p26_final_readonly_readers.py'));fr=importlib.util.module_from_spec(spec);spec.loader.exec_module(fr);fn=fr.owned if kind=='final-owned-state-actual-component' else fr.substrate
    if fn is None and kind in ('final-legacy-regressions','final-db-preservation-abc','final-cleanup-complete'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('composites',os.path.join(os.path.dirname(__file__),'p26_final_composites.py'));fc=importlib.util.module_from_spec(spec);spec.loader.exec_module(fc);fn={'final-legacy-regressions':fc.legacy,'final-db-preservation-abc':fc.final_state,'final-cleanup-complete':fc.cleanup}[kind]
    if fn is None and kind=='final-whole-failed-history':
        import importlib.util
        spec=importlib.util.spec_from_file_location('historyreview',os.path.join(os.path.dirname(__file__),'p26_final_history_review.py'));fh=importlib.util.module_from_spec(spec);spec.loader.exec_module(fh);fn=fh.validate
    if fn is None and kind in ('final-old-artifacts-actual-component','final-receipt-post-actual-component'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('artifactreceipt',os.path.join(os.path.dirname(__file__),'p26_final_artifact_receipt_reader.py'));ar=importlib.util.module_from_spec(spec);spec.loader.exec_module(ar);fn=ar.old_artifacts if kind=='final-old-artifacts-actual-component' else ar.receipt_post
    if fn is None and kind in ('final-BC-typed-snapshot-component','final-H-typed-actual-component'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('bchactual',os.path.join(os.path.dirname(__file__),'p26_final_bc_h_reader.py'));bh=importlib.util.module_from_spec(spec);spec.loader.exec_module(bh);fn=bh.bc if kind=='final-BC-typed-snapshot-component' else bh.whole
    if fn is None and kind=='final-ABC3-actual-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('abcfinal',os.path.join(os.path.dirname(__file__),'p26_final_abc_reader.py'));af=importlib.util.module_from_spec(spec);spec.loader.exec_module(af);fn=af.abc
    if fn is None and kind=='final-cleanup-actual-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('cleanupactual',os.path.join(os.path.dirname(__file__),'p26_final_cleanup_reader.py'));ca=importlib.util.module_from_spec(spec);spec.loader.exec_module(ca);fn=ca.cleanup
    if fn is None and kind=='final-P1post9-actual-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('finalp1',os.path.join(os.path.dirname(__file__),'p26_final_p1_reader.py'));fp=importlib.util.module_from_spec(spec);spec.loader.exec_module(fp);fn=fp.post
    if fn is None and kind=='final-writer-auth-metadata-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('authmetadata',os.path.join(os.path.dirname(__file__),'p26_final_actual_readers.py'));fa=importlib.util.module_from_spec(spec);spec.loader.exec_module(fa);fn=fa.auth_metadata
    if fn is None and kind=='final-main3-restart-actual-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('restartactual',os.path.join(os.path.dirname(__file__),'p26_final_restart_reader.py'));fr=importlib.util.module_from_spec(spec);spec.loader.exec_module(fr);fn=fr.restart
    if fn is None and kind=='final-receipt-baseline-actual-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('baselineactual',os.path.join(os.path.dirname(__file__),'p26_final_actual_readers.py'));fa=importlib.util.module_from_spec(spec);spec.loader.exec_module(fa);fn=fa.receipt_baseline
    if fn is None and kind=='final-quiesce-actual-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('finalactual',os.path.join(os.path.dirname(__file__),'p26_final_actual_readers.py'));fa=importlib.util.module_from_spec(spec);spec.loader.exec_module(fa);fn=fa.quiesce
    if fn is None and kind=='p25-h14-independent-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('h14independent',os.path.join(os.path.dirname(__file__),'p26_h14_independent_reader.py'));hi=importlib.util.module_from_spec(spec);spec.loader.exec_module(hi);fn=hi.independent
    if fn is None and kind=='p25-tail-actual-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('tailactual',os.path.join(os.path.dirname(__file__),'p26_p25_tail_reader.py'));ta=importlib.util.module_from_spec(spec);spec.loader.exec_module(ta);fn=ta.tail
    if fn is None and kind=='p25-h10-actual-crash-component':
        import importlib.util
        spec=importlib.util.spec_from_file_location('h10actual',os.path.join(os.path.dirname(__file__),'p26_h10_actual_reader.py'));ha=importlib.util.module_from_spec(spec);spec.loader.exec_module(ha);fn=ha.actual
    if fn is None and kind=='p25-h10-current-readonly-only':
        import importlib.util
        spec=importlib.util.spec_from_file_location('h10current',os.path.join(os.path.dirname(__file__),'p26_h10_current_reader.py'));hc=importlib.util.module_from_spec(spec);spec.loader.exec_module(hc);fn=hc.current
    if fn is None and kind=='p25-failed-h10-history':
        import importlib.util
        spec=importlib.util.spec_from_file_location('failedh10',os.path.join(os.path.dirname(__file__),'p26_failed_h10_history.py'));fh=importlib.util.module_from_spec(spec);spec.loader.exec_module(fh);fn=fh.history
    if fn is None and kind=='p25-healthy-global':
        import importlib.util
        spec=importlib.util.spec_from_file_location('healthyglobal',os.path.join(os.path.dirname(__file__),'p26_healthy_global_reader.py'));hg=importlib.util.module_from_spec(spec);spec.loader.exec_module(hg);fn=hg.compile_bundle
    if fn is None and kind in ('p25-actual-full','p25-actual-healthy-component'):
        import importlib.util
        spec=importlib.util.spec_from_file_location('p25original',os.path.join(os.path.dirname(__file__),'p26_p25_proofs.py'));p25=importlib.util.module_from_spec(spec);spec.loader.exec_module(p25);fn=p25.full if kind=='p25-actual-full' else p25.healthy_one
    require(fn is not None, 'unsupported original semantic proof kind; NOTREADY: ' + str(kind))
    try:
        out = fn(r, proof, source)
    except (KeyError, TypeError, OSError, UnicodeError, ValueError) as e:
        raise ProofError(str(e)) from e
    if 'original_files' in proof:
        require(proof['original_files'] == r.files, 'original artifact exact set/hash/size differs from Root contract')
    return out, r.files


def selftest():
    import tempfile
    from pathlib import Path
    source = 'b' * 64
    proof = {'kind':'pg', 'outer':'outer', 'inner':'inner', 'required_names':['TestRequired'], 'expected_go':'go1.26.3'}
    cmd = 'go test -tags=integration -count=1 -v -run TestRequired'
    meta = {'required_names':['TestRequired'],'cmd':cmd,'expected_go':'go1.26.3','source_before':source,'source_after':source,'rc':0,'wrapper_rc':0}
    gates = {'names_gate':{'per_name':{'TestRequired':{}},'ok':True},'source_gate':{'before':source,'after':source,'ok':True},'expect_pin_ok':True,'version_ok':True,'original_rc':0,'wrapper_rc':0}
    files = {'outer/outer.rc':'0','outer/outer.cmd':source+' --execute','outer/outer.stdout':'inner original rc=0 wrapper rc=0','outer/outer.stderr':'','outer/RESULT.json':json.dumps({'frozen_S':source,'inner_evidence_dir':'../inner'}), 'inner/01-go-test.cmd':cmd,'inner/01-go-test.raw':'go version go1.26.3 linux/arm64\n=== RUN   TestRequired\n--- PASS: TestRequired (0.01s)\nPASS\n','inner/01-go-test.meta':json.dumps(meta),'inner/01-go-test.gates.json':json.dumps(gates)}
    for suffix in ('.rc','.wrapper.rc','.source-gate.rc','.version-gate.rc','.names-gate.rc'): files['inner/01-go-test'+suffix]='0'
    for suffix in ('.rc','.gate.rc','.cmd','.raw','.meta','.gate.raw'): files['inner/00-precheck'+suffix]='0'
    controls=[]
    def run(name, changes, good=False):
        with tempfile.TemporaryDirectory() as tmp:
            f=dict(files);f.update(changes)
            for rel,data in f.items():
                if data is None: continue
                p=Path(tmp,rel);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(data)
            try: validate(tmp,proof,source);ok=True
            except (ValueError,OSError):ok=False
            controls.append((name,ok==good))
    run('minimal-PG-mechanics',{},True)
    run('missing-raw',{'inner/01-go-test.raw':None})
    run('lying-summary-original-rc',{'inner/01-go-test.rc':'1'})
    run('missing-required-test',{'inner/01-go-test.raw':'go version go1.26.3 linux/arm64\nPASS\n'})
    run('skip-despite-green-summary',{'inner/01-go-test.raw':files['inner/01-go-test.raw'].replace('--- PASS:','--- SKIP:')})
    run('duplicate-required-run',{'inner/01-go-test.raw':files['inner/01-go-test.raw']+'=== RUN   TestRequired\n'})
    run('wrong-source',{'inner/01-go-test.meta':json.dumps(dict(meta,source_after='c'*64))})
    run('wrong-go',{'inner/01-go-test.raw':files['inner/01-go-test.raw'].replace('go1.26.3','go1.25.14')})
    for name,ok in controls: print(f"[selftest:original] {'PASS' if ok else 'FAIL'} {name}")
    print(f'[selftest:original] controls={len(controls)} failed={sum(not ok for _,ok in controls)}')
    return int(any(not ok for _,ok in controls))

if __name__ == '__main__':
    import sys
    if sys.argv[1:] != ['selftest']:
        raise SystemExit('only selftest; imported by verifier/crosscheck')
    raise SystemExit(selftest())
