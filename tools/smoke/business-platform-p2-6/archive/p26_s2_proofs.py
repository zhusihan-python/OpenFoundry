"""S2 original PG/unit/build readers; prior S evidence remains historical only."""
import hashlib
import os
import re
from pathlib import Path


def need(ok,msg):
    if not ok:raise ValueError(msg)


def tools(r,p):
    manifest=r.doc(p['tools_manifest'])
    for entry in manifest['files']:
        label=entry['origin'].split('/tools/smoke/business-platform-p2-6/',1)[1]
        need(label in p['historical_tools'] and hashlib.sha256(r.read(p['historical_tools'][label])).hexdigest()==entry['sha256'],'S2 historical tool byte hash')
    if p.get('new_runner'):
        need(hashlib.sha256(r.read(p['new_runner'])).hexdigest()=='d39ec1a8397a406a385ca56af8c0392b158addb54784b2700c5ce128de4d7655','S2 full Connector new runner blob')
    else:
        key='pg/p26_pg_runner.py';need(hashlib.sha256(r.read(p['historical_tools'][key])).hexdigest()=='017d3305386b40bcb6d81f6f2e391be5b9ffec73669e04e219e7fba94b23137d','S2 original runner distinction')


def chain(r,p,source):
    need(p['epoch']==source,'S2 proof epoch mismatch')
    expected_rcs=17 if p['kind']=='s2-build-original' else (5 if p['kind']=='s2-unit' else (10 if p['step'].startswith('unified114-') else 7))
    need(p['rc_count']==expected_rcs,'S2 mandatory exact original rc scope')
    r.rc(p['outer']+'/rc');cmd=r.doc(p['outer']+'/cmd-before.json');raw=r.text(p['outer']+'/stdout')
    r.read(p['outer']+'/raw');r.read(p['outer']+'/stderr');r.doc(p['outer']+'/safe-env-metadata.json')
    need(source in cmd and '--execute' in cmd and (p['inner'].split('/')[-1] in raw or (p['kind']=='s2-build-original' and p['inner'].split('_')[-1] in raw)),'S2 original outer/source/inner binding')
    need(p['step']==Path(p['outer']).name,'S2 original exact chain identity')
    d=p['inner'];names=sorted(os.listdir(r.path(d)))
    need(len([n for n in names if n.endswith('.rc')])==p['rc_count'],'S2 exact original rc set count')
    for n in names:
        if n.endswith('.rc'):r.rc(d+'/'+n)
        elif n.endswith(('.raw','.meta','.cmd','.json')):r.read(d+'/'+n)
    tools(r,p)


def pg(r,p,source):
    chain(r,p,source);b=p['inner']+'/01-go-test';meta=r.doc(b+'.meta');gates=r.doc(b+'.gates.json');raw=r.text(b+'.raw');cmd=r.text(b+'.cmd')
    need(meta['source_before']==meta['source_after']==source==gates['source_gate']['before']==gates['source_gate']['after'],'S2 PG source before/after')
    need(raw.splitlines()[0].startswith('go version '+p['go']+' ') and meta['expected_go']==p['go'],'S2 PG Go')
    required=p['required_names'];need(required and len(required)==len(set(required)) and set(required)==set(meta['required_names'])==set(gates['names_gate']['per_name']),'S2 required exact name set')
    for name in required:
        counts=[len(re.findall(pattern+re.escape(name)+r'(?:\s|$)',raw,re.M)) for pattern in (r'^=== RUN\s+',r'^--- PASS:\s+',r'^--- SKIP:\s+',r'^--- FAIL:\s+')];need(counts==[1,1,0,0],'S2 original RUN/PASS/SKIP/FAIL '+name)
    need(not re.search(r'^--- (?:SKIP|FAIL):|^FAIL(?:\s|$)',raw,re.M),'S2 PG skip/fail')
    need(meta['cmd']==cmd.strip() and '-count=1' in cmd and '-tags=integration' in cmd and all(n in cmd for n in required),'S2 PG command binding')
    need(('-race' in cmd)==p['step'].endswith('-race'),'S2 normal/race exact mode')
    need(gates['original_rc']==gates['wrapper_rc']==meta['rc']==meta['wrapper_rc']==0 and gates['source_gate']['ok'] is True and gates['names_gate']['ok'] is True and gates['version_ok'] is True,'S2 PG gate/meta original rc')
    return {'kind':'s2-pg','step':p['step'],'required':len(required),'source':source}


def unit(r,p,source):
    chain(r,p,source);b=p['inner']+'/01-unit';meta=r.doc(b+'.meta');gates=r.doc(b+'.gates.json');raw=r.text(b+'.raw');cmd=r.text(b+'.cmd');pkgs=re.findall(r'^ok\s+(\S+)',raw,re.M)
    need(gates['source_gate']['before']==gates['source_gate']['after']==source==meta['source_before'],'S2 unit source')
    need(raw.splitlines()[0].startswith('go version '+p['go']+' ') and meta['expect_go']==p['go'],'S2 unit Go')
    need(meta['cmd']==cmd.strip() and ('go test -count=1 ./...' in cmd if p['service']=='connector' else 'go test -count=1 ./services/'+{'dataset':'dataset-versioning-service','pipeline':'pipeline-build-service'}[p['service']]+'/...' in cmd) and './internal/...' not in cmd,'S2 full-module unit scope')
    need(len(pkgs)==len(set(pkgs))==p['packages'] and set(pkgs)==set(p['package_names']) and any('/cmd/' in pkg for pkg in re.findall(r'^(?:ok|\?)\s+(\S+)',raw,re.M)),'S2 exact full module package set incl cmd')
    need(not re.search(r'^FAIL(?:\s|$)',raw,re.M) and gates['original_rc']==gates['wrapper_rc']==meta['rc']==0 and gates['version_ok'] is True and gates['source_gate']['ok'] is True,'S2 unit original process/gates')
    return {'kind':'s2-unit','service':p['service'],'packages':len(pkgs),'optional_provider_cluster_sidecar':'unverified'}


def build(r,p,source):
    chain(r,p,source);d=p['inner'];result=r.doc(d+'/BUILD-RESULT.json');binary=r.doc(d+'/BINARY.json')
    need(result['expect_source_sha256']==result['source_post']==source and result['service']==p['service'],'S2 build source/service')
    need(result['new_full_id']==p['image'] and result['binary']['sha256']==binary['sha256']==p['binary_sha256'],'S2 original binary/image')
    need(hashlib.sha256(r.read(d+'/out-'+source[:8]+'/service')).hexdigest()==p['binary_sha256'],'S2 actual binary bytes')
    def stage(prefix):
        base=r.unique(d,prefix+'-','.rc');raw,meta=r.step(base);return raw,meta,base
    compile,cm,_=stage('04-compile');need(p['binary_sha256'] in compile and 'go build' in cm['cmd'] and cm['expect_source']==source,'S2 actual compile')
    committed,_,_=stage('07-commit');need(p['image'] in committed,'S2 original commit')
    verified,_,_=stage('08-verify-new-tag');need(verified.strip()==p['image']+'|nonroot:nonroot|/home/nonroot|["/usr/local/bin/service"]|null','S2 original verify config')
    pre,pm,_=stage('00-source-precheck');post,_,_=stage('12-source-post');need(pm['actual']==pm['expect']==source and pm['pinned_before_compile'] is True and source in post,'S2 original source pre/post')
    before,_,_=stage('02-running');after,_,_=stage('13-running-post');need(before==after,'S2 runtime unchanged')
    _,_,cleanup=stage('11-rm-exact-cid');need(result['cleaned_cid'] in r.text(cleanup+'.cmd'),'S2 exact cleanup CID')
    stage('10-cleanup-precheck');need(result['Root_running_snapshot']==p['runtime_before_after'],'S2 runtime identity binding')
    return {'kind':'s2-build-original','service':p['service'],'original_rc0':17,'image':p['image'],'binary_sha256':p['binary_sha256'],'composite':False,'runtime_upgraded':False}
