#!/usr/bin/env python3
"""Pure old-runner input projection from accepted SAME-S original build proofs."""
import argparse,hashlib,json,re,sys
from pathlib import Path
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent;P26=HERE.parents[1];ROOT=P26.parents[2]
sys.path.insert(0,str(P26/'archive'))
import p26_original_proofs as P

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def normal_build(reader,directory,outer,source,image):
    """Verify fresh all-zero W6 build ORIGINALS, with no old failure replay."""
    d=str(Path(directory).resolve().relative_to(ROOT));o=str(Path(outer).resolve().relative_to(ROOT))
    result=reader.doc(d+'/BUILD-RESULT.json');binary=reader.doc(d+'/BINARY.json')
    if result.get('ok') is not True or result['service']!='connector' or result['expect_source_sha256']!=source or result['source_post']!=source or result['new_full_id']!=image:raise ValueError('fresh original build result source/image mismatch')
    reader.rc(o+'/rc');argv=reader.doc(o+'/cmd-before.json');reader.text(o+'/stdout');reader.text(o+'/stderr');reader.text(o+'/raw');env=reader.doc(o+'/safe-env-metadata.json')
    if argv[:3]!=['python3','-I','-B'] or Path(argv[3]).name!='p26_build_executor.py' or argv[4:8]!=['run','--execute','--service','connector'] or argv[argv.index('--expect-source-sha')+1]!=source or env['P26_BUILD_EV_DIR']!=str(Path(directory).resolve().parent):raise ValueError('fresh actual outer argv/source/env binding mismatch')
    stepdata={}
    for rcfile in sorted(Path(directory).glob('*.rc')):
        if rcfile.name.endswith('.gate.rc'):reader.rc(str(rcfile.relative_to(ROOT)));continue
        base=str(rcfile.relative_to(ROOT))[:-3];stepdata[base]=reader.step(base)
    prefixes=['00-source-precheck','01-old-tag','02-running','03-go-cache','03b-volume-openfoundry-gomod','03b-volume-openfoundry-gobuild','04-compile','05-create-base','06-cp','07-commit','08-verify-new-tag','09-old-config','10-cleanup-precheck','11-rm-exact-cid','12-source-post','13-running-post','14-old-tag-post']
    selected={prefix:reader.unique(d,prefix+'-','.rc') for prefix in prefixes}
    if set(selected.values())!=set(stepdata):raise ValueError('fresh build original step set missing/extra')
    pre,meta=stepdata[selected['00-source-precheck']]
    if meta.get('actual')!=source or meta.get('expect')!=source or meta.get('pinned_before_compile') is not True:raise ValueError('original source precheck mismatch')
    raw,compilemeta=stepdata[selected['04-compile']]
    if not raw.splitlines()[0].startswith('go version go1.25.14 ') or binary['sha256'] not in raw or compilemeta.get('expect_source')!=source or compilemeta.get('network')!='none' or compilemeta.get('pull')!='never' or 'go build' not in compilemeta['cmd']:raise ValueError('original offline compiler/version/source/binary proof mismatch')
    actual=reader.read(d+'/out-'+source[:8]+'/service')
    if hashlib.sha256(actual).hexdigest()!=binary['sha256'] or result['binary']['sha256']!=binary['sha256'] or binary['mode']!='0o755' or result['binary']['mode']!='0755':raise ValueError('actual candidate binary bytes/mode mismatch')
    cid=result['cleaned_cid']
    if not re.fullmatch('[a-f0-9]{64}',cid):raise ValueError('exact build CID missing')
    create=stepdata[selected['05-create-base']];cp=stepdata[selected['06-cp']];commit=stepdata[selected['07-commit']];verify=stepdata[selected['08-verify-new-tag']][0];old=stepdata[selected['09-old-config']][0]
    if create[0].strip()!=cid or not create[1]['cmd'].startswith('docker create --pull=never --network none ') or not create[1]['cmd'].strip().endswith(result['old_full_id']):raise ValueError('actual stopped offline base create proof mismatch')
    if cp[1]['binary_sha256']!=binary['sha256'] or cid+':/usr/local/bin/service' not in cp[1]['cmd'] or 'out-'+source[:8]+'/service' not in cp[1]['cmd']:raise ValueError('actual exact binary cp mismatch')
    if image not in commit[0] or commit[1]['cmd']!='docker commit '+cid+' '+result['new_tag'] or verify.split('|',1)[0]!=image or verify.strip().split('|',1)[1]!=old.strip():raise ValueError('actual committed image/safeconfig mismatch')
    cleanup=stepdata[selected['10-cleanup-precheck']]
    if cid not in cleanup[0] or 'p26-build' not in cleanup[0] or stepdata[selected['11-rm-exact-cid']][1]['cmd']!='docker rm '+cid:raise ValueError('actual exact owned build CID cleanup mismatch')
    if source not in stepdata[selected['12-source-post']][0] or stepdata[selected['02-running']][0]!=stepdata[selected['13-running-post']][0] or stepdata[selected['01-old-tag']][0]!=stepdata[selected['14-old-tag-post']][0]:raise ValueError('actual source/runtime/oldtag before-after mismatch')
    return {'kind':'build-original-allzero','original_steps':len(stepdata),'outer_original_rc':0,'binary_sha256':binary['sha256'],'image':image,'original':d,'outer':o}

def main():
    p=argparse.ArgumentParser();p.add_argument('--root-accepted-build',required=True);p.add_argument('--output',required=True);p.add_argument('--original-build-dir');p.add_argument('--original-build-outer');a=p.parse_args()
    accepted=json.loads(Path(a.root_accepted_build).read_text());source=accepted['source']
    if not re.fullmatch('[a-f0-9]{64}',source) or int(source,16)==0:raise ValueError('full actual accepted source required')
    candidate=next(c for c in accepted['candidates'] if c['service']=='connector')
    reader=P.Reader(ROOT)
    if a.original_build_dir:
        if not a.original_build_outer:raise ValueError('fresh build must bind actual original Unix outer')
        proof={'kind':'build-original-allzero','original':str(Path(a.original_build_dir).resolve()),'outer':str(Path(a.original_build_outer).resolve()),'image':candidate['image']}
        verdict=normal_build(reader,a.original_build_dir,a.original_build_outer,source,candidate['image'])
    else:
        if accepted.get('composite_not_original_pass') is not True:raise ValueError('original build directory required for fresh non-composite acceptance')
        roster=json.loads((P26/'archive/p26_formal_roster.json').read_text());proofs=[c['proof'] for item in roster['items'] for c in item.get('contracts',{}).values() if c.get('proof',{}).get('kind')=='build-composite' and c['proof']['image']==candidate['image']]
        if len(proofs)!=1:raise ValueError('exact original accepted composite build proof missing/duplicate')
        proof=proofs[0];verdict=P.build(reader,proof,source)
    out=Path(a.output).resolve()
    if not out.is_relative_to((P26/'results/w10').resolve()) or out.exists(): raise ValueError('exclusive NEW results/w10 output required')
    result={'schema':'p25-selected-build-v1','ok':True,'all_ok':True,'source_sha256':source,'images':{'p26connectorc':candidate['image']},'projection_scope':'accepted composite connector build image for NEW C; actual CID/boot/deployment independently Root-pinned','accepted_composite':{'path':str(Path(a.root_accepted_build).resolve()),'sha256':sha(a.root_accepted_build)},'original_build_proof':proof,'verdict':verdict,'original_files':reader.files,'historical_original_failures_preserved':True,'actual_http_sql_docker_calls':0}
    out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'output':str(out),'same_S':source,'accepted_image':candidate['image'],'original_files_read':len(reader.files),'live_calls':0}))
if __name__=='__main__': main()
