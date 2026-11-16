"""Original-bound closure mutations + NONSECRET scan CLI fixtures only."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile


def mod(name):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def run(workspace):
    original=mod('p26_original_proofs');final=mod('p26_final_proofs');runtime=mod('p26_runtime_controls').run(workspace)
    base=Path(workspace);report=runtime['reports'][0];directory=next((base/'tools/smoke/business-platform-p2-6/results/w6/deployment/S2-main-dataset-upgrade-attempt1').glob('commands-*'))
    rd=original.Reader(workspace);steps={}
    for f in directory.glob('*.rc'):
        name=f.name[:-3];prefix=str(f.relative_to(base))[:-3]
        steps[name]={'command':rd.doc(prefix+'.cmd.json'),'meta':rd.doc(prefix+'.meta.json'),'raw_sha256':hashlib.sha256(rd.read(prefix+'.raw')).hexdigest()}
    source='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
    sourcefiles=list((base/'tools/smoke/business-platform-p2-6/results/w6/S2'/source).rglob('01-go-test.meta'))[:2]
    assert len(sourcefiles)==2
    tool=Path(__file__).with_name('p26_final_proofs.py');tool_rel=str(tool.relative_to(base))
    p={'epoch':source,'outer':'tools/smoke/business-platform-p2-6/results/w6/deployment/S2-dataset-outer-20261115T095807Z','outer_inputs':[],'source_checks':[str(f.relative_to(base)) for f in sourcefiles],'historical_tools':{tool_rel:hashlib.sha256(tool.read_bytes()).hexdigest()},'directory':str(directory.relative_to(base)),'steps':steps}
    final.closed(original.Reader(workspace),p,source)
    controls=[]
    for name,mutation in [('source-epoch',lambda p:p.update(epoch='0'*64)),('missing-source-after',lambda p:p['source_checks'].pop()),('missing-original-step',lambda p:p['steps'].pop(next(iter(p['steps'])))),('changed-original-command',lambda p:p['steps'][next(iter(p['steps']))].update(command=['false'])),('wrong-used-tool',lambda p:p['historical_tools'].update({tool_rel:'0'*64}))]:
        changed=copy.deepcopy(p);mutation(changed)
        try:final.closed(original.Reader(workspace),changed,source);rejected=False
        except ValueError:rejected=True
        controls.append({'name':name,'rejected':rejected})
    # Actual S2 receipt closure is deliberately insufficient for every final scope.
    for name in ('typed_whole','restart','cleanup','receipt','old_artifacts','restart_receipt','secret','quiesce'):
        try:getattr(final,name)(original.Reader(workspace),copy.deepcopy(p),source);rejected=False
        except (ValueError,KeyError,TypeError,OSError):rejected=True
        controls.append({'name':'partial-runtime-cannot-satisfy-'+name,'rejected':rejected})
    with tempfile.TemporaryDirectory(prefix='p26-NONSECRET-scan-audit-') as tmp:
        root=Path(tmp);(root/'payload.bin').write_bytes(b'\x00nonsecret fixture payload');(root/'ok.log').write_text('safe fixture\n')
        cmd=[sys.executable,'-B',str(tool.with_name('p26_secret_scan.py')),'scan',str(root),'--values-stdin','--audit-json']
        cp=subprocess.run(cmd,input=b'NONSECRET-fixture-value-1\nNONSECRET-fixture-value-2\n',capture_output=True)
        doc=json.loads(cp.stdout)
        assert cp.returncode==0 and doc['actual_value_count']==2 and doc['actual_hit_count']==0 and len(doc['scanned_members'])==2
        assert b'NONSECRET-fixture-value' not in cp.stdout
        for path,pin in doc['scanned_members'].items():assert pin=={'sha256':hashlib.sha256((root/path).read_bytes()).hexdigest(),'size':(root/path).stat().st_size}
        empty=subprocess.run(cmd,input=b'',capture_output=True);assert empty.returncode==1
        scan={'fixture_only':True,'original_cli_rc':cp.returncode,'empty_stdin_original_cli_rc':empty.returncode,'audit':doc,'actual_private_values_read':False}
    assert all(c['rejected'] for c in controls)
    return {'status':'CODE-PURE-INTERFACES-ONLY','final_scope_accepted':False,'original_closure_baseline':'actual Dataset55 kind/command/raw closure only','controls':controls,'scan_fixture':scan}

if __name__=='__main__':print(json.dumps(run(sys.argv[1]),indent=2))
