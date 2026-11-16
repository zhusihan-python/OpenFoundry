"""Actual receipt-kind baseline plus in-memory negative controls; no live calls."""
import copy
import importlib.util
import json
from pathlib import Path

def mod(name):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def run(root):
    reader=mod('p26_original_proofs');runtime=mod('p26_runtime_proofs');reports=[];controls=[]
    specs=[('dataset','S2-main-dataset-upgrade-attempt1',55,{'argv':36,'http':19}),('connector','S2-main-connector-upgrade-attempt1',32,{'argv':23,'http':9}),('connector-readonly-completion','S2-connector-readonly-completion-attempt1',25,{'argv':15,'http':10})]
    for service,sub,count,kinds in specs:
        parent=Path(root)/'tools/smoke/business-platform-p2-6/results/w6/deployment'/sub
        dirs=list(parent.glob('commands-*'));assert len(dirs)==1
        directory=str(dirs[0].relative_to(root));r=reader.Reader(root);steps={}
        for path in dirs[0].glob('*.rc'):
            base=directory+'/'+path.name[:-3];raw=r.read(base+'.raw');meta=r.doc(base+'.meta.json')
            import hashlib
            steps[path.name[:-3]]={'meta':meta,'command':r.doc(base+'.cmd.json'),'raw_sha256':hashlib.sha256(raw).hexdigest()}
        p={'directory':directory,'steps':steps,'count':count,'kinds':kinds}
        actual=runtime.group(r,p)
        if service in ('connector','connector-readonly-completion'):
            outer='S2-connector-outer-20261115T100316Z' if service=='connector' else 'S2-connector-completion-outer-20261115T103257Z'
            r.rc('tools/smoke/business-platform-p2-6/results/w6/deployment/'+outer+'/rc',1 if service=='connector' else 0)
        reports.append({'service':service,'actual_kinds':actual,'unique_original_files':len(r.files),'files':r.files})
        for name,mutate in [('missing-step',lambda x:x['steps'].pop(next(iter(x['steps'])))),('wrong-HTTP-child-kind',lambda x:x['steps'][next(k for k in x['steps'] if x['steps'][k]['meta']['kind']=='http')]['meta'].update(original_child_rc=0)),('wrong-Unix-child-rc',lambda x:x['steps'][next(k for k in x['steps'] if x['steps'][k]['meta']['kind']=='argv')]['meta'].update(original_child_rc=1))]:
            q=copy.deepcopy(p);mutate(q)
            try:runtime.group(reader.Reader(root),q);refused=False
            except ValueError:refused=True
            controls.append({'name':service+'-'+name,'refused':refused})
    assert all(c['refused'] for c in controls)
    return {'status':'RUNTIME-ORIGINAL-KINDS-ONLY','final_runtime_accepted':False,'reports':reports,'negative_controls':controls,'live_calls':0}
if __name__=='__main__':
    import sys
    print(json.dumps(run(sys.argv[1]),indent=2))
