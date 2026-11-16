"""Read-only S2 draft validation and memory-only tamper controls."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path


def load(name):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def check(workspace,negative=False):
    b=Path(__file__).parent;doc=json.loads((b/'p26_S2_evidence.candidate.json').read_text());roster=json.loads((b/'p26_S2_roster.candidate.json').read_text());m=load('p26_original_proofs');core=load('p26_formal_core');steps=core.index_steps(doc);out=[];seen={}
    for step in doc['steps']:
        result,files=m.validate(workspace,step['proof'],doc['source']);out.append(dict(id=step['id'],result=result));seen.update(files)
    for item in roster['items']:
        if item.get('contracts'):
            projected=copy.deepcopy(item);projected['status']='accepted';core.validate_item(projected,steps,doc['source'],lambda:m.Reader(workspace))
    h=copy.deepcopy(roster['pre_seal_requirements']['h12-targeted']);h['status']='accepted';core.validate_item(h,steps,doc['source'],lambda:m.Reader(workspace),'h12-targeted')
    controls=[]
    if negative:
        mutations=[('PG-raw-skip','01-go-test.raw',lambda b:b.replace(b'--- PASS:',b'--- SKIP:',1),'pg-new-dataset-normal'),('PG-original-rc','01-go-test.rc',lambda b:b'1','pg-new-dataset-normal'),('PG-source-meta','01-go-test.meta',lambda b:b.replace(doc['source'].encode(),b'0'*64),'pg-new-dataset-normal'),('full-Connector-cmd-missing','01-unit.raw',lambda b:b'\n'.join(l for l in b.split(b'\n') if b'/cmd/' not in l),'unit-connector'),('unit-scope-internal-only','01-unit.cmd',lambda b:b.replace(b'go test -count=1 ./...',b'go test -count=1 ./internal/...'),'unit-connector'),('build-binary-byte','out-ff9cf1df/service',lambda b:b+b'changed','build-original-dataset'),('build-original-verify-rc','08-verify-new-tag-aa5ef2.rc',lambda b:b'1','build-original-dataset'),('old-new-runner-blob-confusion','pg/p26_pg_runner.py',lambda b:b+b'changed','unit-connector')]
        old=m.Reader
        for name,suffix,fn,sid in mutations:
            hit=[False]
            class Overlay(old):
                def read(self,rel):
                    data=super().read(rel)
                    if rel.endswith(suffix):
                        new=fn(data);hit[0]|=new!=data;return new
                    return data
            m.Reader=Overlay;p=copy.deepcopy(steps[sid]['proof']);p.pop('original_files',None)
            try:m.validate(workspace,p,doc['source']);refused=False
            except (ValueError,OSError):refused=True
            controls.append({'name':name,'mutated':hit[0],'refused':refused});m.Reader=old
        wrong=copy.deepcopy(steps['pg-new-dataset-normal']['proof']);wrong['epoch']='b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c'
        try:core.scope_kind('pg-new-27-normal-27-race','pg-new-dataset-normal',wrong);refused=False
        except ValueError:refused=True
        controls.append({'name':'oldS-relabel-as-S2','mutated':True,'refused':refused})
        if not all(c['mutated'] and c['refused'] for c in controls):raise ValueError('S2 negative control failed: '+str(controls))
    return {'status':'S2-ORIGINAL-PG-UNIT-BUILD-DRAFT-VERIFIED','results':out,'unique_original_members':len(seen),'negative_controls':controls,'final_Root_acceptance':False,'runtime_upgraded':False,'all_later_scopes_pending':True}

if __name__=='__main__':
    import sys
    try:print(json.dumps(check(sys.argv[1],len(sys.argv)>2),indent=2))
    except (ValueError,OSError,KeyError,TypeError) as e:print(json.dumps({'status':'NOTREADY','problem':str(e)}));raise SystemExit(1)
