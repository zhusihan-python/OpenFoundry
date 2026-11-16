"""Read-only supplementary original slices. Not final package or formal acceptance."""
import copy
import importlib.util
import json
import os
import sys

def module():
    spec=importlib.util.spec_from_file_location('proof',os.path.join(os.path.dirname(__file__),'p26_original_proofs.py'))
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def check(root, path, negatives=False):
    m=module();doc=json.load(open(path));results=[];files={}
    for p in doc['proofs']:
        out,read=m.validate(root,p,doc['source']);results.append(out);files.update(read)
    controls=[]
    if negatives:
        # Fault only bytes returned in memory; original artifacts are never changed or copied.
        cases=[('download-byte',0,'download.bin',lambda data:data+b'x'),
               ('CP-epoch',1,'cp-poll.meta',lambda data:data.replace(b'"confirmed_epoch": 2',b'"confirmed_epoch": 3')),
               ('ACK-body-hash',2,'proxy-log-poll.meta',lambda data:data.replace(b'"request_body_sha256": "2deb',b'"request_body_sha256": "3deb')),
               ('original-HTTP-status',0,'file-presign.meta',lambda data:data.replace(b'"original_http_status": 302',b'"original_http_status": 200')),
               ('historical-tool',0,'run_first3_formal.py',lambda data:data+b'\n#changed'),
               ('h12-verbatim',3,'new-normal.raw',lambda data:data+b'\n#changed'),
               ('switch-image',4,'after-dataset.stdout',lambda data:data.replace(b'4217c402',b'5217c402')),
               ('old-safe-whole-set',4,'73-sql-controlled-9.raw',lambda data:data.replace(b'"id":',b'"unknown_id":',1)),
               ('new4-expected-rc1',6,'collision-ports.rc',lambda data:b'0')]
        old_reader=m.Reader
        for name,index,suffix,mutation in cases:
            proof=copy.deepcopy(doc['proofs'][index]);proof.pop('original_files',None)
            hit=[False]
            class Overlay(old_reader):
                def read(self,rel):
                    data=super().read(rel)
                    if rel.endswith(suffix) and not ('063308Z-57b547' in rel):
                        altered=mutation(data);hit[0]|=altered!=data;return altered
                    return data
            m.Reader=Overlay
            try:m.validate(root,proof,doc['source']);rejected=False
            except (ValueError,OSError):rejected=True
            controls.append({'name':name,'mutated':hit[0],'rejected':rejected})
            m.Reader=old_reader
        if not all(c['mutated'] and c['rejected'] for c in controls):raise ValueError('negative controls failed: '+json.dumps(controls))
    return {'verdict':'SUPPLEMENTARY-ORIGINAL-OK','formal_complete':False,'results':results,'original_files':files,'negative_controls':controls}

if __name__=='__main__':
    if len(sys.argv) not in (3,4):raise SystemExit('usage: p26_slice_crosscheck.py ROOT CONTRACT [--negative-controls]')
    try:out=check(sys.argv[1],sys.argv[2],len(sys.argv)==4);print(json.dumps(out,indent=2))
    except (ValueError,OSError,KeyError,TypeError) as e:print(json.dumps({'verdict':'REFUSED','problem':str(e)}));raise SystemExit(1)
