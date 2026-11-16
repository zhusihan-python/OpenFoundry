"""Read accepted S2 first3 only; mutate byte views, never originals."""
import copy,importlib.util,json
from pathlib import Path

def run(root):
 b=Path(__file__).parents[1];spec=importlib.util.spec_from_file_location('original',Path(__file__).with_name('p26_original_proofs.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);doc=json.loads((b/'results/w9/2026-11-15-w9-attempt12/S2-first3-contract.json').read_text());results=[];files={}
 for p in doc['proofs']:
  result,read=m.validate(root,p,doc['source']);results.append(result);files.update(read)
 controls=[];old=m.Reader
 cases=[('outer-original-rc',0,'case.rc',lambda b:b'1'),('S2-after-source',0,'case.meta.json',lambda b:b.replace(doc['source'].encode(),b'0'*64)),('download-original-byte',0,'download.bin',lambda b:b+b'bad'),('CP-incremental-epoch',1,'cp-poll.meta',lambda b:b.replace(b'"confirmed_epoch": 2',b'"confirmed_epoch": 3')),('Source-postintent-counter',2,'09-fixture-src.meta',lambda b:b.replace(b'"request_count": 30',b'"request_count": 31')),('ACK201-original-status',2,'proxy-log-poll.meta',lambda b:b.replace(b'"upstream_status": 201',b'"upstream_status": 200')),('current-wrapper-cannot-replace-original',0,'run_p26_s2_actual.py',lambda b:b+b'\n#modified'),('missing-ready-tool-entry',0,'READY-INDEX.json',lambda b:b.replace(b'"source": "http/selftest_p26_s2.py"',b'"source": "http/unknown.py"'))]
 for name,i,suffix,change in cases:
  hit=[False]
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel)
    if rel.endswith(suffix):
     changed=change(raw);hit[0]|=raw!=changed;return changed
    return raw
  m.Reader=Overlay;p=copy.deepcopy(doc['proofs'][i]);p.pop('original_files',None)
  try:m.validate(root,p,doc['source']);rejected=False
  except (ValueError,KeyError,OSError,TypeError):rejected=True
  controls.append({'name':name,'mutated':hit[0],'rejected':rejected});m.Reader=old
 assert all(c['mutated'] and c['rejected'] for c in controls),controls
 return {'status':'S2-FIRST3-ORIGINAL-VERIFIED-DRAFT','results':results,'unique_original_files':len(files),'original_files':files,'negative_controls':controls,'remaining7_read':False,'final_HTTP_complete':False}
if __name__=='__main__':
 import sys
 print(json.dumps(run(sys.argv[1]),indent=2))
