"""Actual healthy compiler plus bounded output/alias/process mutation controls."""
import copy,hashlib,importlib.util,json
from pathlib import Path

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt27';p=json.loads((o/'HEALTHY-GLOBAL-CONTRACT.DRAFT.json').read_text());source=p['epoch'];sp=importlib.util.spec_from_file_location('original',b/'archive/p26_original_proofs.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m);result,files=m.validate(root,p,source);old=m.Reader;checks=[]
 def altered(name,key,change):
  proof=copy.deepcopy(p);target=proof[key]['path'];original=(root/target.removeprefix(str(root)+'/')).read_bytes();doc=json.loads(original);change(doc);modified=(json.dumps(doc,indent=2)+'\n').encode();proof[key]['sha256']=hashlib.sha256(modified).hexdigest();hit=[False]
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel)
    if str(root)+'/'+rel==target:hit[0]=True;return modified
    return raw
  m.Reader=Overlay
  try:m.validate(root,proof,source);rejected=False
  except (ValueError,KeyError,TypeError,OSError):rejected=True
  m.Reader=old;assert hit[0] and rejected,name;checks.append({'name':name,'mutation_with_updated_output_SHA':True,'rejected':True})
 altered('failed-h9-history-cannot-disappear','bundle',lambda d:d.update(original_failures_preserved=[]))
 altered('h5-cannot-invent-assertion','bundle',lambda d:d['scenarios'][4]['gates'].append({'gate':'invented','ok':True,'detail':'invented'}))
 altered('failed-h9-cannot-be-selected','bundle',lambda d:d['case_sources']['h9'].update(source_index=8))
 altered('every-handle-origin-retained','handles',lambda d:d['handles'][0]['bundle_origins'].pop())
 for name,change in [('unregistered-external-RootJSON-refused',lambda d:d['external_json_map'].pop(next(iter(d['external_json_map'])))),('env-cannot-be-RootJSON-snapshot',lambda d:d['external_json_map'][next(iter(d['external_json_map']))].update(workspace_path='tools/smoke/business-platform-p2-6/results/w9/forbidden.env'))]:
  proof=copy.deepcopy(p);change(proof)
  try:m.validate(root,proof,source);rejected=False
  except (ValueError,KeyError,OSError):rejected=True
  assert rejected,name;checks.append({'name':name,'rejected':True})
 class WrongRC(old):
  def read(self,rel):
   raw=super().read(rel)
   return b'1\n' if rel.endswith('/verify-actual/rc') else raw
 m.Reader=WrongRC
 try:m.validate(root,p,source);rejected=False
 except ValueError:rejected=True
 m.Reader=old;assert rejected;checks.append({'name':'original-verify-Unix-rc-required','rejected':True})
 proof={'kind':'p25-actual-full','epoch':source,'sidecar_bases':{'fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'},'engine_source':'tools/smoke/business-platform-p2-6/regressions/vendor/p25-runtime/smoke_p25.py','engine_sha256':'f931dca20e5be294e250f551063f282c97233fe812ed54ced56d32809a6e254d','healthy':copy.deepcopy(p)}
 try:m.validate(root,proof,source);rejected=False
 except (ValueError,KeyError,OSError):rejected=True
 assert rejected;checks.append({'name':'healthy12-cannot-close-P25full15','rejected':True})
 p['original_files']=files;(o/'HEALTHY-GLOBAL-CONTRACT.json').write_text(json.dumps(p,indent=2)+'\n');doc={'result':result,'original_files':files,'unique_originals':len(files),'negative_controls':checks,'live_calls':0,'actual_package_created':False};(o/'01-healthy-global-result.json').write_text(json.dumps(doc,indent=2)+'\n');print('PASS',len(files),len(checks))
if __name__=='__main__':
 import sys
 run(Path(sys.argv[1]).resolve())
