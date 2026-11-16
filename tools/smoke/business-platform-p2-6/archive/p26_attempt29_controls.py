"""Original CURRENT-only component plus coherent rehashed owner/Run-list negatives."""
import copy,hashlib,importlib.util,json
from pathlib import Path

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt29';p=json.loads((o/'CURRENT-COMPONENT.CONTRACT.DRAFT.json').read_text());source=p['epoch'];sp=importlib.util.spec_from_file_location('original',b/'archive/p26_original_proofs.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m);result,files=m.validate(root,p,source);old=m.Reader;checks=[];encode=lambda d:(json.dumps(d,indent=2)+'\n').encode();sha=lambda raw:hashlib.sha256(raw).hexdigest()
 for name,kind in [('coherent-current-owner-cannot-be-retained','owner'),('coherent-current-full-runlist-cannot-have-extra-row','runs')]:
  audit=json.loads((root/p['audit']).read_text());resultpath=audit['actual_result']['path'];doc=json.loads(Path(resultpath).read_text());directory=Path(resultpath).parent;overlay={}
  if kind=='owner':
   owner={'run_id':doc['bodies']['run']['id'],'run_status':'running','since':doc['bodies']['run']['started_at'],'blocked_reason':''};doc['bodies']['cp']['owner']=owner;doc['cp_owner']=owner;audit['cp_owner']=owner;rawpath=str(directory/'03-GET-cp.raw');overlay[rawpath]=encode(doc['bodies']['cp']);expected='CURRENT cursor1/released owner exact confirmed Run/Txn facts'
  else:
   doc['bodies']['runs'].append(dict(doc['bodies']['runs'][0],id='00000000-0000-4000-8000-000000000001',status='running'));doc['full_known_run_count']=2;rawpath=str(directory/'04-GET-runs.raw');overlay[rawpath]=encode(doc['bodies']['runs']);expected='CURRENT complete one-row Run list binds same full Run'
  overlay[resultpath]=encode(doc);audit['actual_result']['sha256']=sha(overlay[resultpath])
  for path,raw in overlay.items():relative=path.removeprefix(str(b.resolve())+'/');audit['files'][relative]={'sha256':sha(raw),'bytes':len(raw)}
  overlay[str(root/p['audit'])]=encode(audit);hits=set()
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel);path=str(root)+'/'+rel
    if path in overlay:hits.add(path);return overlay[path]
    return raw
  m.Reader=Overlay
  try:m.validate(root,p,source);rejected=False;reason=None
  except ValueError as exc:rejected=True;reason=str(exc)
  m.Reader=old;assert rejected and reason==expected and hits==set(overlay),(name,reason);checks.append({'name':name,'GET_result_and_audit_SHA_size_mutated_consistently':True,'original_member_hash_layer_passed':True,'reached_CURRENT_semantic_layer':True,'rejected':True,'reason':reason})
 for name,change in [('CURRENT-cannot-be-historical-success',lambda d:d.update(classification='actual-success')),('CURRENT-runtime15-record-cannot-change',lambda d:d['accepted_runtime_record'].update(sha256='0'*64))]:
  proof=copy.deepcopy(p);change(proof)
  try:m.validate(root,proof,source);rejected=False
  except ValueError:rejected=True
  assert rejected;checks.append({'name':name,'rejected':True})
 p['original_files']=files;(o/'CURRENT-COMPONENT.CONTRACT.json').write_text(json.dumps(p,indent=2)+'\n');(o/'01-CURRENT-result.json').write_text(json.dumps({'result':result,'original_files':files,'unique_originals':len(files),'negative_controls':checks,'live_calls':0,'full_P25_15':False},indent=2)+'\n');print('PASS',len(files),len(checks))
if __name__=='__main__':
 import sys
 run(Path(sys.argv[1]).resolve())
