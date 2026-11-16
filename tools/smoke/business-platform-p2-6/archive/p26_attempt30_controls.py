"""Accepted actual h10 reader and exact projection/immutable gate controls."""
import copy,hashlib,importlib.util,json
from pathlib import Path

def mod(path,name):
 sp=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m);return m

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt30';p=json.loads((o/'H10-ACTUAL-CONTRACT.DRAFT.json').read_text());original=mod(b/'archive/p26_original_proofs.py','original');reader=mod(b/'archive/p26_h10_actual_reader.py','actual');anchor_reader=mod(b/'archive/p26_h10_anchor_reader.py','anchor');semantic=mod(b/'archive/p26_p25_proofs.py','semantic');source=p['epoch'];result,files=original.validate(root,p,source);checks=[];audit=json.loads((root/p['audit']).read_text());anchor=json.loads(Path(audit['anchor']['path']).read_text());post=json.loads(Path(audit['typed_afterpost']['path']).read_text());registry=semantic.specs((b/'results/w9/2026-11-15-w9-attempt21/accepted-producer-snapshot/regressions/vendor/p25-runtime/smoke_p25.py').read_bytes())
 for name,part,change in [('full27-heartbeat-drift','row',lambda x:x.update(heartbeat_at='changed')),('CP-active-since-drift','owner',lambda x:x.update(active_since='changed')),('CP-epoch-drift','owner',lambda x:x.update(confirmed_epoch='9')),('CP-reason-without-DB-expiry','owner',lambda x:x.update(blocked_reason='requires_reconciliation')),('parent-output-drift','parent',lambda x:x.update(output_dataset_id='changed')),('NULL-protocol-cannot-change','clock',lambda x:x.update(recovery_protocol='checkpoint-first-v1'))]:
  values={'row':copy.deepcopy(post['row']),'owner':copy.deepcopy(post['cp_owner_row_full']),'parent':copy.deepcopy(post['parent']),'clock':{'id':post['row']['id'],'recovery_protocol':None,'expired':False}};change(values[part])
  try:anchor_reader.compare(anchor,values['row'],values['owner'],values['parent'],values['clock']);rejected=False
  except ValueError:rejected=True
  assert rejected,name;checks.append({'name':name,'real_actual_anchor_and_post_values':True,'semantic_layer_no_hash_shortcut':True,'rejected':True})
 pre=json.loads(Path(audit['children'][0]['summary']['path']).read_text())['scenarios'][0]
 for name,change in [('pre-mandatory-gates-empty',lambda x:x.update(gates=[])),('pre-mandatory-gate-truncated',lambda x:x['gates'].pop()),('pre-wrong-mandatory-name',lambda x:x['gates'][0].update(gate='G_WRONG'))]:
  row=copy.deepcopy(pre);change(row)
  try:reader.child_semantics(row,'pre',registry,semantic);rejected=False
  except ValueError:rejected=True
  assert rejected;checks.append({'name':name,'semantic_layer_no_hash_shortcut':True,'rejected':True})
 altered=copy.deepcopy(post);altered.update(lease_expired_by_DB_clock=True,frozen_for_next_phase=True)
 try:reader.post_state_semantics(altered);rejected=False
 except ValueError:rejected=True
 assert rejected;checks.append({'name':'historical-post-False-cannot-be-replaced-by-later-freeze','semantic_layer_no_hash_shortcut':True,'rejected':True})
 # Mutate the projected semantic payload and update its self hashes/ledger consistently.
 projected=copy.deepcopy(audit);ip=projected['import_projections'][0];sdoc=json.loads(Path(ip['semantic_file']).read_text());ledger=json.loads(Path(ip['ledger_file']).read_text());idx=next(i for i,x in enumerate(sdoc['handles']) if x['kind']=='source');sdoc['handles'][idx]['name']='forged-semantic-name';canon=lambda x:json.dumps(x,sort_keys=True,separators=(',',':')).encode();ledger['rows'][idx]['semantic_row_sha256']=hashlib.sha256(canon(sdoc['handles'][idx])).hexdigest();encode=lambda x:(json.dumps(x,indent=2)+'\n').encode();sraw=encode(sdoc);ip['semantic_file_sha256']=hashlib.sha256(sraw).hexdigest();ledger['semantic_file_sha256']=ip['semantic_file_sha256'];lraw=encode(ledger);ip['ledger_file_sha256']=hashlib.sha256(lraw).hexdigest();overlay={ip['semantic_file']:sraw,ip['ledger_file']:lraw}
 class Overlay(original.Reader):
  def read(self,rel):
   raw=super().read(rel);return overlay.get(str(root)+'/'+rel,raw)
 try:reader.projections(Overlay(root),p,projected);rejected=False;reason=None
 except ValueError as exc:rejected=True;reason=str(exc)
 assert rejected and reason=='h10 projection must preserve every semantic field; exact two metadata keys only';checks.append({'name':'projection-semantic-field-cannot-be-stripped-or-changed','coherent_updated_projection_and_ledger_hashes':True,'reached_projection_semantics_after_ref_hashes':True,'rejected':True})
 healthy=json.loads((b/'results/w9/2026-11-15-w9-attempt28/HEALTHY-GLOBAL-CONTRACT.after-gate-fix.json').read_text());full={'kind':'p25-actual-full','epoch':source,'sidecar_bases':{'fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'},'engine_source':'tools/smoke/business-platform-p2-6/regressions/vendor/p25-runtime/smoke_p25.py','engine_sha256':'f931dca20e5be294e250f551063f282c97233fe812ed54ced56d32809a6e254d','healthy':healthy,'h10':p}
 try:original.validate(root,full,source);rejected=False;reason=None
 except ValueError as exc:rejected=True;reason=str(exc)
 assert rejected and 'independent natural frozen-held component still required' in reason;checks.append({'name':'real-healthy12-and-h10-crash-cannot-close-full15-without-freeze-tail-h14','healthy_and_actual_crash_really_validated_first':True,'rejected':True})
 p['original_files']=files;(o/'H10-ACTUAL-CONTRACT.json').write_text(json.dumps(p,indent=2)+'\n');(o/'01-h10-actual-result.json').write_text(json.dumps({'result':result,'original_files':files,'unique_originals':len(files),'negative_controls':checks,'live_calls':0,'full_P25_15':False},indent=2)+'\n');print('PASS',len(files),len(checks))
if __name__=='__main__':
 import sys
 run(Path(sys.argv[1]).resolve())
