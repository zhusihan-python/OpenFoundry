"""Frozen failed h10 intake and semantic mandatory healthy-gate regression controls."""
import copy,hashlib,importlib.util,json
from pathlib import Path

def module(path,name):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def run(root):
 b=Path(__file__).parents[1];o=b/'results/w9/2026-11-15-w9-attempt28';m=module(b/'archive/p26_original_proofs.py','original');source='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e';prefix='tools/smoke/business-platform-p2-6/';p={'kind':'p25-failed-h10-history','epoch':source,'classification':'failed-history-only','canonical_root_label':str(root),'audit':prefix+'results/w10/S2-P25-h10-outer-launch-attempt1/FAILED-ORIGINAL-AUDIT.json','launch_rc':prefix+'results/w10/S2-P25-h10-outer-launch-attempt1/rc','outer':prefix+'results/w10/S2-seven-stage-ready-attempt1/p25-h10-outer-actual','retained':str((o/'retained-failed-h10-created-handles.json').relative_to(root)),'retained_ledger':str((o/'retained-failed-h10-created-handles.ORIGIN-LEDGER.json').relative_to(root))}
 result,files=m.validate(root,p,source);p['original_files']=files;(o/'01-failed-h10-history.json').write_text(json.dumps({'result':result,'proof':p,'original_files':files,'unique_originals':len(files)},indent=2)+'\n');print('history PASS',len(files),flush=True)
 semantic=module(b/'archive/p26_p25_proofs.py','semantic');compiler=module(b/'archive/p26_healthy_global_reader.py','compiler');oldcontract=b/'results/w9/2026-11-15-w9-attempt27/HEALTHY-GLOBAL-CONTRACT.DRAFT.json';healthy=json.loads(oldcontract.read_text());bundle=json.loads(Path(healthy['bundle']['path']).read_text());registryraw=(b/'results/w9/2026-11-15-w9-attempt21/accepted-producer-snapshot/regressions/vendor/p25-runtime/smoke_p25.py').read_bytes();registry=semantic.specs(registryraw);cases={x['id']:x for x in bundle['scenarios']};checks=[]
 for sid,case in cases.items():compiler.selected_case_semantics(case,copy.deepcopy(case),copy.deepcopy(case),sid,registry,semantic)
 mutations=[('h1-all-gates-removed','h1',lambda c,r:c.update(gates=[])),('h1-one-gate-truncated','h1',lambda c,r:c['gates'].pop()),('h1-wrong-gate-name','h1',lambda c,r:c['gates'][0].update(gate='G_WRONG')),('h1-unknown-gate-appended','h1',lambda c,r:c['gates'].append({'gate':'G_UNKNOWN','ok':True,'detail':'synthetic'})),('h1-phase-changed','h1',lambda c,r:c.update(phase='tail')),('h1-ID-changed','h1',lambda c,r:c.update(id='h2')),('h5-nonempty-assertion','h5',lambda c,r:c['gates'].append({'gate':'G_UNKNOWN','ok':True,'detail':'synthetic'})),('h5-coverage-true','h5',lambda c,r:c.update(assertion_coverage=True)),('h1-coverage-false','h1',lambda c,r:c.update(assertion_coverage=False)),('h5-registry-coverage-exception-invalid','h5',lambda c,r:r['h5'].update(assertion_coverage=True))]
 for name,sid,change in mutations:
  case=copy.deepcopy(cases[sid]);reg=copy.deepcopy(registry);change(case,reg)
  try:compiler.selected_case_semantics(copy.deepcopy(case),copy.deepcopy(case),copy.deepcopy(case),sid,reg,semantic);rejected=False;why=None
  except ValueError as exc:rejected=True;why=str(exc)
  assert rejected,name;checks.append({'name':name,'source_summary_recorder_and_selected_rows_mutated_consistently':True,'hash_layer_invoked':False,'reached_semantic_gate_validation':True,'rejected':True,'semantic_reason':why})
 out,read=m.validate(root,healthy,source);healthy['original_files']=read;(o/'HEALTHY-GLOBAL-CONTRACT.after-gate-fix.json').write_text(json.dumps(healthy,indent=2)+'\n');(o/'02-healthy-global-after-gate-fix.json').write_text(json.dumps({'result':out,'original_files':read,'unique_originals':len(read),'semantic_negative_controls':checks,'original1191closure_unchanged':True,'original_source_files_unchanged':True},indent=2)+'\n');print('healthy PASS',len(read),'semantic controls',len(checks),flush=True)
 full={'kind':'p25-actual-full','epoch':source,'sidecar_bases':{'fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'},'engine_source':prefix+'regressions/vendor/p25-runtime/smoke_p25.py','engine_sha256':'f931dca20e5be294e250f551063f282c97233fe812ed54ced56d32809a6e254d','healthy':copy.deepcopy(healthy)}
 try:m.validate(root,full,source);rejected=False
 except (ValueError,KeyError,OSError):rejected=True
 assert rejected;(o/'03-full15-still-closed.json').write_text(json.dumps({'valid_engine_and_real_healthy12_supplied':True,'absent_h10_full15_rejected':True,'live_calls':0},indent=2)+'\n')
 # Failure history cannot be relabeled and retained data cannot silently grow or change.
 old=m.Reader;controls=[]
 for name,suffix,change in [('no-invented-pre-Unix-rc','FAILED-ORIGINAL-AUDIT.json',lambda d:d.update(pre_subprocess_original_rc=0)),('no-invented-crash-actions','FAILED-ORIGINAL-AUDIT.json',lambda d:d.update(lifecycle_counts={'kill':1,'start':1})),('retained-must-be-four-exact-rows','retained-failed-h10-created-handles.json',lambda d:d['handles'][0].update(name='changed'))]:
  hit=[False]
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel)
    if rel.endswith(suffix):doc=json.loads(raw);change(doc);hit[0]=True;return (json.dumps(doc,indent=2)+'\n').encode()
    return raw
  m.Reader=Overlay;proof=copy.deepcopy(p);proof.pop('original_files')
  try:m.validate(root,proof,source);rejected=False
  except (ValueError,KeyError,OSError):rejected=True
  m.Reader=old;assert hit[0] and rejected,name;controls.append({'name':name,'rejected':True})
 (o/'04-failed-history-controls.json').write_text(json.dumps({'negative_controls':controls,'live_calls':0},indent=2)+'\n');print('PASS all')
if __name__=='__main__':
 import sys
 run(Path(sys.argv[1]).resolve())
