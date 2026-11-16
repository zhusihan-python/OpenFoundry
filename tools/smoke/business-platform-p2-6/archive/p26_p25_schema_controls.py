"""Read source AST only, validate registry and reject synthetic partial proofs."""
import copy,importlib.util,json
from pathlib import Path

def run():
 p=Path(__file__);s=importlib.util.spec_from_file_location('p25',p.with_name('p26_p25_proofs.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
 source=p.parents[1]/'regressions/vendor/p25-runtime/smoke_p25.py';registry=m.specs(source.read_bytes());assert len(registry)==15
 counts={phase:sum(x['phase']==phase for x in registry.values()) for phase in ('healthy','tail')};assert counts=={'healthy':13,'tail':2}
 cases=[]
 for name,row,spec in [('missing-mandatory-gate',{'failed':False,'phase':'healthy','gates':[]},registry['h1']),('failed-original-cannot-selected',{'failed':True,'phase':'healthy','gates':[]},registry['h1']),('unknown-gate-ID',{'failed':False,'phase':'healthy','gates':[{'gate':'not-an-original-gate','ok':True,'detail':'synthetic'}]},registry['h1'])]:
  try:m.good_case(row,spec);rejected=False
  except ValueError:rejected=True
  assert rejected;cases.append({'name':name,'rejected':True})
 class NoOriginals:
  def read(self,path):raise ValueError('no actual P25 originals')
 for name,proof in [('partial-ID-only',{}),('wrong-sidecar-base',{'epoch':'S2','sidecar_bases':{'fixture':'http://127.0.0.1:17095','proxy_admin':'http://127.0.0.1:17096'}}),('right-base-still-needs-originals',{'epoch':'S2','sidecar_bases':{'fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'},'engine_source':'absent'})]:
  try:m.full(NoOriginals(),proof,'S2');rejected=False
  except (ValueError,KeyError):rejected=True
  assert rejected;cases.append({'name':name,'rejected':True})
 return {'status':'P25-CODE-SCHEMA-ONLY','source_registry':15,'phase_counts':counts,'healthy_selection_excludes_h10':12,'negative_controls':cases,'actual_P25_complete':False,'old_tools_imported_executed':False}
if __name__=='__main__':print(json.dumps(run(),indent=2))
