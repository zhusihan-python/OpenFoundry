#!/usr/bin/env python3
"""Real copied P24 helpers/parsers with fake PG-q process output only."""
import contextlib,copy,io,json,os,subprocess,sys,tempfile
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent));sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import w10_suite_adapter as A
import p26_legacy_outer as O

def main():
 P=A.GUARD.P26;failed=P/'results/w10/S2-seven-stage-ready-attempt1/p24-full-commands-actual';samples=[]
 for p in failed.rglob('*.raw'):
  value=p.read_text(errors='replace')
  if value.startswith('BEGIN\n') and value.rstrip().endswith('COMMIT') and '|false' in value:samples.append((p,value))
 assert samples;origin,tagged=samples[0];fixed='http://127.0.0.1:17097';bindings=O.child_environment_bindings({'suite':'p24','p24_connector2_base':fixed})
 for bad in ({'suite':'p24'},{'suite':'p24','p24_connector2_base':'http://127.0.0.1:17089'}):
  try:O.child_environment_bindings(bad)
  except ValueError:pass
  else:raise AssertionError('missing/drift root origin accepted')
 with patch('socket.socket',side_effect=AssertionError('no network')),tempfile.TemporaryDirectory(dir=P/'results/w10') as td,contextlib.redirect_stdout(io.StringIO()):
  td=Path(td);(td/'.env').write_text('OF_P1_PG_PASSWORD=FAKE\nOF_P1_JWT_SECRET=FAKE\nOF_P1_SERVICE_JWT=FAKE\n');m=A.load(A.TREE/'business-platform-p2-4/smoke_p24.py');m.EV=[]
  # Reproduce the original copied loader: .env does not change process env.
  env={'OF_P24_CONNECTOR2_BASE':'http://127.0.0.1:17089'};env.update(bindings)
  with patch.dict(os.environ,env):m.load_helpers(td)
  assert m.M.BASES['connector2']==fixed
  with patch.object(m,'psql_read',return_value=tagged):
   try:m.stamp_of('11111111-1111-4111-8111-111111111111')
   except RuntimeError:pass
   else:raise AssertionError('captured actual tagged raw accepted')
  calls=[]
  def fake_pg(argv,*args,**kwargs):
   calls.append(list(argv));assert '-q' in argv or '-qAt' in argv;ci=argv.index('-c');sqls=argv[ci+1::2];assert sqls[0]=='BEGIN READ ONLY;' and sqls[-1]=='COMMIT;'
   sql=sqls[1];row='false|true\n' if 'schedule_enabled' in sql else '2026-11-15 12:00:38.449283+00|false\n'
   return subprocess.CompletedProcess(argv,0,row,'')
  facade=A.Facade(fake_pg,{'suite':'p24','stage':'p24-full','owned_sql_templates':[]},td/'ROOT-not-live.json');facade.require_phase=lambda:None
  with patch.object(A.subprocess,'run',side_effect=facade.run):
   assert m.stamp_of('11111111-1111-4111-8111-111111111111')=='2026-11-15 12:00:38.449283+00'
   assert m.persisted_disabled('22222222-2222-4222-8222-222222222222') is True
  assert all(r['rc']==0 for r in facade.records);assert all('BEGIN READ ONLY;' in r['replacement_argv'] for r in facade.records)
 print(json.dumps({'ok':True,'actual_calls':0,'captured_failed_original':str(origin),'captured_tagged_raw_still_rejected_by_unchanged_parser':True,'correct_Root_A17097_overrides_wrong_inherited_17089':True,'missing_drift_negatives':2,'fake_pg_q_rows_accepted_by_unchanged_real_stamp_and_persisted_parsers':True,'derived_actual_argv':calls},indent=2))
if __name__=='__main__':main()
