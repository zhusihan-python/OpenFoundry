#!/usr/bin/env python3
"""Pure selected snapshot checkpoint404 projection; no fixture dependency."""
import importlib.util,json,sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
sys.dont_write_bytecode=True
P=Path(__file__).resolve().parents[2];spec=importlib.util.spec_from_file_location('receipt_controls',P/'deployment/final_dataset_receipt_replay.py');M=importlib.util.module_from_spec(spec);spec.loader.exec_module(M)
def main():
 cp={'error':'incremental checkpoint not initialized for this sync definition'}
 def collect(status=404,body=cp):
  def public(url):
   assert url==M.W.ORIGINS['connector']+'/api/v1/data-connection/syncs/'+M.DEF+'/checkpoint'
   return SimpleNamespace(status=status,body=json.dumps(body).encode()),None
  return M.snapshot_checkpoint(public)
 with patch('subprocess.run',side_effect=AssertionError('no child')),patch('socket.socket',side_effect=AssertionError('no network')):
  before=collect();assert collect()==before and before['status']==404 and 'epoch' not in before
  for status,body in [(200,cp),(401,cp),(404,{'error':'sync job not found'}),(404,dict(cp,confirmed_epoch=0))]:
   try:collect(status,body)
   except AssertionError:pass
   else:raise AssertionError('bad CP accepted')
 print(json.dumps({'ok':True,'checks':['selected ordinary snapshot exact uninitialized CP404 route/body','same before/after projection','initialized/unauthorized/missingDef/extraEpoch refused'],'fixture_calls_required':False,'actual_calls':0},indent=2))
if __name__=='__main__':main()
