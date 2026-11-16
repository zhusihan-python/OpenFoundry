#!/usr/bin/env python3
"""S2 J recreate argv and fail-closed Root image/config pins; no IO children."""
import copy,importlib.util,json,sys
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True
P=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('deadline_s2',P/'deployment/legacy_deadline_driver.py');D=importlib.util.module_from_spec(spec);spec.loader.exec_module(D)
def main():
 r={'candidate_source':D.S2_SOURCE,'expected_candidate_images':{'connector':D.S2_IMAGE},'driver_config_shas':{p:D.sha(D.HERE/p) for p in ('legacy-stage/compose.yaml',D.S2_OVERRIDE,'legacy-stage/deadline7.yaml')}}
 checks=[]
 with patch('subprocess.run',side_effect=AssertionError('no live child')),patch('socket.socket',side_effect=AssertionError('no live network')):
  for action in ('up-deadline:7','restore'):
   actual=D.compose_prefix(r,action);files=[str(D.HERE/'legacy-stage/compose.yaml'),str(D.HERE/D.S2_OVERRIDE)]+([str(D.HERE/'legacy-stage/deadline7.yaml')] if action=='up-deadline:7' else [])
   assert actual==['docker','compose']+[v for p in files for v in ('-f',p)];checks.append({'action':action,'actual_derived_recreate_argv':actual+['--project-name','openfoundry-p26-legacy','up','-d','--no-deps','--no-build','--pull=never','--force-recreate',D.NAME]})
  for kind in ('image-missing','image-old','override-missing','override-drift','base-drift','deadline-drift','source-unknown'):
   bad=copy.deepcopy(r)
   if kind=='image-missing':bad.pop('expected_candidate_images')
   elif kind=='image-old':bad['expected_candidate_images']['connector']=D.IMAGE
   elif kind=='override-missing':bad['driver_config_shas'].pop(D.S2_OVERRIDE)
   elif kind=='source-unknown':bad['candidate_source']='0'*64
   else:bad['driver_config_shas'][{'override-drift':D.S2_OVERRIDE,'base-drift':'legacy-stage/compose.yaml','deadline-drift':'legacy-stage/deadline7.yaml'}[kind]]='0'*64
   for action in ('up-deadline:7','restore'):
    try:D.compose_prefix(bad,action)
    except (AssertionError,KeyError):pass
    else:raise AssertionError(kind+' allowed '+action)
   checks.append({'negative':kind,'both_actions_refused':True})
 print(json.dumps({'ok':True,'actual_calls':0,'driver_sha256':D.sha(D.__file__),'checks':checks},indent=2))
if __name__=='__main__':main()
