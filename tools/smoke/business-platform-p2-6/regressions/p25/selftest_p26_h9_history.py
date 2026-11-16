#!/usr/bin/env python3
"""Actual captured failed h9 through the full NEW bundle validation path."""
import copy,contextlib,io,json,sys,tempfile
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True;sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_p25_runtime as R
import p26_healthy_bundle as B
from selftest_p26_p25_closure import healthy_bundle_test

def main():
 checks=[];failed=R.P26/'results/w10/S2-seven-stage-ready-attempt1/p25-h9-runtime-actual/summary.json';outer=R.P26/'results/w10/S2-seven-stage-ready-attempt1/p25-h9-outer-actual';failedsha=R.H.sha(failed)
 with tempfile.TemporaryDirectory(dir=R.P26/'results/w10') as td,patch('socket.socket',side_effect=AssertionError('no network')),patch('subprocess.run',side_effect=AssertionError('no child')),contextlib.redirect_stdout(io.StringIO()):
  root=Path(td);healthy_bundle_test(root);manifestpath=root/'manifest.json';manifest=json.loads(manifestpath.read_text());oi=json.loads((outer/'RESULT.json').read_text());source={'summary':str(failed),'step':str(outer/'child'),'runtime_result':oi['links']['runtime_result']['path'],'original_rc':1,'case':'h9','history_only':True};manifest['sources'].append(source);history_index=len(manifest['sources'])-1;actual=json.loads(failed.read_text());handles=json.loads(Path(actual['attempts']['handles_file']).read_text())['handles']
  # Only pure fixture final-source/quiet inputs are extended. Captured real h9
  # raw/rc/runtime/Root/summary/recorders remain original and read-only.
  final=json.loads(Path(manifest['sources'][manifest['final_source_index']]['summary']).read_text());fp=Path(final['attempts']['handles_file']);hd=json.loads(fp.read_text());hd['handles']+=handles;hd['count']=len(hd['handles']);fp.write_text(json.dumps(hd));defs={h['ref'] for h in hd['handles'] if h.get('ok') and h.get('kind') in ('def','h10-def')};fr=next(Path(final['attempts']['evidence_dir']).glob('finally-attempt-9999.json'));rd=json.loads(fr.read_text());rd['defs']=sorted(defs);fr.write_text(json.dumps(rd))
  q=R.load('collect_http_quiet_p25.py');fixture=R.load('selftest_aggregate_healthy_p25.py');hps=[json.loads(Path(s['summary']).read_text())['attempts']['handles_file'] for s in manifest['sources']];proof,pp=q.collect(hps,root/'history-global-quiet',api_factory=lambda e,rec:fixture.API(rec));qs=root/'history-global-quiet-step';qs.mkdir();(qs/'cmd.json').write_text(json.dumps(['python3','-I',str(R.COPY/'collect_http_quiet_p25.py'),'--released']+sum((['--handles',p] for p in hps),[])));(qs/'rc').write_text('0\n');(qs/'stdout.log').write_text(json.dumps({'ok':True,'proof_file':str(pp)}));(qs/'stderr.log').write_text('');manifest['quiet_proof']=str(pp);manifest['quiet_step']=str(qs)
  def build(doc):manifestpath.write_text(json.dumps(doc));return B.build(manifestpath)
  result,union=build(manifest);assert result['original_failures_preserved'][-1]['history_only'] is True and result['original_failures_preserved'][-1]['failed']==['finally-quiet'];assert any(p['summary_path']==str(failed) for p in result['sources']);assert all(any((x.get('kind'),x.get('ref'))==(h.get('kind'),h.get('ref')) for x in json.loads(union)['handles']) for h in handles if h.get('ref'));checks.append('captured failedh9 retained unselected; all731handle origins preserved')
  for fault in ('select-history','remove-history-flag','fake-rc0'):
   bad=copy.deepcopy(manifest)
   if fault=='select-history':bad['selection']['h9']=history_index
   elif fault=='remove-history-flag':bad['sources'][-1].pop('history_only')
   else:bad['sources'][-1]['original_rc']=0
   try:build(bad)
   except ValueError:pass
   else:raise AssertionError(fault)
   checks.append(fault+' refused')
  original_load=R.load
  for fault in ('different-global-failure','empty-gates'):
   aggregate=original_load('aggregate_healthy_p25.py');original_source=aggregate.source
   def parsed(*a,**kw):
    values=list(original_source(*a,**kw))
    if str(a[0])==str(failed):
     values=copy.deepcopy(values)
     if fault=='different-global-failure':values[0]['failed']=['finally-rosters']
     else:values[1]['h9']['gates']=[]
    return tuple(values)
   aggregate.source=parsed
   with patch.object(R,'load',side_effect=lambda name:aggregate if name=='aggregate_healthy_p25.py' else original_load(name)):
    try:build(manifest)
    except ValueError:pass
    else:raise AssertionError(fault)
   checks.append(fault+' refused after original raw validation')
 assert R.H.sha(failed)==failedsha
 print(json.dumps({'ok':True,'actual_calls':0,'real_failed_summary_sha256_unchanged':failedsha,'checks':checks},indent=2))
if __name__=='__main__':main()
