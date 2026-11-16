#!/usr/bin/env python3
"""Fixed independent P25 origins, source URL and copied read-only closure."""
import copy,importlib.util,json,sys,tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent;sys.path.insert(0,str(HERE));sys.path.insert(0,str(HERE.parent))
import p26_p25_runtime as R
import p26_legacy_outer as O
import p26_h13_ledger_read as L

def main():
 P=R.P26;root=P/'results/w10/S2-seven-stage-ready-attempt1';argvs=[];refused=[]
 with patch('subprocess.run',side_effect=AssertionError('no child')),patch('socket.socket',side_effect=AssertionError('no network')):
  tail=['--fixture-src-url',R.FIXTURE_SOURCE_URL]
  R.validate_stage_origins(dict(R.STAGE_BASES),'engine',tail)
  for label,mutate in [('old-fixture',lambda b:b.update(fixture='http://127.0.0.1:17095')),('old-proxy',lambda b:b.update(proxy_admin='http://127.0.0.1:17096')),('changed-C',lambda b:b.update(connector='http://127.0.0.1:17097')),('extra-origin',lambda b:b.update(extra='http://127.0.0.1:17105'))]:
   bases=dict(R.STAGE_BASES);mutate(bases)
   try:R.validate_stage_origins(bases,'engine',tail)
   except ValueError:refused.append(label)
   else:raise AssertionError(label)
  for value in ('http://p25fixture:8090','http://p26fixture:8090','http://unowned:8090'):
   try:R.validate_stage_origins(dict(R.STAGE_BASES),'h10',['--fixture-src-url',value])
   except ValueError:refused.append(value)
   else:raise AssertionError(value)
  for path in root.glob('p25-*-root-release-candidate.json'):
   rel=json.loads(path.read_text());R.validate_stage_origins(rel['p25_bases'],rel['p25_mode'],rel['p25_argv']);cmd=O.child_argv(rel,path,Path(json.loads((root/'INDEX.json').read_text())['candidate_source_artifact']['path']));assert cmd==rel['legacy_child_argv'];argvs.append({'candidate':str(path),'actual_derived_child_argv':cmd})
  q=R.load('collect_http_quiet_p25.py');assert q.BASES=={k:R.STAGE_BASES[k] for k in ('fixture','proxy_admin')}
  ledger=json.loads((P/'regressions/vendor/p25-closure-ledger.json').read_text());row=next(x for x in ledger['files'] if Path(x['copy']).name=='collect_http_quiet_p25.py');raw=Path(row['origin']).read_bytes();assert R.H.sha(row['origin'])==row['origin_sha256'];text=raw.decode()
  for pp in row['patches']:assert text.count(pp['old'])==pp['count'];text=text.replace(pp['old'],pp['new'])
  assert text.encode()==Path(row['copy']).read_bytes() and R.H.sha(row['copy'])==row['copy_sha256']
  with tempfile.TemporaryDirectory(dir=P/'results/w10') as td:
   td=Path(td);summary=td/'summary.json';summary.write_text('{}');rel={'p25_mode':'h13-ledger','p25_tool_shas':{'p26_h13_ledger_read.py':L.H.sha(L.__file__)},'p25_inputs':{str(summary):L.H.sha(summary)},'p25_output':str(td),'p25_bases':R.STAGE_BASES};calls=[]
   def http(url,auth=None):calls.append(url);return SimpleNamespace(status=200,body=b'[]',headers={},error=None)
   with patch.object(L.H,'validate_release',return_value=rel),patch.object(L.H.W,'http_call',side_effect=http),patch.object(sys,'argv',[L.__file__,'--release-file',str(td/'root.json'),'--summary',str(summary),'--output',str(td/'read')]):assert L.main()==0
   assert calls==['http://127.0.0.1:17103/admin/log/p25h13']
 print(json.dumps({'ok':True,'actual_calls':0,'exact_derived_argv':argvs,'refused':refused,'quiet_new_copy_provenance_reconstructed':True,'h13_real_collector_fake_HTTP_url':calls},indent=2))
if __name__=='__main__':main()
