#!/usr/bin/env python3
"""Real copied h9 orchestration + runtime wrapper; fake external counters."""
import contextlib,io,json,os,sys,tempfile,types
from pathlib import Path
from unittest.mock import patch
sys.dont_write_bytecode=True;sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_p25_runtime as R
import p26_p25_export as E

def main():
 checks=[]
 with patch('socket.socket',side_effect=AssertionError('no network')),patch('subprocess.run',side_effect=AssertionError('no child')),tempfile.TemporaryDirectory(dir=R.P26/'results/w10') as td,contextlib.redirect_stdout(io.StringIO()):
  td=Path(td)
  for fault in ('natural','timeout','badstats','othercase'):
   m=R.load('smoke_p25.py');clock={'t':0.};probes=[];fixture=m.FakeIncrementalFixture();proxy=m.FakeProxyDataset();conn=m.FakeConnector(fixture,proxy);base=m.FakeApi
   class API(base):
    def __init__(self,bases,jwt,rec,scenario):super().__init__(conn,fixture,proxy,rec,scenario)
    def call(self,b,method,path,body=None,**kwargs):
     if self.scenario=='h9-natural-drain':
      assert method=='GET';probes.append((b,path));active=1 if fault=='timeout' or len(probes)<5 else 0
      obj={'name':'p25h6','inventory_started':4,'inventory_finished':4-active,'active':active} if b=='fixture' else {'commit_started':20,'commit_finished':20,'commit_inflight':0}
      if fault=='badstats':obj['active']=True
      self.rec.record(self.scenario,'fake-external-GET',{'base':b,'method':method,'path':path,'status':200,'body':obj})
      return {'status':200,'json':obj,'error':None}
     return super().call(b,method,path,body,**kwargs)
   m.HttpApi=API;out=td/fault;out.mkdir();ctx=object.__new__(R.Context);ctx.engine=lambda:m;ctx.mode='engine';ctx.attempt=R.H.W.Attempt(out,'fake-wrapper');ctx.facade=types.SimpleNamespace(run=lambda *a,**k:(_ for _ in ()).throw(AssertionError('no child')))
   case='h5' if fault=='othercase' else 'h9';tail=['--phase','healthy','--only',case,'--released','--evidence-dir',str(out/'evidence'),'--result-file',str(out/'summary.json'),'--fixture-src-url',R.FIXTURE_SOURCE_URL]
   with patch.object(R.time,'monotonic',side_effect=lambda:clock['t']),patch.object(R.time,'sleep',side_effect=lambda seconds:clock.__setitem__('t',clock['t']+seconds)),patch.dict(os.environ,P25_CONFIRM='yes',OPENFOUNDRY_SMOKE_JWT='FAKE',OPENFOUNDRY_SMOKE_JWT_ORG2='FAKE2'),patch.object(E,'export_summary',return_value=None):
    try:rc=ctx.invoke_engine(tail)
    except ValueError:
     assert fault in ('timeout','badstats');assert (out/'summary.json').exists() and ctx.h9_drain_failure
     native=json.loads((out/'summary.json').read_text());assert len(native['scenarios'][0]['gates'])==8 and native['quiet']['ok'] is True
    else:
     assert fault in ('natural','othercase') and rc==0;s=json.loads((out/'summary.json').read_text());assert s['quiet']['ok'] and s['failed']==[]
     if case=='h9':assert len(s['scenarios'][0]['gates'])==8 and all(g['ok'] for g in s['scenarios'][0]['gates']) and len(probes)>=6
     else:assert not probes
   checks.append({'case':fault,'pass':True,'drain_fake_GETs':len(probes),'elapsed_fake_seconds':clock['t']})
 print(json.dumps({'ok':True,'actual_calls':0,'checks':checks},indent=2))
if __name__=='__main__':main()
