#!/usr/bin/env python3
"""Bounded offline cached Go unit evidence; never reaches q3/network/SQL."""
import argparse,datetime,hashlib,json,re,subprocess,sys
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(P26))
from common import p26_gates as G
IMAGE='sha256:699337d620559a59b4a2bb298ad59611e535d2ee755a34cf2d2a98f37578dc80'
TEST='services/connector-management-service/internal/handlers/sync_recovery_test.go'
FOCUSED='^TestRunSyncJob(JWTDeadlineUsesAdmittedProtocol|V1JWTExpiryDuringPullZeroGateSendAndSlots)$'
def main():
 p=argparse.ArgumentParser();p.add_argument('stage');p.add_argument('--kind',choices=['format','format-check','version','focused','handlers'],required=True);p.add_argument('--race',action='store_true');a=p.parse_args()
 if not re.fullmatch('[A-Za-z0-9_-]+',a.stage):raise ValueError('exclusive named stage only')
 out=P26/'results/w10/legacy-jwt-compat'/a.stage;out.mkdir(parents=True,exist_ok=False)
 source_before=G.candidate_hash(G.OPENFOUNDRY)
 modules={n:hashlib.sha256((G.OPENFOUNDRY/n).read_bytes()).hexdigest() for n in ('go.mod','go.sum')}
 cmd=['docker','run','--pull=never','--rm','--network','none','-v',str(G.OPENFOUNDRY)+':/src:ro','-v','openfoundry-gomod:/go/pkg/mod','-v','openfoundry-gobuild:/root/.cache/go-build','-w','/src/services/connector-management-service','-e','GOTOOLCHAIN=local','-e','GOFLAGS=-mod=readonly','-e','GOPROXY=off','-e','GOSUMDB=off','-e','CGO_ENABLED='+('1' if a.race else '0'),IMAGE]
 if a.kind=='format':cmd+=['gofmt','/src/'+TEST]
 elif a.kind=='format-check':cmd+=['gofmt','-d','/src/services/connector-management-service/internal/handlers/handlers.go','/src/'+TEST]
 elif a.kind=='version':cmd+=['go','version']
 else:cmd+=['go','test','-count=1','-timeout=8m','-v']+(['-race'] if a.race else [])+(['-run',FOCUSED] if a.kind=='focused' else [])+['./internal/handlers']
 (out/'cmd.json').write_text(json.dumps(cmd,indent=2)+'\n');(out/'source.before.json').write_text(json.dumps({'source_sha256':source_before,'Go_SQL_count':402,'module_sha256':modules},indent=2)+'\n');(out/'started.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})+'\n')
 rc=None
 try:
  with (out/'stdout.raw').open('xb') as stdout,(out/'stderr.raw').open('xb') as stderr:
   child=subprocess.run(cmd,stdout=stdout,stderr=stderr,timeout=600);rc=child.returncode
  (out/'rc').write_text(str(rc)+'\n')
  if a.kind=='format' and rc==0:(G.OPENFOUNDRY/TEST).write_bytes((out/'stdout.raw').read_bytes())
 except Exception as exc:
  (out/'rc').write_text('unavailable\n');(out/'exception.json').write_text(json.dumps({'error':type(exc).__name__,'original_rc':None})+'\n');raise
 finally:
  after=G.candidate_hash(G.OPENFOUNDRY);aftermodules={n:hashlib.sha256((G.OPENFOUNDRY/n).read_bytes()).hexdigest() for n in modules}
  (out/'source.after.json').write_text(json.dumps({'source_sha256':after,'module_sha256':aftermodules,'module_unchanged':aftermodules==modules},indent=2)+'\n')
  (out/'RESULT.json').write_text(json.dumps({'original_rc':rc,'source_before':source_before,'source_after':after,'source_unchanged':source_before==after,'format_only_source_change':a.kind=='format','module_unchanged':aftermodules==modules,'image':IMAGE,'network':'none','unit_scope':'real HTTP handler/coordinator + unit fake storage/httptest in isolated loopback; no q3 or PG','ended_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
 print(json.dumps({'stage':a.stage,'original_rc':rc,'source_before':source_before,'source_after':after,'output':str(out)}))
 return rc
if __name__=='__main__':sys.exit(main())
