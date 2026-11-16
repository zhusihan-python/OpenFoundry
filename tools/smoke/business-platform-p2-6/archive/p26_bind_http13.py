"""Bind exact completed S2 HTTP10 originals. No network or producer imports."""
import hashlib,importlib.util,json
from pathlib import Path
B=Path('tools/smoke/business-platform-p2-6');O=B/'results/w9/2026-11-15-w9-attempt13';S='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
def doc(p):return json.loads(Path(p).read_text())
def rel(p):return str(Path(p).relative_to(Path.cwd())) if str(p).startswith('/') else str(p)
summary=doc(B/'results/w7/S2-HTTP10-actual-closure-attempt1/SUMMARY.json');first=doc(B/'results/w9/2026-11-15-w9-attempt12/S2-first3-contract.json');old=doc(B/'archive/p26_http10_contract.draft.json')['proof'];templates={x['case']:x for x in old['cases']};first={x['case']:x for x in first['proofs']};cases=[]
ready=B/'results/w7/S2-http-ready-attempt1/READY-INDEX.json';tools={x['snapshot']:x['sha256'] for x in doc(ready)['tools']}
for c in summary['cases']:
 if c['case'] in first:cases.append(first[c['case']]);continue
 p=dict(templates[c['case']]);p.pop('original_files',None);p.pop('current_supplement',None);p.update(epoch=S,epoch_schema='S2',inner=rel(c['inner']),outer=rel(c['outer']),tools_checkpoint=str(ready),historical_tools=tools);d=Path(p['inner']);p['pins']=doc(Path(p['outer'])/'pins.json')
 ms={f.name:doc(f) for f in d.glob('*.meta')};p['owned_stdout_inputs']={f.name:f.read_text() for f in d.glob('ops-*.stdout.raw')};p['owned_rc_count']=len([x for x in ms if x.startswith('ops-')]);p['semantic_inputs']={k:v.get('safe_response',v) for k,v in ms.items() if not k.startswith('ops-')};p['transient_rcs']={k:int((d/(k[:-5]+'.rc')).read_text()) for k,v in ms.items() if v.get('original_http_status','absent') is None}
 p['downloads']={}
 for m in ms.values():
  if m.get('download_artifact'):
   f=m['download_artifact'];raw=(d/f).read_bytes();p['downloads'][f]={'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'rows':len(raw.splitlines())}
 cases.append(p)
p={'kind':'http10','epoch':S,'epoch_schema':'S2','cases':cases,'scope_limits':'S2 original10 plus separate CURRENT and postguard; final legacy/restart/cleanup/secret pending'}
p.update(canonical_root_label=str(Path.cwd()),closure_summary=str(B/'results/w7/S2-HTTP10-actual-closure-attempt1/SUMMARY.json'),preservation_baseline='tools/smoke/business-platform-p2-6/results/w10/S2-image-runtime-guards/dataset-attempt1/guard-20261115T095807Z-3a0efb',current_summary=str(B/'results/w7/S2-cross-CURRENT-outer-attempt2-20261115T112835Z/SUMMARY.json'),history_originals=[])
for pattern in ['S2-outer-healthy_snapshot_and_legacy-20261115T105559Z','S2-cross-CURRENT-outer-20261115T112605Z','S2-cross-CURRENT-supplement-20261115T112605Z']:
 for directory in (B/'results/w7').glob(pattern):
  for f in directory.rglob('*'):
   if f.is_file() and f.name=='SUMMARY.json':
    p['history_originals']+=doc(f).get('originals',[])
   if f.is_file() and f.suffix!='.env':p['history_originals'].append({'path':str(f.resolve()),'sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'size':f.stat().st_size})
(O/'S2-HTTP10-contract.json').write_text(json.dumps({'source':S,'proof':p},indent=2)+'\n')
