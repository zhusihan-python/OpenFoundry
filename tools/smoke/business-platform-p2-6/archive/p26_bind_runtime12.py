"""Read-only actual completed runtime contract builder. No private env reads."""
import hashlib,json,os,shutil
from pathlib import Path
B=Path('tools/smoke/business-platform-p2-6');D=B/'results/w6/deployment';O=B/'results/w9/2026-11-15-w9-attempt12';S='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
def doc(p):return json.loads(Path(p).read_text())
def rel(x):return str(Path(x).relative_to(Path.cwd())) if str(x).startswith('/') else str(x)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
index={}
for directory,ds,fs in os.walk(B):
 for n in fs:
  p=Path(directory)/n
  if p.suffix in ('.py','.yaml','.yml') and not p.is_symlink():index.setdefault(sha(p),p)
def toolcopy(digest):
 if digest not in index:raise ValueError('missing actual historical tool '+digest)
 src=index[digest];dest=O/'runtime-tools'/ (digest+src.suffix);dest.parent.mkdir(exist_ok=True)
 if not dest.exists():shutil.copyfile(src,dest)
 return {'path':str(dest),'sha256':digest,'origin':str(src)}
def group(directory,outer,rc):
 d=next(Path(directory).glob('commands-*')) if Path(directory).name.startswith('S2-') else Path(directory);steps={};counts={'argv':0,'http':0}
 for f in d.glob('*.rc'):
  name=f.name[:-3];m=doc(d/(name+'.meta.json'));steps[name]={'meta':m,'command':doc(d/(name+'.cmd.json')),'raw_sha256':sha(d/(name+'.raw'))};counts['argv' if m['kind']=='call' else m['kind']]+=1
 return {'directory':str(d),'steps':steps,'count':len(steps),'kinds':counts,'outer':str(outer),'outer_rc_file':'rc','outer_expected_rc':rc,'outer_inputs':[x.name for x in Path(outer).iterdir() if x.name!='rc']}
def guards(directory):
 out=set()
 def walk(v):
  if isinstance(v,dict):
   for k,x in v.items():
    if k=='evidence_dir' and isinstance(x,str) and '/guard-' in x:out.add(rel(x))
    walk(x)
  elif isinstance(v,list):
   for x in v:walk(x)
 for f in Path(directory).rglob('*.raw'):
  if 'typed-guard' in f.name:
   try:walk(json.loads(f.read_text()))
   except ValueError:pass
 for f in Path(directory).rglob('*.json'):
  if f.name in ('RESULT.json','composite-proof.json'):walk(doc(f))
 return sorted(out)
configs=[('dataset','S2-main-dataset-upgrade-attempt1','S2-dataset-outer-20261115T095807Z',None,None),('connector','S2-main-connector-upgrade-attempt1','S2-connector-outer-20261115T100316Z','S2-connector-readonly-completion-attempt1','S2-connector-completion-outer-20261115T103257Z'),('p26connectorc','S2-legacy-C-upgrade-attempt2','S2-legacy-C2-outer-20261115T103801Z','S2-legacy-C-readonly-tail-attempt1','S2-C-tail-outer-20261115T104318Z'),('p26connectora','S2-legacy-A-upgrade-attempt1','S2-legacy-A-outer-20261115T104804Z',None,None),('p26connectorb','S2-legacy-B-upgrade-attempt1','S2-legacy-B-outer-20261115T105003Z',None,None)]
p={'preservation_anchor_contract':str(B/'archive/p26_P1pre9_contract.draft.json'),'kind':'s2-runtime-aggregate','epoch':S,'upgrades':{},'supplementary_originals':{}}
for service,inner,outer,completion,co in configs:
 inner=D/inner;outer=D/outer;groups=[group(inner,outer,1 if completion else 0)];last=D/completion if completion else inner
 if completion:groups.append(group(last,D/co,0))
 first=Path(groups[0]['directory']);intent=inner/'command-intent.json';names=list(first.glob('*target-before-facts.raw'));after=list(first.glob('*target-after-facts.raw'))
 transition=doc(inner/'image-transition.json') if (inner/'image-transition.json').exists() else doc(last/'composite-proof.json');current=transition['current'];toolsets=[]
 for od in [outer]+([D/co] if completion else []):
  bf=od/('before.json' if (od/'before.json').exists() else 'meta-before.json');af=od/('after.json' if (od/'after.json').exists() else 'meta-after.json');bd=doc(bf);ad=doc(af);digests=dict(bd.get('tools',{}));digests.update({k:v for k,v in bd.items() if k in ('bridge_sha256','executor_sha256')});toolsets.append({'before':str(bf),'after':str(af),'before_source':'source' if 'source' in bd else 'source_before','after_source':'source' if 'source' in ad else 'source_after','blobs':{key:toolcopy(value) for key,value in digests.items()}})
 p['upgrades'][service]={'groups':groups,'intent':str(intent),'before_facts':str(names[0]),'after_facts':str(after[0]),'before_names':str(next(first.glob('*target-before-env_names.raw'))),'after_names':str(next(first.glob('*target-after-env_names.raw'))),'current':current,'guards':guards(inner)+ ([] if last==inner else guards(last)),'tools':toolsets}
 pairs={}
 for beforefile in first.glob('*-before-*.raw'):
  name=__import__('re').sub(r'^\d+-before-','',beforefile.stem)
  if name==service:continue
  candidates=[]
  for g in groups:
   for tag in ['after','unchanged','fresh']:candidates+=list(Path(g['directory']).glob('*-'+tag+'-'+name+'.raw'))
  if candidates:pairs[name]={'before':str(beforefile),'after':str(candidates[-1])}
 p['upgrades'][service]['preserved_facts']=pairs
 # Completed exact files only; no Root/private inputs or current-writing attempts.
 for directory in [inner,outer]+([last,D/co] if completion else []):
  for f in directory.rglob('*'):
   if f.is_file() and not f.is_symlink() and f.suffix!='.env':p['supplementary_originals'][str(f)]={'sha256':sha(f),'size':f.stat().st_size}
aux=D/'http-S2-cohort-20261115T105328Z-5b9590';a=group(aux,D/'S2-aux2-start-outer-20261115T105328Z',0);a['expected_rc1']=[n for n,s in a['steps'].items() if s['meta'].get('original_child_rc')==1];a.update(ledger=str(aux/'created-ledger.json'),schema=str(aux/'w7_aux.json'),facts={role:str(next(aux.glob('*new-p26connector'+role+'2.raw'))) for role in ['a','b']},guards=guards(aux));a['preserved_facts']={}
for f in aux.glob('*preserve-before-*.raw'):
 name=f.stem.split('preserve-before-',1)[1];a['preserved_facts'][name]={'before':str(f),'after':str(next(aux.glob('*preserve-after-'+name+'.raw')))}
a['tools']={k:toolcopy(v) for k,v in doc(Path(a['outer'])/'before.json')['tools'].items()}
p['aux2']=a
for f in aux.iterdir():
 if f.is_file():p['supplementary_originals'][str(f)]={'sha256':sha(f),'size':f.stat().st_size}
mp=D/'S2-main-connector-upgrade-attempt1/tools-before-env-name-order-fix/MANIFEST.json'
p['shared_original_usedtools']={'manifest':str(mp),'rows':[{'origin':x['origin'],'copy':rel(x['copy']),'sha256':x['sha256']} for x in doc(mp)['files']]}
p['history_only']={}
roots=[D/'S2-legacy-C-upgrade-attempt1',D/'S2-legacy-C-outer-20261115T103443Z',B/'results/w7/S2-outer-healthy_snapshot_and_legacy-20261115T105559Z']
roots += [Path(x) for x in doc(roots[-1]/'case.meta.json')['inner_attempts']]
for directory in roots:
 p['history_only'][str(directory)]={'classification':'failed-original-history-only-never-formal','members':{rel(f):{'sha256':sha(f),'size':f.stat().st_size} for f in directory.rglob('*') if f.is_file() and not f.is_symlink() and f.suffix!='.env'}}
(O/'S2-runtime-contract.json').write_text(json.dumps(p,indent=2)+'\n')
print('contract groups',[(k,[x['count'] for x in v['groups']]) for k,v in p['upgrades'].items()])
