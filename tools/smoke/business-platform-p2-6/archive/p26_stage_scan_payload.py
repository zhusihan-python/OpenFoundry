"""Root-signed exact nonprivate scan1 staging. No service/secret/scan/package claim."""
import argparse,datetime,hashlib,importlib.util,json,os,stat,sys
from pathlib import Path
sys.dont_write_bytecode=True
S='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
SCHEMA='p26-scan1-staging-release-v1'
def need(ok,why):
 if not ok:raise ValueError(why)
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def relative(value):
 need(isinstance(value,str) and value and not value.startswith('/') and '\\' not in value and all(x not in ('','.','..') for x in value.split('/')),'unsafe member path')
 need(not any(x.lower().startswith('.env') or x.lower().endswith('.env') or '.env.' in x.lower() or x.lower().endswith('.body') for x in value.split('/')),'private env/body forbidden before filesystem access')
 return value
def regular(root,rel):
 p=Path(root);need(p.is_absolute() and not p.is_symlink(),'invalid origin root')
 for part in relative(rel).split('/'):
  p=p/part;need(not p.is_symlink(),'symlink member')
 need(stat.S_ISREG(p.stat().st_mode),'nonregular member');return p
def load_module(name):
 spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
def prepare(release,now=None):
 need(release['schema']==SCHEMA and release['phase']=='p26-nonprivate-scan1-staging' and release['root_accepted'] is True and release['source_sha256']==S,'explicit Root staging release/source required')
 now=now or datetime.datetime.now(datetime.timezone.utc);need(now<datetime.datetime.fromisoformat(release['expires_utc'].replace('Z','+00:00')),'expired staging phase')
 need(release['stage_tool_sha256']==digest(__file__),'stage tool byte pin')
 planpath=Path(release['members_manifest']);need(planpath.is_absolute() and not planpath.is_symlink() and digest(planpath)==release['members_manifest_sha256'],'exact externally pinned members manifest');plan=json.loads(planpath.read_bytes());need(plan['schema']=='p26-scan1-staging-members-v1' and plan['source_sha256']==S and plan['status']=='FROZEN-NONPRIVATE-STAGING-INPUTS','final cutoff/helper/creation closures must be frozen before staging')
 origins=release['origins'];need(set(origins)=={'workspace','Root-json'} and all(Path(x).is_absolute() for x in origins.values()),'explicit two nonprivate origin roots')
 dest=Path(release['destination']);output=Path(release['output']);need(dest.is_absolute() and output.is_absolute() and dest!=output and not dest.exists() and not output.exists() and not dest.is_relative_to(output) and not output.is_relative_to(dest),'new exclusive staging/output paths')
 for candidate in [dest,output,*map(Path,origins.values())]:
  need(not any(parent.is_symlink() for parent in [candidate,*candidate.parents]),'symlink origin/destination ancestry')
 for root in origins.values():need(not dest.is_relative_to(root) or dest.is_relative_to(Path(origins['workspace'])/'tools/smoke/business-platform-p2-6/results/w9'),'bounded fresh staging destination')
 need(release['support_tool_shas']=={'p26_package_assembler.py':digest(Path(__file__).with_name('p26_package_assembler.py'))},'Root-pinned assembler support bytes before import')
 expected_helpers=load_module('p26_package_assembler').TOOLS;need(set(plan['required_helpers'])==set(expected_helpers) and set(release['helper_shas'])==set(expected_helpers),'final required helper closure exact, no historical31-helper checkpoint')
 for name,digestvalue in release['helper_shas'].items():need(digest(Path(__file__).with_name(name))==digestvalue,'reviewed final helper bytes changed')
 core=load_module('p26_formal_core');pins=core.source_pins(Path(__file__).with_name('p26_source402_allowlist.json'));members=plan['members'];need(members and len({m['destination'] for m in members})==len(members),'nonempty unique exact members')
 sources={};helpers=set();resolved=[]
 for member in members:
  need(set(member)=={'origin','path','destination','sha256','size','role'} and member['origin'] in origins and member['role'] in {'source','helper','actual','history','root-json','metadata'},'explicit member metadata shape')
  relative(member['destination']);relative(member['path']);need(type(member['size']) is int and member['size']>=0 and len(member['sha256'])==64,'exact member size/hash')
  if member['origin']=='Root-json':need(member['path'].endswith('.json') and member['role']=='root-json','Root origin only explicitly mapped nonsecret JSON, no referenced-file traversal')
  elif member['role']!='source':need(member['path'].startswith('tools/smoke/business-platform-p2-6/'),'non-source workspace member scope')
  if member['role']=='source':need(member['origin']=='workspace' and member['path'] in pins and member['destination']=='source/'+member['path'] and {k:member[k] for k in ('sha256','size')}==pins[member['path']],'exact frozen source402+modsum source recipe');sources[member['path']]=member
  if member['role']=='helper':need(member['path']=='tools/smoke/business-platform-p2-6/archive/'+Path(member['destination']).name and member['destination']=='tools/'+Path(member['destination']).name and Path(member['destination']).name in expected_helpers,'exact final helper source/destination');helpers.add(Path(member['destination']).name)
  source=regular(origins[member['origin']],member['path']);need(source.stat().st_size==member['size'] and digest(source)==member['sha256'],'changed exact staged source member');resolved.append((source,member))
 actual=set()
 for subtree in ('services/connector-management-service/','services/dataset-versioning-service/','services/pipeline-build-service/','libs/scheduling-cron/'):
  base=Path(origins['workspace'])/subtree;need(base.is_dir(),'current source root absent')
  for directory,dirs,names in os.walk(base,followlinks=False):
   need(not any((Path(directory)/name).is_symlink() for name in dirs),'source directory symlink')
   for name in names:
    if name.endswith(('.go','.sql')) and (not subtree.startswith('libs/') or name.endswith('.go')):
     source=Path(directory)/name;need(not source.is_symlink() and source.is_file(),'source nonregular');actual.add(str(source.relative_to(origins['workspace'])))
 need(actual==set(pins)-{'go.mod','go.sum'},'current Go/SQL source membership differs from frozen402')
 need(set(sources)==set(pins) and len(sources)==404 and helpers==set(expected_helpers),'full404 source and required helper sets mandatory')
 need(plan['whole_failed_history_final_cutoff']['complete'] is True and plan['whole_failed_history_final_cutoff']['active_S2']==S,'actual final failed-history cutoff certificate required')
 return plan,dest,output,resolved

def stage(release_file):
 release_path=Path(release_file);raw=release_path.read_bytes();release=json.loads(raw);plan,dest,output,members=prepare(release);output.mkdir(parents=True,exist_ok=False);dest.mkdir(parents=True,exist_ok=False);copied=[]
 try:
  for source,m in members:
   need(release_path.read_bytes()==raw,'Root phase changed during staging');need(source.stat().st_size==m['size'] and digest(source)==m['sha256'],'source changed before copy');target=dest/relative(m['destination']);target.parent.mkdir(parents=True,exist_ok=True)
   with source.open('rb') as inp,target.open('xb') as out:
    for chunk in iter(lambda:inp.read(1048576),b''):out.write(chunk)
   need(target.stat().st_size==m['size'] and digest(target)==m['sha256'],'stage copied-byte mismatch');copied.append(m['destination'])
  actual={}
  for directory,dirs,names in os.walk(dest,followlinks=False):
   need(not any((Path(directory)/name).is_symlink() for name in dirs),'staged directory symlink')
   for name in names:
    target=Path(directory)/name;rel=str(target.relative_to(dest));regular(dest,rel);actual[rel]={'sha256':digest(target),'size':target.stat().st_size}
  need(actual=={m['destination']:{'sha256':m['sha256'],'size':m['size']} for _,m in members},'staged regular member exactset/hash/size differs')
  manifest={'schema':'p26-secret-scan-members-v1','source_sha256':S,'members':{m['destination']:{'sha256':m['sha256'],'size':m['size']} for _,m in members}};(output/'scan1-members.json').write_text(json.dumps(manifest,indent=2)+'\n');result={'schema':'p26-nonprivate-scan1-staging-result-v1','status':'STAGED-NOT-SCANNED-NOT-OFFICIAL-PACKAGE','copied_count':len(copied),'bytes':sum(m['size'] for _,m in members),'destination':str(dest),'members_manifest_sha256':release['members_manifest_sha256'],'scan1_members_sha256':digest(output/'scan1-members.json'),'private_values_read':False,'services_called':0,'scan_pass_claimed':False,'official_package_claimed':False};rc=0
 except BaseException:
  result={'schema':'p26-nonprivate-scan1-staging-result-v1','status':'FAILED-PARTIAL-STAGING-PRESERVED','copied_count':len(copied),'private_values_read':False,'services_called':0,'scan_pass_claimed':False,'official_package_claimed':False};rc=1
 (output/'RESULT.json').write_text(json.dumps(result,indent=2)+'\n');(output/'rc').write_text(str(rc)+'\n');return rc
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--release-file',required=True);args=parser.parse_args()
 try:sys.exit(stage(args.release_file))
 except Exception as exc:print('staging refused: '+type(exc).__name__,file=sys.stderr);sys.exit(1)
