#!/usr/bin/env python3
"""Read-only completion of pinned C2; no lifecycle command branches."""
import argparse,copy,json,subprocess,sys
from pathlib import Path
sys.dont_write_bytecode=True
P26=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(P26));sys.path.insert(0,str(P26/'deployment'))
from common import p26_gates as G
from preflight import p26_current_preflight as W
import s2_image_upgrade as U
from final_old_artifact_readonly import evidenced_http,terminal_error
from connector_image_readonly_completion import classify_step_receipts
PHASE='s2-image-readonly-tail-p26connectorc'
CURRENT={'cid':'7dea76388ccc5cecb72f4e6505f411055efc0c25e4cf7ea5bd65b66d93c854a0','image':'sha256:ea8244f03a3b07e72b77bd7539e13ae1d39cf91bf90a26d01675f008c64685d6','started_at':'2026-11-15T10:38:03.960524917Z'}
TOOLS={'deployment/legacy_c_image_readonly_tail.py','deployment/s2_image_upgrade.py','deployment/final_old_artifact_readonly.py','deployment/connector_image_readonly_completion.py','common/p26_gates.py','preflight/p26_current_preflight.py','regressions/adapters/w10_guard_shim.py','regressions/adapters/w10_image_transition.py'}
def ref(p):return {'path':str(Path(p).resolve()),'sha256':G.sha256_file(p)}
def read(binding):
 p=Path(binding['path']);assert p.is_file() and not p.is_symlink() and G.sha256_file(p)==binding['sha256'];return json.loads(p.read_bytes())
def canonical_facts(fact):
 result=copy.deepcopy(fact);mounts=result['mounts'];assert isinstance(mounts,list)
 assert all(isinstance(m,dict) and isinstance(m.get('Destination'),str) and m['Destination'] for m in mounts)
 result['mounts']={m['Destination']:m for m in mounts};assert len(result['mounts'])==len(mounts);return result

def validate_origin(binding):
 m=read(binding);old=read(m['original_root']);proof=read(m['original_proof']);inner=Path(m['inner'])
 assert m['original_outer_rc']==1 and (Path(m['outer'])/'rc').read_text().strip()=='1'
 for item in m['original_files']:assert G.sha256_file(item['path'])==item['sha256']
 expected={str(f.resolve()) for f in inner.iterdir() if f.is_file()}
 assert expected.issubset({f['path'] for f in m['original_files']})
 assert classify_step_receipts(inner)==m['receipt_counts']=={'step_receipts_zero':39,'unix_child_rc0':29,'http_status200_transport0':10}
 for item in m['used_tools']:
  assert G.sha256_file(item['copy'])==item['sha256'] and G.sha256_file(item['origin'])==item['sha256']
 assert old['image_transition']['service']=='p26connectorc' and old['image_transition']['approved_argv']==U.upgrade_argv('p26connectorc')
 assert proof['service']=='p26connectorc' and proof['current']==CURRENT and proof['root_release_sha256']==m['original_root']['sha256']
 assert proof['executor_tool_sha256']==G.sha256_file(U.__file__)==old['image_transition']['executor_tool_sha256']
 assert json.loads(Path(str(proof['receipts']['command'])+'.cmd.json').read_bytes())==U.upgrade_argv('p26connectorc')
 assert json.loads(Path(str(proof['receipts']['discovery'])+'.cmd.json').read_bytes())==U.compose_prefix('p26connectorc')+['ps','-q','p26connectorc']
 before=json.loads(Path(str(proof['receipts']['before_identity'])+'.raw').read_bytes());after=json.loads(Path(str(proof['receipts']['after_identity'])+'.raw').read_bytes())
 U.validate_replacement('p26connectorc',before,after,CURRENT['image'],Path(str(proof['receipts']['before_env_names'])+'.raw').read_text(),Path(str(proof['receipts']['after_env_names'])+'.raw').read_text())
 assert {k:after[k] for k in CURRENT}==CURRENT
 for side in ['before','after']:
  raw=Path(str(proof['receipts'][side+'_aux_config'])+'.raw').read_text();pairs=[x.split('=',1) for x in raw.rstrip('\n').splitlines()];assert len(pairs)==len(dict(pairs))==len(U.config_for('p26connectorc')) and dict(pairs)==U.config_for('p26connectorc')
 base=read({'path':old['deployment_record'],'sha256':old['deployment_record_sha256']})
 facts={json.loads(f.read_bytes())['name'].lstrip('/'):json.loads(f.read_bytes()) for f in inner.glob('*-before-*.raw') if 13<=int(f.name.split('-',1)[0])<=23}
 assert len(facts)==11
 facts['p26connectorc']=after
 return m,old,proof,base,facts

def run(release_file):
 r=G.load_release(release_file);assert r and r['root_accepted'] is True and r['released_for']==[PHASE] and r['stage']=='runtime-image-completion' and r['suite']=='p26-runtime'
 assert 'image_transition' not in r and 'main_boot_transition' not in r
 assert set(r['completion_tools_sha256'])==TOOLS
 for rel,sha in r['completion_tools_sha256'].items():assert G.sha256_file(P26/rel)==sha
 m,old,proof,base,facts=validate_origin(r['c_readonly_tail']['origin_manifest'])
 source=r['candidate_source'];assert G.candidate_hash(G.OPENFOUNDRY)==source==old['candidate_source']
 record=read({'path':r['deployment_record'],'sha256':r['deployment_record_sha256']});expected=copy.deepcopy(base['containers']);expected['p26connectorc']=CURRENT;assert record['containers']==expected
 output=Path(r['c_readonly_tail']['output_dir']);assert output.resolve().is_relative_to(P26/'results/w6/deployment');output.mkdir(exist_ok=False,parents=True);att=W.Attempt(output,'commands');rootsha=G.sha256_file(release_file)
 for env in [W.Q3_ENV_FILE,Path('/private/tmp/openfoundry-p26-aux.env')]:
  for line in env.read_text().splitlines():
   if '=' in line and not line.startswith('#'):att.add_secret(line.split('=',1)[1].strip().strip('\"').strip("'"))
 def gate():
  assert G.sha256_file(release_file)==rootsha and G.load_release(release_file)==r and G.candidate_hash(G.OPENFOUNDRY)==source
  for rel,sha in r['completion_tools_sha256'].items():assert G.sha256_file(P26/rel)==sha
 def command(tag,argv):
  gate();assert argv[:2] in [['docker','inspect'],['docker','exec']] or argv[:3]==['docker','ps','-a'] or argv==['python3','-I','-B',str(U.GUARD),release_file]
  prefix=att.step_begin(tag,argv)
  try:cp=subprocess.run(argv,capture_output=True,text=True,timeout=180)
  except Exception as exc:att.step_finish(tag,prefix,type(exc).__name__,'unavailable',{'original_child_rc':None});raise RuntimeError(tag+' spawn/timeout') from None
  att.step_finish(tag,prefix,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_child_rc':cp.returncode},kind='argv');assert cp.returncode==0;return cp.stdout,prefix
 try:
  after_refs={}
  for name,previous in facts.items():
   text,prefix=command('fresh-'+name,['docker','inspect',previous['cid'],'--format',U.FACTS]);assert canonical_facts(json.loads(text))==canonical_facts(previous)
   if name=='p26connectorc':after_refs['facts']=prefix
  names,prefix=command('fresh-C-envnames',['docker','inspect',CURRENT['cid'],'--format',U.ENV_NAMES]);assert set(U.env_names(names))==set(U.env_names(Path(str(proof['receipts']['after_env_names'])+'.raw').read_text()));after_refs['env_names']=prefix
  text,prefix=command('fresh-C-safe-config',['docker','inspect',CURRENT['cid'],'--format',U.env_format(U.config_for('p26connectorc'))]);pairs=[line.split('=',1) for line in text.rstrip('\n').splitlines()];assert len(pairs)==len(dict(pairs))==len(U.config_for('p26connectorc')) and dict(pairs)==U.config_for('p26connectorc');after_refs['safe_config']=prefix
  gate();health,_=evidenced_http(att,'http://127.0.0.1:17100/healthz');assert health.status==200
  _,catalog,_=W.load_baseline(G.P25_FINAL_DIR)
  for name,sha in catalog['counter_bodies_sha256'].items():
   gate();response,_=evidenced_http(att,W.ORIGINS['fixture']+'/admin/src/'+name);assert response.status==200 and G.sha256_bytes(response.body)==sha
  text,_=command('whole-grants',['docker','exec',record['containers'][W.Q3_POSTGRES]['cid'],'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','openfoundry','-d','connectors','-c','BEGIN READ ONLY; SELECT count(*) FROM sync_schedule_grants; COMMIT;']);assert text.strip()=='0'
  text,_=command('owned-inventory',['docker','ps','-a','--filter','label=of-p26-owner=p26-http','--filter','label=of-p26-stage=p2-6','--format','{{.Names}}']);assert len(text.splitlines())==len(set(text.splitlines())) and set(text.splitlines())=={name for name in facts if name.startswith('p26')}
  text,prefix=command('whole-typed-postguard',['python3','-I','-B',str(U.GUARD),release_file]);guard=json.loads(text.strip().splitlines()[-1]);assert guard['ok'] is True;gate()
  composite={'schema':'p26-legacy-C-image-readonly-composite-v1','ok':True,'original_outer_rc':1,'original_receipt_counts':m['receipt_counts'],'completion_receipt_counts':classify_step_receipts(att.dir),'new_up_executions':0,'source_sha256':source,'current':CURRENT,'origin_manifest':r['c_readonly_tail']['origin_manifest'],'completion_root_sha256':rootsha,'health_status':200,'fresh11_fullfacts_equal_unique_destination_mounts':True,'oldfixture_counters_unchanged':True,'whole_grants_count':0,'owned_inventory_exact':True,'typed_postguard':guard,'typed_postguard_receipt':prefix}
  proofpath=output/'composite-proof.json';U.exclusive_json(proofpath,composite);candidate=U.candidate_record_with_origins(base,'p26connectorc',CURRENT,proofpath,after_refs);candidate['acceptance_status']='UNREVIEWED-COMPOSITE-CANDIDATE';U.exclusive_json(output/'candidate-deployment-record.json',candidate);att.finish(composite);print(att.dir);return 0
 except Exception as exc:return terminal_error(att,exc,[])
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);a=p.parse_args()
 try:rc=run(a.release_file)
 except Exception as exc:print('C readonly tail refused ('+type(exc).__name__+')',file=sys.stderr);rc=1
 sys.exit(rc)
