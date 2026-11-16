"""NONSECRET provider fixtures only. Never opens a real private input/package."""
import copy,datetime,hashlib,importlib.util,json,os,tempfile
from pathlib import Path

def run():
 here=Path(__file__).parent;s=importlib.util.spec_from_file_location('provider',here/'p26_secret_values_memory.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);results=[]
 with tempfile.TemporaryDirectory(prefix='p26-NONSECRET-provider-') as tmp:
  root=Path(tmp).resolve();env=root/'fixture.env';env.write_text('EXACT=NONSECRET-fixture-value-01\nBEARER=Bearer NONSECRET-fixture-value-02\nDATABASE=postgresql://fixture:NONSECRET-fixture-password@127.0.0.1/fake\n');env.chmod(0o600)
  package=root/'package';package.mkdir();(package/'data.bin').write_bytes(b'\x00safe fixture bytes');manifest=root/'manifest.json'
  source='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e';entries=[{'id':key,'path':str(env),'key':key,'role':'NONSECRET-fixture','projection':projection} for key,projection in [('EXACT','exact'),('BEARER','bearer-token'),('DATABASE','url-password')]]
  def manifest_now():manifest.write_text(json.dumps({'schema':'p26-secret-scan-members-v1','source_sha256':source,'members':{str(p.relative_to(package)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'size':p.stat().st_size} for p in package.rglob('*') if p.is_file()}}))
  manifest_now();release={'schema':m.SCHEMA,'root_accepted':True,'phase':m.PHASE,'release_id':'NONSECRET-fixture-only','expires_utc':(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=3)).isoformat(),'source_sha256':source,'package_root':str(package),'package_manifest':str(manifest),'package_manifest_sha256':m.digest(manifest),'output_dir':str(root/'capture'),'tool_shas':{name:m.digest(here/name) for name in m.TOOLS},'registered_inputs':entries,'registered_inputs_sha256':hashlib.sha256(m.canonical(entries)).hexdigest()};rp=root/'release.json';rp.write_text(json.dumps(release))
  rc=m.run(str(rp));assert rc==0;registry=json.loads((root/'capture/registry.json').read_text());audit=json.loads((root/'capture/scan-audit.json').read_text());assert registry['actual_value_count']==audit['actual_value_count']==3 and audit['actual_hit_count']==0 and len(audit['scanned_members'])==1
  results.append({'name':'registered-exact-bearer-DSN-anonymous-pipe','original_rc':rc,'registered_count':3,'members':1,'values_persisted':False})
  forbidden=['NONSECRET-fixture-value-01','NONSECRET-fixture-value-02','NONSECRET-fixture-password']
  for f in (root/'capture').iterdir():
   text=f.read_text();assert all(v not in text and hashlib.sha256(v.encode()).hexdigest() not in text for v in forbidden)
  # Leak fixture returns nonzero and persists no scanner audit/child bytes.
  (package/'data.bin').write_bytes(b'\x00'+forbidden[0].encode());manifest_now();release.update(output_dir=str(root/'capture-leak'),package_manifest_sha256=m.digest(manifest));rp.write_text(json.dumps(release));rc=m.run(str(rp));assert rc==1 and not (root/'capture-leak/scan-audit.json').exists();results.append({'name':'binary-actual-hit-refused','original_rc':rc,'raw_child_output_persisted':False})
  for f in (root/'capture-leak').iterdir():assert all(v not in f.read_text() for v in forbidden)
  for name,change in [('wrong-phase',lambda x:x.update(phase='not-final')),('expired',lambda x:x.update(expires_utc='2000-01-01T00:00:00+00:00')),('wrong-input-registry',lambda x:x.update(registered_inputs_sha256='0'*64)),('changed-tool',lambda x:x['tool_shas'].update(p26_secret_scan_py='0'*64))]:
   q=copy.deepcopy(release);q['output_dir']=str(root/name);change(q);rp.write_text(json.dumps(q));called=[False];original=m.load_values
   def forbidden_loader(entries):called[0]=True;raise ValueError('should not read')
   m.load_values=forbidden_loader
   try:m.run(str(rp));rejected=False
   except (ValueError,KeyError):rejected=True
   finally:m.load_values=original
   assert rejected and not called[0];results.append({'name':name,'refused_before_input_read':True})
 return {'status':'NONSECRET-CODE-PURE-PROVIDER-VERIFIED','results':results,'real_private_reads':0,'real_package_scans':0}
if __name__=='__main__':print(json.dumps(run(),indent=2))
