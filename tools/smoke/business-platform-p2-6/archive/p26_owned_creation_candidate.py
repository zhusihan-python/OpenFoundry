"""Offline creation coverage candidate from closed originals; no service queries.

Creator IDs alone never complete coverage. Missing parent documents and
projected receipt normalizations remain explicit Root-review blockers.
"""
import hashlib,json,re
from pathlib import Path
B=Path('tools/smoke/business-platform-p2-6');O=B/'results/w9/2026-11-15-w9-attempt15';ROOT=Path.cwd();S='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
UUID=re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')

def sha(data):return hashlib.sha256(data).hexdigest()
def doc(p):return json.loads(Path(p).read_text())

def build():
 inputs={};declared={};sources={};defs={};conflicts=[];skipped=[]
 # Exact closed formal slices provide immutable membership, never directory glob.
 for contract in ['p26_http10_contract.draft.json','p26_P1pre9_contract.draft.json']:
  d=doc(B/'archive'/contract);proof=d['proof'];inputs.update(proof.get('original_files',{}))
 for path in [B/'results/w9/2026-11-15-w9-attempt13/S2-HTTP10-contract.json',B/'results/w9/2026-11-15-w9-attempt14/S2-legacy-stages-contract.json']:
  d=doc(path);proofs=d.get('proofs',[d.get('proof')]);
  for proof in proofs:inputs.update(proof.get('original_files',{}))
 for name in ['S2-P24-full7-outer-launch-attempt1','S2-P24-full7-retry1-outer-launch-attempt1','S2-P24-full7-retry2-outer-launch-attempt1']:
  for relative,pin in doc(B/'results/w10'/name/'original-files.json').items():inputs[str(B/relative)]={'sha256':pin['sha256'],'size':pin['bytes']}
 # Historical actual attempts can have created objects even before their formal
 # evidence recorder was repaired. Retain their declared IDs as unresolved.
 inventory=doc(B/'results/w9/2026-11-15-w9-attempt11/15-all-results-history-snapshot.json')
 for path,pin in inventory['members'].items():
  actual_http=bool(re.search(r'/results/w7/attempts/[^/]+-20261115T[0-9]+Z/',path))
  accepted_legacy=pin['classification'].startswith('history-oldS') and '/suite-commands-' in path
  if (actual_http and path.endswith(('.meta','/RESULT.json'))) or (accepted_legacy and 'actual-http-owned-create' in path and path.endswith('.raw')):inputs.setdefault(path,{'sha256':pin['sha256'],'size':pin['size']})
 reads={}
 def reference(path,pointer):return {'path':str((ROOT/path).resolve()),'sha256':inputs[path]['sha256'],'json_pointer':pointer}
 def add(mapping,identity,fragment):
  if not UUID.fullmatch(str(identity)):return
  mapping.setdefault(identity,[]).append(fragment)
 for path,pin in sorted(inputs.items()):
  wanted=path.endswith('.meta') or ('actual-http-owned-create' in path and path.endswith('.raw')) or path.endswith('/RESULT.json') or 'original-result_p24' in path
  if not wanted:continue
  raw=Path(path).read_bytes()
  if sha(raw)!=pin['sha256'] or len(raw)!=pin['size']:skipped.append({'path':path,'reason':'changed since earlier closed cutoff; never adopt current bytes'});continue
  reads[path]=pin
  try:value=json.loads(raw)
  except (ValueError,UnicodeError):continue
  if isinstance(value,dict) and 'owned_roster' in value:
   for e in value['owned_roster']:
    if e.get('kind') in ('sync_def','def'):declared.setdefault(e['id'],[]).append(reference(path,'/owned_roster'))
  if isinstance(value,dict) and 'owned_sync_ids' in value:
   for did in value['owned_sync_ids']:declared.setdefault(did,[]).append(reference(path,'/owned_sync_ids'))
  if path.endswith('.meta') and isinstance(value,dict) and isinstance(value.get('safe_response'),dict):
   command=Path(path[:-5]+'.cmd')
   if not command.is_file() or str(command) not in inputs:continue
   cmdraw=command.read_bytes()
   if sha(cmdraw)!=inputs[str(command)]['sha256']:continue
   text=cmdraw.decode();first=text.splitlines()[0];row=value['safe_response'];rid=row.get('id')
   if not first.startswith('POST ') or value.get('original_http_status') not in (200,201,202):continue
   fragment={'ref':reference(path,'/safe_response'),'document':row,'encoding':'full-safe-response','actual_command':str(command)}
   if re.match(r'POST /api/v1/(?:data-connection/sources|connections)$',first):add(sources,rid,fragment)
   if re.match(r'POST /api/v1/data-connection/syncs$',first):add(defs,rid,fragment);declared.setdefault(rid,[]).append(fragment['ref'])
  elif isinstance(value,dict) and 'safe_actual_creation' in value:
   row=value['safe_actual_creation'];fragment={'ref':reference(path,'/safe_actual_creation'),'document':row,'encoding':'observed-authenticated-creation-projection','actual_command':path[:-4]+'.cmd.json'}
   if value['kind']=='source':add(sources,row['id'],fragment)
   if value['kind']=='def':add(defs,row['id'],fragment);declared.setdefault(row['id'],[]).append(fragment['ref'])
 # Exclusion comes from immutable old guarded parent rows, plus the explicitly
 # typed NEW legacy-null N. No same-owner or name-based ownership inference.
 anchor=doc(B/'archive/p26_P1pre9_contract.draft.json')['proof']['whole_initial_guard'];protected={'e9b80125-63a8-4d4b-b1de-e3bb5763c0b1','b6df951d-6136-4172-8a79-e9b5db0fcda6','482e9171-b006-4e9b-8cbc-16206c678898'}
 for record in anchor['records']:
  if isinstance(record,dict) and 'stdout' in record:
   for line in record['stdout'].splitlines():
    if line.startswith('['):
     for row in json.loads(line):
      if isinstance(row,dict) and ('sync_def_id' in row or 'def_id' in row):protected.add(row.get('sync_def_id',row.get('def_id')))
 candidates=[];unresolved=[]
 for did in sorted(set(declared)|set(defs)):
  if did in protected:continue
  ds=defs.get(did,[])
  if not ds:unresolved.append({'def_id':did,'reason':'declared actual creator roster but no usable POST creation original','declared_origins':declared[did]});continue
  entry=ds[0];d=entry['document'];sid=d.get('source_id');ss=sources.get(sid,[])
  if not ss:unresolved.append({'def_id':did,'source_id':sid,'reason':'Def POST exists; actual Source POST parent missing','definition_creation':entry['ref']});continue
  parent=ss[0];s=parent['document'];actor=s.get('owner_id',s.get('actor'));tenant=s.get('org_id',s.get('tenant'))
  if not actor or not d.get('output_dataset_id'):unresolved.append({'def_id':did,'reason':'actual creation projection incomplete'});continue
  projected=entry['encoding']!='full-safe-response' or parent['encoding']!='full-safe-response'
  candidates.append({'def_id':did,'source_id':sid,'output_dataset_id':d['output_dataset_id'],'actor_id':actor,'org_id':tenant,'definition_creation':entry['ref'],'source_creation':parent['ref'],'definition_document':d,'source_document':s,'projection_normalization_required':projected,'final_quiesce_eligible':False,'Root_review_blockers':['exact instance routing and final persisted schedule/grant read required']+(['source actor→owner_id and tenant→org_id projection needs transparent normalization reader or approved safe parent GET'] if projected else []),'all_def_origins':[x['ref'] for x in ds],'all_source_origins':[x['ref'] for x in ss]})
 return {'status':'PARTIAL OFFLINE ACTUAL-CREATION COVERAGE CANDIDATE; NO FINAL AUTHORITY','source_sha256':S,'schema_target':'p26-final-owned-def-coverage-v1','proposed_definition_ids':[x['def_id'] for x in candidates],'creators':candidates,'unresolved_actual_ids':unresolved,'protected_definitions_excluded':sorted(protected),'changed_old_cutoff_files_not_adopted':skipped,'original_files_read':reads,'Root_coverage_complete':False,'not_yet_covered':['P24 future successful retry','P25 all actual creations including failed attempts','P1post and ABC actual last writes','other historical actual families not in these explicit closed inputs'],'private_files_opened':False,'live_calls':0}
if __name__=='__main__':
 result=build();(O/'final-owned-coverage.CANDIDATE.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'creators':len(result['creators']),'unresolved':len(result['unresolved_actual_ids']),'changed_files':len(result['changed_old_cutoff_files_not_adopted']),'originals_read':len(result['original_files_read'])}))
