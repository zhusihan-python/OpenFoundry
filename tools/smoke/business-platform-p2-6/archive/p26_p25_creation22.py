"""Only actual POST201 Source/Def creators from stable P25 h1..h9 plus fresh h9 retry (failed history remains separate) audits."""
import hashlib,json
from pathlib import Path
B=Path('tools/smoke/business-platform-p2-6');O=B/'results/w9/2026-11-15-w9-attempt22'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def build():
 new={};ledger=[]
 for sid in ('h1','h2','h3','h4','h5','h6','h7','h8','h9','h9-retry1'):
  audit=json.loads((B/'results/w10'/('S2-P25-'+sid+'-outer-launch-attempt1')/('original-files.json' if sid=='h9' else 'original-audit.json')).read_text());sources={};definitions={}
  for relative,pin in (audit if sid=='h9' else audit['files']).items():
   if '/engine-evidence/' not in relative or not relative.endswith('.json'):continue
   path=B/relative;raw=path.read_bytes();assert sha(raw)==pin['sha256'] and len(raw)==pin['bytes'];event=json.loads(raw)
   if not isinstance(event,dict) or event.get('method')!='POST' or event.get('status')!=201 or 'resp_body' not in event:continue
   bodyraw=event['resp_body'].encode();assert sha(bodyraw)==event['resp_sha256'] and len(bodyraw)==event['resp_bytes'];body=json.loads(bodyraw);tail=event['path'].split('://',1)[1].split('/',1)[1]
   ref={'path':str(path.resolve()),'sha256':pin['sha256'],'original_http_status':201,'method':'POST','original_body_sha256':event['resp_sha256'],'original_body_bytes':event['resp_bytes'],'derivation':'json.loads(original event.resp_body); retained fields only, Source config/auth not copied'}
   if tail in ('api/v1/connections','api/v1/data-connection/sources'):sources[body['id']]={'document':body,'origin':ref}
   elif tail=='api/v1/data-connection/syncs':definitions[body['id']]={'document':body,'origin':ref}
  for did,d in definitions.items():
   definition=d['document'];source=sources[definition['source_id']];s=source['document'];assert definition.get('org_id')==s.get('org_id') and did not in new
   new[did]={'definition':{k:definition.get(k) for k in ('id','source_id','output_dataset_id','org_id')},'source':{k:s.get(k) for k in ('id','owner_id','org_id')},'case':sid,'definition_origin':d['origin'],'source_origin':source['origin']}
 raw=(json.dumps({'scope':'DERIVED parsed original POST response JSON fragments; no new observation or invented historical body','definitions':new},indent=2)+'\n').encode();path=O/'P25-parsed-POST-fragments.json';path.write_bytes(raw);creators=[]
 for did,e in new.items():
  creators.append({'def_id':did,'source_id':e['source']['id'],'output_dataset_id':e['definition']['output_dataset_id'],'actor_id':e['source']['owner_id'],'org_id':e['source']['org_id'],'definition_creation':{'path':str(path.resolve()),'sha256':sha(raw),'json_pointer':'/definitions/'+did+'/definition'},'source_creation':{'path':str(path.resolve()),'sha256':sha(raw),'json_pointer':'/definitions/'+did+'/source'},'claim':'parsed actual original POST201 response, explicit derivation ledger; not a CURRENT read','actual_case':e['case'],'origin_ledger':{'definition':e['definition_origin'],'source':e['source_origin']},'quiesce_routing_pending_Root':True})
 old=json.loads((B/'results/w9/2026-11-15-w9-attempt15/exact-current-parent-plan.CANDIDATE.json').read_text());ids={e['def_id'] for e in old['entries']};assert not ids.intersection(new) and not set(new).intersection(old['protected_exclusions'])
 for e in creators:old['entries'].append({'def_id':e['def_id'],'source_id':e['source_id'],'output_dataset_id':e['output_dataset_id'],'expected_actor':e['actor_id'],'expected_tenant':e['org_id'],'creation_basis':'DERIVED parsed original POST201 JSON; origin ledger required','definition_creation':e['definition_creation'],'source_creation':e['source_creation']})
 old['cutoff']='INITIAL QUIESCE through P25; candidate currently only through stable h1..h9 plus fresh h9 retry (failed history remains separate); remaining P25 pending';old['actual_initial_cutoff_complete']=False;(O/'exact-current-parent-plan.through-fresh-h9.CANDIDATE.json').write_text(json.dumps(old,indent=2)+'\n')
 return {'status':'P25 h1..h9 plus fresh h9 retry (failed history remains separate) ACTUAL CREATIONS ONLY; INITIAL CUTOFF INCOMPLETE','creators':creators,'new_definition_count':len(creators),'initial_candidate_Def_count':len(old['entries']),'initial_candidate_Source_count':len({e['source_id'] for e in old['entries']}),'imported_handle_references_not_counted_as_new':True,'CURRENT_SQL_executed':False,'later_P25_pending':True}
if __name__=='__main__':
 d=build();(O/'P25-new-creations.CANDIDATE.json').write_text(json.dumps(d,indent=2)+'\n');print(d['new_definition_count'],d['initial_candidate_Def_count'],d['initial_candidate_Source_count'])
