"""CODE/PURE h10 immutable anchor reader; no producer import, process or live IO.

Future caller must bind actual whole expected sets and original ref contracts.
Passing this component never proves crash actions, natural-expiry history or P25.
"""
import hashlib,json
from datetime import datetime

SAFE27=('attempt','cancel_requested','conn_org_id','control_mode','def_org_id','fixture_src','finished_at','gate_entered','generation','has_hash','has_holder','has_token','has_version','heartbeat_at','id','lease_expires_at','org_id','output_dataset_id','owner_id','requested_by','run_org_id','run_owner_id','source_id','source_name','started_at','status','sync_def_id')
SUFFIX={'controlled':'whole-controlled','CP':'whole-CP','parents':'parents','protocol':'actual-protocol-clock'}

def need(ok,why):
 if not ok:raise ValueError(why)

def ref(r,p,entry):
 prefix=p['canonical_root_label']+'/';need(entry['path'].startswith(prefix),'h10 local original ref');rel=entry['path'][len(prefix):];need(rel.startswith('tools/smoke/business-platform-p2-6/results/w10/'),'h10 actual w10 original');raw=r.read(rel);need(hashlib.sha256(raw).hexdigest()==entry['sha256'],'h10 immutable original SHA');return raw

def exact(expected,actual,key):
 need(all(isinstance(x,dict) and key in x for x in expected+actual),'h10 whole row objects');a={x[key]:x for x in expected};b={x[key]:x for x in actual};need(len(a)==len(expected) and len(b)==len(actual) and a==b,'h10 whole set missing/extra/duplicate/drift')

def bindings(r,p,entries):
 need(set(entries)==set(SUFFIX),'h10 exact4 raw SQL receipts');out={}
 for key,item in entries.items():
  need(item['original_rc']==0 and set(item['files'])=={'.cmd.json','.raw','.rc','.meta.json'},'h10 original SQL rc/member closure')
  raw={suffix:ref(r,p,entry) for suffix,entry in item['files'].items()};argv=json.loads(raw['.cmd.json']);meta=json.loads(raw['.meta.json'])
  need(argv==item['argv'] and raw['.rc'].strip()==b'0' and meta['original_rc']==0,'h10 exact actual command/original rc binding')
  lines=[json.loads(s) for s in raw['.raw'].decode().splitlines() if s.startswith('[') or s=='null'];need(len(lines)==1 and isinstance(lines[0],(list,type(None))),'h10 one original SQL JSON result');out[key]=lines[0] or []
 return out

def observe(r,p,entries,row,owner,parent,clock):
 rows=bindings(r,p,entries)
 exact(p['baseline_controlled_rows']+[row],rows['controlled'],'id');exact(p['baseline_CP_rows']+[owner],rows['CP'],'run_id');exact(p['baseline_parent_rows']+[parent],rows['parents'],'def_id');need(rows['protocol']==[clock],'h10 original protocol clock exact')

def shape(row,owner,parent):
 need(set(row)==set(SAFE27),'h10 full immutable safe27 schema')
 need(row['status']=='running' and row['control_mode']=='controlled' and row['gate_entered']=='false','h10 prepublish controlled held')
 need(row['id']==owner['run_id'] and row['sync_def_id']==owner['sync_def_id']==parent['def_id'] and row['source_id']==owner['source_id']==parent['source_id']==parent['conn_id'] and row['output_dataset_id']==owner['output_dataset_id']==parent['output_dataset_id'],'h10 exact Run/CP/parent identity')
 need(row['owner_id']==row['run_owner_id']==row['requested_by']==parent['conn_owner_id'] and row['org_id']==row['run_org_id']==row['def_org_id']==row['conn_org_id']==parent['org_id']==parent['conn_org_id'],'h10 actor/tenant full parents')
 need(owner['run_status']=='running' and owner['mode']=='cursor' and parent['connector_type']=='rest_api' and row['source_name']==parent['source_name'],'h10 CP/Source facts')

def compare(anchor,row,owner,parent,clock,frozen=False):
 shape(row,owner,parent);need(row==anchor['row'] and parent==anchor['parent'],'h10 immutable full27/parent drift')
 need(clock['id']==row['id'] and clock['recovery_protocol']==anchor['protocol_clock']['recovery_protocol'] is None and type(clock.get('expired')) is bool,'h10 actual DB-clock/null protocol')
 need(not anchor['protocol_clock']['expired'] or clock['expired'],'h10 expiry cannot regress');expected=dict(anchor['cp_owner_row_full'])
 if expected.get('blocked_reason')!=owner.get('blocked_reason'):
  need(expected.get('blocked_reason')=='' and owner.get('blocked_reason')=='requires_reconciliation' and clock['expired'] is True,'h10 reason transition only DB-proven expiry');expected['blocked_reason']='requires_reconciliation'
 need(owner==expected,'h10 every original CP field invariant')
 if frozen:need(clock['expired'] is True and owner['blocked_reason']=='requires_reconciliation','h10 final frozen requires actual expiry plus reason')

def anchor(r,p,source):
 need(len(p['baseline_controlled_rows'])>=9 and len(p['baseline_CP_rows'])>=8 and len(p['baseline_parent_rows'])>=9,'h10 complete Root old/typed whole baselines required')
 doc=json.loads(ref(r,p,p['anchor']));need(doc['schema']=='p26-h10-pre-kill-anchor-v1' and doc['source_S']==source,'h10 prekill source/schema');shape(doc['row'],doc['cp_owner_row_full'],doc['parent'])
 clock=doc['protocol_clock'];need(clock=={'id':doc['row']['id'],'recovery_protocol':None,'expired':False},'h10 prekill actual live lease/null protocol')
 observe(r,p,doc['sql_bindings'],doc['row'],doc['cp_owner_row_full'],doc['parent'],clock);return doc

def observation(r,p,original_anchor,entry,frozen=False):
 doc=json.loads(ref(r,p,entry));need(doc['pre_kill_anchor']==p['anchor'] and doc['pre_kill_sql_bindings']==original_anchor['sql_bindings'],'h10 later baseline cannot replace prekill')
 receipts=doc['actual_observation_sql_receipts'];selected={}
 for key,suffix in SUFFIX.items():
  matches=[v for k,v in receipts.items() if k.endswith('-'+suffix)];need(len(matches)==1,'h10 current full SQL receipt unique');selected[key]=matches[0]
 row=doc['row'];owner=doc['cp_owner_row_full'];parent=doc['parent'];clock={'id':row['id'],'recovery_protocol':None,'expired':doc['lease_expired_by_DB_clock']}
 observe(r,p,selected,row,owner,parent,clock);compare(original_anchor,row,owner,parent,clock,frozen)
 typed=doc['typed_entry'];need(typed['safe_projection_27']==row and typed['cp_owner_row_full']==owner and typed['parent']==parent and typed['pre_kill_anchor']==p['anchor'],'h10 typed export exact immutable checked observations')
 cp=doc['public_view']['baseline_cp'];po=cp['owner'];need(cp['mode']==owner['mode'] and type(cp['confirmed_epoch']) is int and str(cp['confirmed_epoch'])==str(owner['confirmed_epoch']) and po['run_id']==owner['run_id'] and po['run_status']==owner['run_status'] and po.get('blocked_reason','')==owner['blocked_reason'] and datetime.fromisoformat(po['since'].replace('Z','+00:00'))==datetime.fromisoformat(owner['active_since'].replace('Z','+00:00')),'h10 coherent public CP matches SQL')
 need(doc['frozen_for_next_phase']==bool(clock['expired'] and owner['blocked_reason']=='requires_reconciliation'),'h10 frozen flag cannot replace evidence')
 return {'component_only':True,'frozen':frozen,'full_P25_15':False,'actual_crash_actions_verified':False,'public_original_HTTP_binding_pending':True}
