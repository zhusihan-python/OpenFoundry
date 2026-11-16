"""Synthetic CODE/PURE anchor comparison controls; no actual evidence reads."""
import copy,importlib.util,json
from pathlib import Path

def run():
 s=importlib.util.spec_from_file_location('anchor',Path(__file__).with_name('p26_h10_anchor_reader.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
 row={k:None for k in m.SAFE27};row.update(id='run',sync_def_id='def',source_id='source',output_dataset_id='dataset',owner_id='actor',run_owner_id='actor',requested_by='actor',org_id='org',run_org_id='org',def_org_id='org',conn_org_id='org',source_name='synthetic',control_mode='controlled',status='running',gate_entered='false')
 owner={'run_id':'run','sync_def_id':'def','source_id':'source','output_dataset_id':'dataset','run_status':'running','mode':'cursor','confirmed_epoch':'0','active_since':'2035-01-01 00:00:00+00','blocked_reason':''};parent={'def_id':'def','source_id':'source','conn_id':'source','output_dataset_id':'dataset','conn_owner_id':'actor','org_id':'org','conn_org_id':'org','source_name':'synthetic','connector_type':'rest_api'};clock={'id':'run','recovery_protocol':None,'expired':False};a={'row':row,'cp_owner_row_full':owner,'parent':parent,'protocol_clock':clock};out=[]
 m.compare(a,row,owner,parent,clock);out.append({'name':'unchanged-live-lease','accepted':True})
 expired=dict(clock,expired=True);m.compare(a,row,owner,parent,expired);out.append({'name':'expired-blank-is-valid-not-frozen','accepted':True})
 reason=dict(owner,blocked_reason='requires_reconciliation');m.compare(a,row,reason,parent,expired,True);out.append({'name':'only-expired-reason-transition-frozen','accepted':True})
 tests=[('missing27field',lambda r,o,p,c:r.pop('heartbeat_at')),('full27heartbeatdrift',lambda r,o,p,c:r.update(heartbeat_at='changed')),('parentdrift',lambda r,o,p,c:p.update(source_name='changed')),('fullCPactive-since-drift',lambda r,o,p,c:o.update(active_since='changed')),('fullCPepochdrift',lambda r,o,p,c:o.update(confirmed_epoch='1')),('reason-without-expiry',lambda r,o,p,c:o.update(blocked_reason='requires_reconciliation')),('bad-reason',lambda r,o,p,c:o.update(blocked_reason='cleared')),('nonnull-recovery-protocol',lambda r,o,p,c:c.update(recovery_protocol='changed')),('nonbool-expiry',lambda r,o,p,c:c.update(expired=1))]
 for name,change in tests:
  r,o,p,c=map(copy.deepcopy,(row,owner,parent,clock));change(r,o,p,c)
  try:m.compare(a,r,o,p,c);rejected=False
  except (ValueError,KeyError):rejected=True
  assert rejected,name;out.append({'name':name,'rejected':True})
 try:m.compare(a,row,owner,parent,expired,True);rejected=False
 except ValueError:rejected=True
 assert rejected;out.append({'name':'expired-blank-cannot-be-final-frozen','rejected':True})
 try:m.exact([{'id':'a'}],[{'id':'a'},{'id':'unknown'}],'id');rejected=False
 except ValueError:rejected=True
 assert rejected;out.append({'name':'whole-unknown-extra-refused','rejected':True})
 return {'synthetic_only':True,'actual_h10_anchor_read':False,'actual_SQL_calls':0,'actual_file_or_HTTP_originals_read':0,'controls':out,'pass_count':len(out),'full_P25_15':False}
if __name__=='__main__':print(json.dumps(run(),indent=2))
