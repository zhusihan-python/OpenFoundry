#!/usr/bin/env python3
"""Two actual tick Jobs race a due row; one blocked inside TX, other SKIP LOCKED; missed periods coalesce."""
import datetime,json,subprocess,time
from canonical_acceptance import ROOT,K,command,forward,http,pg,save
from failure_acceptance import publish,await_state
from admission_crash_acceptance import sql,wait_for

def main():
 token=(ROOT/'org-token.txt').read_text().strip();seed=json.loads((ROOT/'canonical-seed.json').read_text());key='runtime-tick-coalesce-r3';lock=86639001;name='ofwp32_tick_coalesce';app=name+'_holder';holder=None
 sec=datetime.datetime.now(datetime.timezone.utc).second
 if sec>20:time.sleep(70-sec)
 elif sec<10:time.sleep(10-sec)
 with forward('ofwp32-pbs',50081) as pbs,forward('ofwp32-dvs',50078) as dvs:
  publish(pbs,token,seed,'dataset_input');g=http(pbs,'/api/v1/scheduling/grants',token,'POST',{'schedule_key':key,'pipeline_rid':seed['pipeline']['id'],'token':token},evidence='tick-coalesce-r3-http')
  http(pbs,'/api/v1/scheduling/definitions',token,'POST',{'schedule_key':key,'pipeline_rid':seed['pipeline']['id'],'build_branch':'main','cron':'* * * * *','grant_id':g['id'],'grant_generation':g['generation']},evidence='tick-coalesce-r3-http')
  path='/v1/datasets/'+seed['output']['id']+'/versions';before=http(dvs,path,token,evidence='tick-coalesce-r3-product');save('tick-coalesce-r3-before-versions.json',before)
  try:
   sql("CREATE FUNCTION scheduling."+name+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.schedule_key='"+key+"' THEN PERFORM pg_advisory_xact_lock("+str(lock)+"); END IF; RETURN NEW; END $$; CREATE TRIGGER "+name+" BEFORE INSERT ON scheduling.dispatch_audit FOR EACH ROW EXECUTE FUNCTION scheduling."+name+"();")
   log=open(ROOT/'tick-coalesce-r3-holder.log','w');holder=subprocess.Popen(K+['exec','ofwp32-pg-0','--','psql','-v','ON_ERROR_STOP=1','-U','ofwp32','-d','ofwp32','-Atc',"SET application_name='"+app+"'; SELECT pg_advisory_lock("+str(lock)+"); SELECT pg_sleep(120);"],stdout=log,stderr=log)
   wait_for(lambda:pg("SELECT count(*)::int FROM pg_locks WHERE locktype='advisory' AND objid="+str(lock)+" AND granted;"),'tick holder')
   save('tick-coalesce-r3-seed-due-update.txt',sql("UPDATE schedules.definitions SET next_run_at=date_trunc('minute',now())-interval '3h' WHERE schedule_key='"+key+"';"))
   due=pg("SELECT to_json(next_run_at) FROM schedules.definitions WHERE schedule_key='"+key+"';");save('tick-coalesce-r3-old-due.json',due)
   command(K+['create','job','ofwp32-coalesce-r3-first','--from=cronjob/ofwp32-schedules-tick'],text=True)
   waited=wait_for(lambda:pg("SELECT count(*)::int FROM pg_locks WHERE locktype='advisory' AND objid="+str(lock)+" AND NOT granted;"),'first actual tick inside audit TX');save('tick-coalesce-r3-waiting.json',{'advisory_waiters':waited})
   command(K+['create','job','ofwp32-coalesce-r3-second','--from=cronjob/ofwp32-schedules-tick'],text=True)
   command(K+['wait','--for=condition=complete','job/ofwp32-coalesce-r3-second','--timeout=45s'],text=True)
   second=command(K+['logs','job/ofwp32-coalesce-r3-second'],text=True);save('tick-coalesce-r3-second.log',second);assert any(json.loads(line).get('fired')==0 and json.loads(line).get('msg')=='schedules-tick completed' for line in second.splitlines() if line.startswith('{')),second
   save('tick-coalesce-r3-live-wait.json',pg("SELECT json_build_object('waiting',(SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid="+str(lock)+" AND NOT granted),'visible_audit',(SELECT count(*) FROM scheduling.dispatch_audit WHERE schedule_key='"+key+"'));"))
   sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='"+app+"';")
   command(K+['wait','--for=condition=complete','job/ofwp32-coalesce-r3-first','--timeout=45s'],text=True)
   first=command(K+['logs','job/ofwp32-coalesce-r3-first'],text=True);save('tick-coalesce-r3-first.log',first);assert any(json.loads(line).get('fired')==1 and json.loads(line).get('msg')=='schedules-tick completed' for line in first.splitlines() if line.startswith('{')),first
   audit=pg("SELECT json_agg(row_to_json(a)) FROM scheduling.dispatch_audit a WHERE schedule_key='"+key+"';");assert len(audit)==1 and datetime.datetime.fromisoformat(audit[0]['scheduled_for'])==datetime.datetime.fromisoformat(due),audit
   d=await_state({'event_id':audit[0]['event_id']},{'succeeded'});save('tick-coalesce-r3-final-dispatch.json',d)
   defs=pg("SELECT json_build_object('next_run_at',next_run_at,'future',next_run_at>now(),'revision',definition_revision) FROM schedules.definitions WHERE schedule_key='"+key+"';");assert defs['future'];save('tick-coalesce-r3-result.json',{'audit':audit,'definition':defs,'old_due':due,'second_skipped_locked':True})
   after=http(dvs,path,token,evidence='tick-coalesce-r3-product');save('tick-coalesce-r3-after-versions.json',after);assert len(after['data'])==len(before['data'])+1
  finally:
   sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='"+app+"';");sql('DROP TRIGGER IF EXISTS '+name+' ON scheduling.dispatch_audit; DROP FUNCTION IF EXISTS scheduling.'+name+'();')
   if holder:
    try:holder.wait(timeout=5)
    except subprocess.TimeoutExpired:holder.terminate();holder.wait(timeout=5)
   # Stop this minute schedule only after the admitted opportunity finishes.
   http(pbs,'/api/v1/scheduling/definitions/'+key+'/pause',token,'PATCH',{},evidence='tick-coalesce-r3-http')
 print('PASS two actual concurrent ticks: second SKIP LOCKED=0, first=1; 3h missed schedule coalesced to one audit/product and future due')
if __name__=='__main__':main()
