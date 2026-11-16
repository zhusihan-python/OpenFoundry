"""Narrow SQL fault-injection policy from actual NEW HTTP creation receipts.

No user-supplied UUID is adopted. This registry observes original P1 HTTP
results and leaves every business assertion/return value unchanged.
"""
import base64
import json
import re
from pathlib import Path

UUID=r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'

class Registry:
    def __init__(self,release,attempt=None):
        self.release=release; self.attempt=attempt
        self.datasets={}; self.sources={}; self.defs={}; self.runs={}
        self.pending=set(); self.receipts=[]
    def instrument(self,m):
        if getattr(m,'_w10_owned_observed',False): return
        m._w10_owned_observed=True
        real=m.http
        def http(service,path,payload=None,**kw):
            if hasattr(self,'before_http'): self.before_http()
            token=kw.get('token')
            if self.attempt and token: self.attempt.add_secret(token)
            result=real(service,path,payload,**kw)
            if token and result.status in (200,201,202):
                try:
                    segment=token.split('.')[1]; actor=json.loads(base64.urlsafe_b64decode(segment+'='*((-len(segment))%4)))
                except (ValueError,KeyError,TypeError,IndexError): return result
                self.observe(service,path,kw.get('method') or ('POST' if payload is not None else 'GET'),payload,result.status,result.body,actor)
            return result
        m.http=http
    def observe(self,service,path,method,payload,status,body,actor):
        identity={'actor':actor['sub'],'tenant':actor.get('org_id')}
        if not re.fullmatch(UUID,identity['actor']): return
        kind=None; row=None
        if method=='POST' and status in (200,201) and isinstance(body,dict) and isinstance(payload,dict):
            if service=='dataset' and path=='/v1/datasets': kind='dataset'; row={**identity,'id':body.get('id'),'name':payload.get('name')}
            if service=='connector' and path=='/api/v1/data-connection/sources': kind='source'; row={**identity,'id':body.get('id'),'name':payload.get('name')}
            if service=='connector' and path=='/api/v1/data-connection/syncs':
                sid=payload.get('source_id'); target=payload.get('output_dataset_id')
                if sid in self.sources and target in self.datasets and all(self.sources[sid][k]==identity[k] and self.datasets[target][k]==identity[k] for k in identity):
                    kind='def'; row={**identity,'id':body.get('id'),'source_id':sid,'output_dataset_id':target}
        if kind and re.fullmatch(UUID,str(row['id'])):
            mapping={'dataset':self.datasets,'source':self.sources,'def':self.defs}[kind]
            if row['id'] in mapping and mapping[row['id']]!=row: raise ValueError('owned creation identity changed')
            mapping[row['id']]=row
            receipt={'kind':kind,'method':method,'path':path,'status':status,'safe_actual_creation':row}
            self.receipts.append(receipt)
            if self.attempt: self.attempt.record('actual-http-owned-create',{'method':method,'path':path},json.dumps(receipt),0,{'original_rc':None,'http_status':status})
        match=re.fullmatch(r'/api/v1/data-connection/syncs/('+UUID+r')/runs',path)
        if method=='GET' and service=='connector' and match and match[1] in self.defs:
            parent=self.defs[match[1]]
            if any(parent[k]!=identity[k] for k in identity): return
            rows=body if isinstance(body,list) else body.get('items',[]) if isinstance(body,dict) else []
            for run in rows:
                if isinstance(run,dict) and re.fullmatch(UUID,str(run.get('id'))) and run.get('sync_def_id')==parent['id'] and run.get('org_id')==identity['tenant'] and run.get('requested_by')==identity['actor']:
                    # Parent creation and the same authenticated list bind actor/tenant.
                    self.runs[run['id']]={**identity,'id':run['id'],'def_id':parent['id'],'status':run.get('status')}
    def authorize(self,sql,database):
        enabled=set(self.release.get('owned_sql_templates',[]))
        # Compare to complete exact original strings, never generic SQL regex.
        drop='ALTER TABLE dataset_files DROP CONSTRAINT IF EXISTS p1_smoke_block_publish;'
        if database=='datasets' and 'p1-new-target-check' in enabled:
            if sql==drop and 'p1-check' in self.pending:
                return 'p1-check-cleanup'
            for target in self.datasets:
                expected=drop+' ALTER TABLE dataset_files ADD CONSTRAINT p1_smoke_block_publish '+f"CHECK (NOT (dataset_id = '{target}'::uuid AND logical_path LIKE 'sync-%')) NOT VALID;"
                if sql==expected:
                    if not self.release.get('serial_owned_window'): raise ValueError('serial owned window required')
                    return 'p1-new-target-check'
        if database=='connectors' and 'p21-new-run-expiry' in enabled:
            for rid in self.runs:
                if sql==f"UPDATE sync_runs SET lease_expires_at = NOW() - INTERVAL '5 seconds' WHERE id = '{rid}'::uuid;":
                    return 'p21-new-run-expiry'
        if database=='connectors' and 'p21-new-def-terminal-trigger' in enabled:
            for did in self.defs:
                expected="CREATE OR REPLACE FUNCTION p21_c_terminal_fault() RETURNS trigger AS $$ BEGIN "+f"  IF NEW.sync_def_id::text = '{did}' "+"     AND NEW.status IN ('succeeded','failed','cancelled','error') THEN "+"    RAISE EXCEPTION 'p21 injected terminal-state fault';   END IF;   RETURN NEW; END $$ LANGUAGE plpgsql;"
                if sql==expected:
                    if not self.release.get('serial_owned_window'): raise ValueError('serial owned window required')
                    return 'p21-new-def-terminal-function'
            if sql=='CREATE TRIGGER p21_c_terminal_fault_trg BEFORE UPDATE ON sync_runs FOR EACH ROW EXECUTE FUNCTION p21_c_terminal_fault();' and 'p21-function' in self.pending:
                return 'p21-new-def-terminal-trigger'
            if sql=='DROP TRIGGER IF EXISTS p21_c_terminal_fault_trg ON sync_runs;' and 'p21-trigger' in self.pending:
                return 'p21-trigger-cleanup'
            if sql=='DROP FUNCTION IF EXISTS p21_c_terminal_fault();' and 'p21-function' in self.pending:
                return 'p21-function-cleanup'
        match=re.fullmatch(r"SELECT id FROM connections WHERE id = '("+UUID+r")'::uuid FOR UPDATE NOWAIT;",sql)
        if database=='connectors' and 'p23-owned-source-nowait' in enabled and match and match[1] in self.sources:
            return 'p23-owned-source-nowait'
        return None

    def complete(self,purpose,rc):
        if rc==0:
            cleanup={'p1-check-cleanup':'p1-check','p21-trigger-cleanup':'p21-trigger','p21-function-cleanup':'p21-function'}
            if purpose in cleanup: self.pending.discard(cleanup[purpose])

    def mark_install_attempt(self,purpose):
        key={'p1-new-target-check':'p1-check','p21-new-def-terminal-function':'p21-function','p21-new-def-terminal-trigger':'p21-trigger'}[purpose]
        self.pending.add(key)
        return key
