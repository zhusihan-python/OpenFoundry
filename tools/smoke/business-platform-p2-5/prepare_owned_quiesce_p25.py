#!/usr/bin/env python3
"""Pre-ABC P25-only schedule cleanup and one blocked POST per actual owner.

No Source/target/checkpoint/Run mutation. Refuses before live IO until
root releases this producer AND the readonly quiesce collector.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import types

HERE = Path(__file__).resolve().parent
RELEASED_FOR_EXECUTION = True
H14_VERIFIER_SHA = '00dcbea6b35033d9cd529255714884e8ef35b2f75e60ddd6fd3c493fe25c1832'
BASES = {'connector':'http://127.0.0.1:17094','dataset':'http://127.0.0.1:17078',
         'fixture':'http://127.0.0.1:17092','proxy_admin':'http://127.0.0.1:17093'}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + '\n')


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE/filename)
    result = importlib.util.module_from_spec(spec); spec.loader.exec_module(result)
    return result


def owned_inputs(ev, engine):
    """Only actual P25 handles; cross-org P24 history is never a write scope."""
    ids, inputs, parents = set(), [], {}
    for path in sorted(Path(ev).rglob('handles.json')):
        value = json.loads(path.read_text())
        if not isinstance(value, dict) or not isinstance(value.get('handles'), list):
            raise ValueError('malformed actual handles file')
        selected = [h for h in value['handles'] if h.get('ok') is True and h.get('kind') in ('def','h10-def')]
        if selected:
            if any(not engine._is_uuidish(h.get('ref')) for h in selected):
                raise ValueError('non-UUID actual owned def handle')
            ids.update(h['ref'] for h in selected)
            inputs.append({'kind':'p25-handles','path':str(path.resolve()),'sha256':sha(path)})
    for path in sorted(Path(ev).rglob('*.json')):
        value = json.loads(path.read_text())
        if not isinstance(value,dict) or value.get('method')!='POST' or value.get('status') not in (200,201) or not str(value.get('path','')).endswith('/api/v1/data-connection/syncs'):
            continue
        body = json.loads(value['resp_body']); did, sid = body.get('id'), body.get('source_id')
        if did in ids:
            if not engine._is_uuidish(sid) or (did in parents and parents[did]!=sid):
                raise ValueError('creation-source parent missing/ambiguous')
            parents[did]=sid
            inputs.append({'kind':'p25-def-create-response','path':str(path.resolve()),'sha256':sha(path)})
    if not ids or set(parents)!=ids:
        raise ValueError('every actual P25 def needs its real creation source parent')
    return sorted(ids), parents, inputs


class LiveIO:
    def api(self, engine, recorder):
        return engine.HttpApi(BASES, os.environ['OPENFOUNDRY_SMOKE_JWT'], recorder, 'owned-quiesce')

    def command(self, argv):
        p=subprocess.run(argv,capture_output=True,text=True,timeout=90)
        return p.returncode,p.stdout,p.stderr


class Runner:
    def __init__(self,args,engine,quiesce,io,ids,parents,inputs,owners,declaration):
        self.a,self.m,self.q,self.io=args,engine,quiesce,io
        self.ids,self.parents,self.owners=ids,parents,owners
        if len(set(ids))!=len(ids) or len({o['def_id'] for o in owners})!=len(owners) or any(o['def_id'] not in ids for o in owners):
            raise ValueError('owned/owner set duplicates or out-of-scope owner')
        self.root=Path(args.evidence_root).resolve();self.root.mkdir(parents=True,exist_ok=False)
        self.rec=engine.AttemptRecorder(str(self.root/'http'),'owned-quiesce')
        self.api=io.api(engine,self.rec);self.sequence=0
        self.report={'schema':'p25-preabc-owned-quiesce-v1','ok':False,'inputs':inputs,
                     'expected_defs':ids,'owners':owners,'actions':[]}
        self.proof={'schema':'p25-blocked-proof-v2','ok':False,'phase':'pre-abc','entries':{}}
        self.declaration=declaration
        self.selected=json.loads(Path(args.tail_summary).read_text())
        write(self.root/'expected-defs.json',ids);write(self.root/'owner-roster.json',owners)

    def call(self,method,path,body=None):
        self.sequence+=1;prefix=self.root/('http-%04d'%self.sequence)
        cmd=prefix.with_suffix('.cmd.json');write(cmd,{'method':method,'url':BASES['connector']+path,'body':body,'auth':'in-memory main actor'})
        response=self.api.call('connector',method,path,body=body,timeout=15)
        raw=response.get('raw',b'');raw=raw.encode() if isinstance(raw,str) else raw
        files={'cmd':cmd}
        for kind,data in [('body',raw),('status',str(response['status']).encode()+b'\n'),
                          ('rc',('0\n' if response.get('error') is None else '1\n').encode()),
                          ('stderr',(response.get('error') or '').encode())]:
            p=prefix.with_suffix('.'+kind);p.write_bytes(data);files[kind]=p
        record={'method':method,'url':BASES['connector']+path,'http_status':response['status'],
                'rc':0 if response.get('error') is None else 1,
                'artifacts':{k:{'path':str(p),'sha256':sha(p)} for k,p in files.items()}}
        self.report['actions'].append(record)
        if record['rc']!=0:raise ValueError('HTTP transport failed (raw retained)')
        return response,record

    def get(self,path):
        r,record=self.call('GET',path)
        if r['status']!=200:raise ValueError('required GET failed (raw retained)')
        return r['json'],record

    def snapshot(self):
        views={}
        for did in self.ids:
            cp,record=self.call('GET',self.m.route('checkpoint_view',id=did))
            if cp['status']==404 and cp['json']=={'error':'incremental checkpoint not initialized for this sync definition'}:
                checkpoint={'uninitialized':True}
            elif cp['status']==200 and isinstance(cp['json'],dict):checkpoint=cp['json']
            else:raise ValueError('checkpoint read failed, not exact fresh404')
            runs,_=self.get(self.m.route('sync_runs',id=did))
            if not isinstance(runs,list) or any(r.get('sync_def_id')!=did or not self.m._is_uuidish(r.get('id')) for r in runs) or len({r['id'] for r in runs})!=len(runs):
                raise ValueError('run list malformed/duplicate/parent differs')
            views[did]={'checkpoint':checkpoint,'runs':sorted(runs,key=lambda r:r['id'])}
        return views

    def run(self):
        try:
            # Resolve every parent BEFORE constructing ANY mutating request.
            source_seen=set();targets={}
            for did in self.ids:
                definition,_=self.get(self.m.route('sync_get',id=did))
                if definition.get('id')!=did or definition.get('source_id')!=self.parents[did]:
                    raise ValueError('actual Def/source parent differs from captured creation')
                targets[did]=definition.get('output_dataset_id')
                sid=self.parents[did]
                if sid not in source_seen:
                    source,_=self.get('/api/v1/connections/'+sid)
                    if source.get('id')!=sid:raise ValueError('actual source parent missing')
                    source_seen.add(sid)
            before=self.snapshot();write(self.root/'before.json',before)
            held={(o['def_id'],o['run_id']) for o in self.owners if o['run_ref']=='same'}
            live={(did,r['id']) for did,view in before.items() for r in view['runs'] if r['status'] in ('pending','running')}
            if live!=held:raise ValueError('unknown/missing nonterminal P25 Run before any mutation')
            selected_owners={o['def_id']:o for o in self.selected['final_owner_roster']}
            for owner in self.owners:
                did=owner['def_id'];cp=before[did]['checkpoint'].get('owner') or {}
                if cp.get('run_id')!=owner['run_id'] or cp.get('blocked_reason')!='requires_reconciliation' or cp.get('run_status')!=selected_owners[did]['run_status']:
                    raise ValueError('actual owner differs before probes')
                actual=[r for r in before[did]['runs'] if r['id']==owner['run_id']]
                if len(actual)!=1 or actual[0]['sync_def_id']!=did or actual[0]['status']!=selected_owners[did]['run_status']:
                    raise ValueError('owner actual Run/parent missing before any mutation')
            for selected in self.selected['final_run_roster']:
                actual=next(r for r in before[selected['def_id']]['runs'] if r['id']==selected['run_id'])
                if any(actual.get(k)!=selected.get(k) for k in ('sync_def_id','status','generation','attempt','control_mode','publish_entered_at')):
                    raise ValueError('selected held protected Run changed before mutations')
            for did in self.ids:
                for method,path,body in [('DELETE',self.m.route('schedule_revoke',id=did),None),
                                         ('PATCH',self.m.route('sync_patch',id=did),{'schedule_enabled':False})]:
                    r,_=self.call(method,path,body)
                    if r['status'] not in (200,204):raise ValueError('owned schedule revoke/disable failed')
            for owner in sorted(self.owners,key=lambda row:row['def_id']):
                did=owner['def_id'];r,record=self.call('POST',self.m.route('sync_run',id=did)+'?async=true')
                entry={'http_status':r['status'],'rc':record['rc'],'run_id':owner['run_id'],
                       'body_sha256':record['artifacts']['body']['sha256'],'raw':record['artifacts']}
                self.proof['entries'][did]=entry
                if r['status'] in (200,201,202) and isinstance(r.get('json'),dict) and self.m._is_uuidish(r['json'].get('id')):
                    self.rec.persist_handle('run',r['json']['id'],{'def':did,'origin':'unexpected-block-probe-admission'})
                if r['status']!=409 or not isinstance(r.get('json'),dict) or r['json'].get('code')!='sync_incremental_busy' or r['json'].get('active_run_id')!=owner['run_id']:
                    raise ValueError('owner probe was not exact409/sync_incremental_busy/sameRun; retain unknown admission')
            after=self.snapshot();write(self.root/'after.json',after)
            if before!=after:raise ValueError('public checkpoint/complete Run sets changed during cleanup/probes')
            for did in self.ids:
                definition,_=self.get(self.m.route('sync_get',id=did))
                if definition.get('source_id')!=self.parents[did] or definition.get('output_dataset_id')!=targets[did]:
                    raise ValueError('Def source/target changed')
            self.proof['ok']=True;write(self.root/'blocked-proof.json',self.proof)
            args=types.SimpleNamespace(env_dir=self.a.env_dir,expected_defs=str(self.root/'expected-defs.json'),
                 declared_at=self.declaration['declared_at'],out=str(self.root/'state.json'))
            if self.q.collect_body(args,self.io.command)!=0:raise ValueError('readonly quiesce collection failed')
            state=json.loads((self.root/'state.json').read_text())
            verdict=self.q.adjudicate_quiesce(state,self.owners,self.proof,self.ids)
            write(self.root/'quiesce-verdict.json',verdict)
            if verdict['ok'] is not True:raise ValueError('quiesce rejected: '+str(verdict['reasons']))
            self.report.update(ok=True,blocked_proof=str(self.root/'blocked-proof.json'),state=str(self.root/'state.json'),
                               verdict=str(self.root/'quiesce-verdict.json'),public_state_unchanged=True)
            return 0
        except Exception as exc:
            self.report['error']=type(exc).__name__+': '+str(exc)
            return 1
        finally:
            write(self.root/'blocked-proof.json',self.proof)
            self.report['handles_file']=self.rec.handles_path
            self.report['handles_sha256']=sha(self.rec.handles_path)
            write(self.root/'result.json',self.report)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('ev','tail-summary','tail-bindings','tail-declaration','evidence-root'):
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--env-dir',default='/private/tmp/openfoundry-platform-p1-q3v9')
    parser.add_argument('--released',action='store_true');args=parser.parse_args(argv)
    q=module('p25_owned_check','check_owned_quiesced_p25.py')
    if not RELEASED_FOR_EXECUTION or not q.RELEASED_FOR_EXECUTION or not args.released or os.environ.get('P25_CONFIRM')!='yes':
        print('pre-ABC producer/readonly collector not released; zero live IO',file=sys.stderr);return 3
    try:
        if args.env_dir!='/private/tmp/openfoundry-platform-p1-q3v9' or not os.environ.get('OPENFOUNDRY_SMOKE_JWT'):
            raise ValueError('requires fixed q3 dir and in-memory main actor JWT')
        verifier=HERE/'verify_h14_supplement_p25.py'
        if sha(verifier)!=H14_VERIFIER_SHA:raise ValueError('reviewed tail supplement verifier drift')
        h14=module('p25_owned_h14','verify_h14_supplement_p25.py')
        if h14.verify_supplement(args.tail_summary)['ok'] is not True:raise ValueError('selected tail provenance rejected')
        roster=module('p25_owned_roster','p25_tail_roster.py');engine=roster.http_pure()
        owners=roster.actual_owner_roster(args.tail_declaration,args.tail_summary,args.tail_bindings)
        ids,parents,inputs=owned_inputs(args.ev,engine)
        if not owners or any(o['def_id'] not in ids for o in owners):raise ValueError('actual owner not in exact real P25 handles')
        inputs.extend({'kind':'selected-tail','path':str(Path(p).resolve()),'sha256':sha(p)}
                      for p in (args.tail_summary,args.tail_bindings,args.tail_declaration))
        return Runner(args,engine,q,LiveIO(),ids,parents,inputs,owners,json.loads(Path(args.tail_declaration).read_text())).run()
    except Exception as exc:
        print(type(exc).__name__+': '+str(exc),file=sys.stderr);return 1


if __name__=='__main__':raise SystemExit(main())
