#!/usr/bin/env python3
"""Root-phase-gated executable facade over NEW copied legacy closure.

No old tools are loaded or executed. Suites retain their business assertions.
Only exact guard/compose/driver commands are routed. A release pins the exact
suite argv, deployment and typed roster bytes; absent phase means zero calls.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
sys.dont_write_bytecode = True
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import w10_guard_shim as GUARD
from w10_owned_sql import Registry
from w10_holder_seam import Holders
import w10_abc_held as HELD
TREE=HERE.parent/'vendor/tree'
# p1 denotes P1; p21...p25 denote numbered connector suites.
SUITES={'p1':'business-platform-p1/smoke/smoke.py',
        **{'p2'+str(n):'business-platform-p2-'+str(n)+'/smoke_p2'+str(n)+'.py' for n in range(1,6)}}


def load(path):
    path=Path(path).resolve()
    if not path.is_relative_to(TREE.resolve()): raise ValueError('non-NEW import refused')
    spec=importlib.util.spec_from_file_location('w10_'+path.stem,path)
    mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod); return mod


def candidate(path):
    raw=Path(path).read_text()
    for line in [raw]+raw.splitlines():
        try: obj=json.loads(line)
        except ValueError: continue
        if isinstance(obj,dict) and obj.get('matches_S') is True:
            return obj['offline_recipe_sha']
    raise ValueError('accepted source artifact absent')


class StageAbort(SystemExit):
    """Infrastructure refusal must escape copied scenario Exception catchers."""
    def __init__(self,reason):
        self.reason=reason
        super().__init__(2)


class Facade:
    def __init__(self,real,release,release_path,guard_check=None):
        self.real=real; self.release=release; self.release_path=release_path
        self.records=[]; self.guards=0; self.owned=Registry(release); self.dynamic_pins={}; self.cleanup_commands={}; self.deadline_transition_seen=False; self.holders=Holders(self); self.frozen_b={};  self.guard_check=guard_check or GUARD.check
    def require_phase(self):
        try:
            if GUARD.validate_release(self.release_path,self.release['stage'],self.release['suite'])!=self.release:
                raise ValueError('Root release changed during suite')
        except (ValueError,KeyError,OSError) as exc:
            self.abort_reason='Root phase refusal: '+str(exc)
            if hasattr(self,'attempt'):self.attempt.record('fatal-phase-refusal',{},self.abort_reason,'unavailable',{'original_rc':None,'adapter_exit_code':2})
            raise StageAbort(self.abort_reason) from exc
    def run(self,cmd,*args,**kwargs):
        driver=(isinstance(cmd,(list,tuple)) and len(cmd)>1 and cmd[0]=='sh' and cmd[1]==str(TREE/'business-platform-p2-2/p22_connector2.sh'))
        try:
            cp=self._run(cmd,*args,**kwargs)
            kind=self.records[-1].get('kind') if self.records else None
            infrastructure=driver or kind in ('compose-owned','guard','safe-config-readback') or (isinstance(cmd,(list,tuple)) and list(cmd[:2])==['docker','inspect'])
            if infrastructure and cp.returncode: raise ValueError(str(kind or 'owned-inspect')+' infrastructure original rc='+str(cp.returncode))
            return cp
        except StageAbort as exc:
            self.abort_reason=exc.reason
            if hasattr(self,'attempt'):self.attempt.record('fatal-phase-refusal',list(cmd),exc.reason,'unavailable',{'original_rc':None,'adapter_exit_code':2})
            raise
        except Exception as exc:
            reason=('driver' if driver else 'policy/identity')+' routing refusal: '+type(exc).__name__+': '+str(exc)
            self.abort_reason=reason
            if hasattr(self,'attempt'):self.attempt.record('fatal-routing-refusal',list(cmd),reason,'unavailable',{'original_rc':None,'adapter_exit_code':2})
            raise StageAbort(reason) from exc
    def _run(self,cmd,*args,**kwargs):
        if not isinstance(cmd,(list,tuple)): raise ValueError('shell command refused')
        self.require_phase()
        driver_key=None
        old=list(map(str,cmd)); new=list(old); kind='delegate'; sql_purpose=None; planned_install=None
        guards=[str(TREE/'business-platform-p2-3/p23_live_guard.py'),str(TREE/'business-platform-p2-4/p24_live_guard.py')]
        if any('live_guard.py' in part for part in old):
            if len(old)!=5 or old[:2]!=['python3','-I'] or old[2] not in guards or old[4]!='connector':
                raise ValueError('guard-looking argv does not match exact NEW guard')
            self.guards+=1; kind='guard'
            # Use captured real runner. Never recurse through subprocess.run.
            new=['python3','-I',str(HERE/'w10_guard_shim.py'),str(self.release_path)]
        elif len(old)>2 and old[0]=='sh' and old[1]==str(TREE/'business-platform-p2-2/p22_connector2.sh'):
            key=':'.join(old[2:]); driver_key=key; binding=self.release['driver_commands'][key]
            if GUARD.sha(binding['tool'])!=binding['tool_sha256']: raise ValueError('driver tool drift')
            new=binding['argv']; kind='driver'
        elif old[:2]==['docker','compose']:
            i=2
            while i<len(old) and old[i] in ('-f','--project-name','-p'):
                i+=2
            tail=old[i:]
            if not tail: raise ValueError('empty compose command')
            verb=tail[0]
            if verb in ('restart','stop','start','ps'):
                services=self.release['service_map']
                # Do not rewrite option values; unknown positional service names fail.
                mapped=[]; skip=False
                for part in tail[1:]:
                    if skip: mapped.append(part); skip=False; continue
                    if part in ('-t','--timeout'): mapped.append(part); skip=True
                    elif part.startswith('-'): mapped.append(part)
                    elif part in services: mapped.append(services[part])
                    else: raise ValueError('unowned compose target '+part)
                if not any(x in services.values() for x in mapped): raise ValueError('empty compose service set')
                new=['docker','compose','-f',self.release['aux_compose'],'--project-name',self.release['aux_project'],verb]+mapped
                kind='compose-owned'
            elif verb=='config' and len(tail)==1:
                name=self.release['service_map']['connector2']
                keys=('CONNECTOR_SYNC_ASYNC_WORKERS','CONNECTOR_SYNC_ASYNC_QUEUE','CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS','CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS')
                fmt=''.join('{{range .Config.Env}}{{if or '+ ' '.join('(eq (index (split . "=") 0) "'+k+'")' for k in keys)+'}}{{println .}}{{end}}{{end}}')
                new=['docker','inspect',name,'--format',fmt]; kind='safe-config-readback'
            elif verb=='exec':
                if 'postgres' not in tail or 'psql' not in tail: raise ValueError('only legacy postgres psql exec permitted')
                digest=hashlib.sha256(json.dumps(old,separators=(',',':')).encode()).hexdigest()
                if digest not in self.release.get('sql_write_argv_sha256',[]):
                    ci=new.index('-c') if '-c' in new else -1
                    sql=new[ci+1] if ci>=0 else ''
                    database=new[new.index('-d')+1] if '-d' in new else ''
                    sql_purpose=self.owned.authorize(sql,database)
                    # Enforce read-only in Postgres itself; SELECT calling a
                    # mutating function cannot bypass a lexical SQL check.
                    ci=new.index('-c') if '-c' in new else -1
                    if ci<0: raise ValueError('SQL without bounded -c refused')
                    if sql_purpose=='p23-owned-source-nowait':
                        new=new[:ci]+['-c','BEGIN;']+new[ci:]+['-c','ROLLBACK;']
                    elif sql_purpose is None:
                        # Quiet suppresses only psql command-status tags for our
                        # transaction wrapper. SQL rows and original rc stay raw.
                        quiet=[] if '-q' in new or '-qAt' in new else ['-q']
                        new=new[:ci]+quiet+['-c','BEGIN READ ONLY;']+new[ci:]+['-c','COMMIT;']
                    elif sql_purpose in ('p1-new-target-check','p21-new-def-terminal-function','p21-new-def-terminal-trigger'):
                        cleanup_sqls={
                            'p1-new-target-check':('p1-check',"ALTER TABLE dataset_files DROP CONSTRAINT IF EXISTS p1_smoke_block_publish;"),
                            'p21-new-def-terminal-function':('p21-function',"DROP FUNCTION IF EXISTS p21_c_terminal_fault();"),
                            'p21-new-def-terminal-trigger':('p21-trigger',"DROP TRIGGER IF EXISTS p21_c_terminal_fault_trg ON sync_runs;")}
                        cleanup_key,cleanup_sql=cleanup_sqls[sql_purpose]
                        # No cleanup authority exists while absence is unknown.
                        # Prove the exact public schema/table/object, not just a
                        # name in an unrelated schema or function overload.
                        catalog={
                            'p1-check':"SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public' AND t.relname='dataset_files' AND c.conname='p1_smoke_block_publish'; SELECT current_schema();",
                            'p21-function':"SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname='p21_c_terminal_fault' AND p.pronargs=0; SELECT current_schema();",
                            'p21-trigger':"SELECT count(*) FROM pg_trigger g JOIN pg_class t ON t.oid=g.tgrelid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public' AND t.relname='sync_runs' AND g.tgname='p21_c_terminal_fault_trg'; SELECT current_schema();"}
                        exists_cmd=new[:ci]+['-c',catalog[cleanup_key]]
                        att=getattr(self,'attempt',None); ep=att.step_begin('temporary-object-absent',exists_cmd) if att else None
                        try: existing=self.real(exists_cmd,capture_output=True,text=True,timeout=30)
                        except Exception as exc:
                            if att: att.step_finish('temporary-object-absent',ep,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None})
                            raise
                        if att: att.step_finish('temporary-object-absent',ep,existing.stdout,existing.returncode,{'stderr':existing.stderr,'original_rc':existing.returncode})
                        if existing.returncode or existing.stdout.strip().splitlines()!=['0','public']:
                            raise ValueError('exact temporary object absence/current schema not proven; no cleanup authority')
                        planned_install=(cleanup_key,new[:ci]+['-c',cleanup_sql])
                        # Only current actual-created Def IDs, never old9/8.
                        ids=','.join("'"+d+"'::uuid" for d in self.owned.defs)
                        if ids:
                            HELD.recheck(self)
                            frozen_clause=(' AND id NOT IN ('+','.join("'"+r+"'::uuid" for r in self.frozen_b)+')') if self.frozen_b else ''
                            check=new[:new.index('psql')+1]+['-t','-A','-v','ON_ERROR_STOP=1','-U','openfoundry','-d','connectors','-c',"SELECT count(*) FROM sync_runs WHERE sync_def_id IN ("+ids+") AND status IN ('pending','running')"+frozen_clause+';']
                            att=getattr(self,'attempt',None); r=att.step_begin('owned-inflight-zero',check) if att else None
                            live=self.real(check,capture_output=True,text=True,timeout=60)
                            if att: att.step_finish('owned-inflight-zero',r,live.stdout,live.returncode,{'stderr':live.stderr,'original_rc':live.returncode})
                            if live.returncode or live.stdout.strip()!='0': raise ValueError('owned in-flight not zero before global reversible DDL')
            else: raise ValueError('unapproved compose verb '+verb)
        elif old[:1]==['docker']:
            if len(old)<2 or old[1]!='inspect' or '--format' not in old:
                raise ValueError('unapproved direct docker action')
            fmt=old[old.index('--format')+1]
            if 'Config.Env' in fmt or 'json .' in fmt and fmt!='{{json .State}}':
                raise ValueError('unsafe inspect projection')
            allowed={'{{.State.StartedAt}}','{{json .State}}','{{.State.Running}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.FinishedAt}}'}
            if fmt not in allowed: raise ValueError('inspect format not whitelisted')
            pins={**json.loads(Path(self.release['deployment_record']).read_text())['containers'],**self.dynamic_pins}
            targets=[x for i,x in enumerate(old[2:],2) if x!='--format' and i!=old.index('--format')+1]
            if len(targets)!=1 or targets[0] not in set(pins)|{x['cid'] for x in pins.values()}:
                raise ValueError('inspect target not pinned')
        for part in new:
            if '/tools/smoke/business-platform-p' in part and not str(GUARD.P26) in part:
                raise ValueError('original old tool path refused')
        if kind=='compose-owned' and verb in ('stop','start','restart'):
            if GUARD.G.candidate_hash(GUARD.G.OPENFOUNDRY)!=self.release['candidate_source']:
                raise ValueError('actual source changed before mutation')
            if GUARD.sha(self.release['deployment_record'])!=self.release['deployment_record_sha256']:
                raise ValueError('deployment pins changed before mutation')
            pins={**json.loads(Path(self.release['deployment_record']).read_text())['containers'],**self.dynamic_pins}
            targets=[p for p in new if p in self.release['service_map'].values()]
            for target in targets:
                pin=pins[target]
                # The same CID is retained across stop/start/restart. The
                # released boot is checked before the first stop/restart;
                # stopped→start expects the prior boot and false Running.
                probe=['docker','inspect',target,'--format','{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}']
                att=getattr(self,'attempt',None)
                receipt=att.step_begin('mutation-identity',probe) if att else None
                fact=self.real(probe,capture_output=True,text=True,timeout=30)
                if att: att.step_finish('mutation-identity',receipt,fact.stdout,fact.returncode,{'stderr':fact.stderr})
                expected=[pin['cid'],pin['image'],pin['started_at'],'false' if verb=='start' else 'true']
                if fact.returncode or fact.stdout.strip().splitlines()!=expected:
                    raise ValueError('NEW mutation target actual CID/image/boot/state drift')
        # Driver commands are exact Root-pinned NEW executors. All suites enter
        # with one current guard; guard subprocesses preserve their real rc.
        att=getattr(self,'attempt',None)
        receipt=att.step_begin(kind,{'original':old,'replacement':new}) if att else None
        self.require_phase()
        if planned_install is not None:
            self.owned.mark_install_attempt(sql_purpose)
            self.cleanup_commands[planned_install[0]]=planned_install[1]
        try: cp=self.real(new,*args,**kwargs)
        except Exception as exc:
            if planned_install is not None and isinstance(exc,OSError):
                self.owned.pending.discard(planned_install[0]); self.cleanup_commands.pop(planned_install[0],None)
            if att: att.step_finish(kind,receipt,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None,'failure_kind':'spawn-or-timeout'})
            raise
        if att: att.step_finish(kind,receipt,cp.stdout,cp.returncode,{'stderr':cp.stderr})
        self.records.append({'kind':kind,'original_argv':old,'replacement_argv':new,
            'rc':cp.returncode,'stdout_sha256':hashlib.sha256(cp.stdout if isinstance(cp.stdout,bytes) else (cp.stdout or '').encode()).hexdigest(),
            'stderr_sha256':hashlib.sha256(cp.stderr if isinstance(cp.stderr,bytes) else (cp.stderr or '').encode()).hexdigest()})
        if kind=='compose-owned' and verb in ('stop','start','restart') and cp.returncode==0:
            for target in targets:
                initial=pins[target]
                probe=['docker','inspect',target,'--format','{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}']
                raw,step=self.read_actual('owned-'+verb+'-after',probe)
                lines=raw.strip().splitlines()
                if len(lines)!=4 or lines[:2]!=[initial['cid'],initial['image']] or lines[3]!=('false' if verb=='stop' else 'true'):
                    raise ValueError('authorized compose transition identity/state mismatch')
                if verb=='stop' and lines[2]!=initial['started_at']: raise ValueError('stop changed boot unexpectedly')
                if verb in ('start','restart') and lines[2]==initial['started_at']: raise ValueError('start/restart did not change actual boot')
                current={'cid':lines[0],'image':lines[1],'started_at':lines[2]}
                self.dynamic_pins[target]=current
                if target==self.release['service_map'].get('connector2') and verb in ('start','restart') and 'driver_predecessor_file' in self.release and not self.deadline_transition_seen:
                    chain={'schema':'p26-w10-compose-predecessor-v1','release_id':self.release['release_id'],'adapter_tool_sha256':GUARD.sha(__file__),'previous':{'name':target,**initial},'current':{'name':target,**current},'actual_transition':{'argv':new,'original_rc':cp.returncode,'evidence_step':receipt,'actual_after_step':step}}
                    predecessor=Path(self.release['driver_predecessor_file'])
                    if not predecessor.resolve().is_relative_to((GUARD.P26/'results/w10').resolve()): raise ValueError('predecessor must stay inside results/w10')
                    if predecessor.exists(): raise ValueError('predecessor collision; no overwrite/adoption')
                    with predecessor.open('x') as f: json.dump(chain,f,indent=2)
        if kind=='driver' and cp.returncode==0:
            text=cp.stdout.decode() if isinstance(cp.stdout,bytes) else cp.stdout
            proof=json.loads(text.strip().splitlines()[-1])
            if proof.get('schema')!='p26-w10-driver-result-v1' or proof.get('release_id')!=self.release['release_id'] or proof.get('action')!=driver_key:
                raise ValueError('driver chain phase/action mismatch')
            name=self.release['service_map']['connector2']
            initial={**json.loads(Path(self.release['deployment_record']).read_text())['containers'],**self.dynamic_pins}[name]
            previous=proof['previous']; current=proof['current']
            if previous!={'name':name,**initial} or current['name']!=name or current['image']!=initial['image'] or current['started_at']==initial['started_at']:
                raise ValueError('driver did not extend exact NEW A identity chain')
            if not GUARD.re.fullmatch('[0-9a-f]{64}',current['cid']): raise ValueError('driver new CID not full')
            ev=proof['evidence']
            if GUARD.sha(ev['path'])!=ev['sha256']: raise ValueError('driver original evidence SHA drift')
            original=json.loads(Path(ev['path']).read_text())
            if any(original.get(k)!=proof.get(k) for k in ('release_id','action','previous','current','current_config')):
                raise ValueError('driver result differs from original proof')
            labels={'of-p26-owner':'p26-http','of-p26-stage':'p2-6','of-p26-role':'connector_a','com.docker.compose.project':self.release['aux_project'],'com.docker.compose.service':name}
            if any(original['safe_labels'].get(k)!=v for k,v in labels.items()): raise ValueError('driver ownership labels drift')
            config=dict(zip(('CONNECTOR_SYNC_ASYNC_WORKERS','CONNECTOR_SYNC_ASYNC_QUEUE','CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS','CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS'),(1,64,7 if driver_key=='up-deadline:7' else 300,300)))
            if proof['current_config']!=config: raise ValueError('driver actual config mismatch')
            factcmd=['docker','inspect',name,'--format','{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}']
            factcmd[-1]+=''.join('\n{{index .Config.Labels "'+k+'"}}' for k in labels)
            receipt=att.step_begin('driver-current-identity',factcmd) if att else None
            fact=self.real(factcmd,capture_output=True,text=True,timeout=30)
            if att: att.step_finish('driver-current-identity',receipt,fact.stdout,fact.returncode,{'stderr':fact.stderr,'original_rc':fact.returncode})
            if fact.returncode or fact.stdout.strip().splitlines()!=[current['cid'],current['image'],current['started_at'],'true']+list(labels.values()):
                raise ValueError('driver new A actual identity mismatch')
            self.dynamic_pins[name]={k:current[k] for k in ('cid','image','started_at')}
            self.deadline_transition_seen=True
        if kind=='safe-config-readback' and cp.returncode==0:
            raw=cp.stdout.decode() if isinstance(cp.stdout,bytes) else cp.stdout
            lines=raw.strip().splitlines(); values={}
            for line in lines:
                key,value=line.split('=',1)
                if key not in keys or not value.isdigit(): raise ValueError('safe config key/value invalid')
                values[key]=value
            if set(values)!=set(keys): raise ValueError('effective config safe readback incomplete')
            projected='services:\n  connector2:\n    environment:\n'+''.join('      '+k+': "'+values[k]+'"\n' for k in keys)
            # Real inspect rc is preserved; only safe output is projected into
            # the logical connector2 namespace expected by the copied suite.
            cp=subprocess.CompletedProcess(new,cp.returncode,projected,cp.stderr)
        self.owned.complete(sql_purpose,cp.returncode)
        if kind=='guard': self.records[-1].update(stdout=cp.stdout,stderr=cp.stderr,guard_sha256=GUARD.sha(old[2]),shim_sha256=GUARD.sha(HERE/'w10_guard_shim.py'))
        return cp


    def read_actual(self,name,cmd):
        self.require_phase()
        receipt=self.attempt.step_begin(name,cmd)
        try: cp=self.real(cmd,capture_output=True,text=True,timeout=30)
        except Exception as exc:
            self.attempt.step_finish(name,receipt,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None})
            raise
        self.attempt.step_finish(name,receipt,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_rc':cp.returncode})
        if cp.returncode: raise ValueError(name+' original child rc='+str(cp.returncode))
        return cp.stdout,receipt

    def cleanup(self):
        """Revoke only this invocation's temporary fault objects/holders.

        No phase check here: expiry/revocation cannot strand our global DDL.
        Exact cleanup argv were captured before the corresponding install.
        """
        try: self.holders.cleanup()
        except Exception as exc:
            self.attempt.record('holder-outer-cleanup-error',{},type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None})
        for key in ('p21-trigger','p21-function','p1-check'):
            if key not in self.owned.pending: continue
            cmd=self.cleanup_commands.get(key)
            if cmd is None: continue
            receipt=self.attempt.step_begin('outer-owned-cleanup',{'authority':'this invocation installed object','object':key,'argv':cmd})
            try:
                cp=self.real(cmd,capture_output=True,text=True,timeout=30)
                self.attempt.step_finish('outer-owned-cleanup',receipt,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_rc':cp.returncode})
                if cp.returncode==0: self.owned.pending.discard(key)
            except Exception as exc:
                self.attempt.step_finish('outer-owned-cleanup',receipt,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None})


def execute(opts,runner=None,guard_check=None,loader=load):
    record_path=Path(opts.record).resolve()
    if not record_path.is_relative_to((GUARD.P26/'results/w10').resolve()) or record_path.exists():
        raise ValueError('record must be exclusive NEW results/w10 path')
    release=GUARD.validate_release(opts.release_file,opts.stage,opts.suite)
    tail=opts.suite_tail[1:] if opts.suite_tail[:1]==['--'] else opts.suite_tail
    if (release.get('schema')!='p26-w10-release-v1' or release.get('root_accepted') is not True
        or release.get('suite')!=opts.suite or release.get('suite_argv')!=tail
        or release.get('candidate_source')!=candidate(opts.candidate_source_file)
        or release.get('expect_guard_calls')!=opts.expect_guard_calls):
        raise ValueError('Root exact phase/suite/argv/S/count mismatch')
    if opts.suite!='p25':
        if '--env-dir' not in tail or Path(tail[tail.index('--env-dir')+1]).resolve()!=Path(opts.env_dir).resolve():
            raise ValueError('env-dir must be forwarded exactly')
    # Verify copied closure before imports; byte-equal copies have no fallback.
    for entry in json.loads((TREE.parent/'closure-ledger.json').read_text())['files']:
        if GUARD.sha(entry['copy'])!=entry['copy_sha256']: raise ValueError('copied closure drift')
    guard_check=guard_check or GUARD.check
    initial=guard_check(opts.release_file)
    real=runner or subprocess.run
    facade=Facade(real,release,opts.release_file,guard_check)
    facade.attempt=GUARD.W.Attempt(Path(opts.record).parent,'suite-commands')
    for key,value in os.environ.items():
        if any(k in key for k in ('JWT','TOKEN','SECRET','PASSWORD','CREDENTIAL')): facade.attempt.add_secret(value)
    env_file=Path(opts.env_dir)/'.env'
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if '=' in line and any(k in line.split('=',1)[0] for k in ('JWT','TOKEN','SECRET','PASSWORD','CREDENTIAL')):
                facade.attempt.add_secret(line.split('=',1)[1])
    path=TREE/SUITES[opts.suite]
    mod=loader(path)
    if opts.suite=='p24':
        # The copied P24 CLI only forwards env_dir to load_helpers; its report
        # and restart sideproof still use TMP_DEFAULT. Bind that output root
        # explicitly so no result is written into the historical q3 envdir.
        mod.TMP_DEFAULT=Path(opts.env_dir).resolve()
    legacy_binding=None
    if opts.suite=='p22':
        only=tail[tail.index('--only')+1].upper().split(',') if '--only' in tail else list('DEFGHIJKLMN')
        if 'N' in only:
            probe=release['legacy_cancel_probe']
            typed=json.loads(Path(release['typed_roster']).read_text())
            row=next((r for r in typed['runs'] if r['run_id']==probe['run_id']),None)
            if not row or row['protocol']!='legacy-null' or row['control_mode']!='legacy' or row['expected_state']!='running' or any(row[k]!=probe[k] for k in ('run_id','def_id','source_id','tenant','actor')):
                raise ValueError('N requires actual typed NEW legacy-null running probe')
            original=mod.P21_LEGACY_RUNS['C_committed_unconfirmed']
            if original==probe['run_id']: raise ValueError('old legacy C cancel is forbidden')
            original_identity=mod._legacy_owner_identity
            def new_legacy_identity(ev):
                saved=mod.P21_LEGACY_RUNS
                mod.P21_LEGACY_RUNS=dict(saved,C_committed_unconfirmed=probe['run_id'])
                try: return original_identity(ev)
                finally: mod.P21_LEGACY_RUNS=saved
            mod._legacy_owner_identity=new_legacy_identity
            legacy_binding={'purpose':'N409 uses typed NEW legacy probe; old C never cancelled','original':original,'replacement':probe['run_id']}
    facade.owned.attempt=facade.attempt
    facade.owned.before_http=facade.require_phase
    if opts.suite=='p1': facade.owned.instrument(mod)
    facade.holders.instrument(mod)
    if opts.suite=='p21': HELD.install(mod,facade)
    original_spec=importlib.util.spec_from_file_location
    p1_path=(TREE/SUITES['p1']).resolve()
    class OwnedLoader:
        def __init__(self,real): self.real=real
        def __getattr__(self,name): return getattr(self.real,name)
        def exec_module(self,module):
            self.real.exec_module(module)
            if Path(module.__file__).resolve()==p1_path: facade.owned.instrument(module)
            else: facade.holders.instrument(module)
    def owned_spec(name,location,*args,**kwargs):
        spec=original_spec(name,location,*args,**kwargs)
        if Path(location).resolve() in (p1_path,(TREE/SUITES['p23']).resolve()): spec.loader=OwnedLoader(spec.loader)
        return spec
    importlib.util.spec_from_file_location=owned_spec
    argv_before=sys.argv; run_before=subprocess.run
    rc=1; raised=None
    try:
        sys.argv=[str(path)]+tail
        subprocess.run=facade.run
        try: rc=mod.main() or 0
        except SystemExit as exc:
            rc=exc.code if isinstance(exc.code,int) else 1
            if isinstance(exc,StageAbort): raised=exc.reason
        except Exception as exc: raised=type(exc).__name__+': '+str(exc); rc=1
    finally:
        try: facade.cleanup()
        except Exception as exc:
            raised=(raised or '')+'; outer cleanup '+type(exc).__name__+': '+str(exc); rc=rc or 1
        subprocess.run=run_before; sys.argv=argv_before
        importlib.util.spec_from_file_location=original_spec
        count_ok=facade.guards==opts.expect_guard_calls
        cleanup_ok=not facade.owned.pending and not facade.holders.active
        record={'schema':'p26-w10-execution-v1','suite':opts.suite,'suite_rc':rc,
            'raised':raised,'abort_reason':getattr(facade,'abort_reason',None),'initial_guard':initial,'guard_calls':facade.guards,
            'runtime_frozen_B':facade.frozen_b,'legacy_cancel_binding':legacy_binding,'cleanup_ok':cleanup_ok,'dynamic_new_identity_chain':facade.dynamic_pins,'pending_holders':len(facade.holders.active),'pending_sql_cleanup':sorted(facade.owned.pending),'actual_owned_receipts':facade.owned.receipts,
            'expected_guard_calls':opts.expect_guard_calls,'count_ok':count_ok,'commands':facade.records,
            'suite_sha256':GUARD.sha(path),'release_sha256':GUARD.sha(opts.release_file)}
        # Exclusive create: failure attempts are never overwritten.
        with Path(opts.record).open('x') as f: f.write(facade.attempt.redact(json.dumps(record,indent=2))+'\n')
    return rc if count_ok and cleanup_ok else 2


def main(argv=None):
    ap=argparse.ArgumentParser()
    ap.add_argument('--stage',required=True)
    ap.add_argument('--suite',choices=SUITES,required=True)
    ap.add_argument('--release-file',required=True)
    ap.add_argument('--candidate-source-file',required=True)
    ap.add_argument('--env-dir',required=True)
    ap.add_argument('--expect-guard-calls',type=int,required=True)
    ap.add_argument('--record',required=True)
    ap.add_argument('suite_tail',nargs=argparse.REMAINDER)
    opts=ap.parse_args(argv)
    return execute(opts)
if __name__=='__main__':
    try: sys.exit(main())
    except (ValueError,KeyError,OSError) as exc:
        print('w10 adapter refused: '+str(exc),file=sys.stderr); sys.exit(2)
