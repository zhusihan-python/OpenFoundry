#!/usr/bin/env python3
"""Executable NEW P25 copied-closure entrypoint. Root JSON gate before IO."""
import argparse,base64,hashlib,importlib.util,json,os,subprocess,sys,time
from pathlib import Path
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent; P26=HERE.parents[1]
sys.path.insert(0,str(HERE.parent/'adapters')); sys.path.insert(0,str(HERE))
import w10_guard_shim as H
from w10_suite_adapter import Facade
from p26_p25_transport import install
COPY=HERE.parent/'vendor/p25-runtime'
SOURCE='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'


def load(name):
    path=COPY/name
    spec=importlib.util.spec_from_file_location('p26_copy_'+path.stem,path)
    mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod); return mod


STAGE_BASES={'connector':'http://127.0.0.1:17100','dataset':'http://127.0.0.1:17078','fixture':'http://127.0.0.1:17103','proxy_admin':'http://127.0.0.1:17104'}
FIXTURE_SOURCE_URL='http://p26p25fixture:8090'


def validate_stage_origins(bases,mode,tail):
    if bases!=STAGE_BASES: raise ValueError('P25 owned stage origins mismatch')
    if mode in ('engine','engine-child','h10'):
        if tail.count('--fixture-src-url')!=1 or tail[tail.index('--fixture-src-url')+1]!=FIXTURE_SOURCE_URL:
            raise ValueError('P25 fixed independent fixture source URL required')


def h9_natural_drain(ctx,api,timeout_s=10.0):
    """Observe the canceled fixture HTTP tail; never mutate or excuse it."""
    started=time.monotonic(); deadline=started+timeout_s; sample=0
    while True:
        sample+=1; responses={}
        for base,path in (('fixture','/admin/src/p25h6'),('proxy_admin','/admin/stats')):
            remaining=deadline-time.monotonic()
            if remaining<=0: raise ValueError('h9 natural drain timeout; no force finish')
            response=api.call(base,'GET',path,timeout=min(2.0,remaining))
            if response['status']!=200 or response.get('error') is not None or not isinstance(response.get('json'),dict):
                raise ValueError('h9 natural drain invalid actual GET')
            responses[base]=response['json']
        fixture=responses['fixture'];proxy=responses['proxy_admin']
        if fixture.get('name')!='p25h6': raise ValueError('h9 drain wrong actual fixture src')
        for body,keys in ((fixture,('inventory_started','inventory_finished','active')),(proxy,('commit_started','commit_finished','commit_inflight'))):
            if any(type(body.get(k)) is not int or body[k]<0 for k in keys): raise ValueError('h9 drain invalid counters')
            if body[keys[0]]-body[keys[1]]!=body[keys[2]]: raise ValueError('h9 drain inconsistent lifecycle counters')
        elapsed=time.monotonic()-started
        quiet=fixture['active']==0 and fixture['inventory_started']==fixture['inventory_finished'] and proxy['commit_inflight']==0 and proxy['commit_started']==proxy['commit_finished']
        ctx.attempt.record('h9-natural-drain-sample',{'sample':sample},json.dumps({'elapsed_seconds':elapsed,'fixture':fixture,'proxy':proxy,'quiet':quiet}),0,{'original_rc':None,'kind':'derived-from-recorded-GETs'})
        if elapsed>timeout_s: raise ValueError('h9 natural drain timeout')
        if quiet:return {'samples':sample,'elapsed_seconds':elapsed}
        remaining=deadline-time.monotonic()
        if remaining<=0: raise ValueError('h9 natural drain timeout; no force finish')
        time.sleep(min(.25,remaining))


def check_closure():
    ledger=json.loads((COPY.parent/'p25-closure-ledger.json').read_text())
    for item in ledger['files']:
        if not Path(item['copy']).resolve().is_relative_to(COPY.resolve()) or H.sha(item['copy'])!=item['copy_sha256']:
            raise ValueError('NEW P25 copy closure drift')


def claims(token):
    token=token.removeprefix('Bearer ').removeprefix('bearer ')
    segment=token.split('.')[1]
    obj=json.loads(base64.urlsafe_b64decode(segment+'='*(-len(segment)%4)))
    return {'actor':obj['sub'],'tenant':obj['org_id']}


def check_pg_proof(release,tail):
    if '--pg-proof-file' not in tail: return
    proof_path=tail[tail.index('--pg-proof-file')+1]
    if H.sha(proof_path)!=release.get('p25_inputs',{}).get(proof_path) or json.loads(Path(proof_path).read_text()).get('candidate_source_sha256')!=release['candidate_source']:
        raise ValueError('h12 proof must be actual Root-pinned same current source, not a prior S summary')


def validate_retained_handles_inputs(release, paths):
    seen=set()
    for path in paths:
        actual=Path(path).resolve()
        if not Path(path).is_absolute() or str(actual)!=path or path in seen:
            raise ValueError('retained handles must be unique exact absolute paths')
        seen.add(path)
        if H.sha(actual)!=release.get('p25_inputs',{}).get(path):
            raise ValueError('retained handles not exact Root-pinned actual input')


class Context:
    def __init__(self,args,initial_guard=True):
        self.args=args; self.W=H.W; self.release_path=Path(args.release_file).resolve()
        self.release=H.validate_release(self.release_path,suite='p25')
        if self.release['p25_closure_ledger_sha256']!=H.sha(COPY.parent/'p25-closure-ledger.json'): raise ValueError('Root copy-ledger pin drift')
        check_closure()
        required_tools={'p26_p25_transport.py','p26_p25_typed_guard.py','p26_p25_export.py','p26_p25_supplements.py','p26_healthy_bundle.py','p26_h13_ledger_read.py'}
        if set(self.release['p25_tool_shas'])!=required_tools: raise ValueError('exact P25 supporting tool set required')
        for name,want in self.release['p25_tool_shas'].items():
            if Path(name).name!=name or H.sha(HERE/name)!=want: raise ValueError('Root P25 supporting tool pin drift')
        if H.G.candidate_hash(H.G.OPENFOUNDRY)!=SOURCE or self.release['candidate_source']!=SOURCE:
            raise ValueError('actual source is not frozen full S')
        check_pg_proof(self.release,args.tail)
        self.mode=args.mode
        if self.mode=='engine-child':
            if self.release['p25_mode']!='h10': raise ValueError('child outside h10 release')
            cp=Path(args.child_context).resolve()
            if not cp.is_relative_to(Path(self.release['p25_output']).resolve()) or H.sha(cp)!=args.child_context_sha256:
                raise ValueError('child context path/hash not parent-bound')
            self.child=json.loads(cp.read_text())
            if self.child['release_id']!=self.release['release_id'] or self.child['runtime_sha256']!=H.sha(__file__) or self.child['argv']!=args.tail:
                raise ValueError('child exact argv/release/tool mismatch')
            if self.child['stage'] not in ('pre','post'): raise ValueError('child must be actual h10 pre/post')
            allowed={'--phase','--only','--h10-stage','--h10-defs','--h10-held-def','--owned-handles-file','--h10-prior','--h10-expected-image','--connector-base','--dataset-base','--fixture-base','--proxy-base','--fixture-src-url','--evidence-dir','--result-file','--released'}
            if self.child['stage']=='post': allowed.add('--h10-restart-record')
            parsed={}; i=0
            while i<len(args.tail):
                key=args.tail[i]
                if key not in allowed or key in parsed: raise ValueError('unexpected/duplicate h10 child option')
                parsed[key]=True if key=='--released' else args.tail[i+1]
                i+=1 if key=='--released' else 2
            if set(parsed)!=allowed or parsed['--phase']!='healthy' or parsed['--only']!='h10' or parsed['--h10-stage']!=self.child['stage']:
                raise ValueError('child must be exact healthy/h10/pre-or-post read-only CLI')
        elif self.release.get('p25_mode')!=self.mode or self.release.get('p25_argv')!=args.tail:
            raise ValueError('Root exact P25 mode/argv mismatch')
        if self.mode=='h10':
            parsed_h10=load('h10_crash_runner_p25.py').parser().parse_args(args.tail)
            validate_retained_handles_inputs(self.release,parsed_h10.retained_handles_file)
        for field in ('deployment_record','typed_roster'):
            if H.sha(self.release[field])!=self.release[field+'_sha256']: raise ValueError('Root '+field+' pin drift')
        if self.release['p25_runtime_sha256']!=H.sha(__file__): raise ValueError('Root runtime tool pin drift')
        if os.environ.get('P25_CONFIRM')!='yes': raise ValueError('P25_CONFIRM=yes required')
        output=Path(args.output).resolve()
        if self.mode!='engine-child' and output!=Path(self.release['p25_output']).resolve(): raise ValueError('Root exact output path mismatch')
        if not output.is_relative_to((P26/'results/w10').resolve()) or output.exists(): raise ValueError('exclusive NEW output required')
        for path,want in self.release.get('p25_inputs',{}).items():
            if H.sha(path)!=want: raise ValueError('Root input hash drift '+path)
        output.mkdir(parents=True); self.output=output
        self.attempt=H.W.Attempt(output,'runtime')
        original_finish=self.attempt.step_finish
        def byte_safe_finish(sid,path,raw,rc,meta=None,kind='call'):
            meta={k:(v.decode(errors='replace') if isinstance(v,bytes) else v) for k,v in (meta or {}).items()}
            return original_finish(sid,path,raw,rc,meta,kind)
        self.attempt.step_finish=byte_safe_finish
        for key,value in os.environ.items():
            if any(word in key for word in ('TOKEN','JWT','SECRET','PASSWORD','CREDENTIAL')): self.attempt.add_secret(value)
        self.bases=self.release['p25_bases']
        validate_stage_origins(self.bases,self.mode,args.tail)
        main_identity=claims(os.environ['OPENFOUNDRY_SMOKE_JWT'])
        if main_identity!=self.release['p25_identity']: raise ValueError('wrong actual main JWT identity')
        if claims(os.environ.get('OPENFOUNDRY_SMOKE_ARM_JWT',os.environ['OPENFOUNDRY_SMOKE_JWT']))!=main_identity: raise ValueError('arm identity differs from Source owner')
        if self.mode in ('engine','h10') and claims(os.environ['OPENFOUNDRY_SMOKE_JWT_ORG2'])!=self.release['p25_org2_identity']: raise ValueError('wrong actual org2 JWT identity')
        self.created={}; self.counts={'kill':0,'start':0}; self.runner=None
        self.target='p26connectorc'; self.pin=json.loads(Path(self.release['deployment_record']).read_text())['containers'][self.target]
        self.real_run=subprocess.run
        self.facade=Facade(self.real_run,self.release,self.release_path); self.facade.attempt=self.attempt
        if initial_guard and self.mode not in ('engine-child','freeze-held'): self.initial=H.check(self.release_path)
        else: self.initial={'scope':'parent h10 exact-child argv; parent guards immediately before spawn'}
    def gate(self):
        if H.validate_release(self.release_path,suite='p25')!=self.release: raise ValueError('Root release changed')
        if H.G.candidate_hash(H.G.OPENFOUNDRY)!=SOURCE: raise ValueError('actual source changed during P25')
        if H.sha(__file__)!=self.release['p25_runtime_sha256'] or H.sha(COPY.parent/'p25-closure-ledger.json')!=self.release['p25_closure_ledger_sha256']: raise ValueError('P25 active entry/ledger drift')
        for name,want in self.release['p25_tool_shas'].items():
            if H.sha(HERE/name)!=want: raise ValueError('P25 supporting tool changed during invocation')
        for field in ('deployment_record','typed_roster'):
            if H.sha(self.release[field])!=self.release[field+'_sha256']: raise ValueError('P25 preservation input changed')
    def command(self,tag,argv,timeout=120):
        self.gate(); before=self.attempt.step_begin(tag,argv)
        try: cp=self.real_run(argv,capture_output=True,timeout=timeout)
        except Exception as exc:
            self.attempt.step_finish(tag,before,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None}); raise
        self.attempt.step_finish(tag,before,cp.stdout,cp.returncode,{'stderr':cp.stderr.decode(errors='replace') if isinstance(cp.stderr,bytes) else cp.stderr,'original_rc':cp.returncode})
        self.last_command_evidence={'argv':list(argv),'original_rc':cp.returncode,'files':{suffix:{'path':str(before)+suffix,'sha256':H.sha(str(before)+suffix)} for suffix in ('.cmd.json','.raw','.rc','.meta.json')}}
        return cp
    def sql(self,tag,argv):
        cp=self.command(tag,argv)
        if cp.returncode: raise ValueError(tag+' original rc='+str(cp.returncode))
        return cp.stdout.decode()
    def observe(self,base,method,path,body,result,receipt,token=None):
        if result['status'] not in (200,201,202) or not isinstance(result['json'],dict): return
        response=result['json']; identity=claims(token or os.environ['OPENFOUNDRY_SMOKE_JWT'])
        rid=response.get('id')
        if not isinstance(rid,str) or not H.G.UUID_RE.fullmatch(rid): return
        kind=None
        if method=='POST' and path in ('/v1/datasets','/api/v1/datasets'): kind='dataset'
        if method=='POST' and path in ('/api/v1/connections','/api/v1/data-connection/sources'): kind='source'
        if method=='POST' and path=='/api/v1/data-connection/syncs': kind='def'
        if method=='POST' and path.split('?',1)[0].endswith('/run'): kind='run'
        if kind:
            self.created[rid]={'kind':kind,'id':rid,**identity,'source_id':response.get('source_id') or (body or {}).get('source_id'),'output_dataset_id':response.get('output_dataset_id') or (body or {}).get('output_dataset_id'),'sync_def_id':response.get('sync_def_id'),'name':(body or {}).get('name'),'original_http_step':receipt}
    def engine(self): return install(load('smoke_p25.py'),self)
    def invoke_engine(self,tail):
        engine=self.engine(); previous=sys.argv; oldrun=subprocess.run
        original_scenario=engine.P25Engine.run_scenario
        if self.mode=='engine':
            ctx=self
            def scenario_with_h9_drain(instance,sid,*args,**kwargs):
                result=original_scenario(instance,sid,*args,**kwargs)
                if sid=='h9':
                    ctx.attempt.record('h9-original-scenario-before-drain',{'scenario':sid},json.dumps(result.to_json()),0,{'original_rc':None,'business_gates_unchanged':True})
                    try:ctx.h9_drain_result=h9_natural_drain(ctx,instance.api_factory('h9-natural-drain'))
                    except Exception as exc:
                        ctx.h9_drain_failure=type(exc).__name__+': '+str(exc)
                        ctx.attempt.record('h9-natural-drain-failed',{'scenario':sid},ctx.h9_drain_failure,'unavailable',{'original_rc':None})
                return result
            engine.P25Engine.run_scenario=scenario_with_h9_drain
        try:
            sys.argv=[str(COPY/'smoke_p25.py')]+tail
            # Only actual h13's fixed Postgres fact-reader command uses run.
            subprocess.run=self.facade.run
            rc=engine.main() or 0
            self.engine_original_rc=rc
            if self.mode=='engine' and '--result-file' in tail:
                summary=Path(tail[tail.index('--result-file')+1])
                if summary.exists():
                    from p26_p25_export import export_summary
                    export_summary(self,summary)
            if getattr(self,'h9_drain_failure',None): raise ValueError('h9 natural drain barrier failed; original finally evidence retained')
            return rc
        finally:
            engine.P25Engine.run_scenario=original_scenario
            subprocess.run=oldrun; sys.argv=previous
    def h10_child_argv(self,runner,stage,original):
        tail=original[3:]
        context=runner.root/('child-context-'+stage+'.json')
        context.write_text(json.dumps({'release_id':self.release['release_id'],'runtime_sha256':H.sha(__file__),'stage':stage,'argv':tail,'held':runner.held,'quiet':runner.quiet},indent=2)+'\n')
        return [sys.executable,'-I','-B',str(Path(__file__).resolve()),'--release-file',str(self.release_path),'--mode','engine-child','--output',str(runner.root/('child-'+stage)),'--child-context',str(context),'--child-context-sha256',H.sha(context),'--']+tail
    def invoke_h10(self,tail):
        self.preservation_baseline=H.W.load_baseline(H.G.P25_FINAL_DIR)
        module=load('h10_crash_runner_p25.py'); engine=self.engine()
        args=module.parser().parse_args(tail)
        if args.fixture_src_url!=FIXTURE_SOURCE_URL: raise ValueError('h10 fixed independent fixture URL required')
        validate_retained_handles_inputs(self.release,args.retained_handles_file)
        # A selected-build compatibility projection is derived only from Root
        # pinned actual candidate/build/deployment inputs, never historical A1.
        selected=json.loads(Path(args.selected_build).read_text())
        if H.sha(args.selected_build)!=self.release['p25_inputs'].get(str(Path(args.selected_build).resolve())):
            raise ValueError('h10 selected build not Root-pinned')
        if selected['source_sha256']!=SOURCE or selected['images'][self.target]!=self.pin['image']:
            raise ValueError('h10 selected image/source mismatch')
        if args.evidence_root!=str(self.output/'h10') or args.connector_base!=self.bases['connector'] or args.fixture_base!=self.bases['fixture'] or args.proxy_base!=self.bases['proxy_admin'] or args.dataset_base!=self.bases['dataset']:
            raise ValueError('h10 argv does not bind exact Root resources/output')
        ctx=self
        class IO(module.LiveIO):
            def __init__(self): super().__init__(args.released)
            def api(self,e,bases,recorder): return engine.HttpApi(bases,os.environ['OPENFOUNDRY_SMOKE_JWT'],recorder,'h10-prepare')
            def command(self,argv,timeout):
                ctx.gate(); argv=list(map(str,argv))
                if argv[:2]==['docker','ps']:
                    expected=['docker','ps','--no-trunc','--quiet','--filter','label=com.docker.compose.project=openfoundry-p26-legacy','--filter','label=com.docker.compose.service=p26connectorc']
                    if argv!=expected: raise ValueError('unexpected h10 discovery argv')
                    cp=ctx.command('discover',argv,timeout)
                    if cp.returncode==0 and cp.stdout.decode().strip()!=ctx.pin['cid']: raise ValueError('discovery not Root exact CID')
                    return cp
                if argv[:2]==['docker','inspect']:
                    if argv[-1]!=ctx.pin['cid'] or 'Config.Env' in ' '.join(argv): raise ValueError('h10 inspect target/projection refused')
                elif argv[:4]==['docker','kill','-s','KILL']:
                    if argv!=['docker','kill','-s','KILL',ctx.pin['cid']] or ctx.counts['kill'] or not getattr(ctx,'pre_ok',False): raise ValueError('h10 kill not exact once/after pre')
                    ctx.guard_held('immediately-before-kill')
                    if self.now()>=ctx.runner.started+5: raise ValueError('5s kill budget exhausted by preservation guard; no kill')
                    ctx.counts['kill']+=1
                elif argv[:2]==['docker','start']:
                    if argv!=['docker','start',ctx.pin['cid']] or ctx.counts['kill']!=1 or ctx.counts['start']: raise ValueError('h10 start not exact same CID once')
                    ctx.counts['start']+=1
                elif len(argv)>4 and argv[3]==str(Path(__file__).resolve()):
                    # Actual child process, exact argv built by this copied
                    # runner from actual quiet/held identities and file paths.
                    cp=ctx.command('h10-engine-child',argv,timeout)
                    if '--h10-stage' in argv and argv[argv.index('--h10-stage')+1]=='pre' and cp.returncode==0: ctx.pre_ok=True
                    return cp
                elif '-c' in argv and argv[-1]==ctx.bases['connector']+'/healthz': pass
                else: raise ValueError('h10 unexpected command refused')
                cp=ctx.command('h10-command',argv,timeout)
                if argv[:2]==['docker','inspect'] and cp.returncode==0:
                    item=json.loads(cp.stdout)[0]
                    if item['Id']!=ctx.pin['cid'] or item['Image']!=ctx.pin['image']: raise ValueError('C identity/image drift')
                    if ctx.counts['start']==1: ctx.current_c_pin={'cid':item['Id'],'image':item['Image'],'started_at':item['State']['StartedAt']}
                return cp
        runner=module.Runner(args,engine,IO()); self.runner=runner
        original_smoke=runner.smoke_argv
        runner.smoke_argv=lambda stage:self.h10_child_argv(runner,stage,original_smoke(stage))
        # A fresh NEW case-by-case bundle is verified as actual sources,
        # never relabeled as the historical three repaired identities.
        healthy=json.loads(Path(args.healthy_summary).read_text())
        if healthy.get('schema')!='p26-p25-healthy-bundle-v1': raise ValueError('h10 requires NEW same-S actual-source healthy bundle, no historical failed3 replay')
        from p26_healthy_bundle import verify_bundle
        verify_bundle(args.healthy_summary)
        rc=runner.run()
        if rc==0:
            if self.counts!={'kill':1,'start':1}: raise ValueError('missing real kill/start')
            self.guard_held('after-post',export=True)
        return rc
    def guard_held(self,stage,export=False):
        from p26_p25_typed_guard import guard_held
        return guard_held(self,stage,export)


def parser():
    p=argparse.ArgumentParser(); p.add_argument('--release-file',required=True); p.add_argument('--mode',required=True,choices=['engine','h10','engine-child','quiet','h13-ledger','h14-read','freeze-held'])
    p.add_argument('--output',required=True); p.add_argument('--child-context'); p.add_argument('--child-context-sha256'); p.add_argument('tail',nargs=argparse.REMAINDER); return p

def main(argv=None):
    args=parser().parse_args(argv); args.tail=args.tail[1:] if args.tail[:1]==['--'] else args.tail
    ctx=Context(args); rc=None
    try:
        if args.mode in ('engine','engine-child'): rc=ctx.invoke_engine(args.tail)
        elif args.mode=='h10': rc=ctx.invoke_h10(args.tail)
        else:
            from p26_p25_supplements import execute
            rc=execute(ctx,args.mode,args.tail)
        return rc
    except BaseException as exc:
        ctx.failure=type(exc).__name__+': '+str(exc)
        raise
    finally:
        ctx.attempt.finish({'mode':args.mode,'wrapper_exception':getattr(ctx,'failure',None),'original_engine_or_runner_rc':getattr(ctx,'engine_original_rc',rc),'created_safe':ctx.created,'h10_command_counts':ctx.counts,'source_S':SOURCE,'runtime_sha256':H.sha(__file__),'initial_guard':ctx.initial,'h9_natural_drain':getattr(ctx,'h9_drain_result',None),'h9_natural_drain_failure':getattr(ctx,'h9_drain_failure',None),'pre_kill_anchor':getattr(ctx,'pre_kill_anchor_ref',None)})
if __name__=='__main__':
    try: sys.exit(main())
    except (ValueError,KeyError,OSError) as exc:
        print('P26 P25 refused: '+str(exc),file=sys.stderr); sys.exit(2)
