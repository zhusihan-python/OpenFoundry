"""Runtime seam around the copied long-lived Source-row holder.

The real Popen/stdin/select/PG-backend-pid/ROLLBACK/kill/reap implementation
runs unchanged. Only this invocation's actual HTTP-created Sources may lock.
"""
class Holders:
    def __init__(self,facade): self.facade=facade; self.active={}
    def instrument(self,module):
        cls=getattr(module,'_SourceRowLock',None)
        if cls is None or getattr(cls,'_w10_bound',False): return
        cls._w10_bound=True; acquire=cls.acquire; release=cls.release; readline=cls._readline_raw
        manager=self
        def read(obj,*args,**kwargs):
            line=readline(obj,*args,**kwargs)
            if line is not None:
                manager.facade.attempt.record('holder-stdout-stream',{'source_id':obj.sid},line,'unavailable',{'original_rc':None,'stream':True})
            return line
        def take(obj,*args,**kwargs):
            f=manager.facade
            f.require_phase()
            if obj.sid not in f.owned.sources or obj._cmd_factory is not None:
                raise ValueError('holder Source must be actual-created here; custom command factory forbidden')
            cmd=obj._cmd()
            if cmd[:2]!=['docker','compose'] or 'postgres' not in cmd or 'psql' not in cmd or '-c' in cmd:
                raise ValueError('unexpected interactive holder command')
            script="BEGIN;\nSELECT id FROM connections WHERE id = '"+obj.sid+"'::uuid FOR UPDATE;\nSELECT pg_backend_pid();\n"
            receipt=f.attempt.step_begin('holder-acquire',{'argv':cmd,'stdin':script,'owned_source':f.owned.sources[obj.sid]})
            manager.active[id(obj)]={'holder':obj,'proc':None}
            try:
                pid=acquire(obj,*args,**kwargs)
                if not isinstance(pid,int) or pid<=0: raise ValueError('invalid actual PG holder PID')
                f.attempt.step_finish('holder-acquire',receipt,{'backend_pid':pid,'client_pid':obj.proc.pid},'unavailable',{'original_rc':None,'child_still_running':True})
                return pid
            except Exception as exc:
                f.attempt.step_finish('holder-acquire',receipt,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None})
                raise
            finally:
                manager.active[id(obj)]['proc']=obj.proc
        def drop(obj,*args,**kwargs):
            # Independent cleanup authority: releasing a Source lock created
            # here must remain possible after expiry/revocation.
            item=manager.active.get(id(obj))
            if item is None: return release(obj,*args,**kwargs)
            proc=obj.proc or item['proc']; f=manager.facade
            receipt=f.attempt.step_begin('holder-release',{'source_id':obj.sid,'backend_pid':obj.pid,'stdin':'ROLLBACK;\n'})
            original_rc=release(obj,*args,**kwargs)
            raw={'original_release_return':original_rc}
            try:
                if proc is not None and proc.poll() is None:
                    proc.kill(); out,err=proc.communicate(timeout=10)
                    raw.update(forced_cleanup=True,stdout=out.decode(errors='replace'),stderr=err.decode(errors='replace'))
                elif proc is not None:
                    out,err=proc.communicate(timeout=1)
                    raw.update(stdout=out.decode(errors='replace'),stderr=err.decode(errors='replace'))
            except Exception as exc: raw['cleanup_exception']=type(exc).__name__+': '+str(exc)
            reaped=proc is None or proc.poll() is not None
            f.attempt.step_finish('holder-release',receipt,raw,original_rc if original_rc is not None else 'unavailable',{'original_rc':original_rc,'reaped':reaped,'final_process_rc':getattr(proc,'returncode',None)})
            if reaped: manager.active.pop(id(obj),None)
            return original_rc
        cls._readline_raw=read; cls.acquire=take; cls.release=drop
    def cleanup(self):
        for item in list(self.active.values()):
            obj=item['holder']
            if obj.proc is None: obj.proc=item['proc']
            obj.release(timeout=5)
