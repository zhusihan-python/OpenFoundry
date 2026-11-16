#!/usr/bin/env python3
import json,sys,tempfile,uuid
from pathlib import Path
from types import SimpleNamespace as NS
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_suite_adapter as A
H=A.GUARD

def main():
    with tempfile.TemporaryDirectory(dir=H.P26/'results/w10') as td:
        t=Path(td); rpath=t/'release.json'
        rel={'schema':'p26-w10-release-v1','root_accepted':True,'suite':'p23','stage':'p23full8','expires_utc':'2099-01-01T00:00:00Z'}
        rpath.write_text(json.dumps(rel)); cmds=[]
        def runner(cmd,**kw): cmds.append(cmd); return NS(returncode=0,stdout='',stderr='')
        f=A.Facade(runner,rel,rpath); f.attempt=H.W.Attempt(t,'pure-holder')
        m=A.load(A.TREE/A.SUITES['p23']); m.ARGS=NS(env_dir=td)
        source=str(uuid.uuid4()); f.owned.sources[source]={'id':source,'actor':str(uuid.uuid4()),'tenant':str(uuid.uuid4())}
        spawned=[]
        class Child:
            def __init__(self,*args,**kwargs):
                self.returncode=None; self.pid=91; self.writes=[]; self.inputs=[]
                self.stdin=NS(write=lambda data:self.writes.append(data),flush=lambda:None); spawned.append(self)
            def poll(self): return self.returncode
            def communicate(self,input=None,timeout=None):
                self.inputs.append(input); self.returncode=0; return b'ROLLBACK\n',b''
            def kill(self): self.returncode=-9
        original=m.subprocess.Popen
        m._SourceRowLock._readline_raw=lambda *args:b'12345\n'
        f.holders.instrument(m)
        m.subprocess.Popen=Child
        try:
            holder=m._SourceRowLock(source)
            assert holder.acquire()==12345
            assert spawned[0].writes==[("BEGIN;\nSELECT id FROM connections WHERE id = '"+source+"'::uuid FOR UPDATE;\nSELECT pg_backend_pid();\n").encode()]
            rel['expires_utc']='2000-01-01T00:00:00Z'; rpath.write_text(json.dumps(rel))
            assert holder.release()==0 and b'ROLLBACK;\n' in spawned[0].inputs and not f.holders.active
            rel['expires_utc']='2099-01-01T00:00:00Z'; rpath.write_text(json.dumps(rel))
            before=len(spawned)
            try: m._SourceRowLock(str(uuid.uuid4())).acquire()
            except (ValueError,A.StageAbort): pass
            else: assert False
            assert len(spawned)==before
        finally: m.subprocess.Popen=original
        rel['expires_utc']='2000-01-01T00:00:00Z'; rpath.write_text(json.dumps(rel))
        # A partial ABC install must be revoked even after release expiry.
        f.owned.pending={'p21-function','p21-trigger','p1-check'}
        for key,sql in [('p21-function','DROP FUNCTION IF EXISTS p21_c_terminal_fault();'),('p21-trigger','DROP TRIGGER IF EXISTS p21_c_terminal_fault_trg ON sync_runs;'),('p1-check','ALTER TABLE dataset_files DROP CONSTRAINT IF EXISTS p1_smoke_block_publish;')]:
            f.cleanup_commands[key]=['fake-owned-psql','-c',sql]
        f.cleanup()
        assert not f.owned.pending and len(cmds)==3
        assert cmds[0][-1].startswith('DROP TRIGGER') and cmds[1][-1].startswith('DROP FUNCTION')
        print(json.dumps({'ok':True,'actual_http_sql_docker_calls':0,'checks':['real copied long-lived Popen acquire sends exact owned Source lock stdin','actual original PG PID path retained','expiry does not prevent ROLLBACK and child reap','unowned Source/expired acquire rejected before Popen','outer exact temporary-object cleanup survives expired phase; trigger before function']},indent=2))
if __name__=='__main__': main()
