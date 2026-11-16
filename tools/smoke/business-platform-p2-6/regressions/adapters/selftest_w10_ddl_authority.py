#!/usr/bin/env python3
"""Catalog uncertainty cannot confer DROP authority; actual own attempts can."""
import ast,json,sys,tempfile,uuid,subprocess
from pathlib import Path
from types import SimpleNamespace as NS
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_suite_adapter as A
H=A.GUARD

def main():
    actor,org,src,target,did=[str(uuid.uuid4()) for _ in range(5)]
    module=A.load(A.TREE/A.SUITES['p21'])
    tree=ast.parse((A.TREE/A.SUITES['p21']).read_text()); sqls={}
    for n in ast.walk(tree):
        if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='psql_connectors' and n.args:
            try: value=eval(compile(ast.Expression(n.args[0]),'NEW copied SQL','eval'),{'sync_def_id':did,**vars(module)})
            except (NameError,TypeError): continue
            if isinstance(value,str) and value.startswith('CREATE OR REPLACE FUNCTION'): sqls['function']=value
            if isinstance(value,str) and value.startswith('CREATE TRIGGER'): sqls['trigger']=value
    results=[]
    for fault in ('catalog_timeout','catalog_oserror','function_exists','wrong_schema','trigger_exists','install_nonzero','install_timeout','install_oserror'):
        with tempfile.TemporaryDirectory(dir=H.P26/'results/w10') as td:
            t=Path(td); path=t/'release.json'; rel={'schema':'p26-w10-release-v1','root_accepted':True,'suite':'p21','stage':'abc','expires_utc':'2099-01-01T00:00:00Z','owned_sql_templates':['p21-new-def-terminal-trigger'],'serial_owned_window':True};path.write_text(json.dumps(rel)); commands=[]
            def real(cmd,**kw):
                sql=cmd[-1]; commands.append(sql)
                if 'pg_proc' in sql:
                    if fault=='catalog_timeout': raise subprocess.TimeoutExpired(cmd,30)
                    if fault=='catalog_oserror': raise OSError('catalog spawn')
                    text='1\npublic\n' if fault=='function_exists' else '0\nprivate\n' if fault=='wrong_schema' else '0\npublic\n'
                    return NS(returncode=0,stdout=text,stderr='')
                if 'pg_trigger' in sql: return NS(returncode=0,stdout='1\npublic\n' if fault=='trigger_exists' else '0\npublic\n',stderr='')
                if 'SELECT count(*) FROM sync_runs' in sql: return NS(returncode=0,stdout='0\n',stderr='')
                if sql.startswith('CREATE OR REPLACE'):
                    if fault=='install_timeout': raise subprocess.TimeoutExpired(cmd,30)
                    if fault=='install_oserror': raise OSError('not spawned')
                    return NS(returncode=42 if fault=='install_nonzero' else 0,stdout='',stderr='injected')
                return NS(returncode=0,stdout='',stderr='')
            f=A.Facade(real,rel,path); f.attempt=H.W.Attempt(t,'catalog-test')
            f.owned.sources[src]={'actor':actor,'tenant':org};f.owned.datasets[target]={'actor':actor,'tenant':org};f.owned.defs[did]={'id':did,'actor':actor,'tenant':org,'source_id':src,'output_dataset_id':target}
            def command(sql): return ['docker','compose','-f',td+'/compose.yaml','--project-name','q3','exec','-T','postgres','psql','-U','openfoundry','-d','connectors','-t','-A','-c',sql]
            try:
                result=f.run(command(sqls['function']))
                if fault=='install_nonzero': assert result.returncode==42
                if fault=='trigger_exists': f.run(command(sqls['trigger']))
            except (ValueError,OSError,subprocess.TimeoutExpired,A.StageAbort): pass
            rel['expires_utc']='2000-01-01T00:00:00Z';path.write_text(json.dumps(rel));f.cleanup()
            drops=[c for c in commands if c.startswith('DROP ')]
            if fault in ('catalog_timeout','catalog_oserror','function_exists','wrong_schema','install_oserror'): assert drops==[],(fault,drops)
            if fault in ('trigger_exists','install_nonzero','install_timeout'): assert drops==['DROP FUNCTION IF EXISTS p21_c_terminal_fault();'],(fault,drops)
            assert not f.owned.pending
            results.append({'fault':fault,'actual_drops':drops})
    print(json.dumps({'ok':True,'actual_http_sql_docker_calls':0,'cases':results},indent=2))
if __name__=='__main__': main()
