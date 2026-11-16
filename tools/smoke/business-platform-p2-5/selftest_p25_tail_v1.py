#!/usr/bin/env python3
"""Targeted Runtime HTTP-v1 CLI positives/negatives; file IO only."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent


def main():
    spec = importlib.util.spec_from_file_location('tail_v1_test', HERE/'p25_tail_roster.py')
    runtime = importlib.util.module_from_spec(spec); spec.loader.exec_module(runtime)
    engine = runtime.http_pure()
    uid = lambda i: '00000000-0000-4000-8000-%012d' % i
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        known = {'h10-held': {'def_id':uid(1),'source_id':uid(2),'run_id':uid(3)}}
        owners = [{'def_id':uid(1),'source_id':uid(2),'run_id':uid(3),
                   'run_status':'running','blocked_reason':'requires_reconciliation'}]
        runs = [{'def_id':uid(1),'sync_def_id':uid(1),'source_id':uid(2),'run_id':uid(3),
                 'status':'running','generation':1,'attempt':1,'control_mode':'controlled','publish_entered_at':None}]
        semantic = {'owner_rows':[{'def_tag':'h10-held','run_ref':'same','blocked_reason':'requires_reconciliation'}],
                    'run_rows':[{'def_tag':'h10-held','semantics':'p25-crash-held-prepublish','count':1}], 'produced_by':'p25-tail'}
        for i, mode in enumerate(['no_txn_200','not_committed','wrong_target','http_500','drop_after_verify']):
            owners.append({'def_id':uid(10+i*3),'source_id':uid(11+i*3),'run_id':uid(12+i*3),
                           'run_status':'failed','blocked_reason':'requires_reconciliation','mode':mode})
            semantic['owner_rows'].append({'def_tag':mode,'mode':mode,'run_ref':'terminal-failed','blocked_reason':'requires_reconciliation'})
        (root/'spec.json').write_text(json.dumps(semantic)); (root/'known.json').write_text(json.dumps(known))
        declaration=root/'decl.json'
        argv=[sys.executable,'-I',str(HERE/'p25_tail_roster.py'),'declare','--spec',str(root/'spec.json'),'--known-bindings',str(root/'known.json'),'--out',str(declaration)]
        process=subprocess.run(argv,capture_output=True,text=True);assert process.returncode==0,process.stderr
        # Existing declaration can never be silently overwritten.
        assert subprocess.run(argv,capture_output=True).returncode==1
        decl=json.loads(declaration.read_text()); v=engine.adjudicate_tail(decl,owners,runs);assert v['ok'],v
        binding_path=root/'bindings.json';summary_path=root/'summary.json'
        summary={'phase':'tail','failed':[],'quiet':{'ok':True},'final_owner_roster':owners,'owner_roster':owners,
                 'final_run_roster':runs,'tail_bindings':v['bindings'],'tail_bindings_file':str(binding_path),
                 'tail_adjudication':{'ok':True,'problems':[],'declaration_sha256':decl['declaration_sha256']}}
        binding={'schema':'p25-tail-bindings-v1','declaration':str(declaration),'declaration_sha256':decl['declaration_sha256'],
                 'bindings':v['bindings'],'bindings_sha256':runtime.canonical_sha(v['bindings']),'problems':[]}
        checks=[]
        for case in ['good','missing-owner','extra-owner','wrong-reason','wrong-parent','published-held','missing-run','binding-hash','summary-hash']:
            s=copy.deepcopy(summary);b=copy.deepcopy(binding)
            if case=='missing-owner':s['final_owner_roster'].pop()
            if case=='extra-owner':s['final_owner_roster'].append(dict(owners[0],def_id=uid(90),run_id=uid(91)))
            if case=='wrong-reason':s['final_owner_roster'][0]['blocked_reason']=''
            if case=='wrong-parent':s['final_run_roster'][0]['sync_def_id']=uid(90)
            if case=='published-held':s['final_run_roster'][0]['publish_entered_at']='2026-11-14T10:00:00Z'
            if case=='missing-run':s['final_run_roster']=[]
            summary_path.write_text(json.dumps(s));b['summary_sha256']=runtime.sha(summary_path)
            if case=='binding-hash':b['bindings_sha256']='0'*64
            if case=='summary-hash':b['summary_sha256']='0'*64
            binding_path.write_text(json.dumps(b))
            process=subprocess.run([sys.executable,'-I',str(HERE/'p25_tail_roster.py'),'verify','--declaration',str(declaration),
                '--summary',str(summary_path),'--bindings',str(binding_path),'--out',str(root/'verdict.json')],capture_output=True,text=True)
            assert (process.returncode==0)==(case=='good'),(case,process.stderr,(root/'verdict.json').read_text())
            checks.append(case)
        bad=copy.deepcopy(decl);bad['spec']['owner_rows'][0]['blocked_reason']='';declaration.write_text(json.dumps(bad))
        assert runtime.verify(declaration,summary_path,binding_path)['ok'] is False
        try:runtime.declare(semantic,[])
        except ValueError:pass
        else:raise AssertionError('nonobject known_bindings accepted')
        print(json.dumps({'checks':len(checks)+4,'cases':checks,'pure':True,'ok':True}))
    return 0


if __name__=='__main__':
    sys.exit(main())
