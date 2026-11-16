"""Original-only aggregate/scope controls; zero live calls."""
import copy,importlib.util,json
from pathlib import Path

def mod(n):
 s=importlib.util.spec_from_file_location(n,Path(__file__).with_name(n+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def run(workspace):
 b=Path(__file__).parents[1];out=b/'results/w9/2026-11-15-w9-attempt12';m=mod('p26_original_proofs');core=mod('p26_formal_core');p=json.loads((out/'S2-runtime-contract.json').read_text());result,files=m.validate(workspace,p,p['epoch']);controls=[]
 def bad(name,fn):
  try:fn();rejected=False
  except (ValueError,OSError,KeyError,TypeError):rejected=True
  controls.append({'name':name,'rejected':rejected})
 mutations=[('missing-independent-B',lambda p:p['upgrades'].pop('p26connectorb')),('Connector-no-readonly-completion',lambda p:p['upgrades']['connector']['groups'].pop()),('C-no-readonly-tail',lambda p:p['upgrades']['p26connectorc']['groups'].pop()),('rewrite-Connector-original-outer1',lambda p:p['upgrades']['connector']['groups'][0].update(outer_expected_rc=0)),('aux2-lsof1-relabel0',lambda p:p['aux2'].update(expected_rc1=[])),('aux2-port-pair-missing',lambda p:p['aux2']['facts'].pop('b')),('oldS-relabel',lambda p:p.update(epoch='b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c')),('omit-other-mainfacts',lambda p:p['upgrades']['dataset']['preserved_facts'].pop(next(iter(p['upgrades']['dataset']['preserved_facts']))))]
 for name,change in mutations:
  q=copy.deepcopy(p);q.pop('original_files');change(q);bad(name,lambda q=q:m.validate(workspace,q,p['epoch']))
 # Actual byte overlays are ephemeral; no original evidence is edited.
 original=m.Reader
 for name,suffix,change in [('full-typed-safe27','.raw',lambda b:b.replace(b'7800c7ba',b'8800c7ba')),('actual-target-image','25-target-before-facts.raw',lambda b:b.replace(b'sha256:',b'badsha:')),('original-used-tool','py',lambda b:b+b'\n# changed'),('preserved-anchor-original','05-command.raw',lambda b:b+b' ')]:
  hit=[False]
  class Overlay(original):
   def read(self,rel):
    raw=super().read(rel)
    if rel.endswith(suffix):
     altered=change(raw);hit[0]|=altered!=raw;return altered
    return raw
  m.Reader=Overlay;q=copy.deepcopy(p);q.pop('original_files');bad(name,lambda:m.validate(workspace,q,p['epoch']));controls[-1]['mutated']=hit[0];m.Reader=original
 bad('old-four-slices-cannot-satisfy-S2',lambda:core.scope_kind('q3-deploy-actual','q3-S2-runtime-aggregate',{'kind':'deployment-slice','epoch':p['epoch']}))
 step={'id':'q3-S2-runtime-aggregate','classification':'actual','rc_kind':'unix','rc':0,'artifact':p['aux2']['directory']+'/RESULT.json','rc_artifact':p['aux2']['outer']+'/rc','proof':p}
 item={'id':'q3-deploy-actual','status':'pending','satisfiers':[step['id']],'contracts':{step['id']:{k:v for k,v in step.items() if k!='id'}}};core.validate_item(item,{step['id']:step},p['epoch'],lambda:original(workspace))
 assert all(x['rejected'] and x.get('mutated',True) for x in controls),controls
 # Bind only this concrete aggregate; remaining mandatory items stay pending.
 rp=b/'archive/p26_S2_roster.candidate.json';roster=json.loads(rp.read_text());roster['items']=[item if x['id']=='q3-deploy-actual' else x for x in roster['items']];rp.write_text(json.dumps(roster,indent=2)+'\n')
 ep=b/'archive/p26_S2_evidence.candidate.json';e=json.loads(ep.read_text());e['steps']=[x for x in e['steps'] if x['id']!=step['id']]+[step];ep.write_text(json.dumps(e,indent=2)+'\n')
 return {'status':'S2-RUNTIME-ORIGINAL-AGGREGATE-VERIFIED-DRAFT','result':result,'original_files':files,'negative_controls':controls,'final_Root_acceptance':False}
if __name__=='__main__':
 import sys
 print(json.dumps(run(sys.argv[1]),indent=2))
