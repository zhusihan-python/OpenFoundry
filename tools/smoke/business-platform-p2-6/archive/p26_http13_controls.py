"""Full S2 HTTP immutable original and mandatory-scope controls."""
import copy,importlib.util,json
from pathlib import Path

def mod(n):
 s=importlib.util.spec_from_file_location(n,Path(__file__).with_name(n+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def run(root):
 b=Path(__file__).parents[1];out=b/'results/w9/2026-11-15-w9-attempt13';m=mod('p26_original_proofs');core=mod('p26_formal_core');doc=json.loads((out/'S2-HTTP10-contract.json').read_text());p=doc['proof'];result,files=m.validate(root,p,doc['source']);controls=[]
 def bad(name,fn):
  try:fn();rejected=False
  except (ValueError,OSError,TypeError,KeyError):rejected=True
  controls.append({'name':name,'rejected':rejected})
 for name,mutation in [('missing-required-case',lambda p:p['cases'].pop()),('oldS-epoch',lambda p:p.update(epoch='b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c')),('missing-separate-CURRENT',lambda p:p.pop('current_summary')),('missing-postguard-manifest',lambda p:p.pop('closure_summary')),('wrong-owned-original-count',lambda p:p['cases'][3].update(owned_rc_count=64))]:
  q=copy.deepcopy(p);q.pop('original_files',None);mutation(q)
  if name=='oldS-epoch':bad(name,lambda q=q:core.scope_kind('http-live-full-suite','http10-exact-aggregate',q))
  else:bad(name,lambda q=q:m.validate(root,q,doc['source']))
 old=m.Reader
 mutations=[('natural-clock-expiry','lease-observe.stdout.raw',lambda b:b.replace(b'"lease_expired" : true',b'"lease_expired" : false')),('owned-Unix-child-rc','lease-observe.rc',lambda b:b'1'),('CURRENT-real-helper','helper-source.py',lambda b:b+b'\n# changed'),('CURRENT-exact-download','009-CURRENT-file-bytes.download.bin',lambda b:b+b'x'),('CURRENT-parent-tuple','003-CURRENT-fullsafe-run.meta',lambda b:b.replace(b'0a58bf5d',b'1a58bf5d')),('CURRENT-origin-binding','RESULT.json',lambda b:b.replace(b'c8adb818',b'd8adb818')),('historical-ABA-A-download','018-05-a-proof-file-bytes.download.bin',lambda b:b+b'x'),('postguard-whole-oldsafe','.raw',lambda b:b.replace(b'7800c7ba',b'8800c7ba')),('bootchain-original-CID','AUX2-BOOT-CHAIN.json',lambda b:b.replace(b'ffe58ff5',b'afe58ff5')),('S2-original-wrapper','run_p26_s2_actual.py',lambda b:b+b'\n# modified')]
 for name,suffix,mutation in mutations:
  hit=[False]
  class Overlay(old):
   def read(self,rel):
    raw=super().read(rel)
    if rel.endswith(suffix):
     altered=mutation(raw);hit[0]|=altered!=raw;return altered
    return raw
  m.Reader=Overlay;q=copy.deepcopy(p);q.pop('original_files',None);bad(name,lambda q=q:m.validate(root,q,doc['source']));controls[-1]['mutated']=hit[0];m.Reader=old
 bad('old-wrapper-schema-cannot-masquerade-S2',lambda:core.scope_kind('http-live-full-suite','http10-exact-aggregate',{'kind':'http10','epoch':doc['source']}))
 assert all(c['rejected'] and c.get('mutated',True) for c in controls),controls
 step={'id':'http10-exact-aggregate','classification':'actual','rc_kind':'unix','rc':0,'artifact':p['closure_summary'],'rc_artifact':p['cases'][0]['outer']+'/case.rc','proof':p}
 item={'id':'http-live-full-suite','status':'pending','satisfiers':[step['id']],'contracts':{step['id']:{k:v for k,v in step.items() if k!='id'}}};core.validate_item(item,{step['id']:step},doc['source'],lambda:old(root))
 rp=b/'archive/p26_S2_roster.candidate.json';roster=json.loads(rp.read_text());roster['items']=[item if x['id']=='http-live-full-suite' else x for x in roster['items']];rp.write_text(json.dumps(roster,indent=2)+'\n')
 ep=b/'archive/p26_S2_evidence.candidate.json';e=json.loads(ep.read_text());e['steps']=[x for x in e['steps'] if x['id']!=step['id']]+[step];ep.write_text(json.dumps(e,indent=2)+'\n')
 return {'status':'S2-HTTP10-ORIGINAL-MANDATORY-AGGREGATE-DRAFT','result':result,'unique_original_files':len(files),'original_files':files,'negative_controls':controls,'all_later_scopes_pending':True,'live_calls':0}
if __name__=='__main__':
 import sys
 print(json.dumps(run(sys.argv[1]),indent=2))
