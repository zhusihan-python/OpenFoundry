from pathlib import Path
import hashlib,json
root=Path('/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-5')
out=root.parent/'business-platform-p2-6/regressions/vendor/p25-runtime'; out.mkdir(exist_ok=True)
source='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
ledger=[]
def copy(name,changes=()):
 raw=(root/name).read_bytes(); text=raw.decode(); patches=[]
 for old,new,count,purpose in changes:
  actual=text.count(old)
  if actual!=count: raise ValueError((name,purpose,actual,count))
  text=text.replace(old,new); patches.append({'old':old,'new':new,'count':count,'purpose':purpose})
 data=text.encode(); (out/name).parent.mkdir(parents=True,exist_ok=True); (out/name).write_bytes(data)
 entry={'origin':str(root/name),'origin_sha256':hashlib.sha256(raw).hexdigest(),'copy':str(out/name),'copy_sha256':hashlib.sha256(data).hexdigest(),'patches':patches,'narrow_patch_sha256':hashlib.sha256(json.dumps(patches,sort_keys=True).encode()).hexdigest() if patches else None}
 ledger.append(entry); return entry['copy_sha256']
# Only the isolated crash validator's identity pins change; other engine bytes
# remain exact origin bytes. Avoid replacing unrelated historical labels.
raw=(root/'smoke_p25.py').read_text(); a=raw.index('def _validate_crash_record('); b=raw.index('\ndef ',a+1)
section=raw[a:b]; changed=section.replace('openfoundry-platform-p1-q3v9','openfoundry-p26-legacy').replace('p25connector','p26connectorc')
engine=copy('smoke_p25.py',[(section,changed,1,'h10 crash-validator actual NEW compose project/service pins only'),('rc, out, err = -1, b"", type(exc).__name__.encode()','rc, out, err = None, b"", type(exc).__name__.encode()',1,'h13 SQL spawn/timeout has unavailable original child rc, never fabricated -1'),('"rc": rc,\n            "stdout"','"rc": rc, "original_rc": rc,\n            "stdout"',1,'h13 original child rc explicit nullable metadata')])
runner=copy('h10_crash_runner_p25.py',[("        expected = {s['id'] for s in self.m.SCENARIOS","        if healthy.get('schema') == 'p26-p25-healthy-bundle-v1':\n            spec = importlib.util.spec_from_file_location('p26_healthy_bundle', HERE.parents[1] / 'p25' / 'p26_healthy_bundle.py')\n            bundle = importlib.util.module_from_spec(spec); spec.loader.exec_module(bundle)\n            if bundle.verify_bundle(self.a.healthy_summary).get('ok') is not True: raise ValueError('NEW actual-source bundle rejected')\n        expected = {s['id'] for s in self.m.SCENARIOS",1,'verify exact NEW current-source bundle; no fixed historical failed3'),("healthy.get('schema') != 'p25-healthy-aggregate-v1'","healthy.get('schema') not in ('p25-healthy-aggregate-v1','p26-p25-healthy-bundle-v1')",1,'accept only independently verified actual NEW retained-owner provenance'),("tag = 'failedhealthy-h13-' + owner['def_id']","tag = 'retained-healthy-' + owner['def_id']",1,'do not label future retained case identities as historical h13'),("ENGINE_SHA = 'de6eb2bcbc6be34b57f6f7ab4da3be535fec9b140c2646bcbfcc9214395259a1'",f"ENGINE_SHA = '{engine}'",1,'bind actual narrow-patched NEW engine bytes'),("SOURCE_SHA = 'a1bff07f3760fce7b5661b169ba26a165712c950035d4e4ec9948639d51ffd27'",f"SOURCE_SHA = '{source}'",1,'current full accepted S; historical originals unchanged'),("PROJECT = 'openfoundry-platform-p1-q3v9'","PROJECT = 'openfoundry-p26-legacy'",1,'NEW aux project'),("SERVICE = 'p25connector'","SERVICE = 'p26connectorc'",1,'NEW actual C service'),("rc, out, err = -1, exc.stdout or b'', exc.stderr or b''","rc, out, err = None, exc.stdout or b'', exc.stderr or b''",1,'timeout has no fabricated child exit'),("rc, out, err = -1, b'', type(exc).__name__.encode()","rc, out, err = None, b'', type(exc).__name__.encode()",1,'spawn failure has no fabricated child exit'),("(directory / 'rc').write_text(str(rc) + '\\n')","(directory / 'rc').write_text(('unavailable' if rc is None else str(rc)) + '\\n')",1,'retain unavailable separately from actual child rc')])
# Fixed independent fixture hostname is supplied only by the validated runtime.
runner_entry=ledger[-1]
for old,new in [("{'fixture_src_url': FIXTURE_URL", "{'fixture_src_url': args.fixture_src_url"),("'--proxy-base', self.a.proxy_base, '--fixture-src-url', FIXTURE_URL", "'--proxy-base', self.a.proxy_base, '--fixture-src-url', self.a.fixture_src_url"),("incremental_source_config(FIXTURE_URL, 'p25h13'", "incremental_source_config(self.a.fixture_src_url, 'p25h13'")]:
 path=out/'h10_crash_runner_p25.py';text=path.read_text();assert text.count(old)==1;path.write_text(text.replace(old,new));runner_entry['patches'].append({'old':old,'new':new,'count':1,'purpose':'Root fixed independent P25 fixture URL already validated by NEW runtime'})
runner_entry['copy_sha256']=hashlib.sha256(path.read_bytes()).hexdigest();runner_entry['narrow_patch_sha256']=hashlib.sha256(json.dumps(runner_entry['patches'],sort_keys=True).encode()).hexdigest();runner=runner_entry['copy_sha256']
copy('collect_http_quiet_p25.py',[("ENGINE_SHA = 'de6eb2bcbc6be34b57f6f7ab4da3be535fec9b140c2646bcbfcc9214395259a1'",f"ENGINE_SHA = '{engine}'",1,'NEW engine SHA'),("BASES = {'fixture': 'http://127.0.0.1:17092', 'proxy_admin': 'http://127.0.0.1:17093'}","BASES = {'fixture': 'http://127.0.0.1:17095', 'proxy_admin': 'http://127.0.0.1:17096'}",1,'NEW stage-owned P25 sidecar ports'),("BASES = {'fixture': 'http://127.0.0.1:17095', 'proxy_admin': 'http://127.0.0.1:17096'}","BASES = {'fixture': 'http://127.0.0.1:17103', 'proxy_admin': 'http://127.0.0.1:17104'}",1,'Root independent NEW P25 sidecars; original P26 fixture/proxy remain unchanged')])
for name in ('aggregate_healthy_p25.py','selftest_aggregate_healthy_p25.py'):
 copy(name)
copy('verify_h13_ledger_p25.py',[("linked(argv[argv.index('-I')+1])","linked(next(token for token in argv if token.endswith('.py')))",1,'actual NEW collector script path after optional Python -B flag')])
copy('fixtures/dataset_fault_proxy_p25.py')
copy('fixtures/rest_source_p25.py')
copy('selftest_h10_crash_runner_p25.py',[("'p25connector':",'r.SERVICE:',2,'pure fixture selected-image key follows NEW actual service')])
(out.parent/'p25-closure-ledger.json').write_text(json.dumps({'schema':'p26-p25-copied-closure-v1','source_S':source,'files':ledger,'note':'only NEW copies imported/executed; originals read-only; old fixed-three-replacement aggregator retained only for applicable exact facts, never default NEW selection'},indent=2)+'\n')
print(json.dumps({'copy_count':len(ledger),'engine_sha256':engine,'runner_sha256':runner}))
