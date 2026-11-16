"""Bind only exact stable S2 P1pre9/P22/P23 originals; no live or private IO."""
import hashlib,importlib.util,json
from pathlib import Path
B=Path('tools/smoke/business-platform-p2-6');O=B/'results/w9/2026-11-15-w9-attempt14';S='ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e'
def doc(p):return json.loads(Path(p).read_text())
proofs=[]
for suite,stage,launch in [('p1','p1pre9','S2-P1pre9-outer-launch-attempt1'),('p22','p22-full','S2-P22-full11-outer-launch-attempt1'),('p23','p23-full','S2-P23-full8-outer-launch-attempt1')]:
 launch=B/'results/w10'/launch;audit=doc(launch/'original-audit.json');files={str(B/path):pin for path,pin in audit['files'].items()};outer=B/'results/w10/S2-seven-stage-ready-attempt1'/(stage+'-outer-actual');adapter=next(Path(x) for x in files if x.endswith('/adapter.json'));commands=next(Path(x).parent for x in files if '/suite-commands-' in x);report=outer/'original-result_'+suite+'.json' if False else None
 report=next(Path(x) for x in files if x.endswith('/p1-report.json')) if suite=='p1' else outer/('original-result_'+suite+'.json')
 rep=doc(report);p={'canonical_root_label':str(Path.cwd()),'kind':'legacy-actual-stage','epoch':S,'suite':suite,'stage':stage,'launch_rc':str(launch/'rc'),'outer':str(outer),'actual_child_argv':doc(outer/'child/cmd.json'),'report':str(report),'adapter':str(adapter),'commands_dir':str(commands),'business_assertion_inputs':rep['evidence'],'suite_source':{'p1':'regressions/vendor/tree/business-platform-p1/smoke/smoke.py','p22':'regressions/vendor/tree/business-platform-p2-2/smoke_p22.py','p23':'regressions/vendor/tree/business-platform-p2-3/smoke_p23.py'}[suite],'original_commands':{},'audit':str(launch/'original-audit.json')}
 for f in commands.glob('*.rc'):
  name=f.name[:-3];p['original_commands'][name]={'rc':int(f.read_text()) if f.read_text().strip().lstrip('-').isdigit() else f.read_text().strip(),'command':doc(commands/(name+'.cmd.json')),'meta':doc(commands/(name+'.meta.json')),'raw':(commands/(name+'.raw')).read_text()}
 proofs.append(p)
(O/'S2-legacy-stages-contract.json').write_text(json.dumps({'source':S,'proofs':proofs,'full_legacy_complete':False},indent=2)+'\n')
