"""Read-only historical byte inventory. Never imports or executes historical tools."""
import hashlib
import json
import os
import stat
from pathlib import Path

OLD_S='b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c'
MANIFEST_SHA='1682e49206b9491296194304c6eea078f4248f020928d9e1ca10dffa770bae4c'
LABEL='/Volumes/ExternalSD/projects/OpenFoundry'


def need(ok,why):
    if not ok:raise ValueError(why)


def sha(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(1048576),b''):h.update(b)
    return h.hexdigest()


def safe(root,rel):
    need(isinstance(rel,str) and not rel.startswith('/') and all(x not in ('','.','..') for x in rel.split('/')),'nonlocal history path')
    p=Path(root)
    for part in rel.split('/'):
        p=p/part;need(not p.is_symlink(),'history symlink')
    need(stat.S_ISREG(p.stat().st_mode),'nonregular history file');return p


def snapshot(root,label=LABEL,expected=OLD_S,manifest_sha=MANIFEST_SHA):
    base=Path(root);manifest=base/'manifest.json';need(sha(manifest)==manifest_sha,'old snapshot manifest changed')
    doc=json.loads(manifest.read_text());rows=doc['files'];need(len(rows)==404 and len({r['relative'] for r in rows})==404,'old snapshot exact404 rows')
    declared={r['relative'] for r in rows};actual={str(p.relative_to(base/'source')) for p in (base/'source').rglob('*') if p.is_file()};need(declared==actual,'old snapshot missing/extra paths')
    pins={}
    for r in rows:
        p=safe(base/'source',r['relative']);digest=sha(p);need(digest==r['sha256'] and p.stat().st_size==r['bytes'],'old snapshot content changed')
        need(r['origin']==label+'/'+r['relative'],'old original path LABEL mismatch');pins[r['relative']]={'sha256':digest,'size':r['bytes']}
    source_files=sorted(r for r in declared if r not in ('go.mod','go.sum'));need(len(source_files)==402 and all(r.endswith(('.go','.sql')) for r in source_files),'old source402 framing')
    stream=b'go.mod\n'+safe(base/'source','go.mod').read_bytes()+b'go.sum\n'+safe(base/'source','go.sum').read_bytes()+b'content-hashes\n'
    stream+=b''.join((pins[r]['sha256']+'  '+label+'/'+r+'\n').encode() for r in source_files)
    need(hashlib.sha256(stream).hexdigest()==expected and safe(base,'canonical-absolute-labels.bin').read_bytes()==stream,'old canonical stream byte replay differs')
    return {'status':'HISTORICAL-SNAPSHOT-BYTES-VERIFIED','source':expected,'manifest_sha256':sha(manifest),'Go_SQL':402,'module_files':2,'members':pins,'canonical_stream_sha256':hashlib.sha256(stream).hexdigest(),'eligible_for_S2_formal':False}


def inventory(workspace,config):
    root=Path(workspace);members={};issues=[];original_rcs={};known=config['explicit_paths']
    for directory in config['roots']:
        base=root/directory;need(base.is_dir() and directory.startswith('tools/smoke/business-platform-p2-6/results/'),'history root must be explicit NEW results tree')
        for d,dirs,names in os.walk(base,followlinks=False):
            for n in dirs[:]:
                p=Path(d)/n
                if p.is_symlink():issues.append({'path':str(p.relative_to(root)),'kind':'symlink-not-read'});dirs.remove(n)
            for n in names:
                p=Path(d)/n;rel=str(p.relative_to(root))
                if any(x.lower().startswith('.env') or x.lower().endswith('.env') or '.env.' in x.lower() for x in p.relative_to(base).parts) or p.suffix=='.body':meta=p.lstat();issues.append({'path':rel,'kind':'private-or-body-not-read','size':meta.st_size,'mode':oct(stat.S_IMODE(meta.st_mode))});continue
                if p.is_symlink() or not stat.S_ISREG(p.stat().st_mode):issues.append({'path':rel,'kind':'nonregular-not-read'});continue
                before=p.stat();digest=sha(p);after=p.stat()
                if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):issues.append({'path':rel,'kind':'changed-during-read'})
                classification='history-oldS-original' if rel in known else 'history-retained-original-epoch-unclaimed'
                relative=rel[len(directory):].lstrip('/')
                if relative.startswith('S2') or '/S2' in relative or '/unit-connector_20261115T093630Z/' in relative:
                    classification='history-S2-origin-not-automatically-formal'
                if '/legacy-jwt-compat/focused-red/' in rel:classification='history-intermediate-red-original'
                if '/legacy-jwt-compat/focused-green' in rel:classification='history-S2-focused-green-original'
                for cohort in config['explicit_cohorts']:
                    if rel==cohort['root'] or rel.startswith(cohort['root']+'/'):classification=cohort['classification'];break
                members[rel]={'sha256':digest,'size':after.st_size,'classification':classification,'eligible_for_S2_formal':False}
                if (n=='rc' or n.endswith('.rc')) and after.st_size<20:
                    text=p.read_text(errors='replace').strip()
                    if text.isdigit():original_rcs[rel]=int(text)
    return {'status':'HISTORY INVENTORY CANDIDATE; NOT ROOT CERTIFIED WHOLE HISTORY','active_S2':None,'full_failed_history_review_complete':False,'roots':config['roots'],'explicit_cohorts':config['explicit_cohorts'],'members':members,'original_rc_observations':original_rcs,'rc_note':'Verbatim numeric contents only; HTTP normalized/status and Unix kinds are not conflated or interpreted as whole-chain verdicts','issues':issues,'actual_copy':False}

if __name__=='__main__':
    import argparse
    ap=argparse.ArgumentParser();sub=ap.add_subparsers(dest='cmd',required=True);p=sub.add_parser('snapshot-check');p.add_argument('snapshot');p=sub.add_parser('inventory');p.add_argument('workspace');p.add_argument('config');args=ap.parse_args()
    try:out=snapshot(args.snapshot) if args.cmd=='snapshot-check' else inventory(args.workspace,json.load(open(args.config)));print(json.dumps(out,indent=2))
    except (ValueError,OSError,KeyError,TypeError) as e:print(json.dumps({'status':'NOTREADY','problem':str(e)}));raise SystemExit(1)
