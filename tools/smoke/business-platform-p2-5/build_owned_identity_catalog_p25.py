#!/usr/bin/env python3
"""Readonly catalog from all actual EV history + one fixed whole-union SELECT."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys

HERE=Path(__file__).resolve().parent
UUID=re.compile(r'[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}')


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def sources(ev):
    required={str(p.resolve()):'p24-result' for p in ev.rglob('p24-report.json')}
    required.update({str(p.resolve()):'p25-handles' for p in ev.rglob('handles.json')})
    bodies={}
    for p in ev.rglob('*.json'):
        try:obj=json.loads(p.read_text())
        except (ValueError,UnicodeDecodeError):continue
        if not isinstance(obj,dict) or obj.get('status') not in (200,201):continue
        path=str(obj.get('path') or '')
        valid=(obj.get('method')=='POST' and path.endswith('/api/v1/data-connection/syncs')) or (obj.get('method')=='GET' and re.search(r'/api/v1/data-connection/syncs/[0-9a-f-]{36}$',path))
        if not valid:continue
        try:body=json.loads(obj['resp_body']) if isinstance(obj.get('resp_body'),str) else obj.get('resp_body')
        except ValueError:continue
        if isinstance(body,dict) and body.get('id') and body.get('source_id'):
            required[str(p.resolve())]='p25-def-create-response' if obj['method']=='POST' else 'p25-def-read-response';bodies[str(p.resolve())]=body
    parents,origins,ids={}, {},set()
    def add(did,sid,path):
        if not UUID.fullmatch(str(did)):raise ValueError('non-UUID observed def identity')
        ids.add(did);origins.setdefault(did,set()).add(path)
        if sid:
            if not UUID.fullmatch(str(sid)) or (did in parents and parents[did]!=sid):raise ValueError('source parent identity drift')
            parents[did]=sid
    inputs=[]
    for path,kind in sorted(required.items()):
        obj=json.loads(Path(path).read_text());inputs.append({'kind':kind,'path':path,'sha256':sha(path)})
        if kind=='p24-result':
            for did,row in (obj.get('owned_sync_ids') or {}).items():add(did,row.get('source_id'),path)
        elif kind=='p25-handles':
            for row in obj.get('handles') or []:
                if row.get('kind') in ('def','h10-def') and row.get('ref'):add(row['ref'],row.get('source_id'),path)
        else:body=bodies[path];add(body['id'],body['source_id'],path)
    if not ids or set(parents)!=ids:raise ValueError('whole identity union empty/missing real parent')
    return inputs,parents,origins


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--ev',required=True);p.add_argument('--out',required=True);p.add_argument('--raw-dir',required=True);p.add_argument('--released',action='store_true');a=p.parse_args()
    if not a.released or os.environ.get('P25_CONFIRM')!='yes':print('readonly catalog not released',file=sys.stderr);return 3
    ev=Path(a.ev).resolve();target=Path(a.out);raw=Path(a.raw_dir);assert not target.exists();raw.mkdir(parents=True,exist_ok=False)
    inputs,parents,origins=sources(ev)
    spec=importlib.util.spec_from_file_location('catalog_fixed_collector',HERE/'collect_postfacts_p25.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    argv=m.psql_argv(Path(m.ENV_DIR_DEFAULT),m.identity_def_sql(sorted(parents)))
    cmd=raw/'cmd.txt';cmd.write_text(' '.join(argv)+'\n')
    (raw/'argv.json').write_text(json.dumps(argv)+'\n')
    r=subprocess.run(argv,capture_output=True,timeout=90)
    out=raw/'stdout';err=raw/'stderr';rc=raw/'rc';out.write_bytes(r.stdout);err.write_bytes(r.stderr);rc.write_text(str(r.returncode)+'\n')
    if r.returncode:print('readonly fixed-union query failed',file=sys.stderr);return r.returncode
    rows=json.loads(r.stdout.decode().strip().splitlines()[-1]);assert isinstance(rows,list)
    byid={row['id']:row for row in rows};assert set(byid)==set(parents) and len(byid)==len(rows)
    entry={'kind':'catalog-def-read','path':str(out.resolve()),'sha256':sha(out)}
    for key,path in [('cmd',cmd),('rc',rc),('stderr',err)]:entry[key+'_path']=str(path.resolve());entry[key+'_sha256']=sha(path)
    inputs.append(entry);defs=[]
    for did,row in sorted(byid.items()):
        assert set(row)=={'id','source_id','cron','enabled','cursor_null'} and row['source_id']==parents[did] and row['enabled']=='false' and row['cursor_null']=='true'
        defs.append({'def_id':did,'source_id':row['source_id'],'cron':row['cron'],'derived_from':sorted(origins[did]|{str(out.resolve())})})
    with target.open('x') as f:json.dump({'schema':'p25-owned-identity-catalog-v1','inputs':inputs,'defs':defs},f,indent=2);f.write('\n')
    print(json.dumps({'ok':True,'catalog':str(target.resolve()),'defs':len(defs),'source_inputs':len(inputs),'query_rc':r.returncode}))
    return 0


if __name__=='__main__':raise SystemExit(main())
