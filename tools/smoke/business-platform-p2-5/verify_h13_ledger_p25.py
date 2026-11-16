#!/usr/bin/env python3
"""Pure verifier of the one saved h13 ledger GET against four real successful Runs."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex

HERE=Path(__file__).resolve().parent

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def module():
    s=importlib.util.spec_from_file_location('h13_quiet_util',HERE/'collect_http_quiet_p25.py')
    q=importlib.util.module_from_spec(s);s.loader.exec_module(q);return q


def verify_ledger(step, summary, resolve_path=None):
    resolve=resolve_path or Path;q=module();m=q.engine();step=Path(step)
    links=[]
    def linked(path):
        p=resolve(str(path));links.append({'path':str(path),'sha256':sha(p)});return p
    if linked(step/'rc').read_text().strip()!='0':raise ValueError('ledger GET outer original rc not0')
    command=linked(step/'cmd.txt').read_text().strip()
    argv=shlex.split(command[5:] if command.startswith('CMD: ') else command)
    linked(step/'stderr.log')
    if len(argv)<3 or '-I' not in argv:raise ValueError('ledger actual collector command missing')
    linked(argv[argv.index('-I')+1])
    meta=json.loads(linked(step/'stdout.log').read_text())
    if meta.get('schema')!='p25-h13-ledger-read-v1' or type(meta.get('status')) is not int or meta['status']!=200 or type(meta.get('original_rc')) is not int or meta['original_rc']!=0:
        raise ValueError('ledger actual HTTP200/originalrc0 missing')
    if str(Path(meta.get('h13_summary','')).absolute())!=str(Path(summary).absolute()) or meta.get('h13_summary_sha256')!=sha(linked(summary)):
        raise ValueError('ledger successful h13 summary binding mismatch')
    raw=linked(meta['body_file']);ledger=json.loads(raw.read_text())
    if sha(raw)!=meta.get('body_sha256') or ledger!=meta.get('body') or not isinstance(ledger,list):
        raise ValueError('ledger raw bytes/metadata mismatch')
    if linked(Path(meta['body_file']).parent/'http_status').read_text().strip()!='200' or linked(Path(meta['body_file']).parent/'original.rc').read_text().strip()!='0':
        raise ValueError('ledger raw status/originalrc mismatch')
    doc=json.loads(resolve(str(summary)).read_text())
    if doc.get('failed')!=[] or [s.get('id') for s in doc.get('scenarios',[])]!=['h13'] or any(g.get('ok') is not True for g in doc['scenarios'][0]['gates']):
        raise ValueError('ledger source not successful actual h13')
    evidence=doc['attempts']['evidence_dir'];records=[]
    for p in sorted(resolve(evidence).glob('*-attempt-*.json')):
        data=json.loads(p.read_text());records.append((p.name,data))
    runs,defs,ticks,manual={}, {},set(),set()
    for name,d in records:
        if d.get('status') not in (200,201,202) or not isinstance(d.get('resp_body'),str):continue
        try:b=json.loads(d['resp_body'])
        except ValueError:continue
        if isinstance(b,dict) and b.get('status')=='succeeded' and b.get('sync_def_id') and b.get('incremental'):
            if b['id'] in runs and runs[b['id']]!=b:raise ValueError('successful run changed between raw reads')
            runs[b['id']]=b
        if isinstance(b,dict) and b.get('source_id') and b.get('output_dataset_id') and b.get('id'):defs[b['id']]=b
        if isinstance(b,list) and '/schedule/ticks' in d.get('path',''):
            ticks.update(t.get('run_id') for t in b if isinstance(t,dict) and t.get('outcome')=='enqueued')
        if d.get('method')=='POST' and d.get('path','').endswith('/run') and d.get('query')=='async=true' and isinstance(b,dict):manual.add(b.get('id'))
    if len(runs)!=4 or len(set(r['sync_def_id'] for r in runs.values()))!=2:raise ValueError('expected exactly four successful runs on two actual defs')
    if len(set(runs)&ticks)!=3 or len(set(runs)&manual)!=1 or (set(runs)&ticks)&manual:raise ValueError('expected three scheduled + one real async manual run')
    parents=set();windows=[];results=[];consumed=set()
    for rid,run in sorted(runs.items(),key=lambda item:m._rfc3339(item[1].get('started_at'))):
        did=run['sync_def_id'];dv=defs.get(did)
        if not dv or not m._is_uuidish(dv.get('source_id')):raise ValueError('run actual def/source parent missing')
        parents.add(dv['source_id'])
        base=(run.get('incremental') or {}).get('base_epoch')
        if type(base) is not int or base not in (0,1) or run['incremental'].get('mode')!='cursor':raise ValueError('real run captured base epoch invalid')
        start,end=m._rfc3339(run.get('started_at')),m._rfc3339(run.get('publish_entered_at'))
        if not start or end<=start:raise ValueError('run fetch/publish interval invalid')
        if any(start<=b and end>=a for a,b in windows):raise ValueError('successful run fetch intervals overlap')
        windows.append((start,end))
        group=[(i,e) for i,e in enumerate(ledger) if type(e.get('ts')) in (int,float) and start<=e['ts']<=end]
        pages=m.SRC_WINDOWS['p25h13'][base];cursors=['']+[m._page_key(page) for page in pages[1:]]
        token=None if base==0 else m.src_token('p25h13',base-1)
        if len(group)!=len(pages):raise ValueError('ledger page count differs from exact successful run window')
        for (i,e),cursor in zip(group,cursors):
            if i in consumed or e.get('since_param')!=m.P25_SINCE_PARAM or e.get('token')!=token or e.get('window')!=base or e.get('cursor')!=cursor or type(e.get('page_size')) is not int or e['page_size']!=50:
                raise ValueError('ledger token/window/cursor/page_size mismatch')
            consumed.add(i)
        results.append({'run_id':rid,'def_id':did,'source_id':dv['source_id'],'base_epoch':base,
                        'started_at':run['started_at'],'publish_entered_at':run['publish_entered_at'],
                        'ledger_indices':[i for i,_ in group],'expected_pages':len(pages)})
    if len(parents)!=1 or any(sorted(r['incremental']['base_epoch'] for r in runs.values() if r['sync_def_id']==did)!=[0,1] for did in {r['sync_def_id'] for r in runs.values()}):
        raise ValueError('actual successful two-batch source/def lineage inconsistent')
    handles=json.loads(linked(doc['attempts']['handles_file']).read_text())['handles']
    if not any(h.get('kind')=='source' and h.get('ok') and h.get('ref') in parents and h.get('config',{}).get('sync',{}).get('query_params',{}).get('src')=='p25h13' for h in handles):
        raise ValueError('actual source parent not bound to p25h13 fixture config')
    links.extend({'path':str(Path(evidence)/name),'sha256':sha(resolve(str(Path(evidence)/name)))} for name,_ in records)
    return {'ok':True,'schema':'p25-h13-ledger-verdict-v1','runs':results,'used_ledger_indices':sorted(consumed),
            'excluded_historical_indices':[i for i in range(len(ledger)) if i not in consumed], 'inputs':links}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--step',required=True);p.add_argument('--summary',required=True);p.add_argument('--output',required=True);a=p.parse_args(argv)
    try:result=verify_ledger(a.step,a.summary)
    except Exception as exc:result={'ok':False,'problems':[str(exc)]}
    target=Path(a.output)
    if target.exists():raise ValueError('refuse overwrite')
    target.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'ok':result['ok'],'output':str(target)}));return 0 if result['ok'] else 1

if __name__=='__main__':raise SystemExit(main())
