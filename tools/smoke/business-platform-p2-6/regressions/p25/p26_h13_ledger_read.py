#!/usr/bin/env python3
"""One actual read-only fixture ledger child; parent records original Unix rc."""
import argparse,hashlib,json,sys
from pathlib import Path
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent;sys.path.insert(0,str(HERE.parent/'adapters'))
import w10_guard_shim as H

def main():
    p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);p.add_argument('--summary',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    r=H.validate_release(a.release_file,suite='p25')
    if r['p25_mode']!='h13-ledger' or r['p25_tool_shas'].get(Path(__file__).name)!=H.sha(__file__): raise ValueError('Root collector tool/mode not pinned')
    summary=Path(a.summary).resolve();out=Path(a.output).resolve()
    if H.sha(summary)!=r['p25_inputs'].get(str(summary)) or out!=Path(r['p25_output']).resolve()/'read': raise ValueError('Root exact h13 inputs/output mismatch')
    if out.exists(): raise ValueError('exclusive child output required')
    out.mkdir(parents=True)
    url=r['p25_bases']['fixture']+'/admin/log/p25h13'
    if url!='http://127.0.0.1:17103/admin/log/p25h13': raise ValueError('wrong owned fixture origin')
    att=H.W.Attempt(out,'ledger-get');before=att.step_begin('GET',{'method':'GET','url':url})
    response=H.W.http_call(url,auth=None)
    att.step_finish('GET',before,response.body,'unavailable',{'http_status':response.status,'original_rc':None,'error':response.error})
    path=out/'body.json';path.write_bytes(response.body);(out/'http_status').write_text(str(response.status)+'\n')
    rc=0 if response.status==200 and response.error is None else 1
    (out/'original.rc').write_text(str(rc)+'\n')
    body=json.loads(response.body)
    if not isinstance(body,list): raise ValueError('ledger body not actual list')
    print(json.dumps({'schema':'p25-h13-ledger-read-v1','status':response.status,'original_rc':rc,'body':body,'body_file':str(path),'body_sha256':H.sha(path),'h13_summary':str(summary),'h13_summary_sha256':H.sha(summary)}))
    return rc
if __name__=='__main__':
    try: sys.exit(main())
    except (ValueError,KeyError,OSError) as e:
        print('h13 read refused: '+str(e),file=sys.stderr);sys.exit(2)
