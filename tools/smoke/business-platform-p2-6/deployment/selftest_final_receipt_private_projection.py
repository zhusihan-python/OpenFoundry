#!/usr/bin/env python3
"""Only fake private projections; never reads actual intents or sends HTTP."""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

sys.dont_write_bytecode = True
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('receipt_private_test',HERE/'final_dataset_receipt_replay.py')
M=importlib.util.module_from_spec(spec);spec.loader.exec_module(M)


def run(out):
    out.mkdir(parents=True,exist_ok=False)
    marker='FAKE_PRIVATE_INLINE_AUTH_CANDIDATE_MUST_NOT_ENTER_EVIDENCE'
    body=json.dumps({'idempotency_key':M.KEY,'branch':'main','create_if_missing':False,'files':[{'content_base64':base64.b64encode(marker.encode()).decode()}]}).encode()
    fp=hashlib.sha256(body).hexdigest()
    payload=json.dumps({'body_base64':base64.b64encode(body).decode(),'request_fingerprint':fp,'body_bytes':len(body)}).encode()+b'\n'
    results=[]
    for name,change,error,code,expected_ok in [
        ('valid-exact-memory-body',None,None,0,True),
        ('wrong-fingerprint',lambda x:x.update(request_fingerprint='0'*64),None,0,False),
        ('wrong-length',lambda x:x.update(body_bytes=len(body)+1),None,0,False),
        ('nonzero-original-child',None,None,23,False),
        ('timeout-private-partials',None,subprocess.TimeoutExpired(['fake-private-child'],30,output=payload,stderr=marker.encode()),None,False),
        ('spawn-private-message',None,OSError(marker),None,False),
        ('duplicate-private-rows','duplicate',None,0,False),
    ]:
        att=M.W.Attempt(out,name)
        raw=payload
        if callable(change):
            value=json.loads(payload);change(value);raw=json.dumps(value).encode()+b'\n'
        if change=='duplicate':raw+=raw
        def fake(*a,**k):
            if error:raise error
            return subprocess.CompletedProcess([],code,raw,marker.encode())
        accepted=False
        with patch.object(M,'BODY_SHA',fp):
            try:
                returned=M.private_material(att,['fake-SELECT-private-columns-only'],runner=fake)
                assert returned==body
                accepted=True
            except (AssertionError,RuntimeError):pass
        assert accepted==expected_ok
        rcs=[f.read_text().strip() for f in att.dir.glob('*.rc')]
        assert 'pending' not in rcs
        assert ('unavailable' in rcs) if error else (str(code) in rcs)
        for f in att.dir.iterdir():
            text=f.read_bytes()
            assert marker.encode() not in text and body not in text and base64.b64encode(body) not in text
        results.append({'case':name,'pass':True,'accepted':accepted,'raw_material_archived':False})
    (out/'RESULT.json').write_text(json.dumps({'pure_only':True,'actual_private_SELECTs':0,'actual_POSTs':0,'passed':len(results),'cases':results,'helper_sha256':M.G.sha256_file(HERE/'final_dataset_receipt_replay.py')},indent=2)+'\n')
    print(out);print(str(len(results))+' private-projection cases passed; actual SELECT/POST=0')


if __name__=='__main__':run(Path(sys.argv[1]).resolve())
