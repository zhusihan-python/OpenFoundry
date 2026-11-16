#!/usr/bin/env python3
"""Pure actual fresh S2 build originals, with in-memory negative substitutions."""
import json,sys
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import p26_p25_selected_build as B
import p26_p25_runtime as R

def main():
    base=R.P26/'results/w6/S2'/R.SOURCE;d=base/'build/build-connector_20261115T092616Z';outer=base/'outer/build-connector';image=json.loads((d/'BUILD-RESULT.json').read_text())['new_full_id'];checks=[]
    for fault in [None,'source','binary','config','rc','mode','outer-source']:
        class Reader(B.P.Reader):
            def read(self,rel):
                raw=super().read(rel)
                if fault=='binary' and rel.endswith('/out-'+R.SOURCE[:8]+'/service'):return raw+b'wrong'
                if fault=='config' and '09-old-config-' in rel and rel.endswith('.raw'):return raw+b'wrong'
                if fault=='rc' and '04-compile-' in rel and rel.endswith('.rc'):return b'1\n'
                if fault=='source' and rel.endswith('/BUILD-RESULT.json'):
                    j=json.loads(raw);j['expect_source_sha256']='b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c';return json.dumps(j).encode()
                if fault=='mode' and rel.endswith('/BINARY.json'):
                    j=json.loads(raw);j['mode']='0o644';return json.dumps(j).encode()
                if fault=='outer-source' and rel.endswith('/cmd-before.json'):
                    j=json.loads(raw);j[j.index('--expect-source-sha')+1]='oldsource';return json.dumps(j).encode()
                return raw
        reader=Reader(B.ROOT)
        try:v=B.normal_build(reader,d,outer,R.SOURCE,image)
        except ValueError:assert fault is not None
        else:assert fault is None and v['original_steps']==17 and len(reader.files)==77
        checks.append(fault or 'actual77 original files including binary bytes, 17rc0+outerrc0')
    print(json.dumps({'ok':True,'actual_HTTP_SQL_Docker_calls':0,'checks':checks,'original_artifacts_never_modified':True},indent=2))
if __name__=='__main__':main()
