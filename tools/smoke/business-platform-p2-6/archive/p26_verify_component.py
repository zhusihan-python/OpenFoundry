"""Offline original-evidence contract validation; stdlib and own readers only."""
import argparse,importlib.util,json
from pathlib import Path

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--root',required=True);ap.add_argument('--contract',required=True);args=ap.parse_args();contract=json.loads(Path(args.contract).read_bytes());spec=importlib.util.spec_from_file_location('originals',Path(__file__).with_name('p26_original_proofs.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);result,files=m.validate(args.root,contract,contract['epoch']);print(json.dumps({'result':result,'original_files':len(files)},sort_keys=True))
if __name__=='__main__':main()
