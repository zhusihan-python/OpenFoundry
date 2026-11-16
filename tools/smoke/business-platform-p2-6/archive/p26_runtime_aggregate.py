"""S2 five independent upgrades + separate cohort2 original proof reader."""
import hashlib,json,os,re
from pathlib import Path


def need(ok,why):
    if not ok:raise ValueError(why)


def canonical(value):
    if isinstance(value,list):return sorted((canonical(x) for x in value),key=lambda x:json.dumps(x,sort_keys=True))
    if isinstance(value,dict):return {k:canonical(v) for k,v in value.items()}
    return value


def business(r,d):
    observations={};counts={}
    for f in sorted(os.listdir(r.path(d))):
        if not f.endswith('.cmd.json'):continue
        base=d+'/'+f[:-9];cmd=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json');r.rc(base+'.rc');raw=r.text(base+'.raw')
        if isinstance(cmd,list) and 'psql' in cmd:
            key=cmd[-1];payload=[json.loads(x) for x in raw.splitlines() if x.startswith('[') or x=='null'];need(len(payload)==1,'guard original SQL JSON');value=payload[0] or []
            if "r.control_mode = 'controlled' AND r.status IN ('pending','running')" in key:counts['controlled']=len(value)
            if 'cp_owners' in key or 'lease_owner' in key:pass
            if value and 'run_id' in value[0] and 'confirmed_epoch' in value[0]:counts['cp']=len(value)
            observations[key]=canonical(value)
        elif isinstance(cmd,dict) and cmd.get('method')=='GET':observations[cmd['url']]={'status':meta['status'],'body':canonical(json.loads(raw))}
    need(counts.get('controlled')==9,'runtime whole controlled exact9')
    # Original CP query row schema includes run_id and its full owner fields.
    cps=[v for k,v in observations.items() if isinstance(v,list) and len(v)==8 and v and isinstance(v[0],dict) and 'run_id' in v[0]]
    need(len(cps)==1,'runtime wholeCP8 absent/ambiguous')
    return observations


def aggregate(r,p,source):
    need(p['epoch']==source and set(p['upgrades'])=={'dataset','connector','p26connectorc','p26connectora','p26connectorb'},'runtime exact five S2 upgrades')
    expected={'dataset':[(55,36,19,0)],'connector':[(32,23,9,1),(25,15,10,0)],'p26connectorc':[(39,29,10,1),(26,16,10,0)],'p26connectora':[(57,38,19,0)],'p26connectorb':[(57,38,19,0)]}
    import importlib.util
    spec=importlib.util.spec_from_file_location('runtime',Path(__file__).with_name('p26_runtime_proofs.py'));runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
    shared=p['shared_original_usedtools'];manifest=r.doc(shared['manifest'])
    need(len(shared['rows'])==len(manifest['files'])==9,'original beforefix exact9 historical tools')
    for toolpin,actual in zip(shared['rows'],manifest['files']):
        need(toolpin['origin']==actual['origin'] and toolpin['sha256']==actual['sha256'],'original used tool manifest binding')
        need(hashlib.sha256(r.read(toolpin['copy'])).hexdigest()==actual['sha256'],'original shared beforefix tool bytes')
    up_commands=[];guards=[]
    for service,entry in p['upgrades'].items():
        need(len(entry['groups'])==len(expected[service]),'runtime exact original/completion groups')
        for group,scope in zip(entry['groups'],expected[service]):
            need((group['count'],group['kinds']['argv'],group['kinds']['http'],group['outer_expected_rc'])==scope,'runtime exact original kind/count/outer scope')
            runtime.group(r,group)
            for step in group['steps'].values():
                cmd=step['command']
                if isinstance(cmd,list) and 'up' in cmd:
                    up_commands.append(cmd);need(service==cmd[-1] and '--no-deps' in cmd and '--no-build' in cmd and '--pull=never' in cmd,'runtime only exact up')
        intent=r.doc(entry['intent']);need(intent['source_sha256']==source and intent['service']==service,'runtime actual intent source/service')
        before=r.doc(entry['before_facts']);after=r.doc(entry['after_facts'])
        need(after['cid']==entry['current']['cid'] and after['image']==after['config_image']==entry['current']['image'] and after['started_at']==entry['current']['started_at'] and after['running'] is True,'runtime actual new identity')
        need(before['cid']!=after['cid'] and canonical(before['mounts'])==canonical(after['mounts']) and before['ports']==after['ports'] and set(before['networks'])==set(after['networks']),'runtime original safe facts changed')
        for side in ['before','after']:
            names=r.text(entry[side+'_names']).split();need(len(names)==len(set(names)),'runtime env names duplicate')
            if side=='before':previous=set(names)
            else:need(previous==set(names),'runtime env name set changed')
        need(len(entry['preserved_facts'])==10,'runtime full other10 facts required')
        for name,refs in entry['preserved_facts'].items():need(canonical(r.doc(refs['before']))==canonical(r.doc(refs['after'])),'runtime non-target full facts changed '+name)
        guards+=entry['guards']
        for tools in entry['tools']:
            before=r.doc(tools['before']);after=r.doc(tools['after'])
            need(before[tools['before_source']]==after[tools['after_source']]==source,'runtime actual source drift')
            if 'tools' in before:need(before['tools']==after['tools'],'runtime actual tool drift')
            declared=dict(before.get('tools',{}));declared.update({k:v for k,v in before.items() if k in ('bridge_sha256','executor_sha256')})
            need({k:v['sha256'] for k,v in tools['blobs'].items()}==declared,'runtime used tools differ actual before map')
            for label,pin in tools['blobs'].items():need(hashlib.sha256(r.read(pin['path'])).hexdigest()==pin['sha256'],'runtime historical used tool byte mismatch')
    need(len(up_commands)==5,'runtime must be exactly five original up executions')
    aux=p['aux2'];need(len(aux['steps'])==35 and aux['kinds']=={'argv':33,'http':2},'aux2 exact35 step scope')
    # lsof rc1 is empty/no collision, and is never relabeled Unix success.
    expected_rc1=aux['expected_rc1'];need(len(expected_rc1)==1,'aux2 exact expected lsof1')
    for name,step in aux['steps'].items():
        base=aux['directory']+'/'+name;r.rc(base+'.rc',1 if name in expected_rc1 else 0)
        cmd=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json');raw=r.read(base+'.raw')
        need(cmd==step['command'] and meta==step['meta'] and hashlib.sha256(raw).hexdigest()==step['raw_sha256'],'aux2 original changed')
        if name in expected_rc1:need(cmd[0]=='lsof' and raw==b'\n' and meta['original_child_rc']==1 and meta['stderr']=='','aux2 lsof1 must be empty/no collision')
        elif meta['kind'] in ('argv','call'):need(meta['original_child_rc']==0,'aux2 original Unix rc')
        else:need(meta['kind']=='http' and meta['status']==200 and meta['transport_outcome']==0 and meta['original_child_rc'] is None,'aux2 actual HTTP')
    r.rc(aux['outer']+'/rc');ab=r.doc(aux['outer']+'/before.json');aa=r.doc(aux['outer']+'/after.json')
    need(ab['source']==aa['source']==source and ab['tools']==aa['tools'],'aux2 actual source/tools drift')
    need({k:v['sha256'] for k,v in aux['tools'].items()}==ab['tools'],'aux2 used tool map')
    for label,pin in aux['tools'].items():need(hashlib.sha256(r.read(pin['path'])).hexdigest()==pin['sha256'],'aux2 original tool blob')
    ledger=r.doc(aux['ledger']);schema=r.doc(aux['schema']);need(set(schema)=={'a','b'},'aux2 exact A2/B2')
    for role in ['a','b']:
        fact=r.doc(aux['facts'][role]);need(fact==ledger[role],'aux2 creation ledger versus original inspect');need(fact['name']=='/p26connector'+role+'2' and fact['cid']==schema[role]['cid'] and fact['image']==schema[role]['image'] and fact['running'] is True,'aux2 actual created facts')
        need(fact['labels']['of-p26-owner']=='p26-http' and fact['labels']['of-p26-stage']=='p2-6' and fact['labels']['of-p26-role']=='connector_'+role,'aux2 actual labels')
        need(fact['ports']['50088/tcp']==[{'HostIp':'127.0.0.1','HostPort':str(17101 if role=='a' else 17102)}],'aux2 loopback exact port')
    need(len(aux['preserved_facts'])==11,'aux2 all previous11 preserved')
    for name,refs in aux['preserved_facts'].items():need(canonical(r.doc(refs['before']))==canonical(r.doc(refs['after'])),'aux2 altered old container '+name)
    guards+=aux['guards'];need(guards,'runtime preservation originals absent')
    baseline=None
    for directory in guards:
        current=business(r,directory)
        if baseline is None:baseline=current
        else:need(current==baseline,'runtime old+typed SQL/public whole original changed')
    # The first current observation is compared with an independently closed
    # original pre-S2 baseline; old S only supplies preserved rows, never S2 rc.
    anchor=r.doc(p['preservation_anchor_contract'])
    spec=importlib.util.spec_from_file_location('historicalanchor',Path(__file__).with_name('p26_legacy_proofs.py'));legacy=importlib.util.module_from_spec(spec);spec.loader.exec_module(legacy)
    legacy.p1_slice(r,anchor['proof'],anchor['source'])
    anchor_rows=business(r,anchor['proof']['guard'])
    for sql,rows in anchor_rows.items():
        if not isinstance(rows,list):
            need(sql in baseline and baseline[sql]==rows,'runtime old public changed versus original anchor')
        elif 'r.control_mode' in sql or (rows and 'run_id' in rows[0]):
            need(sql in baseline and baseline[sql]==rows,'runtime old whole9/CP8 changed versus original anchor')
        elif rows and 'def_id' in rows[0]:
            current_parents=[v for k,v in baseline.items() if isinstance(v,list) and v and 'def_id' in v[0] and 'conn_id' in v[0]]
            need(len(current_parents)==1,'runtime parent whole original missing')
            oldids={x['def_id'] for x in rows};need(canonical([x for x in current_parents[0] if x['def_id'] in oldids])==rows,'runtime old full parent differs original anchor')
    for rel,pin in p['supplementary_originals'].items():need(hashlib.sha256(r.read(rel)).hexdigest()==pin['sha256'],'runtime supplementary original changed')
    for directory,history in p['history_only'].items():
        need(history['classification']=='failed-original-history-only-never-formal','failed history cannot be formal')
        for rel,pin in history['members'].items():need(hashlib.sha256(r.read(rel)).hexdigest()==pin['sha256'],'failed runtime/HTTP history original changed')
    return {'kind':'s2-runtime-aggregate','upgrades':5,'composite_original_outer1_preserved':2,'separate_aux2':2,'guards_recomputed':len(guards),'final_cleanup_restart_legacy':False}
