"""Read-only S2 runtime original receipts; never imports deployment executors.

A failed Connector execution is a preserved execution group (outer rc1),
followed by a distinct readonly completion group. No rc rewriting.
"""
import hashlib
import json
import os
import re


def need(ok, why):
    if not ok: raise ValueError(why)


def group(r, p):
    """Validate exact original step and process-kind closure, no RESULT verdict."""
    directory=p['directory']
    actual={n[:-3] for n in os.listdir(r.path(directory)) if n.endswith('.rc')}
    need(actual==set(p['steps']) and len(actual)==p['count'], 'runtime exact original step set')
    counts={'argv':0,'http':0}
    for name in sorted(actual):
        base=directory+'/'+name
        r.rc(base+'.rc');cmd=r.doc(base+'.cmd.json');meta=r.doc(base+'.meta.json');raw=r.read(base+'.raw')
        need(bool(cmd) and cmd==p['steps'][name]['command'],'runtime original exact command')
        kind=meta['kind'];need(kind in counts,'runtime unknown original kind');counts[kind]+=1
        if kind=='argv':need(meta['original_child_rc']==0,'runtime original Unix child rc')
        else:
            need(meta['original_child_rc'] is None and meta['status']==200 and meta['transport_outcome']==0,'runtime HTTP status versus child rc')
            body=raw[:-1] if len(raw)==meta['bytes']+1 and raw.endswith(b'\n') else raw
            need(meta['scrub_hits']==0 and hashlib.sha256(body).hexdigest()==meta['body_sha256'] and len(body)==meta['bytes'],'runtime HTTP original response hash/bytes')
        need(meta==p['steps'][name]['meta'] and hashlib.sha256(raw).hexdigest()==p['steps'][name]['raw_sha256'],'runtime contract original input changed')
    need(counts==p['kinds'],'runtime exact Unix/HTTP kind counts')
    if p.get('outer'):
        r.rc(p['outer']+'/'+p.get('outer_rc_file','outer.rc'),p['outer_expected_rc'])
        for name in p['outer_inputs']:r.read(p['outer']+'/'+name)
    return counts


def upgrade(r,p,source):
    need(p['epoch']==source,'runtime S2 source')
    groups=[group(r,g) for g in p['groups']]
    service=p['service'];need(service in ('dataset','connector','p26connectorc','p26connectora','p26connectorb'),'runtime service')
    original=p['groups'][0]
    if service=='dataset':need(original['count']==55,'Dataset exact55 original steps')
    elif service=='connector':
        need(original['count']==32 and original['kinds']=={'argv':23,'http':9} and original['outer_expected_rc']==1,'Connector original partial32 rc1 must remain')
        need(len(groups)==2 and p['groups'][1]['readonly'] is True,'Connector separate readonly completion required')
        for name in p['groups'][1]['steps']:
            cmd=r.doc(p['groups'][1]['directory']+'/'+name+'.cmd.json')
            need((isinstance(cmd,dict) and cmd.get('method')=='GET') or (isinstance(cmd,list) and not set(cmd).intersection({'up','restart','rm','stop','start','kill','create','run'})),'readonly completion has mutation')
    intent=r.doc(p['intent']);need(intent['source_sha256']==source and intent['service']==service,'runtime actual intent source/service')
    before=json.loads(r.text(p['before_facts']));after=json.loads(r.text(p['after_facts']))
    need(before['cid']!=after['cid'] and after['cid']==p['cid'] and after['image']==after['config_image']==p['image'] and after['running'] is True,'runtime exact new CID/image')
    need(before['mounts']==after['mounts'] and before['ports']==after['ports'] and set(before['networks'])==set(after['networks']),'runtime mounts ports network preserved')
    def names(rel):
        values=[x for x in r.text(rel).splitlines() if x]
        need(values and len(values)==len(set(values)) and all(re.fullmatch(r'[A-Z_][A-Z0-9_]*',x) for x in values),'runtime env names duplicate/invalid')
        return set(values)
    need(names(p['before_env_names'])==names(p['after_env_names']),'runtime env names unique set preserved')
    # Business/typed guard observations are exact whole original JSON inputs,
    # bound by explicit Root reviewed full-input contracts, not Root booleans.
    need(p['preservation_inputs'] and p['health_input'],'runtime business originals missing')
    for rel,expected in p['preservation_inputs'].items():need(r.doc(rel)==expected,'runtime full typed preservation observation changed')
    need(p['health_input'] in p['preservation_inputs'],'runtime actual health membership')
    for rel,digest in p['historical_tools'].items():need(hashlib.sha256(r.read(rel)).hexdigest()==digest,'runtime historical tool blob')
    need(p['historical_tools'],'runtime historical tools absent')
    return {'kind':'s2-runtime-upgrade','service':service,'original_groups':groups,'source':source,'final_scope_complete':False}
