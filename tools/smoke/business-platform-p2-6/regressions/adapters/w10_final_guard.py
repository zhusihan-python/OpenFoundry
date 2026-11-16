#!/usr/bin/env python3
"""Final READ ONLY old+typed preservation, loops/grants/old artifacts proof.

W10 compares actual WHOLE controlled/CP with old+Root-typed; W8's old-only
9/8 comparators are deliberately not called. Every other selected W8 predicate
retains its original implementation. No mutation, adoption or unknown cleanup.
"""
import argparse,hashlib,json,re,subprocess,sys
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import w10_guard_shim as H
W=H.W
STAGE='final-typed-preservation'
TOOLS={
 'regressions/adapters/w10_final_guard.py','regressions/adapters/w10_guard_shim.py','regressions/adapters/w10_boot_transition.py',
 'common/p26_gates.py','common/p26_source_hash.py','cohort/p26_cohort_cli.py','preflight/p26_current_preflight.py'}

def execute(release_file,output,runner=None,transport=None):
    r=H.validate_release(release_file,STAGE,'p26-final')
    out=Path(output).resolve()
    if str(out)!=r['final_output'] or not out.is_relative_to((H.P26/'results/w10').resolve()):raise ValueError('Root exact final output required')
    if set(r['final_tools_sha256'])!=TOOLS:raise ValueError('exact final active tool map required')
    def gate():
        if H.validate_release(release_file,STAGE,'p26-final')!=r:raise ValueError('Root final release changed')
        if H.G.candidate_hash(H.G.OPENFOUNDRY)!=r['candidate_source']:raise ValueError('actual source drift')
        for p,s in r['final_tools_sha256'].items():
            if H.sha(H.P26/p)!=s:raise ValueError('final active tool drift '+p)
        for field in ('typed_roster','deployment_record','final_aux_ledger'):
            if H.sha(r[field])!=r[field+'_sha256']:raise ValueError('final Root input drift '+field)
    gate();out.mkdir(parents=True,exist_ok=False);att=W.Attempt(out,'final')
    secret=W.read_jwt_secret();att.add_secret(secret)
    runner=runner or (lambda cmd:subprocess.run(cmd,capture_output=True,text=True,timeout=120))
    transport=transport or W.http_call
    result={'ok':False,'mutations':0,'root_release_id':r['release_id'],'actual_source_before':r['candidate_source'],'tools_before':r['final_tools_sha256']}
    def command(sid,argv):
        gate();before=att.step_begin(sid,argv)
        try:cp=runner(argv)
        except Exception as exc:
            att.step_finish(sid,before,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None});raise
        att.step_finish(sid,before,cp.stdout,cp.returncode,{'stderr':cp.stderr,'original_rc':cp.returncode})
        if cp.returncode:raise ValueError(sid+' original rc='+str(cp.returncode))
        return cp.stdout.decode() if isinstance(cp.stdout,bytes) else cp.stdout
    def http(sid,url,auth=None):
        gate();p=att.step_begin(sid,{'method':'GET',**W.safe_url_projection(url),'sent_auth':bool(auth)})
        try:resp=transport(url,auth=auth)
        except Exception as exc:
            att.step_finish(sid,p,type(exc).__name__+': '+str(exc),'unavailable',{'original_rc':None});raise
        att.step_finish(sid,p,resp.body,'unavailable',{'original_rc':None,'http_status':resp.status,'error':resp.error,'body_sha256':hashlib.sha256(resp.body).hexdigest()})
        if resp.error:raise ValueError(sid+' transport error')
        return resp
    try:
        result['whole_old_plus_typed']=H.check(release_file,runner=runner,transport=transport)
        historical,catalog,derived=W.load_baseline(H.G.P25_FINAL_DIR)
        catalog['public_views']=W.public_view_baselines(derived,catalog)
        substrate=W.load_substrate_facts();problems=W.check_substrate(substrate)
        if problems:raise ValueError('substrate baseline '+str(problems))
        att.record('sealed-baseline',{},json.dumps({'historical':historical,'substrate':substrate}),0,{'kind':'local'})
        rec=json.loads(Path(r['deployment_record']).read_text());pins=rec['containers']
        if not set(H.G.Q3_CONTAINERS).issubset(pins):raise ValueError('final pins must include main5 and old fixture')
        # Optional quiet aux are a separate exact ledger; W10's base live pin
        # set contains only main5/oldfixture, all of which must remain Running.
        aux=json.loads(Path(r['final_aux_ledger']).read_text())
        if aux.get('schema')!='p26-final-owned-aux-v1' or aux.get('source_sha256')!=r['candidate_source']:raise ValueError('Root final owned aux ledger schema/source')
        if not {'p26connectora','p26connectorb','p26connectorc'}.issubset(aux['containers']):raise ValueError('all three NEW aux connectors must be retained in quiet proof')
        for name,pin in aux['containers'].items():
            if name in pins and (any(pin[k]!=pins[name][k] for k in ('cid','image','started_at')) or pin['running'] is not pins[name].get('expected_running',True)):raise ValueError('final aux ledger differs from Root deployment pin')
            if not re.fullmatch('[a-f0-9]{64}',pin['cid']) or not re.fullmatch('sha256:[a-f0-9]{64}',pin['image']):raise ValueError('aux full pinned identity required; no main overlap')
            if name in ('p26connectora','p26connectorb','p26connectorc') and pin['running'] is not False:raise ValueError('final NEW aux executors must be stopped')
            if pin['ownership']['of-p26-owner']!='p26-http' or pin['ownership']['of-p26-stage']!='p2-6':raise ValueError('Root NEW ownership binding absent')
            fmt='{{.Id}}\n{{.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}\n{{json .Config.Labels}}'
            raw=command('quiet-NEW-owned-'+name,['docker','inspect',pin['cid'],'--format',fmt]).strip().splitlines()
            if len(raw)!=5 or raw[:4]!=[pin['cid'],pin['image'],pin['started_at'],str(pin['running']).lower()]:raise ValueError('actual NEW aux quiet identity drift '+name)
            labels=json.loads(raw[4])
            if any(labels.get(k)!=v for k,v in pin['ownership'].items()):raise ValueError('actual NEW aux ownership labels drift '+name)
        inventory=command('whole-current-NEW-owned-container-inventory',['docker','ps','--all','--no-trunc','--filter','label=of-p26-owner=p26-http','--filter','label=of-p26-stage=p2-6','--format','{{.ID}}|{{.Names}}|{{.State}}'])
        actual=[]
        for line in inventory.strip().splitlines():
            parts=line.split('|')
            if len(parts)!=3:raise ValueError('malformed whole NEW-owned Docker inventory')
            actual.append({'cid':parts[0],'name':parts[1],'state':parts[2]})
        expected=[{'cid':p['cid'],'name':name,'state':'running' if p['running'] else 'exited'} for name,p in aux['containers'].items()]
        # A known Root-pinned main may carry this ownership label. It remains
        # in the explicit whole comparison; unknown names/CIDs are never hidden.
        for row in actual:
            if row['name'] in pins and row['name'] not in aux['containers']:
                p=pins[row['name']];expected.append({'cid':p['cid'],'name':row['name'],'state':'running' if p.get('expected_running',True) else 'exited'})
        H.exact(expected,actual,'name')
        expects={svc:pins[getattr(W,'Q3_'+svc.upper())]['image'] for svc in ('dataset','pipeline','connector')}
        sub,org=W.jwt_identity_from_baseline(derived,catalog);token=W.sign_testing_jwt(secret,sub,org);att.add_secret(token)
        skipped={'sql-controlled-9','sql-cp-owners','sql-cp-parents','jwt-sign'}
        predicates=[]
        for spec in W.build_step_specs(catalog):
            sid=spec['id']
            if sid in skipped or sid.startswith(('view-cp-','view-runs-')):continue
            if spec['kind']=='argv':
                raw=command(sid,spec['argv'])
                if sid=='docker-ps':
                    names={ln.split('\t')[1] for ln in raw.splitlines() if ln.count('\t')>=3}
                    if not set(H.G.Q3_CONTAINERS).issubset(names):raise ValueError('main5/fixture Docker inventory missing')
                elif sid.startswith('inspect-'):
                    ok,detail=W.evaluate_inspect(sid,spec['container'],raw,catalog,expects)
                    if not ok:raise ValueError(sid+': '+str(detail))
                elif 'gate' in spec:
                    g=spec['gate'][1](raw,{'catalog':catalog})
                    if not g['ok']:raise ValueError(sid+': '+str(g))
            elif spec['kind']=='http':
                response=http(sid,spec['url'],token if spec.get('auth') else None)
                if response.status!=200:raise ValueError(sid+' HTTP status '+str(response.status))
                if 'counter' in spec and hashlib.sha256(response.body).hexdigest()!=spec['expect_body_sha256']:raise ValueError('old fixed fixture source counter drift '+sid)
                if sid=='h14-txn':
                    g=W.compare_h14_txn(catalog['h14_txn_projection'],response.status,json.loads(response.body))
                    if not g['ok']:raise ValueError(str(g))
            elif spec['kind']=='download':
                response=http(sid,spec['url'],token)
                if response.status!=302:raise ValueError('old download must preserve real redirect contract')
                headers={k.lower():v for k,v in response.headers.items()};url=W.validate_download_redirect(headers.get('location',''))
                final=http(sid+'-anonymous-follow',url,None)
                if final.status!=200 or len(final.body)!=catalog['h14_download_body_len'] or hashlib.sha256(final.body).hexdigest()!=catalog['h14_download_body_sha256']:raise ValueError('old file full bytes/hash/readability changed')
                # Raw bytes are stored separately, never in a JSON summary.
                (out/'old-h14-download.bin').write_bytes(final.body)
            else:raise ValueError('unhandled readonly W8 spec '+sid)
            predicates.append(sid)
        prefix=list(W.C.OLDPG_READONLY_ARGV['cp_owners']);prefix=prefix[:prefix.index('-c')]
        # WHOLE table, not an ID-filtered list: every unexpected grant refuses.
        sql="BEGIN READ ONLY; SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT sync_def_id::text FROM sync_schedule_grants ORDER BY sync_def_id) t; COMMIT;"
        grants=command('whole-current-schedule-grants-zero',prefix+['-q','-c',sql])
        a=grants.find('[');z=grants.rfind(']')
        if a<0 or z<a or json.loads(grants[a:z+1])!=[]:raise ValueError('current WHOLE schedule grants not exactly zero')
        for svc,baseline in catalog['base3_container_baseline'].items():
            raw=command('retained-old-image-'+svc,['docker','image','inspect',baseline['image'],'--format','{{.Id}}'])
            if raw.strip()!=baseline['image']:raise ValueError('old image missing/drift '+svc)
        gate();result.update(ok=True,predicates=predicates,whole_current_schedule_grants=0,base_connector_loops='W8 env-NAMES absence/defaults comparator passed',typed_aux_quiet=True,old_download_sha256=catalog['h14_download_body_sha256'],old_fixture_counters_unchanged=True)
        rc=0
    except Exception as exc:result.update(error=type(exc).__name__+': '+str(exc));rc=2
    finally:
        result['actual_source_after']=H.G.candidate_hash(H.G.OPENFOUNDRY)
        result['tools_after']={p:H.sha(H.P26/p) for p in r['final_tools_sha256']}
        if result['actual_source_after']!=r['candidate_source'] or result['tools_after']!=r['final_tools_sha256']:result['ok']=False;rc=2
        att.finish(result);(out/'RESULT.json').write_text(att.redact(json.dumps(result,indent=2))+'\n')
    return rc


def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('--release-file',required=True);p.add_argument('--output',required=True);a=p.parse_args(argv);return execute(a.release_file,a.output)
if __name__=='__main__':
    try:sys.exit(main())
    except Exception as exc:print('final typed guard refused: '+type(exc).__name__+': '+str(exc),file=sys.stderr);sys.exit(2)
