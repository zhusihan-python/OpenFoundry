"""Failed-stage original intake. A failure can never satisfy a success scope."""
import hashlib,json


def need(ok,why):
    if not ok:raise ValueError(why)


def p24_retry1(r,p,source):
    need(p['kind']=='failed-history-p24-retry1' and p['classification']=='failed-history-only','failed intake classification')
    summary=r.doc(p['launch']+'/FAILED-SUMMARY.json');members=r.doc(p['launch']+'/original-files.json');need(summary['source']==source and len(members)==826,'failed P24 exact826 original scope')
    r.rc(p['launch']+'/rc',1)
    need(summary['original_launch_rc']==summary['original_child_rc']==summary['suite_rc']==1 and summary['counts']=={'pass':5,'fail':2} and summary['official'] is False,'failed P24 cannot be converted to PASS')
    need(summary['named']=={'g1':'pass','g2':'fail','g3':'pass','g4':'fail','g5':'pass','g6':'pass','g7':'pass'},'failed P24 exact original failed gates')
    for relative,pin in members.items():
        need(not relative.lower().endswith('.env'),'private env forbidden');raw=r.read('tools/smoke/business-platform-p2-6/'+relative);need(hashlib.sha256(raw).hexdigest()==pin['sha256'] and len(raw)==pin['bytes'],'failed P24 original member changed')
    report=r.doc(p['report']);need(report['counts']==summary['counts'] and report['official'] is False,'failed P24 actual report/summary')
    cleanup=report['cleanup']['per_sync'];need(len(cleanup)==13 and set(cleanup)==set(report['owned_sync_ids']),'failed P24 complete13 owned cleanup')
    need(all(x['disable_status']==x['revoke_status']==200 and x['get_confirmed'] and x['persisted_confirmed'] and x['grant_gone'] and x['runs_terminal'] and x['runs_state_unresolved']=={} for x in cleanup.values()),'failed P24 owned cleanup incomplete')
    outer=p['outer'];need(r.doc(outer+'/tools-before.json')==r.doc(outer+'/tools-after.json'),'failed P24 actual tool drift')
    for side in ('source-before','source-after'):
        r.rc(outer+'/'+side+'/rc');need(source in r.text(outer+'/'+side+'/stdout.log'),'failed P24 actual source drift')
    r.rc(outer+'/child/rc',1)
    ledger=r.doc(p['launch']+'/sideproof-copy-ledger.json');need(len(ledger)==6,'failed P24 six original sideproof copies')
    for row in ledger:
        path=row['copy'].removeprefix(p['canonical_root_label']+'/');raw=r.read(path);need(hashlib.sha256(raw).hexdigest()==row['sha256'] and len(raw)==row['bytes'],'failed P24 safe sideproof copy changed')
    return {'kind':'failed-history-p24-retry1','original_rc':1,'pass':5,'fail':2,'cleanup_definitions':13,'formal_success_eligible':False}
