"""Real pgoutput -> Kafka -> DVS, including process loss after a durable publish."""
from functools import partial
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import URLError

HERE = Path(__file__).resolve().parent
STATE = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("fixture", HERE.parent / "data-integration-wp2-3/orders.py")
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def compose(*args, stdin=None, capture=True):
    return subprocess.run(["docker", "compose", *args], input=stdin, text=True, check=True,
                          stdout=subprocess.PIPE if capture else None, timeout=90).stdout


def sql(statement, *, service="postgres", database="checkpoints"):
    user = "postgres" if service == "source" else "openfoundry"
    return compose("exec","-T",service,"psql","-XAtq","-v","ON_ERROR_STOP=1","-U",user,"-d",database,stdin=statement).strip()


def wait(label, fn):
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        try:
            result=fn()
        except (URLError,ConnectionError):
            result=False
        if result:
            return result
        time.sleep(0.5)
    raise AssertionError(label)


def main():
    p1=fixture.module("wp15_p1",HERE.parent/"business-platform-p1/smoke/smoke.py")
    dvs="http://127.0.0.1:"+os.environ["WP23_DVS_PORT"]
    connect="http://"+compose("port","connect","8083").strip()
    p1.BASES.update(dataset=dvs)
    p1.SECRET_BYTES=os.environ["JWT_SECRET"].encode()
    ids=p1.build_identities()
    owner=ids["user_a"].token
    reader=p1.mint_token(org=ids["org_a"],permissions=[])
    foreign=ids["user_b"].token
    wait("DVS ready",lambda:urlopen(dvs+"/healthz",timeout=3).status==200)
    dataset=p1.create_dataset(owner,"CDC orders","json")["id"]
    os.environ.update(CDC_DATASET_ID=dataset,WP34_PROJECT="wp15",WP34_TOKEN=reader,WP34_DVS=dvs,WP34_EVIDENCE=str(STATE))
    graph=fixture.module("wp15_graph",HERE.parent/"data-integration-wp3-4/harness34.py")
    graph.PBS="http://"+compose("port","pipeline","50081").strip()
    wait("Pipeline Build ready",lambda:urlopen(graph.PBS+"/healthz",timeout=3).status==200)
    pinned_builds=fixture.module("wp15_pinned",HERE/"pinned_builds.py")
    handoff={"input_dataset":dataset,"output_dataset":p1.create_dataset(owner,"CDC order amounts","json")["id"],
             "builds":[]}
    build_orders = partial(pinned_builds.build, STATE, fixture, p1, graph, handoff, owner, reader)
    (Path(os.environ["CDC_SECRETS"])/"writer").write_text(owner)
    report={"revision":subprocess.check_output(["git","-C",str(HERE),"rev-parse","HEAD"],text=True).strip(),
            "dirty":bool(subprocess.check_output(["git","-C",str(HERE),"status","--porcelain"])),
            "dataset":dataset,"versions":[],"checks":[]}

    def check(name, condition):
        assert condition,name
        print("PASS",name,flush=True)
        report["checks"].append(name)
        fixture.write_json(STATE/"orders-report.json",report)

    def source(query):
        return sql(query,service="source",database="orders")

    def position():
        return int(sql("SELECT last_offset FROM orders_cdc_checkpoint WHERE dataset_id='"+dataset+"'"))

    def versions():
        return [v for v in graph.list_versions(dataset) if v.get("transaction_id")]

    def read(label,expected):
        latest=max(versions(),key=lambda v:v["version"])
        actual=graph.scan_all(dataset,latest["version"])["rows"]
        check(label,actual==expected)
        txn=latest["transaction_id"]
        files=[f for f in p1.list_files(reader,dataset) if f.get("transaction_id")==txn]
        check(label+" file belongs to the published transaction",len(files)==1)
        downloaded=p1.download_file(reader,dataset,files[0]["id"])
        check(label+" file matches the scan",downloaded.status==200 and [json.loads(line) for line in downloaded.body.splitlines()]==actual)
        (STATE/f"orders-v{latest['version']}.ndjson").write_bytes(downloaded.body)
        tr=graph.get_output_transaction(dataset,txn)
        report["versions"].append({"version":latest["version"],"transaction":txn,"rows":actual,
                                  "file_id":files[0]["id"],"sha256":hashlib.sha256(downloaded.body).hexdigest(),
                                  "offset":int(tr["metadata"]["offset"]),"lsn":int(tr["metadata"]["lsn"]),
                                  "order_id":tr["metadata"]["order_id"],"operation":tr["metadata"]["op"]})
        return latest["version"]

    sql("CREATE ROLE worker LOGIN PASSWORD '"+os.environ["WORKER_PASSWORD"]+"'; CREATE DATABASE checkpoints OWNER worker",database="datasets")
    source("CREATE ROLE cdc_reader LOGIN REPLICATION PASSWORD '"+os.environ["READER_PASSWORD"]+"'; CREATE SCHEMA sales; CREATE TABLE sales.orders(order_id text PRIMARY KEY, amount_cents bigint, batch text); ALTER TABLE sales.orders REPLICA IDENTITY FULL; GRANT USAGE ON SCHEMA sales TO cdc_reader; GRANT SELECT ON sales.orders TO cdc_reader; CREATE PUBLICATION orders_pub FOR TABLE sales.orders; INSERT INTO sales.orders VALUES ('0007',1255,'first'),('0011',0,'first')")
    # Stream several throwaway rows, then block the initial SELECT before its
    # last row. A killed Connect task cannot mark this source snapshot complete.
    source("""INSERT INTO sales.orders SELECT 'old-'||n,1,'abandoned' FROM generate_series(1,8)n;
           CREATE FUNCTION wp15_snapshot_gate() RETURNS boolean LANGUAGE plpgsql AS $$
           BEGIN PERFORM pg_sleep(180); RETURN true; END $$""")
    wait("Connect ready",lambda:urlopen(connect+"/connectors",timeout=3).status==200)
    cfg={"connector.class":"io.debezium.connector.postgresql.PostgresConnector","tasks.max":"1",
         "database.hostname":"source","database.port":"5432","database.dbname":"orders","database.user":"cdc_reader",
         "database.password":os.environ["READER_PASSWORD"],"topic.prefix":"wp15","table.include.list":"sales.orders",
         "plugin.name":"pgoutput","publication.name":"orders_pub","publication.autocreate.mode":"disabled",
         "slot.name":"wp15_orders","snapshot.mode":"initial","snapshot.fetch.size":"2","max.batch.size":"1",
         "snapshot.select.statement.overrides":"sales.orders",
         "snapshot.select.statement.overrides.sales.orders":
             "SELECT * FROM sales.orders WHERE order_id LIKE 'old-%' UNION ALL "
             "SELECT * FROM sales.orders WHERE order_id NOT LIKE 'old-%' AND wp15_snapshot_gate()","provide.transaction.metadata":"true",
         "tombstones.on.delete":"false","skipped.operations":"none","key.converter.schemas.enable":"false","value.converter.schemas.enable":"false",
         "key.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter"}
    req=Request(connect+"/connectors/wp15/config",method="PUT",data=json.dumps(cfg).encode(),headers={"Content-Type":"application/json"})
    with urlopen(req,timeout=30) as response: check("registered real Debezium connector",response.status in (200,201))

    with urlopen(connect+"/",timeout=5) as response:
        report["connect_version"]=json.load(response)["version"]
    report["postgres_version"]=source("SHOW server_version")
    topic="wp15.sales.orders"
    def kafka(tool,*args,stdin=None):
        return compose("exec","-T","kafka","/opt/kafka/bin/kafka-"+tool+".sh","--bootstrap-server","kafka:9092",*args,stdin=stdin)
    wait("snapshot reached order topic",lambda:topic in kafka("topics","--list").splitlines())
    compose("up","-d","worker")
    def staged():
        return sql("SELECT to_regclass('orders_cdc_checkpoint') IS NOT NULL")=="t" and sql(
            "SELECT CASE WHEN rows <> '{}'::jsonb THEN 1 ELSE 0 END FROM orders_cdc_checkpoint WHERE dataset_id='"+dataset+"'") not in ("","0")
    wait("incomplete snapshot checkpoint",staged)
    check("partial source snapshot invisible to readers",not versions())
    compose("kill","-s","SIGKILL","connect")
    source("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='cdc_reader' AND pid<>pg_backend_pid()")
    source("DELETE FROM sales.orders WHERE order_id LIKE 'old-%'; CREATE OR REPLACE FUNCTION wp15_snapshot_gate() RETURNS boolean LANGUAGE sql AS 'SELECT true'")
    compose("start","connect")
    wait("restarted source snapshot published",lambda:len(versions())==1)
    wait("new bootstrap checkpointed",lambda:sql("SELECT bootstrap_complete FROM orders_cdc_checkpoint WHERE dataset_id='"+dataset+"'")=="t")
    base=position()
    check("Connect restart rebuilt from a new snapshot",base>1)

    first,second=fixture.SAMPLES
    check("initial snapshot published as one complete version",len(versions())==1)
    pinned=read("independent reader sees both initial orders",first)
    build_orders(report["versions"][0],first)
    source("INSERT INTO sales.orders VALUES ('0023',9321,'second')")
    wait("insert applied",lambda:position()==base+1)
    read("insert visible at a new version",first+[second[1]])

    # A fixture-only checkpoint trigger waits after the real DVS commit. The session
    # remains in pg_sleep until the worker is killed; no application failure hook.
    sql("""CREATE FUNCTION wp15_pause() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_sleep(180); RETURN NEW; END $$;
        CREATE TRIGGER wp15_pause BEFORE UPDATE ON orders_cdc_checkpoint
        FOR EACH ROW EXECUTE FUNCTION wp15_pause()""")
    prior=len(versions())
    source("UPDATE sales.orders SET amount_cents=1742,batch='second' WHERE order_id='0007'")
    wait("new dataset version before local checkpoint",lambda:len(versions())==prior+1)
    check("checkpoint still precedes published update",position()==base+1)
    update_rows=[second[0],first[1],second[1]]
    update_version=read("published update already readable before interruption",update_rows)
    compose("kill","-s","SIGKILL","worker")
    sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='checkpoints' AND usename='worker' AND wait_event='PgSleep'")
    sql("DROP TRIGGER wp15_pause ON orders_cdc_checkpoint; DROP FUNCTION wp15_pause()")
    compose("start","worker")
    wait("worker resumed and checkpointed update",lambda:position()==base+2)
    check("post-publish crash replay kept the same dataset version",len(versions())==prior+1 and max(v["version"] for v in versions())==update_version)
    source("DELETE FROM sales.orders WHERE order_id='0011'")
    wait("delete applied",lambda:position()==base+3)
    final=read("delete removed only the intended order",second)
    build_orders(report["versions"][-1],second)
    build_orders(report["versions"][0],first)
    pinned_builds.verify_replay(STATE,p1,graph,reader,handoff,report["revision"])
    check("pinned initial version survives changes and restart",graph.scan_all(dataset,pinned)["rows"]==first)
    path=f"/v1/datasets/{dataset}/files/rows:scan?branch=main&version={final}&all_files=true"
    check("foreign tenant cannot scan CDC output",p1.http("dataset",path,token=foreign).status in (403,404))
    records=kafka("console-consumer","--topic",topic,"--partition","0","--offset","0","--max-messages",str(base+4),
                  "--property","print.key=true","--property","print.offset=true","--property","print.partition=true")
    parsed=[]
    for line in records.splitlines():
        partition,offset,key,raw=line.split("\t")
        event=json.loads(raw)
        parsed.append(dict(partition=int(partition.split(":")[1]),offset=int(offset.split(":")[1]),op=event["op"],
                           key=json.loads(key)["order_id"],lsn=event["source"]["lsn"]))
    report["events"]=parsed
    check("real WAL and Kafka coordinates captured for snapshot, insert, update and delete",
          [r["op"] for r in parsed[base-1:]]==["r","r","c","u","d"] and [r["offset"] for r in parsed]==list(range(base+4))
          and all(r["lsn"]>0 and r["partition"]==0 for r in parsed))
    check("published versions match their source WAL and Kafka coordinates",
          all((v["lsn"],v["order_id"],v["operation"])==(parsed[v["offset"]]["lsn"],parsed[v["offset"]]["key"],parsed[v["offset"]]["op"]) for v in report["versions"])
          and [v["offset"] for v in report["versions"]]==list(range(base,base+4)))
    initial=report["versions"][0]
    original=p1.download_file(reader,dataset,initial["file_id"])
    check("original file remains byte-identical",original.status==200 and hashlib.sha256(original.body).hexdigest()==initial["sha256"])
    # Re-deliver the actual UPDATE envelope at a new broker offset, after its original delete successor.
    key,raw=records.splitlines()[base+2].split("\t")[2:]
    kafka("console-producer","--topic",topic,"--property","parse.key=true",stdin=key+"\t"+raw+"\n")
    wait("duplicate delivery acknowledged",lambda:position()==base+4)
    check("late duplicate source event cannot change rows or create a version",
          max(v["version"] for v in versions())==final and graph.scan_all(dataset,final)["rows"]==second)
    def acknowledged():
        group=kafka("consumer-groups","--describe","--group","orders-cdc-"+dataset)
        return any(len(x)>5 and x[1:4]==[topic,"0",str(base+5)] for x in (line.split() for line in group.splitlines()))
    check("Kafka group committed through replay offset",wait("Kafka acknowledgment",acknowledged))
    report["final_group_next_offset"]=base+5
    source("TRUNCATE sales.orders")
    wait("unexpected operation stops worker",lambda:bool(compose("ps","--status","exited","-q","worker").strip()))
    check("unsupported truncate preserves offset and published data",position()==base+4 and
          max(v["version"] for v in versions())==final and graph.scan_all(dataset,final)["rows"]==second)
    fixture.write_json(STATE/"orders-report.json",report)

if __name__=="__main__":
    main()
