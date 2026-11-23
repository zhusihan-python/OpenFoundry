"""MySQL source fixture for the version-pinned orders consumer."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("table_orders", HERE.parent / "data-integration-wp2-5/orders.py")
suite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)
CONNECTOR = "mysql"
TABLE_FIELD = "mysql_table"
FAILURES = [
    ("disabled login", "ALTER USER 'orders_reader'@'%' ACCOUNT LOCK", "source connection failed"),
    ("row limit", """ALTER USER 'orders_reader'@'%' ACCOUNT UNLOCK;
        INSERT INTO sales.orders WITH RECURSIVE seq(n) AS
        (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<10001)
        SELECT /*+ SET_VAR(cte_max_recursion_depth=11000) */ CONCAT('bulk-',n),1,'bulk' FROM seq""", "row or byte limit"),
    ("oversized row", "TRUNCATE sales.orders; INSERT INTO sales.orders VALUES ('too-large',1,REPEAT('x',8388608))", "row or byte limit"),
    ("missing primary key", "TRUNCATE sales.orders; ALTER TABLE sales.orders DROP PRIMARY KEY", "requires a primary key"),
    ("nontransactional table", "ALTER TABLE sales.orders ENGINE=MyISAM", "requires an InnoDB base table"),
    ("missing table", "DROP TABLE sales.orders", "unavailable or unreadable"),
]


def sql(statement, user="root", *, denied=False, sslmode="verify-full"):
    passwords = {"root": "WP25_ADMIN_PASSWORD", "orders_reader": "WP25_READER_PASSWORD",
                 "other_reader": "WP25_OTHER_PASSWORD"}
    env = dict(os.environ, MYSQL_PWD=os.environ[passwords[user]])
    cmd = ["docker", "exec", "-i", "-e", "MYSQL_PWD", os.environ["WP25_DB_CONTAINER"],
           "mysql", "--batch", "--raw", "--skip-column-names", "--default-character-set=utf8mb4", "-u", user]
    if user != "root":
        mode = "DISABLED" if sslmode == "disable" else "VERIFY_IDENTITY"
        cmd.extend(["-h", "orders-db", "--ssl-mode="+mode, "--ssl-ca=/fixture-tls/server.crt"])
    result = subprocess.run(cmd, input=statement, capture_output=True, text=True, env=env, timeout=40)
    if sslmode == "disable":
        return result.returncode != 0 and any(code in result.stderr for code in ("ERROR 3159", "ERROR 1045", "ERROR 2061"))
    if denied:
        return result.returncode != 0 and "denied" in result.stderr.lower()
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def verified_tls():
    return (sql("SELECT @@global.require_secure_transport") == "1" and
            sql("SHOW SESSION STATUS LIKE 'Ssl_cipher'", "orders_reader").startswith("Ssl_cipher\tTLS"))


def replace_orders(records):
    encoded = json.dumps(records).encode().hex()
    sql(f"""START TRANSACTION; DELETE FROM sales.orders;
        INSERT INTO sales.orders SELECT * FROM JSON_TABLE(CONVERT(UNHEX('{encoded}') USING utf8mb4),
        '$[*]' COLUMNS (order_id VARCHAR(64) PATH '$.order_id', amount_cents BIGINT PATH '$.amount_cents',
        batch TEXT PATH '$.batch')) AS orders; COMMIT""")


def setup():
    # The runner generates fresh hexadecimal passwords for these two fixture users.
    for role, var in [("orders_reader","WP25_READER_PASSWORD"), ("other_reader","WP25_OTHER_PASSWORD")]:
        password = os.environ[var]
        if not password.isalnum():
            raise ValueError("unexpected fixture password format")
        sql(f"CREATE USER '{role}'@'%' IDENTIFIED BY '{password}' REQUIRE SSL")
    sql("""CREATE TABLE sales.orders (order_id VARCHAR(64) PRIMARY KEY, amount_cents BIGINT, batch LONGTEXT) ENGINE=InnoDB;
        CREATE TABLE sales.regional_orders (region VARCHAR(64), order_id VARCHAR(64), exact_total DECIMAL(30,5),
        memo TEXT, PRIMARY KEY(region,order_id)) ENGINE=InnoDB PARTITION BY LIST COLUMNS(region)
        (PARTITION p_east VALUES IN ('east'), PARTITION p_west VALUES IN ('west'));
        INSERT INTO sales.regional_orders VALUES ('west','0011',NULL,NULL),('east','0007',12345678901234567890.01230,'订单 · café');
        GRANT SELECT ON sales.orders TO 'orders_reader'@'%';
        GRANT SELECT ON sales.regional_orders TO 'orders_reader'@'%'""")


if __name__ == "__main__":
    setup()
    suite.main(Path(sys.argv[1]), sys.modules[__name__])
