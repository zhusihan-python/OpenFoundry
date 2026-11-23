"""Shared WP8.3 driver helpers: writer access, timing markers, quarantine."""

import json
from pathlib import Path
import re
import subprocess
import sys
from tempfile import NamedTemporaryFile
import time
from uuid import UUID


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.with_name("data-integration-wp3-13")))
import acceptance as writer

stack = writer.stack
expect = writer.expect
MISSING = "dataset_file_bytes_missing: the versioned file has no readable backing bytes at its registered storage path"
BACKING = "/var/of/backing"


def unix_ms():
    return time.time_ns() // 1_000_000


def atomic_json(path, data):
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as pending:
        pending_path = Path(pending.name)
        try:
            json.dump(data, pending, indent=2, sort_keys=True)
            pending.write("\n")
            pending.close()
            pending_path.replace(path)
        finally:
            pending_path.unlink(missing_ok=True)


def wait_for(label, read, deadline):
    while time.monotonic() < deadline:
        value = read()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {label}")


def get(url):
    status, body = stack.http("GET", url, timeout=5, allow=(200,))
    expect("HTTP GET", status, 200)
    return body


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=10, check=True).stdout.strip()


def owned_dvs():
    project = stack.PROJECT
    expect("WP8.3 Compose project name", bool(re.fullmatch(r"of-wp33c-wp83-[a-z0-9-]+", project)), True)
    name = f"{project}-dvs"
    identity, instance, compose_project, service, raw_mounts = docker(
        "inspect", "--type", "container", "--format",
        '{{.Id}}\n{{.Name}}\n{{index .Config.Labels "com.docker.compose.project"}}\n'
        '{{index .Config.Labels "com.docker.compose.service"}}\n{{json .Mounts}}', name,
    ).splitlines()
    expect("owned DVS Compose container", (instance, compose_project, service), (f"/{name}", project, "dvs"))
    expect("immutable DVS container ID", bool(re.fullmatch(r"[a-f0-9]{64}", identity)), True)
    mount, = [item for item in json.loads(raw_mounts)
              if item["Destination"] == "/var/of" or item["Destination"].startswith("/var/of/")]
    volume = f"{project}_dvs-data"
    expect("owned writable DVS fixture volume",
           (mount["Type"], mount.get("Name"), mount["Destination"], mount["RW"]),
           ("volume", volume, "/var/of", True))
    labels = json.loads(docker("volume", "inspect", "--format", "{{json .Labels}}", volume))
    expect("owned DVS volume labels",
           (labels["com.docker.compose.project"], labels["com.docker.compose.volume"]), (project, "dvs-data"))
    return identity


def scoped_object(old, baseline):
    dataset, transaction = (str(UUID(old[key])) for key in ("dataset", "transaction"))
    expect("fixture uses canonical dataset and transaction IDs", (old["dataset"], old["transaction"]),
           (dataset, transaction))
    projected, = [file for file in baseline["files"] if file["logical_path"] == "a.parquet"]
    key = f"datasets/{dataset}/transactions/{transaction}/a.parquet"
    expect("the target is the exact historical fixture object",
           (projected["dataset_id"], projected["transaction_id"], projected["physical_uri"]),
           (dataset, transaction, f"local:///{key}"))
    source = f"{BACKING}/{key}"
    expect("backing inventory contains the selected object", source in baseline["backing_files"], True)
    signed, = [file for file in baseline["signed_files"] if file["id"] == projected["id"]]
    return source, {**signed, "dataset": dataset, "physical_uri": projected["physical_uri"]}


def move_to_quarantine(container, source, directory):
    quarantine = f"{directory}/a.parquet"
    docker("exec", container, "sh", "-ec", """
        test -f "$1"
        test ! -L "$1"
        test "$(realpath "$1")" = "$1"
        test -d "$2"
        test "$(realpath "$2")" = "$2"
        test "$(stat -c %a "$2")" = 700
        test "$(stat -c %d "$1")" = "$(stat -c %d "$2")"
        test ! -e "$3"
        test ! -L "$3"
        mv -n "$1" "$3"
        test ! -e "$1"
        test ! -L "$1"
        test -f "$3"
        test ! -L "$3"
    """, "quarantine", source, directory, quarantine)


def restore_from_quarantine(container, source, directory):
    quarantine = f"{directory}/a.parquet"
    docker("exec", container, "sh", "-ec", """
        test -d "$2"
        test "$(realpath "$2")" = "$2"
        test ! -L "$3"
        if test -e "$3"; then
            test -f "$3"
            test "$(realpath "$3")" = "$3"
            test ! -e "$1"
            test ! -L "$1"
            test "$(realpath "$(dirname "$1")")" = "$(dirname "$1")"
            mv -n "$3" "$1"
        fi
        test -f "$1"
        test ! -L "$1"
        test "$(realpath "$1")" = "$1"
        test ! -e "$3"
        test ! -L "$3"
        rmdir "$2"
    """, "restore", source, directory, quarantine)
