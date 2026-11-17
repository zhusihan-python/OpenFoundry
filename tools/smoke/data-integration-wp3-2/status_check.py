#!/usr/bin/env python3
"""Strict Kafka Connect /status gate (root review round 5).

Replaces the pod-side grep -c approach, which counted LINES — the
status body is one compact JSON line, so running_states/total_states
were both 1 forever and the >=2 gate could never pass. This parser
loads the RAW body as JSON and requires:

  * connector.state == "RUNNING"
  * tasks is a non-empty list
  * every tasks[i].state == "RUNNING"

Exit 0 only when all hold; the parsed summary prints on stdout and the
raw body is preserved by the caller as evidence.

Usage: status_check.py <raw-status.json>
"""
import json
import sys


def main() -> int:
    with open(sys.argv[1]) as fh:
        raw = fh.read()
    d = json.loads(raw)
    conn = d.get("connector", {})
    tasks = d.get("tasks", [])
    conn_state = conn.get("state")
    if not isinstance(tasks, list):
        print(f"connector={conn_state} tasks=NOT-A-LIST")
        return 1
    task_states = [t.get("state") for t in tasks]
    ok = (conn_state == "RUNNING" and len(tasks) >= 1
          and all(s == "RUNNING" for s in task_states))
    print(f"connector={conn_state} tasks={len(tasks)} "
          f"task_states={task_states} verdict={'OK' if ok else 'NOT-READY'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
