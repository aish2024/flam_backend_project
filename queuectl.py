#!/usr/bin/env python3
"""
queuectl - A minimal, production-grade background job queue with CLI, workers,
retries (exponential backoff), and DLQ — all persisted in SQLite.

Python 3.10+
Stdlib only (no external deps) for easy run.
"""
from __future__ import annotations
import argparse
import contextlib
import datetime as dt
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from typing import Any, Dict, Optional, Tuple

APP_NAME = "queuectl"
DB_FILE = os.environ.get("QUEUECTL_DB", os.path.join(os.path.dirname(__file__), "queue.db"))
HEARTBEAT_SECS = 5

def utcnow() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

def parse_iso8601(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))

def ensure_db():
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('pending','processing','completed','failed','dead')),
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_error TEXT,
            next_run_at TEXT,
            locked_by TEXT,
            locked_at TEXT,
            priority INTEGER NOT NULL DEFAULT 0,
            return_code INTEGER,
            stdout TEXT,
            stderr TEXT,
            duration_ms INTEGER
        )
        """)
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_state_next ON jobs(state, next_run_at)
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """)
        # defaults
        defaults = {
            "backoff_base": "2",
            "max_retries_default": "3",
            "job_timeout_sec": "0"  # 0 = no timeout
        }
        for k, v in defaults.items():
            conn.execute("INSERT OR IGNORE INTO config(key,value) VALUES(?,?)", (k, v))
        conn.execute("""
        CREATE TABLE IF NOT EXISTS workers(
            worker_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('starting','idle','busy','stopping','stopped')),
            current_job_id TEXT
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS control(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """)
        conn.execute("INSERT OR IGNORE INTO control(key,value) VALUES('shutdown','0')")

def get_config(conn, key: str) -> str:
    cur = conn.execute("SELECT value FROM config WHERE key=?", (key,))
    row = cur.fetchone()
    if not row:
        raise KeyError(key)
    return row[0]

def set_config(key: str, value: str):
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        conn.execute("INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

def print_json(data):
    sys.stdout.write(json.dumps(data, indent=2) + "\n")

def enqueue_job(job_json: str, priority: int = 0):
    ensure_db()
    job = json.loads(job_json)
    now = utcnow()
    job.setdefault("id", str(uuid.uuid4()))
    job.setdefault("state", "pending")
    job.setdefault("attempts", 0)
    job.setdefault("max_retries", 3)
    job.setdefault("created_at", now)
    job.setdefault("updated_at", now)
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        conn.execute(
            "INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, priority) VALUES(?,?,?,?,?,?,?,?)",
            (job["id"], job["command"], job["state"], job["attempts"], job["max_retries"], job["created_at"], job["updated_at"], priority)
        )
    print(f"Enqueued job {job['id']}")

def list_jobs(state: Optional[str] = None, limit: int = 100):
    ensure_db()
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        if state:
            cur = conn.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at LIMIT ?", (state, limit))
        else:
            cur = conn.execute("SELECT * FROM jobs ORDER BY created_at LIMIT ?", (limit,))
        rows = [dict(r) for r in cur.fetchall()]
    print_json(rows)

def status():
    ensure_db()
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        counts = {}
        for st in ['pending','processing','completed','failed','dead']:
            c = conn.execute("SELECT COUNT(*) FROM jobs WHERE state=?", (st,)).fetchone()[0]
            counts[st] = c
        w = [dict(r) for r in conn.execute("SELECT * FROM workers ORDER BY started_at").fetchall()]
        shutdown = conn.execute("SELECT value FROM control WHERE key='shutdown'").fetchone()[0]
    print_json({"jobs": counts, "workers": w, "shutdown": shutdown})

def dlq_list(limit: int = 100):
    ensure_db()
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM jobs WHERE state='dead' ORDER BY updated_at DESC LIMIT ?", (limit,))
        rows = [dict(r) for r in cur.fetchall()]
    print_json(rows)

def dlq_retry(job_id: str):
    ensure_db()
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        now = utcnow()
        cur = conn.execute("SELECT id FROM jobs WHERE id=? AND state='dead'", (job_id,))
        if not cur.fetchone():
            print(f"Job {job_id} not found in DLQ.", file=sys.stderr)
            sys.exit(1)
        conn.execute("""
            UPDATE jobs
               SET state='pending', attempts=0, next_run_at=NULL, last_error=NULL, updated_at=?
             WHERE id=?
        """, (now, job_id))
    print(f"Moved job {job_id} from DLQ to pending.")

def acquire_job(conn: sqlite3.Connection, worker_id: str) -> Optional[Dict[str,Any]]:
    # Try to atomically claim one eligible job
    conn.row_factory = sqlite3.Row
    now = utcnow()
    # Use an IMMEDIATE transaction to lock database for writes and prevent races.
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("""
            SELECT id FROM jobs
             WHERE state IN ('pending','failed')
               AND (next_run_at IS NULL OR next_run_at <= ?)
             ORDER BY priority DESC, created_at
             LIMIT 1
        """, (now,)).fetchone()
        if not row:
            conn.execute("COMMIT")
            return None
        job_id = row["id"]
        # Double-check state is still claimable and move to processing
        up = conn.execute("""
            UPDATE jobs
               SET state='processing', locked_by=?, locked_at=?, updated_at=?
             WHERE id=? AND state IN ('pending','failed')
        """, (worker_id, now, now, job_id))
        if up.rowcount != 1:
            conn.execute("ROLLBACK")
            return None
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        conn.execute("COMMIT")
        return dict(job)
    except Exception:
        conn.execute("ROLLBACK")
        raise

def compute_backoff(base: int, attempts: int) -> int:
    # attempts is the number already attempted (before incrementing for this failure)
    # Next delay = base ** attempts
    try:
        return int(base) ** int(max(attempts,0))
    except Exception:
        return 2 ** int(max(attempts,0))

def run_job(conn: sqlite3.Connection, worker_id: str, job: Dict[str,Any]) -> None:
    # Update worker status to busy with this job
    now = utcnow()
    conn.execute("UPDATE workers SET status='busy', current_job_id=?, last_seen_at=? WHERE worker_id=?", (job["id"], now, worker_id))
    # Load configs
    backoff_base = int(get_config(conn, "backoff_base"))
    job_timeout = int(get_config(conn, "job_timeout_sec"))
    # Execute command
    start = time.time()
    try:
        completed = subprocess.run(job["command"], shell=True, capture_output=True, text=True, timeout=None if job_timeout==0 else job_timeout)
        rc = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
    except subprocess.TimeoutExpired as e:
        rc = -1
        stdout = e.stdout or ""
        stderr = (e.stderr or "") + f"TIMEOUT after {job_timeout}s"
    duration_ms = int((time.time() - start)*1000)
    now2 = utcnow()
    if rc == 0:
        conn.execute("""
            UPDATE jobs SET state='completed', attempts=attempts, return_code=?, stdout=?, stderr=?, duration_ms=?, updated_at=?, locked_by=NULL, locked_at=NULL
             WHERE id=?
        """, (rc, stdout, stderr, duration_ms, now2, job["id"]))
    else:
        # failure path: increment attempts, schedule retry or DLQ
        cur = conn.execute("SELECT attempts, max_retries FROM jobs WHERE id=?", (job["id"],)).fetchone()
        attempts = int(cur[0]) + 1
        max_retries = int(cur[1])
        if attempts > max_retries:
            # DLQ
            conn.execute("""
                UPDATE jobs SET state='dead', attempts=?, last_error=?, return_code=?, stdout=?, stderr=?, duration_ms=?, updated_at=?, locked_by=NULL, locked_at=NULL
                 WHERE id=?
            """, (attempts, f"Exit code {rc}", rc, stdout, stderr, duration_ms, now2, job["id"]))
        else:
            delay = compute_backoff(backoff_base, attempts)  # base ** attempts
            next_run = dt.datetime.utcnow() + dt.timedelta(seconds=delay)
            next_run_iso = next_run.replace(tzinfo=dt.timezone.utc).isoformat()
            conn.execute("""
                UPDATE jobs SET state='failed', attempts=?, last_error=?, return_code=?, stdout=?, stderr=?, duration_ms=?, next_run_at=?, updated_at=?, locked_by=NULL, locked_at=NULL
                 WHERE id=?
            """, (attempts, f"Exit code {rc}", rc, stdout, stderr, duration_ms, next_run_iso, now2, job["id"]))
    # Back to idle
    conn.execute("UPDATE workers SET status='idle', current_job_id=NULL, last_seen_at=? WHERE worker_id=?", (utcnow(), worker_id))

def worker_loop(worker_id: str):
    ensure_db()
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        conn.execute("INSERT OR REPLACE INTO workers(worker_id, started_at, last_seen_at, status, current_job_id) VALUES(?,?,?,?,NULL)",
                     (worker_id, utcnow(), utcnow(), "starting"))
    stop = False

    def handle_signal(signum, frame):
        # mark stopping flag in db; the main loop will exit after current job
        with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as c2:
            c2.execute("UPDATE workers SET status='stopping', last_seen_at=? WHERE worker_id=?", (utcnow(), worker_id))

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    # Main poll loop
    idle_sleep = 1.0
    while True:
        with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            # Check global shutdown or worker stopping
            shutdown = conn.execute("SELECT value FROM control WHERE key='shutdown'").fetchone()[0]
            wstatus = conn.execute("SELECT status FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
            cur_status = wstatus[0] if wstatus else "stopped"
            if cur_status == "stopping" or shutdown == "1":
                # exit gracefully if not processing
                conn.execute("UPDATE workers SET status='stopped', last_seen_at=? WHERE worker_id=?", (utcnow(), worker_id))
                break
            # Heartbeat
            conn.execute("UPDATE workers SET status='idle', last_seen_at=? WHERE worker_id=?", (utcnow(), worker_id))
            # Try acquire a job
            job = acquire_job(conn, worker_id)
            if not job:
                time.sleep(idle_sleep)
                continue
            # Run job
            run_job(conn, worker_id, job)
            # loop continues

def spawn_workers(count: int):
    ensure_db()
    # Clear shutdown flag and ensure workers table
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        conn.execute("UPDATE control SET value='0' WHERE key='shutdown'")
    procs = []
    for i in range(count):
        worker_id = f"{os.getpid()}-{i}-{uuid.uuid4().hex[:8]}"
        # spawn new python process running this file with internal _worker mode
        p = subprocess.Popen([sys.executable, os.path.abspath(__file__), "_internal", "worker", "--id", worker_id])
        procs.append((worker_id, p))
        print(f"Started worker {worker_id} (pid={p.pid})")
    # Write supervisor pidfile to terminate later
    with open(os.path.join(os.path.dirname(DB_FILE), ".workers.supervisor.pid"), "w") as f:
        f.write(str(os.getpid()))
    with open(os.path.join(os.path.dirname(DB_FILE), ".workers.pids"), "w") as f:
        for wid, p in procs:
            f.write(f"{wid},{p.pid}\n")
    # Wait here so CLI doesn't exit immediately? We'll detach after starting all workers.
    # Detach by not waiting; just return.
    return

def stop_workers():
    ensure_db()
    with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
        conn.execute("UPDATE control SET value='1' WHERE key='shutdown'")
    # Also try to send SIGTERM to child pids if we have them
    pids_path = os.path.join(os.path.dirname(DB_FILE), ".workers.pids")
    if os.path.exists(pids_path):
        with open(pids_path) as f:
            lines = [l.strip() for l in f if l.strip()]
        for line in lines:
            try:
                _, pid_s = line.split(",", 1)
                pid = int(pid_s)
                with contextlib.suppress(Exception):
                    os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
    print("Signaled workers to stop. They will finish current jobs then exit.")

def config_cmd(args):
    if args.action == "set":
        set_config(args.key, args.value)
        print(f"Config set {args.key}={args.value}")
    elif args.action == "get":
        ensure_db()
        with sqlite3.connect(DB_FILE, isolation_level=None, timeout=30) as conn:
            if args.key:
                try:
                    val = get_config(conn, args.key)
                    print_json({args.key: val})
                except KeyError:
                    print_json({args.key: None})
            else:
                conn.row_factory = sqlite3.Row
                rows = conn.execute("SELECT key, value FROM config ORDER BY key").fetchall()
                print_json({r["key"]: r["value"] for r in rows})

def main():
    parser = argparse.ArgumentParser(prog="queuectl", description="CLI job queue with workers, retries, and DLQ (SQLite).")
    parser.add_argument("--db", help="Path to SQLite DB (default: queue.db next to script).", default=None)
    sub = parser.add_subparsers(dest="cmd")

    # enqueue
    p_enq = sub.add_parser("enqueue", help="Enqueue a job. Pass JSON string or use --file.")
    p_enq.add_argument("json", nargs="?", help="Job JSON, e.g. '{\"id\":\"job1\",\"command\":\"echo hi\"}'")
    p_enq.add_argument("--file", help="Path to JSON file containing job definition.")
    p_enq.add_argument("--priority", type=int, default=0, help="Job priority (higher first).")
    # worker
    p_w = sub.add_parser("worker", help="Manage workers")
    wsub = p_w.add_subparsers(dest="wcmd")
    p_ws = wsub.add_parser("start", help="Start one or more workers")
    p_ws.add_argument("--count", type=int, default=1)
    p_wst = wsub.add_parser("stop", help="Gracefully stop running workers")
    # status
    p_status = sub.add_parser("status", help="Show summary of job states and active workers")
    # list
    p_list = sub.add_parser("list", help="List jobs")
    p_list.add_argument("--state", choices=['pending','processing','completed','failed','dead'], help="Filter by state")
    p_list.add_argument("--limit", type=int, default=100)
    # dlq
    p_dlq = sub.add_parser("dlq", help="Dead Letter Queue operations")
    dsub = p_dlq.add_subparsers(dest="dcmd")
    d_list = dsub.add_parser("list", help="List DLQ jobs")
    d_list.add_argument("--limit", type=int, default=100)
    d_retry = dsub.add_parser("retry", help="Retry a DLQ job by id")
    d_retry.add_argument("job_id")
    # config
    p_cfg = sub.add_parser("config", help="Manage configuration")
    cfgsub = p_cfg.add_subparsers(dest="action")
    cset = cfgsub.add_parser("set")
    cset.add_argument("key")
    cset.add_argument("value")
    cget = cfgsub.add_parser("get")
    cget.add_argument("key", nargs="?")
    # internal worker entry
    p_internal = sub.add_parser("_internal", help=argparse.SUPPRESS)
    isub = p_internal.add_subparsers(dest="icmd")
    iw = isub.add_parser("worker", help=argparse.SUPPRESS)
    iw.add_argument("--id", required=True)

    args = parser.parse_args()

    # override DB if provided
    global DB_FILE
    if args.db:
        DB_FILE = args.db
    ensure_db()

    if args.cmd == "enqueue":
        if not args.json and not args.file:
            print("Provide job JSON or --file.", file=sys.stderr)
            sys.exit(1)
        data = args.json
        if args.file:
            with open(args.file) as f:
                data = f.read()
        enqueue_job(data, priority=args.priority)
    elif args.cmd == "worker":
        if args.wcmd == "start":
            spawn_workers(args.count)
        elif args.wcmd == "stop":
            stop_workers()
        else:
            print("Specify 'start' or 'stop'", file=sys.stderr)
            sys.exit(1)
    elif args.cmd == "status":
        status()
    elif args.cmd == "list":
        list_jobs(state=args.state, limit=args.limit)
    elif args.cmd == "dlq":
        if args.dcmd == "list":
            dlq_list(limit=args.limit)
        elif args.dcmd == "retry":
            dlq_retry(args.job_id)
        else:
            print("Specify 'list' or 'retry'", file=sys.stderr)
            sys.exit(1)
    elif args.cmd == "config":
        config_cmd(args)
    elif args.cmd == "_internal" and args.icmd == "worker":
        worker_loop(args.id)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
