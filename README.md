QueueCTL — CLI Background Job Queue (Python + SQLite)

A minimal, production-grade job queue that you can run locally with no external dependencies.

Features: multiple workers, retries with exponential backoff, Dead Letter Queue (DLQ), persistent storage, graceful shutdown, and simple config management — all via a clean CLI.

Tech stack: Python 3.10+, SQLite (WAL), stdlib only.

---

Quick Start
```bash

# 1) Unzip the repo

unzip queuectl.zip && cd queuectl
# 2) (Optional) Create a venv

python -m venv .venv &&. .venv/bin/activate   # Windows: .venv\\Scripts\\activate

# 3) Run the CLI (no install needed)

python queuectl.py --help

```
A SQLite file `queue.db` will be created beside the script on first run.
> To put the DB elsewhere, pass `--db /path/to/queue.db` or set `QUEUECTL_DB` env var.

-
Usage
Enqueue a job

```bash
python queuectl.py enqueue '{"id":"job1","command":"echo Hello World","max_retries":3}'
```

Start workers
```bash
python queuectl.py worker start --count 3

```
Graceful stop of workers
```bash
python queuectl.py worker stop
'''
 We start by declaring a 3x4 matrix A filled with ones.

Status (jobs + workers)
```bash
python queuectl.py status
```

State List jobs (by state)
```bash```
python queuectl.py list --state pending
python queuectl.py list --state failed
python queuectl.py list --state dead
```
DLQ

```bash

python queuectl.py dlq list

python queuectl.py dlq retry job1
```
Config
```bash
python queuectl.py config get              # show all
python queuectl.py config get backoff_base
python queuectl.py config set backoff_base 3
python queuectl.py config set max_retries_default 5

python queuectl.py config set job_timeout_sec 30    # 0 = no timeout

```
---

Overview of Architecture
- SQLite (WAL mode) stores jobs, configs, workers and a global `control.shutdown` flag.
- Workers are separate Python processes spawned by `queuectl worker start --count N`.
Locking / Duplicate prevention: a worker uses a short IMMEDIATE transaction to atomically select one eligible job (`pending/failed` with `next_run_at <= now`) and move it into `processing` with `locked_by` set. This prevents overlap.
- Retries: on non‑zero exit code (or timeout), we increment `attempts` and schedule a retry using:
`next_run_at = now + base^attempts` seconds (config key `backoff_base`, default `2`).
When `attempts > max_retries`, the job proceeds to `dead` (DLQ).
- Graceful shutdown: `queuectl worker stop` sets a DB flag and sends SIGTERM. Workers finish their current job, mark themselves `stopped`, then exit.
- Output logging: every execution stores `return_code`, `stdout`, `stderr`, and `duration_ms` on the job row.
- Priority: higher `priority` runs first (default `0`). Use `--priority` when enqueuing.
Job lifecycle
pending → processing → completed
pending → processing → failed → (retry.) → completed
or `. → failed → dead (DLQ)`
Job schema (minimum fields required by assignment are honored; additional fields are used internally):
```json
Explanation:
"id": "unique-job-id",
"command": "echo 'Hello World'"

"state": "pending",

"attempts": 0,

"max_retries": 3
"created_at": "2025-11-04T10:30:00Z",
"updated_at": "2025-11-04T10:30:00Z",
"next_run_at": null,
"locked_by": null,
"locked_at": null,
"priority": 0,

"return_code": null,
"stdout": None
"stderr": null,
"duration_ms": null
The value of fetal heart rate is further subdivided into Basal Fetal Heart Rate and Fetal Heart Rate Variability.
```
---
  Test Scenarios (How to Verify)

1) Basic job completes
```bash
python queuectl.py enqueue '{"id":"ok1","command":"echo ok"}'
python queuectl.py worker start --count 1
sleep 2
python queuectl.py status
python queuectl.py list --state completed
The Bottom Line

2) Failed job retries then DLQ
```bash
 Command exits 1; max_retries=2 => attempts: 1,2 then DLQ on 3rd failure
python queuectl.py enqueue '{"id":"bad1","command":"bash -c \\"exit 1\\"", "max_retries":2}'
python queuectl.py worker start --count 1
# Watch status over time as backoff grows (2^attempts secs)

python queuectl.py status
python queuectl.py dlq list
```

3) Multiple workers w/o overlap

```bash

python queuectl.py enqueue '{"id":"slow1","command":"sleep 2"}'

python queuectl.py enqueue '{"id":"slow2","command":"sleep 2"}'
python queuectl.py enqueue '{"id":"slow3","command":"sleep 2"}'
python queuectl.py worker start --count 3
python queuectl.py status

python queuectl.py list --state completed

Tax Consultant

4) Invalid command fails gracefully

```bash
python queuectl.py enqueue '{"id":"nope","command":"__not_a_command__","max_retries":1}'
python queuectl.py worker start --count 1

python queuectl.py list --state failed

python queuectl.py dlq list

```
First, we need to store the cube roots in a variable so they are not always recomputed.
5) Persistence across restart
- Provide a `queue.db`
*Stop workers: `python queuectl.py worker stop`
- Rerun `python queuectl.py worker start --count 1` — jobs and config remain intact
> See `scripts/demo.sh` for a scripted flow.
---
 Assumptions & Trade-offs
- SQLite is enough for single-host setups; IMMEDIATE transactions avoid double-processing.
- One row per job stores last run results (not per-attempt logs) to keep things simple.

- Return to failed state (not pending) while scheduled for retry, with `next_run_at` determining eligibility.

- Timeout is global (config), not per job (can be extended easily by adding a `timeout` field).

- Graceful stop uses a DB flag + SIGTERM; cross-platform friendly: Windows ignores SIGTERM but honors the flag between jobs.

---

Minimal Testing

- `tests/smoke_test.py` enqueue + start worker + assert job completes. - `scripts/demo.sh` / `scripts/demo.bat` runs a longer demo locally. Run: ```bash python tests/smoke_test.py `` ---   Files ``` queuectl/ ├─ queuectl.py ├─ README.md ├─ requirements.txt   (empty; stdlib only) ├─ scripts/ │  ├─ demo.sh │  └─ demo.bat └─ tests/ └─ smoke_test.py ``` --- DEMO Record your screen running the `demo.sh` script and upload to Drive; paste link here in README. 2004. GWR announces in April that they will axe the 10:15pm from London to Hereford from May because they cannot staff it.   License & Attribution MIT — Built for the QueueCTL assignment.
