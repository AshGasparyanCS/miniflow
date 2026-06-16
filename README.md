# miniflow — a mini-Airflow DAG scheduler

A job-scheduling system in the spirit of Airflow: define a DAG of tasks with
dependencies, and miniflow runs them in dependency order, in parallel where it
can, with per-task retries and failure handling. Run state is persisted to a
database, and a live web UI shows every run and task as it progresses.

Built with **FastAPI** + **SQLAlchemy** and a **custom worker pool** (no Celery,
no external broker — the scheduling logic is the interesting part, so it's
implemented directly). Persists to **PostgreSQL** in production; uses SQLite for
local dev and tests with no code changes.

## Why a custom worker pool

The core design choice is a clean split between *deciding* and *doing*:

```
            +-----------------------------------------------+
 submit --> |              Scheduler thread                 |
 (FastAPI)  |  the ONLY writer of run/task state            |
            |                                               |
            |  each tick, per running DAG:                  |
            |   1. drain finished results -> DB             |
            |   2. cascade upstream failures               |
            |   3. dispatch ready tasks ----------------+   |
            |   4. mark run success/failed              |   |
            +-------------------------------------------|---+
                         ^  results queue               |  submit(command)
                         |  (thread-safe)               v
            +------------+-------------------------------+--+
            |              Worker pool (N threads)          |
            |  ONLY runs task commands; touches no DB       |
            +-----------------------------------------------+
                         |
                         v   state in PostgreSQL / SQLite
                 +---------------------------+
                 | dag_runs | task_instances |
                 +---------------------------+
```

Because exactly one thread ever writes state, there are **no database races** —
the scheduler is the brain, the pool is the muscle. This is also how real
schedulers separate the scheduling loop from executors.

### Task lifecycle

```
pending ──(all upstreams success)──> running ──exit 0──> success
   │                                    │
   │                                    └─exit≠0─> up_for_retry ──(backoff)──> running
   │                                                    │ (retries exhausted)
   │                                                    v
   └──(any upstream failed)──> upstream_failed       failed ──> cascades to
                                                                downstream tasks
```

## DAG definition format

A DAG is JSON: a `dag_id` and a list of tasks. Each task has a shell `command`,
optional `upstreams`, and optional `retries` / `retry_delay`.

```json
{
  "dag_id": "etl",
  "tasks": [
    { "task_id": "extract",   "command": "echo extract",   "retries": 2, "retry_delay": 5 },
    { "task_id": "transform", "command": "echo transform", "upstreams": ["extract"] },
    { "task_id": "load",      "command": "echo load",      "upstreams": ["transform"] }
  ]
}
```

A task succeeds when its command exits 0. Before running, miniflow validates the
DAG: unique task ids, all referenced upstreams exist, and the graph is acyclic
(a cycle is rejected with a clear error).

## API

| Method & path | Purpose |
|---|---|
| `POST /api/runs` | Submit a DAG definition; returns `{run_id}`. |
| `GET /api/runs` | List recent runs with per-state task counts. |
| `GET /api/runs/{id}` | One run with every task's state, attempt count, and logs. |
| `GET /api/examples` | Built-in example DAGs. |
| `POST /api/examples/{name}` | Trigger an example run. |
| `GET /` | Live dashboard. |

## Run it

### Local (SQLite, zero infra)

```bash
pip install -r requirements.txt
python -m miniflow                       # http://localhost:8000
```

Open the dashboard, click an example DAG (▶ `diamond_parallel`,
`retry_then_pass`, `fail_cascade`, `linear_etl`) and watch tasks move through
their states live. Or drive it from the API:

```bash
curl -X POST localhost:8000/api/runs -H 'content-type: application/json' -d '{
  "dag_id":"demo",
  "tasks":[
    {"task_id":"a","command":"echo hi"},
    {"task_id":"b","command":"echo bye","upstreams":["a"]}
  ]}'
```

### Docker (PostgreSQL, production-like)

```bash
docker compose up --build           # Postgres + app, UI on :8000
```

The app reads `DATABASE_URL`; compose points it at the Postgres service. Nothing
else changes between SQLite and Postgres — that's the SQLAlchemy layer doing its
job.

## Examples (what each demonstrates)

| Example | Shows |
|---|---|
| `linear_etl` | Strict dependency ordering: extract → transform → load. |
| `diamond_parallel` | Two independent branches run **concurrently**, then join. |
| `retry_then_pass` | A flaky task fails, retries with backoff, then succeeds. |
| `fail_cascade` | A task exhausts retries and **fails**; its downstream is marked `upstream_failed`, while an independent branch still succeeds. |

## Tests

```bash
pytest -q     # 15 tests
```

| Area | Checks |
|---|---|
| `test_dag` | Topological order, cycle rejection, bad/duplicate/self deps, downstream closure. |
| `test_executor` | Dependency ordering (downstream starts only after upstream finishes), parallel branches overlap, retry-then-success, failure cascade with attempt counts. |
| `test_api` | Submit + poll to completion, example trigger, validation 400s, run listing. |

Tests run against a temp SQLite DB and exercise the real scheduler thread + worker pool.

## Project layout

```
miniflow/
  db.py         engine/session (SQLite + Postgres)
  models.py     DagRun + TaskInstance ORM, state constants
  dag.py        DAG dataclasses, validation, topo sort, cycle detection
  executor.py   scheduler thread + worker pool (the core)
  api.py        FastAPI routes + UI
  examples.py   built-in demo DAGs
  static/       dashboard
tests/          dag / executor / api tests
docker-compose.yml, Dockerfile, requirements.txt
```

## Design notes & limitations

- **Custom worker pool** uses Python threads, which is the right model here
  because tasks are subprocesses (the work happens in a separate process, so the
  GIL isn't the bottleneck). Swapping in a process pool or remote executors would
  only touch `executor.py`.
- **Single scheduler thread** keeps state authoritative and race-free. To scale
  schedulers horizontally you'd add row-level locking / `SELECT ... FOR UPDATE`
  so multiple schedulers could claim tasks; the schema already supports it.
- **Tasks are shell commands.** Adding typed operators (Python callables, HTTP
  calls) is a matter of extending the task-execution function.
- Scheduling is poll-based (a short tick); fine for this scale. No cron-style
  recurring schedules yet — runs are triggered via the API/UI.
