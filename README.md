# miniflow: a mini-Airflow DAG scheduler

A job-scheduling system in the spirit of Airflow. You define a DAG of tasks with dependencies, and miniflow runs them in dependency order, in parallel where it can, with per-task retries and failure handling. Run state is saved to a database, and a live web UI shows every run and task as it happens.

Built with **FastAPI** and **SQLAlchemy** plus a **custom worker pool** (no Celery, no external broker, because the scheduling logic is the interesting part and I wanted to write it directly). It persists to **PostgreSQL** in production and uses SQLite for local dev and tests, with no code changes between the two.

## Why a custom worker pool

The core idea is a clean split between *deciding* and *doing*:

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

Because exactly one thread ever writes state, there are **no database races**. The scheduler is the brain, the pool is the muscle. This is also roughly how real schedulers separate the scheduling loop from the executors.

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

A DAG is just JSON: a `dag_id` and a list of tasks. Each task has a shell `command`, optional `upstreams`, and optional `retries` / `retry_delay`.

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

A task succeeds when its command exits 0. Before running anything, miniflow validates the DAG: task ids are unique, every referenced upstream exists, and the graph is acyclic (a cycle gets rejected with a clear error).

## API

| Method and path | Purpose |
|---|---|
| `POST /api/runs` | Submit a DAG definition, get back `{run_id}`. |
| `GET /api/runs` | List recent runs with per-state task counts. |
| `GET /api/runs/{id}` | One run with every task's state, attempt count, and logs. |
| `GET /api/examples` | Built-in example DAGs. |
| `POST /api/examples/{name}` | Trigger an example run. |
| `GET /` | Live dashboard. |

## Run it

### Local (SQLite, no infra needed)

```bash
pip install -r requirements.txt
python -m miniflow                       # http://localhost:8000
```

Open the dashboard, click an example DAG (the buttons: `diamond_parallel`, `retry_then_pass`, `fail_cascade`, `linear_etl`) and watch the tasks move through their states live. Or drive it from the API:

```bash
curl -X POST localhost:8000/api/runs -H 'content-type: application/json' -d '{
  "dag_id":"demo",
  "tasks":[
    {"task_id":"a","command":"echo hi"},
    {"task_id":"b","command":"echo bye","upstreams":["a"]}
  ]}'
```

### Docker (PostgreSQL, closer to production)

```bash
docker compose up --build           # Postgres + app, UI on :8000
```

The app reads `DATABASE_URL`, and compose points it at the Postgres service. Nothing else changes between SQLite and Postgres, which is the SQLAlchemy layer doing its job.

## Examples (and what each one shows off)

| Example | Shows |
|---|---|
| `linear_etl` | Strict dependency ordering: extract, then transform, then load. |
| `diamond_parallel` | Two independent branches run **at the same time**, then join. |
| `retry_then_pass` | A flaky task fails, retries with backoff, then succeeds. |
| `fail_cascade` | A task burns through its retries and **fails**, its downstream gets marked `upstream_failed`, and an independent branch still succeeds anyway. |

## Tests

```bash
pytest -q     # 15 tests
```

| Area | Checks |
|---|---|
| `test_dag` | Topological order, cycle rejection, bad/duplicate/self deps, downstream closure. |
| `test_executor` | Dependency ordering (a downstream task only starts after its upstream finishes), parallel branches actually overlap, retry-then-success, failure cascade with the right attempt counts. |
| `test_api` | Submit and poll to completion, example trigger, validation 400s, run listing. |

Tests run against a temp SQLite DB and exercise the real scheduler thread plus worker pool, not mocks.

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

## Design notes and limitations

- **The custom worker pool uses Python threads,** which is the right call here because tasks are subprocesses. The actual work happens in a separate process, so the GIL isn't the bottleneck. Swapping in a process pool or remote executors would only touch `executor.py`.
- **One scheduler thread** keeps state authoritative and race-free. To scale schedulers horizontally you'd add row-level locking (`SELECT ... FOR UPDATE`) so multiple schedulers could claim tasks. The schema already supports it.
- **Tasks are shell commands.** Adding typed operators (Python callables, HTTP calls) is just a matter of extending the task-execution function.
- Scheduling is poll-based on a short tick, which is fine at this scale. No cron-style recurring schedules yet, runs are triggered through the API or UI.
