"""The scheduler and worker pool.

Design: a single background scheduler thread owns every state mutation in the
database. A pool of worker threads only *executes* task commands and reports
results back through a thread-safe queue. Because exactly one thread ever writes
run/task state, there are no database races — the worker pool is purely the
"muscle", the scheduler is the "brain".

Each scheduler tick, for every running DAG run, the scheduler:
  1. drains finished task results and records success / retry / failure,
  2. cascades upstream failures to downstream tasks,
  3. dispatches any task whose upstreams have all succeeded (and any retry whose
     backoff has elapsed) to the worker pool, in parallel,
  4. marks the run success/failed once nothing is left to do.
"""
from __future__ import annotations

import json
import subprocess
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from queue import Empty, Queue

from sqlalchemy import select

from .dag import DagDef
from .models import (
    ACTIVE_TASK_STATES,
    FAILED,
    PENDING,
    RUN_FAILED,
    RUN_RUNNING,
    RUN_SUCCESS,
    RUNNING,
    SUCCESS,
    UP_FOR_RETRY,
    UPSTREAM_FAILED,
    DagRun,
    TaskInstance,
    utcnow,
)

TASK_TIMEOUT = 120  # seconds; a runaway task can't hang the scheduler forever


@dataclass
class _Result:
    ti_id: int
    returncode: int
    output: str


def _run_command(ti_id: int, command: str) -> _Result:
    """Worker-thread body: run the command, capture output. Touches no DB."""
    try:
        proc = subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=TASK_TIMEOUT
        )
        return _Result(ti_id, proc.returncode, (proc.stdout or "") + (proc.stderr or ""))
    except subprocess.TimeoutExpired:
        return _Result(ti_id, 124, f"task timed out after {TASK_TIMEOUT}s")
    except Exception as e:  # noqa: BLE001
        return _Result(ti_id, 1, f"executor error: {e}")


class Executor:
    def __init__(self, session_factory, max_workers: int = 4, poll_interval: float = 0.05):
        self._sf = session_factory
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="worker")
        self._results: Queue[_Result] = Queue()
        self._inflight: set[int] = set()      # ti ids currently in a worker
        self._dags: dict[str, DagDef] = {}     # run_id -> parsed DAG (cache)
        self._poll = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ---- lifecycle ----

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._pool.shutdown(wait=False, cancel_futures=True)

    # ---- submission ----

    def submit_run(self, dag: DagDef) -> str:
        """Validate and persist a new run; the scheduler picks it up next tick."""
        dag.validate()
        import uuid

        run_id = uuid.uuid4().hex
        with self._sf() as s:
            run = DagRun(
                id=run_id,
                dag_id=dag.dag_id,
                state=RUN_RUNNING,
                dag_json=json.dumps(dag.to_dict()),
                started_at=utcnow(),
            )
            s.add(run)
            for t in dag.tasks:
                s.add(
                    TaskInstance(
                        run_id=run_id,
                        task_id=t.task_id,
                        state=PENDING,
                        command=t.command,
                        max_retries=t.retries,
                        retry_delay=t.retry_delay,
                    )
                )
            s.commit()
        self._dags[run_id] = dag
        return run_id

    def wait_run(self, run_id: str, timeout: float = 30.0) -> str:
        """Block until the run reaches a terminal state; return that state."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._sf() as s:
                run = s.get(DagRun, run_id)
                if run and run.state in (RUN_SUCCESS, RUN_FAILED):
                    return run.state
            time.sleep(0.02)
        raise TimeoutError(f"run {run_id} did not finish within {timeout}s")

    # ---- scheduler loop ----

    def _loop(self) -> None:
        session = self._sf()
        try:
            while not self._stop.is_set():
                self._drain_results(session)
                runs = session.scalars(
                    select(DagRun).where(DagRun.state == RUN_RUNNING)
                ).all()
                for run in runs:
                    self._schedule_run(session, run)
                session.commit()
                time.sleep(self._poll)
        finally:
            session.close()

    def _dag_for(self, run: DagRun) -> DagDef:
        dag = self._dags.get(run.id)
        if dag is None:  # rebuilt after a process restart
            dag = DagDef.from_dict(json.loads(run.dag_json))
            self._dags[run.id] = dag
        return dag

    def _drain_results(self, session) -> None:
        while True:
            try:
                res = self._results.get_nowait()
            except Empty:
                break
            ti = session.get(TaskInstance, res.ti_id)
            self._inflight.discard(res.ti_id)
            if ti is None:
                continue
            ti.finished_at = utcnow()
            ti.log = res.output[-8000:]
            if res.returncode == 0:
                ti.state = SUCCESS
            elif ti.try_number <= ti.max_retries:
                # Attempts so far <= allowed retries -> back off and try again.
                ti.state = UP_FOR_RETRY
                ti.next_attempt_at = utcnow() + timedelta(seconds=ti.retry_delay)
            else:
                ti.state = FAILED
        session.commit()

    def _schedule_run(self, session, run: DagRun) -> None:
        dag = self._dag_for(run)
        tm = dag.task_map()
        tis = session.scalars(
            select(TaskInstance).where(TaskInstance.run_id == run.id)
        ).all()
        by_id = {ti.task_id: ti for ti in tis}

        # (1) Cascade upstream failures: a pending task with any failed upstream
        # can never run, so mark it upstream_failed. Repeat to closure.
        changed = True
        while changed:
            changed = False
            for ti in tis:
                if ti.state != PENDING:
                    continue
                ups = tm[ti.task_id].upstreams
                if any(by_id[u].state in (FAILED, UPSTREAM_FAILED) for u in ups):
                    ti.state = UPSTREAM_FAILED
                    ti.finished_at = utcnow()
                    changed = True

        # (2) Dispatch ready tasks (deps satisfied, or retry backoff elapsed).
        now = utcnow()
        for ti in tis:
            if ti.id in self._inflight:
                continue
            ready = False
            if ti.state == PENDING:
                ups = tm[ti.task_id].upstreams
                ready = all(by_id[u].state == SUCCESS for u in ups)
            elif ti.state == UP_FOR_RETRY:
                ready = ti.next_attempt_at is None or _aware(ti.next_attempt_at) <= now
            if ready:
                ti.try_number += 1
                ti.state = RUNNING
                ti.started_at = now
                ti.finished_at = None
                self._inflight.add(ti.id)
                fut: Future = self._pool.submit(_run_command, ti.id, ti.command)
                fut.add_done_callback(self._on_task_done)

        # (3) Completion check.
        states = [ti.state for ti in tis]
        active = any(s in ACTIVE_TASK_STATES for s in states)
        inflight_here = any(ti.id in self._inflight for ti in tis)
        if not active and not inflight_here:
            run.state = RUN_FAILED if any(
                s in (FAILED, UPSTREAM_FAILED) for s in states
            ) else RUN_SUCCESS
            run.finished_at = now

    def _on_task_done(self, fut: Future) -> None:
        # Runs in a worker thread: just hand the result to the scheduler.
        try:
            self._results.put(fut.result())
        except Exception:  # noqa: BLE001
            pass


def _aware(dt):
    # SQLite returns naive datetimes; treat them as UTC for comparison.
    from datetime import timezone

    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
