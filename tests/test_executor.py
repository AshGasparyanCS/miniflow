from sqlalchemy import select

from miniflow.dag import DagDef, TaskDef
from miniflow.examples import EXAMPLES
from miniflow.models import (
    FAILED,
    RUN_FAILED,
    RUN_SUCCESS,
    SUCCESS,
    UPSTREAM_FAILED,
    TaskInstance,
)


def tasks_by_id(sf, run_id) -> dict:
    with sf() as s:
        tis = s.scalars(select(TaskInstance).where(TaskInstance.run_id == run_id)).all()
        return {ti.task_id: ti for ti in tis}


def test_dependency_order(executor):
    ex, sf = executor
    run_id = ex.submit_run(EXAMPLES["linear_etl"])
    assert ex.wait_run(run_id) == RUN_SUCCESS

    t = tasks_by_id(sf, run_id)
    assert all(ti.state == SUCCESS for ti in t.values())
    # A downstream task must not start before its upstream has finished.
    assert t["transform"].started_at >= t["extract"].finished_at
    assert t["load"].started_at >= t["transform"].finished_at


def test_parallel_execution(executor):
    ex, sf = executor
    run_id = ex.submit_run(EXAMPLES["diamond_parallel"])
    assert ex.wait_run(run_id, timeout=10) == RUN_SUCCESS

    t = tasks_by_id(sf, run_id)
    a, b = t["branch_a"], t["branch_b"]
    # The two 1-second branches run concurrently, so the wall-clock span from the
    # first branch starting to the last finishing is well under 2 seconds.
    span = max(a.finished_at, b.finished_at) - min(a.started_at, b.started_at)
    assert span.total_seconds() < 1.8, f"branches did not overlap (span={span})"


def test_retry_then_success(executor):
    ex, sf = executor
    run_id = ex.submit_run(EXAMPLES["retry_then_pass"])
    assert ex.wait_run(run_id, timeout=10) == RUN_SUCCESS

    t = tasks_by_id(sf, run_id)
    assert t["flaky"].state == SUCCESS
    assert t["flaky"].try_number >= 2, "flaky task should have retried at least once"
    assert t["report"].state == SUCCESS


def test_failure_cascade(executor):
    ex, sf = executor
    run_id = ex.submit_run(EXAMPLES["fail_cascade"])
    assert ex.wait_run(run_id, timeout=10) == RUN_FAILED

    t = tasks_by_id(sf, run_id)
    assert t["bad_step"].state == FAILED
    # bad_step is retried once (2 attempts) before failing.
    assert t["bad_step"].try_number == 2
    # downstream of the failure is skipped as upstream_failed...
    assert t["after_bad"].state == UPSTREAM_FAILED
    # ...but an independent branch still succeeds.
    assert t["independent"].state == SUCCESS


def test_custom_dag_submission(executor):
    ex, sf = executor
    dag = DagDef("custom", [
        TaskDef("a", "echo hello"),
        TaskDef("b", "echo world", upstreams=["a"]),
    ])
    run_id = ex.submit_run(dag)
    assert ex.wait_run(run_id) == RUN_SUCCESS
    t = tasks_by_id(sf, run_id)
    assert "hello" in t["a"].log
    assert "world" in t["b"].log
