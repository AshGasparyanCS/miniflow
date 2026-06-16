"""Built-in example DAGs that show off the scheduler's behaviour."""
from __future__ import annotations

from .dag import DagDef, TaskDef

EXAMPLES: dict[str, DagDef] = {
    # extract -> transform -> load, strictly sequential.
    "linear_etl": DagDef(
        dag_id="linear_etl",
        tasks=[
            TaskDef("extract", "echo extracting data && sleep 0.3"),
            TaskDef("transform", "echo transforming && sleep 0.3", upstreams=["extract"]),
            TaskDef("load", "echo loading && sleep 0.3", upstreams=["transform"]),
        ],
    ),
    # start fans out to two independent branches that run in parallel, then join.
    "diamond_parallel": DagDef(
        dag_id="diamond_parallel",
        tasks=[
            TaskDef("start", "echo start && sleep 0.2"),
            TaskDef("branch_a", "echo branch A working && sleep 1", upstreams=["start"]),
            TaskDef("branch_b", "echo branch B working && sleep 1", upstreams=["start"]),
            TaskDef("join", "echo joined both branches", upstreams=["branch_a", "branch_b"]),
        ],
    ),
    # a flaky task that fails twice then succeeds on its third attempt.
    "retry_then_pass": DagDef(
        dag_id="retry_then_pass",
        tasks=[
            TaskDef("setup", "echo setup ok"),
            TaskDef(
                "flaky",
                # Fails unless a marker file exists; each attempt creates it, so
                # the first attempt fails and a later one succeeds.
                'f=/tmp/miniflow_flaky_$PPID; if [ -f "$f" ]; then echo recovered; '
                'else touch "$f"; echo "failing this attempt"; exit 1; fi',
                upstreams=["setup"],
                retries=3,
                retry_delay=0.3,
            ),
            TaskDef("report", "echo report generated", upstreams=["flaky"]),
        ],
    ),
    # a task that always fails, demonstrating downstream upstream_failed cascade.
    "fail_cascade": DagDef(
        dag_id="fail_cascade",
        tasks=[
            TaskDef("prepare", "echo prepared"),
            TaskDef("bad_step", "echo doomed && exit 2", upstreams=["prepare"], retries=1, retry_delay=0.2),
            TaskDef("after_bad", "echo should not run", upstreams=["bad_step"]),
            TaskDef("independent", "echo runs regardless", upstreams=["prepare"]),
        ],
    ),
}
