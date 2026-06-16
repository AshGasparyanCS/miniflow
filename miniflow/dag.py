"""DAG definitions and validation.

A DAG is a set of tasks plus upstream-dependency edges. Before a DAG can run we
validate that task ids are unique, every referenced upstream exists, and the
graph is acyclic (a cycle would make the dependency order undefined).
"""
from __future__ import annotations

from dataclasses import dataclass, field


class DagValidationError(ValueError):
    pass


@dataclass
class TaskDef:
    task_id: str
    command: str
    upstreams: list[str] = field(default_factory=list)
    retries: int = 0          # extra attempts after the first
    retry_delay: float = 0.0  # seconds between attempts

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "command": self.command,
            "upstreams": list(self.upstreams),
            "retries": self.retries,
            "retry_delay": self.retry_delay,
        }


@dataclass
class DagDef:
    dag_id: str
    tasks: list[TaskDef]

    @staticmethod
    def from_dict(d: dict) -> "DagDef":
        if "dag_id" not in d or "tasks" not in d:
            raise DagValidationError("DAG must have 'dag_id' and 'tasks'")
        tasks = []
        for t in d["tasks"]:
            if "task_id" not in t or "command" not in t:
                raise DagValidationError("each task needs 'task_id' and 'command'")
            tasks.append(
                TaskDef(
                    task_id=t["task_id"],
                    command=t["command"],
                    upstreams=list(t.get("upstreams", [])),
                    retries=int(t.get("retries", 0)),
                    retry_delay=float(t.get("retry_delay", 0.0)),
                )
            )
        return DagDef(dag_id=d["dag_id"], tasks=tasks)

    def to_dict(self) -> dict:
        return {"dag_id": self.dag_id, "tasks": [t.to_dict() for t in self.tasks]}

    def task_map(self) -> dict[str, TaskDef]:
        return {t.task_id: t for t in self.tasks}

    def validate(self) -> None:
        ids = [t.task_id for t in self.tasks]
        if not ids:
            raise DagValidationError("DAG has no tasks")
        if len(ids) != len(set(ids)):
            raise DagValidationError("duplicate task ids")
        idset = set(ids)
        for t in self.tasks:
            for u in t.upstreams:
                if u not in idset:
                    raise DagValidationError(f"task {t.task_id!r} depends on unknown task {u!r}")
                if u == t.task_id:
                    raise DagValidationError(f"task {t.task_id!r} depends on itself")
        # Cycle detection: a full topological sort must consume every node.
        self.topological_order()

    def topological_order(self) -> list[str]:
        """Kahn's algorithm; raises if the graph contains a cycle."""
        tm = self.task_map()
        indeg = {tid: 0 for tid in tm}
        children: dict[str, list[str]] = {tid: [] for tid in tm}
        for t in self.tasks:
            for u in t.upstreams:
                indeg[t.task_id] += 1
                children[u].append(t.task_id)
        ready = sorted(tid for tid, d in indeg.items() if d == 0)
        order: list[str] = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            for c in sorted(children[n]):
                indeg[c] -= 1
                if indeg[c] == 0:
                    ready.append(c)
            ready.sort()
        if len(order) != len(tm):
            raise DagValidationError("DAG contains a cycle")
        return order

    def downstream_closure(self, task_id: str) -> set[str]:
        """All tasks transitively downstream of task_id."""
        children: dict[str, list[str]] = {t.task_id: [] for t in self.tasks}
        for t in self.tasks:
            for u in t.upstreams:
                children[u].append(t.task_id)
        seen: set[str] = set()
        stack = list(children[task_id])
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            stack.extend(children[n])
        return seen
