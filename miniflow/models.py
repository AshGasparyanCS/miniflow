"""Persisted state: DAG runs and their task instances.

A DagRun is one execution of a DAG; each TaskInstance is one task within that
run, carrying its own state, attempt count, logs, and timing. The full DAG
definition is stored as JSON on the run so a run is self-contained and can be
re-scheduled after a process restart.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


# ---- run states ----
RUN_RUNNING = "running"
RUN_SUCCESS = "success"
RUN_FAILED = "failed"

# ---- task states ----
PENDING = "pending"            # waiting on upstreams
RUNNING = "running"            # dispatched to a worker
SUCCESS = "success"
FAILED = "failed"             # exhausted retries
UP_FOR_RETRY = "up_for_retry"  # failed but will retry after a delay
UPSTREAM_FAILED = "upstream_failed"  # an upstream failed, so this won't run

TERMINAL_TASK_STATES = {SUCCESS, FAILED, UPSTREAM_FAILED}
ACTIVE_TASK_STATES = {PENDING, RUNNING, UP_FOR_RETRY}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DagRun(Base):
    __tablename__ = "dag_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    dag_id: Mapped[str] = mapped_column(String(200), index=True)
    state: Mapped[str] = mapped_column(String(20), index=True, default=RUN_RUNNING)
    dag_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    tasks: Mapped[list["TaskInstance"]] = relationship(
        back_populates="run", cascade="all, delete-orphan", lazy="selectin"
    )


class TaskInstance(Base):
    __tablename__ = "task_instances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("dag_runs.id"), index=True)
    task_id: Mapped[str] = mapped_column(String(200))
    state: Mapped[str] = mapped_column(String(20), default=PENDING, index=True)
    command: Mapped[str] = mapped_column(Text)

    try_number: Mapped[int] = mapped_column(Integer, default=0)   # attempts started
    max_retries: Mapped[int] = mapped_column(Integer, default=0)  # extra attempts allowed
    retry_delay: Mapped[float] = mapped_column(Float, default=0.0)  # seconds

    log: Mapped[str] = mapped_column(Text, default="")
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    run: Mapped[DagRun] = relationship(back_populates="tasks")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "task_id": self.task_id,
            "state": self.state,
            "try_number": self.try_number,
            "max_retries": self.max_retries,
            "command": self.command,
            "log": self.log,
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
        }


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None
