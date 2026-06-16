"""HTTP API and web UI.

Endpoints
  POST /api/runs              submit a DAG definition (JSON) -> {run_id}
  GET  /api/runs             list recent runs (summary)
  GET  /api/runs/{id}        one run with its task instances
  GET  /api/examples         built-in example DAGs
  POST /api/examples/{name}  run an example -> {run_id}
  GET  /health               liveness
  GET  /                     dashboard
"""
from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import desc, select

from .dag import DagDef, DagValidationError
from .db import init_db, make_engine, make_session_factory
from .examples import EXAMPLES
from .executor import Executor
from .models import DagRun, _iso

STATIC_DIR = Path(__file__).parent / "static"


def create_app(database_url: str | None = None, max_workers: int = 4, start: bool = True) -> FastAPI:
    engine = make_engine(database_url)
    init_db(engine)
    session_factory = make_session_factory(engine)
    executor = Executor(session_factory, max_workers=max_workers)
    if start:
        executor.start()

    app = FastAPI(title="miniflow", version="1.0")
    app.state.executor = executor
    app.state.session_factory = session_factory

    def run_summary(run: DagRun) -> dict:
        counts: dict[str, int] = {}
        for ti in run.tasks:
            counts[ti.state] = counts.get(ti.state, 0) + 1
        return {
            "id": run.id,
            "dag_id": run.dag_id,
            "state": run.state,
            "task_counts": counts,
            "n_tasks": len(run.tasks),
            "created_at": _iso(run.created_at),
            "started_at": _iso(run.started_at),
            "finished_at": _iso(run.finished_at),
        }

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/api/runs")
    def submit(payload: dict) -> dict:
        try:
            dag = DagDef.from_dict(payload)
            dag.validate()
        except DagValidationError as e:
            raise HTTPException(status_code=400, detail=str(e))
        run_id = executor.submit_run(dag)
        return {"run_id": run_id}

    @app.get("/api/runs")
    def list_runs(limit: int = 50) -> dict:
        with session_factory() as s:
            runs = s.scalars(select(DagRun).order_by(desc(DagRun.created_at)).limit(limit)).all()
            return {"runs": [run_summary(r) for r in runs]}

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> dict:
        with session_factory() as s:
            run = s.get(DagRun, run_id)
            if not run:
                raise HTTPException(status_code=404, detail="run not found")
            dag = json.loads(run.dag_json)
            return {
                **run_summary(run),
                "dag": dag,
                "tasks": [ti.to_dict() for ti in sorted(run.tasks, key=lambda t: t.task_id)],
            }

    @app.get("/api/examples")
    def list_examples() -> dict:
        return {"examples": [{"name": n, "dag": d.to_dict()} for n, d in EXAMPLES.items()]}

    @app.post("/api/examples/{name}")
    def run_example(name: str) -> dict:
        if name not in EXAMPLES:
            raise HTTPException(status_code=404, detail="unknown example")
        run_id = executor.submit_run(EXAMPLES[name])
        return {"run_id": run_id}

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app
