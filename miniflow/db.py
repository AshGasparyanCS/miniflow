"""Database engine and session management.

Defaults to SQLite for local dev and tests; set DATABASE_URL to a Postgres DSN
(as docker-compose does) for production. The model layer is engine-agnostic, so
nothing else changes between the two.
"""
from __future__ import annotations

import os

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DEFAULT_URL = "sqlite:///./miniflow.db"


class Base(DeclarativeBase):
    pass


def make_engine(url: str | None = None) -> Engine:
    url = url or os.environ.get("DATABASE_URL", DEFAULT_URL)
    connect_args: dict = {}
    if url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
    engine = create_engine(url, connect_args=connect_args, future=True)

    if url.startswith("sqlite"):
        # WAL + a busy timeout let the scheduler thread and request handlers
        # write concurrently without tripping "database is locked".
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragmas(dbapi_conn, _):  # noqa: ANN001
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA busy_timeout=5000")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

    return engine


def make_session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_db(engine: Engine) -> None:
    # Import models so their tables register on Base.metadata before create_all.
    from . import models  # noqa: F401

    Base.metadata.create_all(engine)
