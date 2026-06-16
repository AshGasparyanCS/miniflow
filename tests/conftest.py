import pytest
from sqlalchemy import select

from miniflow.db import init_db, make_engine, make_session_factory
from miniflow.executor import Executor
from miniflow.models import TaskInstance


@pytest.fixture
def executor(tmp_path):
    url = f"sqlite:///{tmp_path}/test.db"
    engine = make_engine(url)
    init_db(engine)
    sf = make_session_factory(engine)
    ex = Executor(sf, max_workers=4, poll_interval=0.02)
    ex.start()
    yield ex, sf
    ex.stop()


def tasks_by_id(sf, run_id) -> dict[str, TaskInstance]:
    with sf() as s:
        tis = s.scalars(select(TaskInstance).where(TaskInstance.run_id == run_id)).all()
        return {ti.task_id: ti for ti in tis}
