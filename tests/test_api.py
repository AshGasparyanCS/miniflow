import time

from fastapi.testclient import TestClient

from miniflow.api import create_app


def make_client(tmp_path):
    app = create_app(database_url=f"sqlite:///{tmp_path}/api.db", max_workers=4, start=True)
    return TestClient(app)


def poll_until_done(client, run_id, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/api/runs/{run_id}").json()
        if r["state"] in ("success", "failed"):
            return r
        time.sleep(0.05)
    raise AssertionError("run did not finish")


def test_submit_and_track(tmp_path):
    client = make_client(tmp_path)
    dag = {
        "dag_id": "api_test",
        "tasks": [
            {"task_id": "a", "command": "echo step-a"},
            {"task_id": "b", "command": "echo step-b", "upstreams": ["a"]},
        ],
    }
    run_id = client.post("/api/runs", json=dag).json()["run_id"]
    result = poll_until_done(client, run_id)
    assert result["state"] == "success"
    states = {t["task_id"]: t["state"] for t in result["tasks"]}
    assert states == {"a": "success", "b": "success"}


def test_invalid_dag_rejected(tmp_path):
    client = make_client(tmp_path)
    bad = {"dag_id": "bad", "tasks": [{"task_id": "a", "command": "x", "upstreams": ["nope"]}]}
    resp = client.post("/api/runs", json=bad)
    assert resp.status_code == 400


def test_run_example_endpoint(tmp_path):
    client = make_client(tmp_path)
    names = [e["name"] for e in client.get("/api/examples").json()["examples"]]
    assert "linear_etl" in names

    run_id = client.post("/api/examples/linear_etl").json()["run_id"]
    result = poll_until_done(client, run_id)
    assert result["state"] == "success"


def test_runs_list(tmp_path):
    client = make_client(tmp_path)
    client.post("/api/examples/linear_etl")
    runs = client.get("/api/runs").json()["runs"]
    assert len(runs) >= 1
    assert runs[0]["dag_id"] == "linear_etl"
