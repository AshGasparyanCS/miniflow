import pytest

from miniflow.dag import DagDef, DagValidationError, TaskDef


def test_topological_order_respects_deps():
    dag = DagDef("d", [
        TaskDef("a", "echo a"),
        TaskDef("b", "echo b", upstreams=["a"]),
        TaskDef("c", "echo c", upstreams=["a"]),
        TaskDef("d", "echo d", upstreams=["b", "c"]),
    ])
    order = dag.topological_order()
    assert order.index("a") < order.index("b")
    assert order.index("a") < order.index("c")
    assert order.index("b") < order.index("d")
    assert order.index("c") < order.index("d")


def test_cycle_is_rejected():
    dag = DagDef("d", [
        TaskDef("a", "echo a", upstreams=["c"]),
        TaskDef("b", "echo b", upstreams=["a"]),
        TaskDef("c", "echo c", upstreams=["b"]),
    ])
    with pytest.raises(DagValidationError, match="cycle"):
        dag.validate()


def test_unknown_upstream_rejected():
    dag = DagDef("d", [TaskDef("a", "echo a", upstreams=["ghost"])])
    with pytest.raises(DagValidationError, match="unknown"):
        dag.validate()


def test_duplicate_ids_rejected():
    dag = DagDef("d", [TaskDef("a", "echo 1"), TaskDef("a", "echo 2")])
    with pytest.raises(DagValidationError, match="duplicate"):
        dag.validate()


def test_self_dependency_rejected():
    dag = DagDef("d", [TaskDef("a", "echo a", upstreams=["a"])])
    with pytest.raises(DagValidationError):
        dag.validate()


def test_downstream_closure():
    dag = DagDef("d", [
        TaskDef("a", "x"),
        TaskDef("b", "x", upstreams=["a"]),
        TaskDef("c", "x", upstreams=["b"]),
        TaskDef("d", "x"),
    ])
    assert dag.downstream_closure("a") == {"b", "c"}
    assert dag.downstream_closure("d") == set()
