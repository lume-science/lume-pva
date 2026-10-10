"""Tests for lume_pva.runner configuration generation.

Runner.__init__ starts PVA/CA servers, so these tests only exercise the pure
configuration logic (Runner.generate_config) using a stub model object — no
servers are started and no network calls are made.
"""

from queue import Queue
from typing import Any

import numpy as np
import pytest
from lume.variables import NDVariable, ScalarVariable, Variable

from lume_pva.runner import PutMode, Runner


class StubModel:
    """Minimal stand-in for a LUMEModel: generate_config only reads
    supported_variables."""

    def __init__(self, variables: dict[str, Variable]) -> None:
        self.supported_variables = variables


@pytest.fixture
def model() -> StubModel:
    return StubModel(
        {
            "input_a": ScalarVariable(name="input_a"),
            "output_b": ScalarVariable(name="output_b", read_only=True),
            "image": NDVariable(name="image", shape=(4, 4), dtype=np.float64, read_only=True),
        }
    )


def test_runner_defaults(model: StubModel) -> None:
    config = Runner.generate_config(model)

    for name, var_config in config["variables"].items():
        assert var_config["name"] == name
        assert var_config["pv"] == name

    assert set(config["variables"].keys()) == {"input_a", "output_b", "image"}
    # only one read write
    assert config["variables"]["input_a"]["mode"] == "rw"
    assert config["variables"]["output_b"]["mode"] == "ro"
    assert config["variables"]["image"]["mode"] == "ro"

    # continuous mode is default
    assert config["remote_model_mode"] == "continuous"

    # No prefix
    assert config["prefix"] == ""


def test_set_prefix(model: StubModel) -> None:
    config = Runner.generate_config(model, prefix="TEST:")

    assert config["prefix"] == "TEST:"


def test_mark_rw_variables_ro_remote(model: StubModel) -> None:
    config = Runner.generate_config(model, remote_inputs=True)

    assert config["variables"]["input_a"]["mode"] == "remote"
    # Read-only variables stay served by the runner
    assert config["variables"]["output_b"]["mode"] == "ro"


def test_pv_name_transformer(model: StubModel) -> None:
    config = Runner.generate_config(
        model, name_transformer=lambda var, name: f"XFORM:{name.upper()}"
    )
    assert config["variables"]["input_a"]["pv"] == "XFORM:INPUT_A"
    # Variable names must remain untouched — only the PV name changes
    assert config["variables"]["input_a"]["name"] == "input_a"


def test_no_variables() -> None:
    empty_model = StubModel({})
    config = Runner.generate_config(empty_model)

    assert config["variables"] == {}


def _make_runner_control_stub(protocol: list[str]) -> Runner:
    runner = Runner.__new__(Runner)
    runner._config = {
        "prefix": "",
        "protocol": protocol,
    }
    runner.providers = {}
    runner.pvdb = {}
    runner.snapshot_control_pv = ""
    runner.reset_control_pv = ""
    runner.supports_pva = "pva" in protocol
    runner.supports_ca = "ca" in protocol

    # _create_control_pvs wires callbacks to these methods; simple stubs are enough.
    runner.take_snapshot = lambda: None
    runner._enqueue = lambda *args, **kwargs: None
    return runner


def test_control_pvs_do_not_create_pva_sharedpvs_for_ca_only() -> None:
    runner = _make_runner_control_stub(["ca"])

    runner._create_control_pvs()

    assert runner.snapshot_control_pv == "SNAPSHOT"
    assert runner.reset_control_pv == "RESET"
    assert "SNAPSHOT" not in runner.providers
    assert "RESET" not in runner.providers
    assert runner.pvdb["SNAPSHOT"]["type"] == "int"
    assert runner.pvdb["RESET"]["type"] == "int"


def test_control_pvs_create_pva_sharedpvs_when_pva_enabled() -> None:
    runner = _make_runner_control_stub(["pva"])

    runner._create_control_pvs()

    assert "SNAPSHOT" in runner.providers
    assert "RESET" in runner.providers
    assert "SNAPSHOT" not in runner.pvdb
    assert "RESET" not in runner.pvdb


def _make_runner_model_info_stub(model: StubModel, variables: dict) -> Runner:
    runner = Runner.__new__(Runner)
    runner.model = model
    runner._config = {
        "prefix": "",
        "description": "stub",
        "variables": variables,
    }
    runner.types = {}
    runner.pvs = {}
    runner.providers = {}
    return runner


def test_model_info_lists_only_configured_variables(model: StubModel) -> None:
    # The config serves only one of the model's three variables
    runner = _make_runner_model_info_stub(
        model, {"input_a": {"name": "input_a", "pv": "input_a", "mode": "rw"}}
    )

    runner._create_model_info()

    info = runner.pvs["MODEL_INFO"].current()
    listed = [(v["name"], v["pvname"], v["mode"]) for v in info["supported_variables"]]
    assert listed == [("input_a", "input_a", "rw")]
    assert "MODEL_INFO" in runner.providers


class _SnapshotFailsModel(StubModel):
    """Stub model whose ``get`` raises, so the state snapshot of a cycle fails."""

    def __init__(self, variables: dict[str, Variable]) -> None:
        super().__init__(variables)
        self.set_calls: list[dict[str, Any]] = []

    def get(self, names: list[str]) -> dict[str, Any]:
        raise RuntimeError("model state is unavailable")

    def set(self, values: dict[str, Any]) -> None:
        self.set_calls.append(values)


class _OneCycleQueue(Queue):
    """Queue that ends ``Runner.run`` once drained instead of blocking forever."""

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        if block and self.empty():
            raise KeyboardInterrupt
        return super().get(block, timeout)


def _make_runner_cycle_stub(model: StubModel) -> Runner:
    runner = Runner.__new__(Runner)
    runner.model = model
    runner._config = {"prefix": "", "put_mode": PutMode.Complete}
    runner.queue = _OneCycleQueue()
    runner.update_rate = 0.0
    runner.providers = {}
    runner.pvdb = {}
    runner.status_control_pv = "STATUS"
    runner._cached_state = {"input_a": 1.0}
    return runner


def test_failed_state_snapshot_completes_waiting_puts(model: StubModel) -> None:
    failing_model = _SnapshotFailsModel(model.supported_variables)
    runner = _make_runner_cycle_stub(failing_model)
    errors: list[str | None] = []
    runner._enqueue({"input_a": {"value": 2.0, "ts": 1.0}}, done=errors.append)

    # Returns once the queue is drained; the failed cycle must not end the loop
    runner.run()

    # The waiting put is told about the failure
    assert errors == ["model state is unavailable"]
    # There is no snapshot for this cycle, so the older one is not applied again
    assert failing_model.set_calls == [{}]
