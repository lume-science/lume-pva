"""Tests for the variable handlers in lume_pva.variables.

These tests exercise the public handler interface (create_type, pack_value,
unpack_value, default_value, value_to_native, native_to_value, is_supported,
ca_pvspec) against in-memory p4p Value objects. No servers are started and no
network or disk I/O is performed.
"""

from typing import Any

import numpy as np
import pytest
import torch
from lume.variables import (
    BoolVariable,
    EnumVariable,
    IntVariable,
    NDVariable,
    ScalarVariable,
    StrVariable,
    Variable,
)
from lume_torch.variables import TorchNDVariable, TorchScalarVariable
from p4p import Type

from lume_pva.epics import epicsAlarmSeverity, epicsAlarmStatus
from lume_pva.variables import (
    EnumVariableHandler,
    NDVariableHandler,
    ScalarVariableHandler,
    SimpleScalarHandler,
    TorchScalarVariableHandler,
    VariableHandler,
    find_variable_handler,
)


class DerivedScalarVariable(ScalarVariable):
    pass


class DerivedBoolVariable(BoolVariable):
    pass


@pytest.mark.parametrize(
    ("variable", "value", "expected", "expected_type"),
    [
        pytest.param(ScalarVariable(name="x"), 3.5, 3.5, float, id="float"),
        pytest.param(IntVariable(name="i"), 4, 4, int, id="int"),
        pytest.param(IntVariable(name="i"), 3.9, 3, int, id="int-truncates-float"),
        pytest.param(ScalarVariable(name="x"), -123.25, -123.25, float, id="negative"),
        pytest.param(ScalarVariable(name="x"), 1e308, 1e308, float, id="very-large"),
        pytest.param(StrVariable(name="s"), "hello", "hello", str, id="string"),
        pytest.param(StrVariable(name="s"), "", "", str, id="empty-string"),
        pytest.param(BoolVariable(name="b"), True, True, bool, id="bool-true"),
        pytest.param(BoolVariable(name="b"), False, False, bool, id="bool-false"),
        pytest.param(
            TorchScalarVariable(name="x"),
            torch.tensor(2.5),
            2.5,
            float,
            id="torch_to_float",
        ),
        pytest.param(
            EnumVariable(name="e", options=["x", "y", "z"]),
            "x",
            "x",
            str,
            id="enum_name",
        ),
        pytest.param(
            EnumVariable(name="e", options=["x", "y", "z"]),
            2,
            "z",
            str,
            id="enum_index",
        ),
    ],
)
def test_value_pack_unpack_roundtrip(
    variable: Variable,
    value: Any,
    expected: Any,
    expected_type: type,
) -> None:
    handler = find_variable_handler(type(variable))
    assert isinstance(handler, VariableHandler)
    type_ = handler.create_type(variable)

    unpacked = handler.unpack_value(variable, handler.pack_value(variable, type_, value))

    assert unpacked == expected
    assert isinstance(unpacked, expected_type)


@pytest.mark.parametrize(
    ("variable", "value", "expected"),
    [
        (
            NDVariable(name="arr", shape=(2, 3), dtype=np.float64),
            np.zeros((2, 3), dtype=np.float64),
            np.zeros((2, 3), dtype=np.float64),
        ),
        # TODO: make type-conversion behavior consistent, scalar handlers coerce but
        # array handlers do not
        # (NDVariable(name="arr", shape=(2, 3), dtype=np.float64),
        #  np.array(((2.45, 3.2),(1.0, 0.0)), dtype=np.float64),
        #  np.array(((2, 3),(1, 0)), dtype=np.int64),),
    ],
)
def test_numpy_array_roundtrip(
    variable: NDVariable,
    value: np.ndarray,
    expected: np.ndarray,
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None
    type_ = handler.create_type(variable)

    unpacked = handler.unpack_value(variable, handler.pack_value(variable, type_, value))

    assert unpacked.shape == expected.shape
    assert unpacked.shape == expected.shape
    assert unpacked.dtype is expected.dtype
    np.testing.assert_allclose(unpacked, expected)


@pytest.mark.parametrize(
    ("variable", "value", "expected"),
    [
        (
            TorchNDVariable(name="tarr", shape=(2, 3), dtype=torch.float32),
            torch.ones(2, 3, dtype=torch.float32),
            torch.ones(2, 3, dtype=torch.float32),
        ),
        (
            TorchNDVariable(name="tarr", shape=(2, 2, 3), dtype=torch.float32),
            torch.arange(0, 1.2, 0.1, dtype=torch.float32).reshape(2, 2, 3),
            torch.arange(0, 1.2, 0.1, dtype=torch.float32).reshape(2, 2, 3),
        ),
    ],
)
def test_torch_array_roundtrip(
    variable: NDVariable,
    value: torch.Tensor,
    expected: torch.Tensor,
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None
    type_ = handler.create_type(variable)

    unpacked = handler.unpack_value(variable, handler.pack_value(variable, type_, value))

    assert unpacked.shape == variable.shape
    assert unpacked.shape == expected.shape
    assert unpacked.dtype is expected.dtype
    torch.testing.assert_close(unpacked, expected)


@pytest.mark.parametrize(
    "variable",
    [
        pytest.param(ScalarVariable(name="x"), id="scalar"),
        pytest.param(IntVariable(name="i"), id="int"),
        pytest.param(BoolVariable(name="b"), id="bool"),
        pytest.param(StrVariable(name="s"), id="str"),
        pytest.param(NDVariable(name="nd", shape=(2, 2), dtype=np.int16), id="nd"),
        pytest.param(TorchScalarVariable(name="ts"), id="torchscalar"),
        pytest.param(EnumVariable(name="enum", options=["A", "B", "C"]), id="enum"),
    ],
)
def test_valid_p4p_type(variable: Variable) -> None:
    handler = find_variable_handler(type(variable))
    assert isinstance(handler.create_type(variable), Type)


# timestamp?  display/controls metadata mismatched?
# Enum variable metadata unset? (display)
@pytest.mark.parametrize(
    ("variable", "value", "ctrl_dict", "disp_dict", "alarm_dict"),
    [
        (
            ScalarVariable(name="x", value_range=(0.0, 10.0), unit="mm", default_value=5.0),
            3.5,
            {"limitLow": 0.0, "limitHigh": 10.0, "minStep": 0.0},
            {
                "limitLow": 0.0,
                "limitHigh": 10.0,
                "description": "",
                "format": "",
                "units": "mm",
            },
            {"severity": 0, "status": 0, "message": ""},
        ),
        # (
        #     EnumVariableHandler(),
        #     EnumVariable(name="enum", options=["A", "B", "C"]),
        #     "A",
        #     {'limitLow': 0.0, 'limitHigh': 10.0, 'minStep': 0.0},
        #     {'limitLow': 0.0, 'limitHigh': 0.0, 'description': '', 'format': '', 'units': 'mm'},
        #     {'severity': 0, 'status': 0, 'message': ''}
        # ),
    ],
)
def test_control_limits_and_units_metadata(
    variable, value, ctrl_dict, disp_dict, alarm_dict
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None
    type_ = handler.create_type(variable)

    packed = handler.pack_value(variable, type_, value)

    assert packed.display.todict() == disp_dict
    assert packed.control.todict() == ctrl_dict
    assert packed.alarm.todict() == alarm_dict


@pytest.mark.parametrize(
    (
        "variable",
        "value",
        "size",
    ),
    [
        (NDVariable(name="n", shape=(2, 3)), np.zeros((2, 3)), [2, 3]),
        (NDVariable(name="n", shape=(2, 2, 3)), np.zeros((2, 2, 3)), [2, 2, 3]),
        (TorchNDVariable(name="n", shape=(2, 3)), torch.zeros((2, 3)), [2, 3]),
        (
            TorchNDVariable(name="n", shape=(2,)),
            torch.zeros((2,)),
            [
                2,
            ],
        ),
    ],
)
def test_dimension_size_metadata(variable, value, size):
    handler = find_variable_handler(type(variable))
    assert handler is not None
    type_ = handler.create_type(variable)
    packed = handler.pack_value(variable, type_, value)

    assert [d["size"] for d in packed["dimension"]] == size
    assert packed["compressedSize"] == value.nbytes
    assert packed["uncompressedSize"] == value.nbytes


@pytest.mark.parametrize(
    (
        "variable",
        "expected_value",
    ),
    [
        (ScalarVariable(name="scalar", default_value=5.0), 5.0),
        (StrVariable(name="str", default_value="hi"), "hi"),
        (IntVariable(name="str", default_value=1), 1),
        (BoolVariable(name="bool", default_value=True), True),
        (EnumVariable(name="enum", options=["A", "B", "C"], default_value="B"), "B"),
        (EnumVariable(name="enum", options=["A", "B", "C"]), "A"),
        (TorchScalarVariable(name="torchscalar", default_value=1.0), 1.0),
        (TorchScalarVariable(name="torchscalar"), 0),
        # (NDVariable(name="nd", shape=(2, 3), dtype=np.int64), np.array(((1,2,3), (4,5,6))) ),
    ],
)
def test_default_value_passthrough(
    variable: Variable,
    expected_value: Any,
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None

    type_ = handler.create_type(variable)
    packed = handler.pack_value(variable, type_, None)

    # Assert
    assert handler.unpack_value(variable, packed) == expected_value


@pytest.mark.parametrize(
    (
        "variable",
        "expected_value",
    ),
    [
        (
            NDVariable(
                name="nd",
                shape=(2, 3),
                dtype=np.int64,
                default_value=np.array(((1, 2, 3), (4, 5, 6))),
            ),
            np.array(((1, 2, 3), (4, 5, 6))),
        ),
        (
            NDVariable(name="nd", shape=(4, 5), dtype=np.int64),
            np.zeros((4, 5)),
        ),  # zero filled with no default
        # string arrays fail to roundtrip
        # (NDVariable(name="nd", shape=(2,), dtype=np.dtypes.StringDType(),),
        #  np.array(["", ""], dtype=np.dtypes.StringDType())),
        (
            TorchNDVariable(
                name="ndtorch",
                shape=(2, 3),
                dtype=torch.int64,
                default_value=torch.tensor(np.array(((1, 2, 3), (4, 5, 6)))),
            ),
            torch.tensor(np.array(((1, 2, 3), (4, 5, 6)))),
        ),
        (
            TorchNDVariable(
                name="ndtorch",
                shape=(5, 5),
                dtype=torch.int64,
            ),
            torch.zeros(5, 5),
        ),
    ],
)
def test_default_array_value_passthrough(
    variable: Variable,
    expected_value: Any,
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None

    type_ = handler.create_type(variable)
    packed = handler.pack_value(variable, type_, None)

    bool_mask = handler.unpack_value(variable, packed) == expected_value
    assert bool_mask.all()


@pytest.mark.parametrize(
    ("variable", "expected_default"),
    [
        pytest.param(ScalarVariable(name="scalar", default_value=5.0), 5.0, id="scalar"),
        pytest.param(IntVariable(name="int", default_value=2), 2, id="int"),
        pytest.param(BoolVariable(name="bool", default_value=True), True, id="bool"),
        pytest.param(StrVariable(name="str", default_value="hi"), "hi", id="str"),
        pytest.param(
            EnumVariable(name="enum", options=["A", "B", "C"], default_value="B"),
            "B",
            id="enum",
        ),
        pytest.param(
            TorchScalarVariable(name="torchscalar", default_value=1.0), 1.0, id="torchscalar"
        ),
    ],
)
def test_default_value_method_scalar_like(
    variable: Variable,
    expected_default: Any,
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None

    assert handler.default_value(variable) == expected_default


@pytest.mark.parametrize(
    ("variable", "expected_default"),
    [
        pytest.param(
            NDVariable(
                name="nd",
                shape=(2, 3),
                dtype=np.int64,
                default_value=np.array(((1, 2, 3), (4, 5, 6))),
            ),
            np.array(((1, 2, 3), (4, 5, 6))),
            id="nd",
        ),
        pytest.param(
            TorchNDVariable(
                name="ndtorch",
                shape=(2, 3),
                dtype=torch.int64,
                default_value=torch.tensor(np.array(((1, 2, 3), (4, 5, 6)))),
            ),
            torch.tensor(np.array(((1, 2, 3), (4, 5, 6)))),
            id="torchnd",
        ),
    ],
)
def test_default_value_method_array_like(
    variable: Variable,
    expected_default: Any,
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None

    actual = handler.default_value(variable)
    bool_mask = actual == expected_default
    assert bool_mask.all()


@pytest.mark.parametrize(
    "variable",
    [
        pytest.param(ScalarVariable(name="scalar", default_value=5.0), id="scalar"),
        pytest.param(IntVariable(name="int", default_value=2), id="int"),
        pytest.param(BoolVariable(name="bool", default_value=True), id="bool"),
        pytest.param(StrVariable(name="str", default_value="hi"), id="str"),
        pytest.param(
            EnumVariable(name="enum", options=["A", "B", "C"], default_value="B"),
            id="enum",
        ),
        pytest.param(TorchScalarVariable(name="torchscalar", default_value=1.0), id="torchscalar"),
        pytest.param(
            NDVariable(
                name="nd",
                shape=(2, 3),
                dtype=np.int64,
                default_value=np.array(((1, 2, 3), (4, 5, 6))),
            ),
            id="nd",
        ),
        pytest.param(
            TorchNDVariable(
                name="ndtorch",
                shape=(2, 3),
                dtype=torch.int64,
                default_value=torch.tensor(np.array(((1, 2, 3), (4, 5, 6)))),
            ),
            id="torchnd",
        ),
    ],
)
def test_pack_value_none_matches_default_value(variable: Variable) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None

    type_ = handler.create_type(variable)
    packed = handler.pack_value(variable, type_, None)
    unpacked = handler.unpack_value(variable, packed)
    expected = handler.default_value(variable)

    if isinstance(expected, np.ndarray):
        np.testing.assert_array_equal(unpacked, expected)
    elif isinstance(expected, torch.Tensor):
        torch.testing.assert_close(unpacked, expected)
    else:
        assert unpacked == expected


@pytest.mark.parametrize(
    ("variable"),
    [
        pytest.param(
            ScalarVariable(name="x", value_range=(0.0, 10.0), default_value=5.0),
            id="scalarvar",
        ),
        pytest.param(
            TorchScalarVariable(name="x", value_range=(0.0, 10.0), default_value=5.0),
            id="torchscalarvar",
        ),
    ],
)
@pytest.mark.parametrize(
    ("value", "expected_severity", "expected_status"),
    [
        pytest.param(
            3.5,
            epicsAlarmSeverity.NO_ALARM,
            epicsAlarmStatus.NO_STATUS,
            id="within_range",
        ),
        pytest.param(
            0.0,
            epicsAlarmSeverity.NO_ALARM,
            epicsAlarmStatus.NO_STATUS,
            id="at_lower_boundary",
        ),
        pytest.param(
            10.0,
            epicsAlarmSeverity.NO_ALARM,
            epicsAlarmStatus.NO_STATUS,
            id="at_upper_boundary",
        ),
        pytest.param(
            -1.0,
            epicsAlarmSeverity.MAJOR_ALARM,
            epicsAlarmStatus.DRIVER_STATUS,
            id="below_range",
        ),
        pytest.param(
            11.0,
            epicsAlarmSeverity.MAJOR_ALARM,
            epicsAlarmStatus.DRIVER_STATUS,
            id="above_range",
        ),
    ],
)
def test_alarm_metadata_from_value_range(
    variable: ScalarVariable | TorchScalarVariable,
    value: float,
    expected_severity: epicsAlarmSeverity,
    expected_status: epicsAlarmStatus,
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None
    type_ = handler.create_type(variable)

    packed = handler.pack_value(variable, type_, value)

    assert packed["alarm"]["severity"] == int(expected_severity)
    assert packed["alarm"]["status"] == int(expected_status)


@pytest.mark.parametrize(
    ("variable", "value", "expected_val", "expected_type"),
    [
        (ScalarVariable(name="s"), np.float64(2.5), 2.5, float),
        (IntVariable(name="i"), 2.5, 2, int),
        (
            NDVariable(name="n", shape=(2, 3)),
            np.arange(6, dtype=np.float64).reshape(2, 3),
            [0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
            list,
        ),
    ],
)
def test_value_to_native(
    variable: Variable,
    value: Any,
    expected_val: Any,
    expected_type: Any,
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None
    native = handler.value_to_native(variable, value)

    assert native == expected_val
    assert isinstance(native, expected_type)


@pytest.mark.parametrize(
    ("variable", "value", "expected_exception"),
    [
        pytest.param(
            ScalarVariable(name="x"),
            "not-a-number",
            TypeError,
            id="non_numeric_value",
        ),
        pytest.param(
            TorchScalarVariable(name="x"),
            "not-a-torch-number",
            TypeError,
            id="non_torch_numeric_value",
        ),
        pytest.param(
            ScalarVariable(
                name="strict",
                value_range=(0.0, 10.0),
                default_validation_config="error",
            ),
            50.0,
            ValueError,
            id="out_of_range",
        ),
        pytest.param(
            BoolVariable(name="b"),
            "not-a-bool",
            TypeError,
            id="non_bool_value",
        ),
        pytest.param(
            StrVariable(name="s"),
            123,
            TypeError,
            id="non_str_value",
        ),
        pytest.param(
            EnumVariable(name="x", options=["A", "B", "C"]),
            "not-an-option",
            ValueError,
            id="invalid_option",
        ),
        pytest.param(
            NDVariable(name="arr", shape=(2, 3)),
            np.zeros((3, 3), dtype=np.float64),
            ValueError,
            id="wrong_arr_shape",
        ),
        pytest.param(
            NDVariable(name="arr", shape=(2, 3)),
            np.zeros((2, 3), dtype=np.int32),
            ValueError,
            id="wrong_dtype",
        ),
        pytest.param(
            NDVariable(name="arr", shape=(2, 3)),
            [[1, 2, 3], [4, 5, 6]],
            TypeError,
            id="not_array",
        ),
    ],
)
def test_raise_packing_invalid_value(
    variable: Variable,
    value: Any,
    expected_exception: type[Exception],
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None
    type_ = handler.create_type(variable)

    with pytest.raises(expected_exception):
        handler.pack_value(variable, type_, value)


@pytest.mark.parametrize(
    ("variable", "expected_spec"),
    [
        pytest.param(
            StrVariable(name="s"),
            {"type": "char", "count": 1024},
            id="string_waveform",
        ),
        pytest.param(BoolVariable(name="b"), {}, id="bool_no_extras"),
        pytest.param(
            ScalarVariable(name="x"),
            {
                "unit": None,
                "type": "float",
                "lolim": 0,
                "hilim": 0,
                "lolo": 0,
                "hihi": 0,
            },
            id="scalar_no_extras",
        ),
        pytest.param(TorchScalarVariable(name="x"), {}, id="tscalar_no_extras"),
        pytest.param(
            TorchNDVariable(name="x", shape=(2, 3)),
            {"count": 6, "type": "float"},
            id="tnd",
        ),
        pytest.param(
            NDVariable(name="x", shape=(2, 3)),
            {"count": 6, "type": "float"},
            id="nd_no_extras",
        ),
        pytest.param(
            EnumVariable(name="e", options=["A", "B"]),
            {
                "type": "enum",
                "enums": ["A", "B"],
            },
            id="enum_mbbi",
        ),
    ],
)
def test_ca_pvspec(
    variable: Variable,
    expected_spec: dict[str, Any],
) -> None:
    handler = find_variable_handler(type(variable))
    assert handler is not None

    assert handler.ca_pvspec(variable) == expected_spec


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (np.float64, True),
        (np.float32, True),
        (np.int16, True),
        (np.int32, True),
        (np.int64, True),
        (np.uint16, True),
        (np.uint32, True),
        (np.uint64, True),
        (np.str_, True),
        (np.dtypes.StringDType(), True),
        (np.complex128, False),
    ],
    ids=str,
)
def test_handler_report_dtype_support(dtype: type[np.generic], expected: bool) -> None:
    var = NDVariable(name="v", shape=(2,), dtype=np.dtype(dtype))
    handler = find_variable_handler(NDVariable)
    assert handler is not None

    assert handler.is_supported(var) is expected


@pytest.mark.parametrize(
    ("variable_type", "expected_handler_type"),
    [
        (ScalarVariable, ScalarVariableHandler),
        (IntVariable, ScalarVariableHandler),
        (NDVariable, NDVariableHandler),
        (TorchScalarVariable, TorchScalarVariableHandler),
        (TorchNDVariable, NDVariableHandler),
        (BoolVariable, SimpleScalarHandler),
        (StrVariable, SimpleScalarHandler),
        (EnumVariable, EnumVariableHandler),
    ],
)
def test_should_return_matching_handler_for_each_variable_type(
    variable_type: type[Variable],
    expected_handler_type: type[VariableHandler],
) -> None:
    handler = find_variable_handler(variable_type)

    assert isinstance(handler, expected_handler_type)
    assert isinstance(handler, VariableHandler)


def test_should_return_none_for_unknown_type() -> None:
    assert find_variable_handler(dict) is None


@pytest.mark.parametrize(
    ("variable_type", "expected_handler_type"),
    [
        (DerivedScalarVariable, ScalarVariableHandler),
        (DerivedBoolVariable, SimpleScalarHandler),
    ],
)
def test_should_resolve_handler_for_variable_subclasses(
    variable_type: type[Variable],
    expected_handler_type: type[VariableHandler],
) -> None:
    handler = find_variable_handler(variable_type)

    assert isinstance(handler, expected_handler_type)
    assert isinstance(handler, VariableHandler)
