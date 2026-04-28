from abc import ABC, abstractmethod
from lume.variables import Variable, ScalarVariable, NDVariable
from lume_torch.variables import TorchScalarVariable, TorchNDVariable
from typing import Any, Dict
from p4p import Type, Value
from p4p.nt import NTScalar, NTNDArray
from lume_pva.epics import epicsAlarmSeverity, epicsAlarmStatus
from numpy import ndarray
import numpy as np
import torch

class VariableHandler(ABC):
    """Base class for all variable type handlers"""
    def __init__(self):
        pass
    
    def create_type(self, variable: Variable) -> Type:
        """
        Creates p4p Type describing the Variable
        
        Parameters
        ----------
        variable : Variable
            The variable
        
        Returns
        -------
        Type :
            A p4p type describing the variable's value and other properties
        """
        raise NotImplementedError()

    @abstractmethod
    def pack_value(self, variable: Variable, type: Type, value: Any | None) -> Value:
        """
        Generates a p4p Value type off of a Variable and its associated value
        
        Parameters
        ----------
        variable : Variable
            The variable. Must be a subclass of Variable
        value : Any | None
            The value. If None is specified, the default of the variable should be used instead.
        
        Returns
        -------
        Value :
            A fully constructed Value() type that may be posted by p4p
        """
        raise NotImplementedError()

    @abstractmethod
    def unpack_value(self, variable: Variable, value: Value) -> Any:
        """
        Unpacks a p4p Value into the native Python type
        
        Parameters
        ----------
        variable : Variable
            The variable. Must be a subclass of Variable
        value : Value
            The value to unpack

        Returns
        -------
        Any :
            The unpacked value
        """
        raise NotImplementedError()

class ScalarVariableHandler(VariableHandler):
    """Variable handler for LUME ScalarVariable, and the TorchScalarVariable type"""

    ScalarType = int | float | np.floating

    @staticmethod
    def set_metadata(variable: Variable, v: Value, value: Any) -> None:
        """
        Sets control, display and alarm metadata on the value
        """
        value_range = getattr(variable, 'value_range', None)
        if value_range is not None:
            v['control']['limitLow'] = value_range[0]
            v['control']['limitHigh'] = value_range[1]

        unit = getattr(variable, 'unit')
        if unit is not None:
            v['display']['units'] = unit

        # This should arguably be moved somewhere else..but since value_range is specific to
        # variable types, we pretty much have to handle it here.
        # TODO: Could detect presence of limitLow/limitHigh in common code, and set based on that
        if value_range is not None:
            if value < value_range[0]:
                v['alarm']['severity'] = int(epicsAlarmSeverity.MAJOR_ALARM)
                v['alarm']['status'] = int(epicsAlarmStatus.DRIVER_STATUS)
            elif value > value_range[1]:
                v['alarm']['severity'] = int(epicsAlarmSeverity.MAJOR_ALARM)
                v['alarm']['status'] = int(epicsAlarmStatus.DRIVER_STATUS)
            else:
                v['alarm']['severity'] = int(epicsAlarmSeverity.NO_ALARM)
                v['alarm']['status'] = int(epicsAlarmStatus.NO_STATUS)


    def create_type(self, variable: ScalarVariable) -> Type:
        return NTScalar.buildType('d', control=True, display=True)

    def pack_value(self, variable: ScalarVariable, type_: Type, value: ScalarType | None) -> Value:
        if value is None: # Use default if not provided
            if variable.default_value is None:
                value = 0
            else:
                value = variable.default_value

        if not isinstance(value, (float, int, np.floating)):
            raise ValueError(f'ScalarVariable {variable.name} expects float, int or np.floating, but got {type(value)}')

        v = Value(
            type_, {'value': float(value)}
        )
        self.set_metadata(variable, v, value)
        return v

    def unpack_value(self, variable: ScalarVariable, value: Value) -> float:
        return float(value['value'])


class NDVariableHandler(VariableHandler):
    """Variable handler for LUME NDVariable type"""

    def _typecode(self, variable: NDVariable) -> str:
        match variable.dtype:
            case np.float64 | torch.float64:
                return 'doubleValue'
            case np.float32 | torch.float32:
                return 'floatValue'
            case np.byte | torch.int8:
                return 'byteValue'
            case np.bool | torch.bool:
                return 'booleanValue'
            case np.int16 | torch.int16:
                return 'shortValue'
            case np.int32 | torch.int32:
                return 'intValue'
            case np.int64 | torch.int64:
                return 'longValue'
            case np.ubyte | torch.uint8:
                return 'ubyteValue'
            case np.uint16 | torch.uint16:
                return 'ushortValue'
            case np.uint32 | torch.uint32:
                return 'uintValue'
            case np.uint64 | torch.uint64:
                return 'ulongValue'
            case np.str_:
                return 'stringValue'
            case _:
                raise TypeError(f'Unsupported numpy type {variable.dtype}')

    def create_type(self, variable: NDVariable) -> Type:
        # NTNDArray (per the NT spec) does not support string[] as a value type. We'll deviate from the standard a bit here
        if variable.dtype in [np.str_]:
            extras = [
                ('value', ('U', None, [
                    ('stringValue', 'as')
                ])),
            ]
            return Type(extras, base=NTNDArray.buildType())
        else:
            return NTNDArray.buildType()
    
    def pack_value(self, variable: NDVariable | TorchNDVariable, type_: Type, value: ndarray | torch.Tensor | None) -> Value:
        if value is None: # Use default if not provided
            if variable.default_value is not None:
                value = variable.default_value
            elif isinstance(variable, TorchNDVariable):
                value = torch.zeros(size=variable.shape, dtype=variable.dtype)
            elif isinstance(variable, NDVariable):
                value = np.zeros(shape=variable.shape, dtype=variable.dtype)

        if not isinstance(value, ndarray):
            raise ValueError(f'NDVariable expectes an ndarray, but got {type(value)}')

        # Convert to numpy type for p4p's sake
        if isinstance(variable, TorchNDVariable):
            value = value.numpy()

        v = Value(
            type_, {'value': (self._typecode(variable), value.flatten())}
        )

        v['compressedSize'] = value.nbytes
        v['uncompressedSize'] = value.nbytes
        
        v['dimension'] = [{
                'size': dim,
                'fullSize': dim, # No compression
                'binning': 1,
                'reverse': False,
                'offset': 0
        } for dim in variable.shape]

        return v

    def unpack_value(self, variable: NDVariable | TorchNDVariable, value: Value) -> ndarray | torch.Tensor:
        arr = value['value']
        if isinstance(arr, np.ndarray):
            if isinstance(variable, TorchNDVariable):
                return torch.reshape(torch.from_numpy(arr), variable.shape)
            else:
                return arr.reshape(variable.shape)
        else:
            raise ValueError(f'Internal error: invalid value type {type(arr)}')

class TorchScalarVariableHandler(VariableHandler):
    """Handler for TorchScalarVariable"""

    TorchScalarType = torch.Tensor | float | int

    def create_type(self, variable: TorchScalarVariable) -> Type:
        return NTScalar.buildType('d', control=True, display=True)

    def pack_value(self, variable: TorchScalarVariable, type_: Type, value: TorchScalarType | None) -> Value:
        if value is None: # Use default if not provided
            if variable.default_value is None:
                value = 0
            else:
                value = variable.default_value

        if not isinstance(value, (torch.Tensor, float, int)):
            raise ValueError(f'ScalarVariable {variable.name} expects torch.Tensor, int or float, but got {type(value)}')

        v = Value(
            type_, {'value': float(value)}
        )
        ScalarVariableHandler.set_metadata(variable, v, float(value))
        return v

    def unpack_value(self, variable: ScalarVariable, value: Value) -> float:
        return float(value['value'])


def find_variable_handler(type) -> VariableHandler | None:
    VARIABLE_HANDLERS = {
        ScalarVariable: ScalarVariableHandler(),
        NDVariable: NDVariableHandler(),
        TorchScalarVariable : TorchScalarVariableHandler(),
        TorchNDVariable: NDVariableHandler(),
    }
    return VARIABLE_HANDLERS.get(type, None)