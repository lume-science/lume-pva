from abc import ABC, abstractmethod
from lume.variables import Variable, ScalarVariable, NDVariable
from typing import Any, Dict
from p4p import Type, Value
from p4p.nt import NTScalar, NTNDArray
from lume_pva.epics import epicsAlarmSeverity, epicsAlarmStatus
from numpy import ndarray
import numpy as np

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
    """Variable handler for LUME ScalarVariable type"""

    def create_type(self, variable: ScalarVariable) -> Type:
        return NTScalar.buildType('d', control=True, display=True)

    def pack_value(self, variable: ScalarVariable, type: Type, value: float | None) -> Value:
        if value is None: # Use default if not provided
            if variable.default_value is None:
                value = 0
            else:
                value = variable.default_value

        v = Value(
            type, {'value': value}
        )

        if variable.value_range is not None:
            v['control']['limitLow'] = variable.value_range[0]
            v['control']['limitHigh'] = variable.value_range[1]

        if variable.unit is not None:
            v['display']['units'] = variable.unit

        # This should arguably be moved somewhere else..but since value_range is specific to
        # variable types, we pretty much have to handle it here.
        # TODO: Could detect presence of limitLow/limitHigh in common code, and set based on that
        if variable.value_range is not None:
            if value < variable.value_range[0]:
                v['alarm']['severity'] = int(epicsAlarmSeverity.MAJOR_ALARM)
                v['alarm']['status'] = int(epicsAlarmStatus.DRIVER_STATUS)
            elif value > variable.value_range[1]:
                v['alarm']['severity'] = int(epicsAlarmSeverity.MAJOR_ALARM)
                v['alarm']['status'] = int(epicsAlarmStatus.DRIVER_STATUS)
            else:
                v['alarm']['severity'] = int(epicsAlarmSeverity.NO_ALARM)
                v['alarm']['status'] = int(epicsAlarmStatus.NO_STATUS)

        return v

    def unpack_value(self, variable: ScalarVariable, value: Value) -> float:
        return float(value['value'])


class NDVariableHandler(VariableHandler):
    """Variable handler for LUME NDVariable type"""

    def _typecode(self, variable: NDVariable) -> str:
        match variable.dtype:
            case np.float64:
                return 'doubleValue'
            case np.float32:
                return 'floatValue'
            case np.byte:
                return 'byteValue'
            case np.bool:
                return 'booleanValue'
            case np.int16:
                return 'shortValue'
            case np.int32:
                return 'intValue'
            case np.int64:
                return 'longValue'
            case np.ubyte:
                return 'ubyteValue'
            case np.uint16:
                return 'ushortValue'
            case np.uint32:
                return 'uintValue'
            case np.uint64:
                return 'ulongValue'
            case _:
                raise TypeError(f'Unsupported numpy type {variable.dtype}')

    def create_type(self, variable: NDVariable) -> Type:
        return NTNDArray.buildType()
    
    def pack_value(self, variable: NDVariable, type: Type, value: ndarray | None) -> Value:
        if value is None: # Use default if not provided
            if variable.default_value is not None:
                value = variable.default_value
            else:
                value = np.zeros(shape=variable.shape, dtype=variable.dtype)

        v = Value(
            type, {'value': (self._typecode(variable), value.flatten())}
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

    def unpack_value(self, variable: NDVariable, value: Value) -> ndarray:
        arr = value['value']
        if isinstance(arr, np.ndarray):
            return arr.reshape(variable.shape)
        else:
            raise ValueError(f'Internal error: invalid value type {type(arr)}')


def find_variable_handler(type) -> VariableHandler | None:
    VARIABLE_HANDLERS = {
        ScalarVariable: ScalarVariableHandler(),
        NDVariable: NDVariableHandler(),
    }
    return VARIABLE_HANDLERS.get(type, None)