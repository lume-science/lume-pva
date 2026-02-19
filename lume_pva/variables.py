from abc import ABC, abstractmethod
from lume.variables import Variable, ScalarVariable
from typing import Any, Dict
from p4p import Type, Value
from p4p.nt import NTScalar
from lume_pva.epics import epicsAlarmSeverity, epicsAlarmStatus


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
    """Variable handler for LUME ScalarVariables"""

    def create_type(self, variable: ScalarVariable) -> Type:
        return NTScalar.buildType('d', control=True)

    def pack_value(self, variable: ScalarVariable, type: Type, value: float | None) -> Value:
        if value is None: # Use default if not provided
            value = variable.default_value

        v = Value(
            type, {'value': value}
        )

        if variable.value_range is not None:
            v['control']['limitLow'] = variable.value_range[0]
            v['control']['limitHigh'] = variable.value_range[1]

        # This should arguably be moved somewhere else. Since value_range is specific to
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


def find_variable_handler(type) -> VariableHandler | None:
    VARIABLE_HANDLERS = {
        ScalarVariable: ScalarVariableHandler(),
    }
    return VARIABLE_HANDLERS.get(type, None)