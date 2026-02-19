# EPICS related enums and definitions
from enum import IntEnum

class epicsAlarmSeverity(IntEnum):
    """Must match epicsAlarmSeverity values from alarm.h"""
    NO_ALARM = 0
    MINOR_ALARM = 1
    MAJOR_ALARM = 2
    INVALID_ALARM = 3

class epicsAlarmStatus(IntEnum):
    """Must match EPICS alarm status defined in NT paper"""
    NO_STATUS = 0
    DEVICE_STATUS = 1
    DRIVER_STATUS = 2
    RECORD_STATUS = 3
    DB_STATUS = 4
    CONF_STATUS = 5
    UDF_STATUS = 6
    CLIENT_STATUS = 7