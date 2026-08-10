# EPICS environment for local lume-pva development
# Source this in every terminal:

# Client-side: where to search for PVs
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO

# Server-side: where to send CA beacons (suppresses "Network is unreachable" warnings)
export EPICS_CAS_BEACON_ADDR_LIST=127.0.0.1
export EPICS_CAS_AUTO_BEACON_ADDR_LIST=NO

