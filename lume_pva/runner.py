from lume.model import LUMEModel, Variable
from lume.variables import Variable
from p4p.server import ServerOperation
from p4p.server.thread import SharedPV
from p4p.client.thread import Subscription, Disconnected, RemoteError, Cancelled
from p4p.nt import NTScalar
from p4p import Value, Type
from typing import Any, Dict, Callable, TypedDict
from queue import Queue
from enum import IntEnum
from lume_pva.variables import VariableHandler, find_variable_handler
import time
import math
import p4p.server
import p4p.client.thread
import p4p.nt
import logging
import pvua

LOG = logging.getLogger('LumePva')

VALID_PV_MODES = ['rw', 'ro', 'remote']
VALID_MODEL_MODES = ['continuous', 'snapshot']

DEFAULT_MODEL_MODE = 'continuous'
DEFAULT_PV_MODE = 'rw'

class RunnerConfig(TypedDict):
    """
    Attributes
    ----------
    remote_model_mode : str
        Remote model mode. Determines behavior of this model's remote PVs.
        - 'continuous': Remote input PVs are updated continuously with PV monitors, and model is evaluated on change.
        - 'snapshot': Remote input PVs are only updated when the 'SNAPSHOT' PV is poked.
        Default is 'continuous'
    prefix : str
        Additional prefix to append to PV names. May be None if you don't need any.
        Remote PVs are unaffected by this setting, this only applies to PVs we are serving.
    variables : Dict[str, RunnerVariable]
        List of model variables
    """
    class RunnerVariable(TypedDict):
        """
        Attributes
        ----------
        name : str
            Name of the input or output. Must match one of the model's supported variables.
        pv : str
            Name of the PV to serve or consume. If not provided, it will be defaulted to 'name'
        mode : str
            Operation mode of the PV. May be one of:
            - 'ro': Read-only PV served by this server
            - 'rw': Read-write PV served by this server. Errors if Variable.read_only
            - 'remote': Remote PV living on some other remote machine.
            Default is 'rw'
        """
        name: str
        pv: str
        mode: str

    remote_model_mode: str
    prefix: str
    variables: Dict[str, RunnerVariable]

class Runner:
    """Simple runner for LUMEModel derived models"""
    
    pvs: Dict[str, SharedPV]
    pv_handlers: Dict[str, VariableHandler]
    # List of all output PVs that need to be updated after simulation
    outputs: list[str]
    values: Dict[str, Value]
    subs: Dict[str, Subscription]
    
    class Handler:
        """
        Handles PUT and RPC requests to a specific PV
        """

        model: LUMEModel
        variable: Variable

        def __init__(self, variable: Variable, runner, read_only: bool):
            self.model = runner.model
            self.variable = variable
            self.runner = runner
            self.ro = read_only

        def put(self, pv: SharedPV, op: ServerOperation):
            if self.ro:
                op.done(error='Read only PV')
            else:
                # Update PVs in simulator
                self.runner.queue.put({self.variable.name: op.value()})
                pv.post(op.value())
                op.done()

        def rpc(self, op: ServerOperation):
            op.done()

    def __init__(
            self, 
            model: LUMEModel,
            prefix='',
            config: RunnerConfig | None = None,
        ):
        """
        Init a Runner for the specified model
        
        Parameters
        ----------
        model : LUMEModel
            A LUMEModel object implementing the LUMEModel interface
        prefix: str
            Prefix to append to PV names. Only applies to PVs served by the Runner
        config: RunnerConfig|None
            Configuration for this runner. If 'None' a default configuration is generated.
            Note that you may call Runner.generate_config yourself to get+modify a configuration.
            Overrides the 'prefix' parameter.
        """
        self.model = model
        self.pvs = {}
        self.pv_handlers = {}
        self.queue = Queue()
        self.new_values = {}
        self.outputs = []
        self.types = {}
        self.subs = {}
        self.context = p4p.client.thread.Context()
        self.providers = {} # Just for renaming
        self.snapshot_pvs = []
        self.pv_to_var = {} # Map pv name -> variable name
        self.pvua_context = pvua.Context()

        # Generate default config
        if config is None:
            config = self.generate_config(model, prefix)
        self._config = config

        # Validate some configuration options
        if config.get('remote_model_mode', DEFAULT_MODEL_MODE) not in VALID_MODEL_MODES:
            raise KeyError(f'Model has invalid model mode {config["remote_model_mode"]}. Must be one of {VALID_MODEL_MODES}')

        # Setup PVs
        for c in self.config['variables'].values():
            # Set default PV name if not provided
            if 'pv' not in c:
                c['pv'] = c['name']
            pv = c['pv']

            # Validate some other things first
            if c['name'] not in self.model.supported_variables:
                raise KeyError(f'Variable "{c["name"]}" not found in model variables')
            if 'mode' in c and c['mode'] not in VALID_PV_MODES:
                raise KeyError(f'Variable "{c["name"]} has invalid mode "{c["mode"]}". Must be one of {VALID_PV_MODES}')

            # Lookup variable based on name
            var = self.model.supported_variables[c['name']]

            # Determine a default mode, if there is none
            if 'mode' not in c:
                c['mode'] = 'ro' if var.read_only else 'rw'

            # Validate r/w setting
            if c['mode'] == 'rw' and var.read_only:
                raise ValueError(f'Variable {c["name"]} was configured with read-write permissions, but the variable is read-only')

            handler = find_variable_handler(type(var))
            if handler is None:
                raise RuntimeError(f'Unknown type "{type(var)}"')

            # Cache handler and type for later
            self.pv_handlers[var.name] = handler
            self.types[var.name] = handler.create_type(var)

            self.pv_to_var[pv] = var.name

            if c['mode'] in ['ro', 'rw']:
                # Generate a PV to be served
                self._add_pv(
                    pv,
                    var,
                    ro=c['mode'] == 'ro',
                    prefix=self.config.get('prefix', '')
                )
            else:
                # Create a client monitor
                self._add_client(
                    pv,
                    var,
                    monitor = self.config['remote_model_mode'] == 'continuous' # Use monitor if in continuous mode
                )

        # Create an informational PV (i.e. including list of variables, etc.)
        self._create_model_info()

        # Create additional control PVs
        self._create_control_pvs()

        # Start the server
        self.server = p4p.server.Server(providers=[self.providers])

    @staticmethod
    def generate_config(
        model: LUMEModel,
        prefix: str = '',
        remote_inputs: bool = False,
    ) -> RunnerConfig:
        """
        Generate a configuration for the specified model.

        Parameters
        ----------
        model : LUMEModel
            Instance of a LUMEModel object
        prefix : str
            PV name prefix
        remote_inputs : bool
            When true, model inputs (values not marked as rw) are configured as monitors for remote variables

        Returns
        -------
        RunnerConfig :
            A new configuration built based on the supplied parameters. May be tweaked as you wish before
            passing to the Runner() constructor.
        """
        config = {
            'remote_model_mode': 'continuous',
            'prefix': prefix,
            'variables': {}
        }
        for k, v in model.supported_variables.items():
            mode = 'ro' if v.read_only else 'rw'
            if remote_inputs and not v.read_only:
                mode = 'remote'
            config['variables'][k] = {
                'name': k,
                'pv': k,
                'mode': mode,
            }
        return config

    def _add_pv(self, pv: str, var: Variable, ro: bool, prefix: str) -> None:
        """
        Create a new PV

        Parameters
        ----------
        pv : str
            Name of the PV
        var : Variable
            LUME variable this PV is implementing
        ro : bool
            True if read-only
        prefix : str
            String to prefix the PV name with
        """
        pvobj = SharedPV(
            handler=Runner.Handler(
                variable=var,
                runner=self,
                read_only=ro
            ),
            initial=self._generate_value(pv, None)
        )
        self.pvs[var.name] = pvobj
        self.providers[f'{prefix}{pv}'] = pvobj

    def _add_client(self, pv: str, var: Variable, monitor: bool) -> bool:
        """Setup a new monitor for the specified PV"""
        if monitor:
            self.subs[pv] = self.context.monitor(
                pv,
                lambda x, k=pv: self._monitor_callback(k, x)
            )
        else:
            self.snapshot_pvs.append(pv)
        return True

    def _create_model_info(self):
        """Creates a model info PV"""
        pv = 'model_info'

        self.types[pv] = Type([
            ('class', 's'),
            ('variables', ('aS', None, [
                ('name', 's'),
                ('read_only', '?'),
                ('pvname', 's'),
            ]))
        ])

        val = Value(self.types[pv])
        val['class'] = str(type(self.model))

        vars = []
        for k, v in self.model.supported_variables.items():
            info = {
                'name': v.name,
                'read_only': v.read_only,
                'pvname': self.config['variables'][k]['pv']
            }
            vars.append(info)

        val['variables'] = vars

        self.pvs[pv] = SharedPV(
            initial=val
        )

    def _create_control_pvs(self):
        """Create any required control PVs"""
        pvname = f'{self.config["prefix"]}SNAPSHOT'
        if pvname in self.providers:
            raise RuntimeError(f'Fatal name conflict: {pvname} for the snapshot PV already exists!')

        self.providers[pvname] = SharedPV(
            initial=NTScalar('d').wrap(0)
        )

        @self.providers[pvname].put
        def onPut(pv, op):
            self.take_snapshot()
            op.done()

        return None

    def take_snapshot(self) -> None:
        """
        Take a snapshot of the remote PVs, and simulate the model
        """
        LOG.debug(f'Snapshot taken for PVs: {self.snapshot_pvs}')
        new_values = {}
        for pv in self.snapshot_pvs:
            new_values[self.pv_to_var[pv]] = self.pvua_context.get(pv)
        self.queue.put(new_values)

    def _monitor_callback(self, pv: str, param):
        """Callback from p4p monitor updates"""
        if isinstance(param, Value) or hasattr(param, 'raw'):
            unpacked = self.pv_handlers[pv].unpack_value(
                self.model.supported_variables[pv],
                param.raw
            )
            self.queue.put({pv: param.raw})
        else:
            print(type(param))

    def _generate_value(self, pv: str, value: Any | None) -> Value:
        """
        Generates a new value for posting to the PV.
        Handles alarm updates, timestamp updates, and generating the value in the first place. This handles the
        'common' metadata that the variable handlers shouldn't need to handle.
        """
        v = self.pv_handlers[pv].pack_value(
            self.model.supported_variables[pv],
            self.types[pv],
            value
        )

        # Ensure timestamp is current
        self._update_timestamp(v)
        return v

    def _update_timestamp(self, value: Value, ts=None) -> None:
        """
        Helper to update timestamp on a value
        """
        if ts is None:
            ts = time.time()
        value['timeStamp']['nanoseconds'] = math.fmod(ts, 1.0) * 1e9
        value['timeStamp']['secondsPastEpoch'] = int(ts)

    @property
    def config(self) -> RunnerConfig:
        """Access the underlying config"""
        return self._config

    def run(self):
        """
        Runs the simulation, blocks forever.
        Dequeues PV updates from the updater thread, sets values on the model, and updates outputs.
        """
        while True:
            up = self.queue.get()
            new_values = {}
            for k, v in up.items():
                # If needed, unpack value and add it to the new list of PVs
                if isinstance(v, Value):
                    new_values[k] = self.pv_handlers[k].unpack_value(
                        self.model.supported_variables[k],
                        v
                    )
                else:
                    new_values[k] = v

            # Set and simulate
            self.model.set(new_values)

            # Get new simulated values
            out_values = self.model.get(self.model.supported_variables)

            # Update output PVs with new values
            for k, v in out_values.items():
                LOG.debug(f'Post: {k} -> {v}')
                
                # Only post to outputs
                if not self.model.supported_variables[k].read_only:
                    continue
                
                self.pvs[k].post(
                    self._generate_value(
                        k, v
                    )
                )
