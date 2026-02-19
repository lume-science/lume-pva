from lume.model import LUMEModel, Variable
from lume.variables import Variable
from p4p.server import ServerOperation
from p4p.server.thread import SharedPV
from p4p.client.thread import Subscription, Disconnected, RemoteError, Cancelled
from p4p import Value, Type
from typing import Any, Dict, Callable
from queue import Queue
from lume_pva.variables import VariableHandler, find_variable_handler
import time
import math
import p4p.server
import p4p.client.thread
import p4p.nt
import logging

LOG = logging.getLogger('LumePva')

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

        def __init__(self, variable: Variable, runner):
            self.model = runner.model
            self.variable = variable
            self.runner = runner

        def put(self, pv: SharedPV, op: ServerOperation):
            if self.variable.read_only:
                op.done(error='Read only PV')
            else:
                # Update PVs in simulator
                self.runner.queue.put({self.variable.name: op.value()})
                pv.post(op.value())
                op.done()

        def rpc(self, op: ServerOperation):
            op.done()

    def default_renamer(name: str, variable: Variable):
        """Default PV name transformer that does nothing"""
        return name

    def __init__(
            self, 
            model: LUMEModel,
            client=False,
            prefix='',
            renamer: Callable[[str, Variable], str] = default_renamer
        ):
        """
        Init a Runner for the specified model
        
        Parameters
        ----------
        model : LUMEModel
            A LUMEModel object implementing the LUMEModel interface
        client : bool
            If true, setup inputs as client monitors and update the model as these update
        prefix: str
            Prefix to append to PV names. Only applies to PVs served by the Runner
        renamer : Callable[[str], str]
            Callable object that renames the specified variable before creating a PV. This can be used to map
            variable names to different PV names.
            Renamers are run for all input and output variables.
            For example, you can uppercase all served variables with:
            
            def myrenamer(name):
                return name.upper()

            If input_prefix or output_prefix is also specified, they will be applied on top of the renamer.

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

        # Build a list of PVs
        for k, v in model.supported_variables.items():
            handler = find_variable_handler(type(v))
            if handler is None:
                raise RuntimeError(f'Unknown type "{type(v)}"')

            self.pv_handlers[k] = handler
            self.types[k] = handler.create_type(v)

            # This is asked to be created as a remote variable
            # TODO: Probably need a better way to determine this
            if client and not v.read_only:
                self.subs[k] = self.context.monitor(
                    renamer(k, v),
                    lambda x, k=k: self._monitor_callback(k, x)
                )
                continue

            # Create a PV for the variable
            pvobj = SharedPV(
                handler=Runner.Handler(variable=v, runner=self),
                initial=self._generate_value(k, None)
            )
            self.pvs[k] = pvobj
            self.providers[f'{prefix}{renamer(k, v)}'] = pvobj

        self._create_model_info()

        self.server = p4p.server.Server(providers=[self.providers])

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
                'pvname': v.name, # TODO
            }
            vars.append(info)

        val['variables'] = vars

        self.pvs[pv] = SharedPV(
            initial=val
        )

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

    def run(self):
        """
        Runs the simulation, blocks forever.
        Dequeues PV updates from the updater thread, sets values on the model, and updates outputs.
        """
        while True:
            up = self.queue.get()
            for k, v in up.items():
                # Unpack value and add it to the new list of PVs
                self.new_values[k] = self.pv_handlers[k].unpack_value(
                    self.model.supported_variables[k],
                    v
                )

            # Set and simulate
            self.model.set(self.new_values)
            self.new_values = {}

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
