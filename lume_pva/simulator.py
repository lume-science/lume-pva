import p4p
import threading
import math
import random
import time
import pcaspy
import copy
from p4p.nt import NTScalar
from p4p.server import Server
from p4p.server.thread import SharedPV
from typing import Dict

class SimpleSimulator():
    """Simple PV simulator for float PVs"""

    pvs: dict
    sleep_time: int
    providers: Dict[str, SharedPV]

    MIN_RATE = 0.01

    pva_mode_lookup = {
        'float': 'd',
        'int': 'i',
    }

    ca_mode_lookup = {
        'float': 'float',
        'int': 'int'
    }
    
    def _mode_to_pva_type(self, mode: str) -> str:
        if mode not in SimpleSimulator.pva_mode_lookup.keys():
            raise ValueError(f'Unknown type {mode} found in config')
        return SimpleSimulator.pva_mode_lookup[mode]

    def _mode_to_ca_type(self, mode: str) -> str:
        if mode not in SimpleSimulator.ca_mode_lookup.keys():
            raise ValueError(f'Unknown type {mode} found in config')
        return SimpleSimulator.ca_mode_lookup[mode]

    def __init__(self, pvs: dict):
        """
        Initialize the simple PV simulator
        
        Parameters
        ----------
        pvs : dict
            Dictionary of PVs, in the format:
            {
                "name": {
                    "type": "random_uniform",
                    "init": 0.5,
                    "range": [-0.5, 1.0],
                    "rate": 
                }
            }
        
        """
        self.server = pcaspy.SimpleServer()
        self.pvs = copy.deepcopy(pvs)
        self.should_exit = False
        self.sleep_time = 0.001

        # Create PVs
        self.providers = {}
        for k, v in self.pvs.items():
            self.providers[k] = SharedPV(
                nt=NTScalar(self._mode_to_pva_type(v.get('type', 'float'))),
                initial=v.get('init', 0)
            )
            self.server.createPV('', {
                k: {
                    'type': self._mode_to_ca_type(v.get('type', 'float')),
                    'default': v.get('init', 0),
                    'prec': 3,
                    'scan': 1
                }
            })

        # Create PVA server
        self.pva_srv = Server(providers=[self.providers])
        self.driver = pcaspy.Driver()

        # Run thread
        self.thread = threading.Thread(target=self._thread_proc)
        self.thread.start()

    def _thread_proc(self):
        while not self.should_exit:
            self._update_pvs()
            self.server.process(self.sleep_time)

    def _update_pvs(self):
        """Update the simulated PVs"""
        for k, v in self.pvs.items():
            nv = None
            typ = v.get('mode', 'random_uniform')
            lastup = v.get('last_updated', 0)

            if time.time() - lastup < v.get('rate', 0.1):
                continue

            v['last_updated'] = time.time()

            # Uniform range update
            if typ == 'random_uniform':
                if 'range' in v and v['range'] != 0:
                    nv = random.uniform(
                        v['range'][0],
                        v['range'][1]
                    )
            # Constants
            elif typ == 'const':
                pass # Nothing to do for constants

            # Update the value if we have a new one
            if nv is not None:
                self.providers[k].post(nv, timestamp=time.time())
                self.driver.setParam(k, nv, timestamp=time.time())
                self.driver.updatePV(k)
        
    def wait(self):
        """Waits for the thread to exit (forever, usually)"""
        self.thread.join()

    def stop(self):
        """Halts processing and stops the server"""
        self.should_exit = True
        self.thread.join()
        self.pva_srv.stop()
