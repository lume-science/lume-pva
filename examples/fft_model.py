from lume.model import LUMEModel
from lume.variables import NDVariable, ScalarVariable
from typing import Any
from lume_pva.runner import Runner
from lume_pva.simulator import SimpleSimulator
import numpy as np

class FFTModel(LUMEModel):
    """
    Simple model that computes the fourier transform of 3 summed input signals
    
    Inputs:
     - signal_a
     - signal_b
     - signal_c
    
    Outputs:
     - freq
    """
    
    def __init__(self):
        """Initialize the model with default state."""
        # Define initial values
        self._initial_state = {
            "signal_a": np.zeros(shape=(1024), dtype=np.float64),
            "signal_b": np.zeros(shape=(1024), dtype=np.float64),
            "signal_c": np.zeros(shape=(1024), dtype=np.float64),
            "fft_real": np.zeros(shape=(1024), dtype=np.float64),
            "fft_imag": np.zeros(shape=(1024), dtype=np.float64),
            "string_array": np.array(['hello', 'this', 'is', 'a', 'string', 'array'], dtype=np.str_)
        }

        # Current state (will be modified during simulation)
        self._state = self._initial_state.copy()
        
        # Define supported variables
        self._variables = {
            "signal_a": NDVariable(
                name="signal_a",
                unit="dimensionless",
                read_only=False,
                shape=(1024,),
                dtype=np.float64,
            ),
            "signal_b": NDVariable(
                name="signal_a",
                unit="dimensionless",
                read_only=False,
                shape=(1024,),
                dtype=np.float64,
            ),
            "signal_c": NDVariable(
                name="signal_a",
                unit="dimensionless",
                read_only=False,
                shape=(1024,),
                dtype=np.float64,
            ),
            "fft_real": NDVariable(
                name="fft_real",
                unit="dimensionless",
                shape=(1024,),
                dtype=np.float64,
                read_only=True  # This is computed, not set directly
            ),
            "fft_imag": NDVariable(
                name="fft_imag",
                unit="dimensionless",
                shape=(1024,),
                dtype=np.float64,
                read_only=True  # This is computed, not set directly
            ),
            "string_array": NDVariable(
                name="string_array",
                unit="dimensionless",
                shape=(6,),
                dtype=np.str_,
                read_only=True, # This is a static output
            )
        }
    
    @property
    def supported_variables(self) -> dict[str, ScalarVariable]:
        """Return the dictionary of supported variables."""
        return self._variables
    
    def _get(self, names: list[str]) -> dict[str, Any]:
        """
        Internal method to retrieve current values for specified variables.
        
        Parameters
        ----------
        names : list[str]
            List of variable names to retrieve
            
        Returns
        -------
        dict[str, Any]
            Dictionary mapping variable names to their current values
        """
        return {name: self._state[name] for name in names}
    
    def _set(self, values: dict[str, Any]) -> None:
        """
        Internal method to set input variables and compute outputs.
        
        This method:
        1. Updates input variables in the state
        2. Performs calculations to update output variables
        3. Stores results in the state
        
        Parameters
        ----------
        values : dict[str, Any]
            Dictionary of variable names and values to set
        """
        # Update input values in state
        for name, value in values.items():
            self._state[name] = value
        
        # Perform calculations to update outputs
        signal_a = self._state["signal_a"]
        signal_b = self._state["signal_b"]
        signal_c = self._state["signal_c"]
        
        signal = signal_a + signal_b + signal_c

        # Calculate outputs
        fft = np.fft.fft(signal)
        self._state["fft_real"] = fft.real
        self._state["fft_imag"] = fft.imag

    def reset(self) -> None:
        """Reset the model to its initial state."""
        self._state = self._initial_state.copy()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()

    # Configure logging for debug if requested
    import logging
    logging.basicConfig(level=logging.DEBUG if args.v else logging.INFO)

    sim = SimpleSimulator(pvs={
        'signal_a': {
            'type': 'array1d',
            'mode': 'expr',
            'expr': '4*sin(2*pi*t)',
            'rate': 0.1,
            'nvalues': 1024,
        },
        'signal_b': {
            'type': 'array1d',
            'mode': 'expr',
            'expr': '2.1*sin(4.3*pi*t)',
            'rate': 0.1,
            'nvalues': 1024,
        },
        'signal_c': {
            'type': 'array1d',
            'mode': 'expr',
            'expr': '3.3*sin(0.5544*pi*t)',
            'rate': 0.1,
            'nvalues': 1024,
        },
    })

    model = FFTModel()
    config = Runner.generate_config(model)

    config['remote_model_mode'] = 'continuous'

    for k in ['signal_a', 'signal_b', 'signal_c']:
        config['variables'][k]['mode'] = 'remote'

    runner = Runner(model=model, config=config)
    runner.run()