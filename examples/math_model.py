#!/usr/bin/env python3
from lume.model import LUMEModel
from lume.variables import ScalarVariable
from typing import Any
from lume_pva.runner import Runner
from lume_pva.simulator import SimpleSimulator

class SimpleMathModel(LUMEModel):
    """
    A simple mathematical model that demonstrates basic LUMEModel implementation.
    
    This model computes:
    - sum_output = input_a + input_b
    """
    
    def __init__(self):
        """Initialize the model with default state."""
        # Define initial values
        self._initial_state = {
            "input_a": 1.0,
            "input_b": 1.0,
            "sum_output": 2.0
        }
        # Current state (will be modified during simulation)
        self._state = self._initial_state.copy()
        
        # Define supported variables
        self._variables = {
            "input_a": ScalarVariable(
                name="input_a",
                default_value=1.0,
                value_range=(-10.0, 10.0),
                unit="dimensionless",
                read_only=False
            ),
            "input_b": ScalarVariable(
                name="input_b", 
                default_value=1.0,
                value_range=(-10.0, 10.0),
                unit="dimensionless",
                read_only=False
            ),
            "sum_output": ScalarVariable(
                name="sum_output",
                default_value=2.0,
                unit="dimensionless", 
                read_only=True  # This is computed, not set directly
            ),
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
        input_a = self._state["input_a"]
        input_b = self._state["input_b"]
        
        # Calculate outputs
        self._state["sum_output"] = input_a + input_b
    
    def reset(self) -> None:
        """Reset the model to its initial state."""
        self._state = self._initial_state.copy()
        
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--auto', action='store_true', help='Hooks the math model up to a dummy PVA server')
    args = parser.parse_args()

    # Configure logging for debug if requested
    import logging
    logging.basicConfig(level=logging.DEBUG if args.v else logging.INFO)

    # If running in auto mode, provision a dummy server that gives random values for PVs
    if args.auto:
        sim = SimpleSimulator(pvs={
            'input_a': {
                'type': 'float',
                'mode': 'random_uniform',
                'range': [-100, 100],
                'rate': 1
            },
            'input_b': {
                'type': 'float',
                'mode': 'random_uniform',
                'range': [-100, 100],
                'rate': 1
            }
        })

    runner = Runner(model=SimpleMathModel(), client=args.auto)
    runner.run()