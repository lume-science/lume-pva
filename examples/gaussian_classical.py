#!/usr/bin/env python3
"""
gaussian_classical.py — Classical Gaussian Denoiser

Subscribes to noisy_signal from the simulator, estimates mean and sigma
using scipy curve_fit.

Run the simulator first:
    python examples/gaussian_sim.py

Then run this:
    python examples/gaussian_classical.py
"""
from typing import Any

import numpy as np
from scipy.optimize import curve_fit
from lume.model import LUMEModel
from lume.variables import NDVariable, ScalarVariable

from lume_pva.runner import Runner


NUM_POINTS = 256


def gaussian(x, amplitude, mean, sigma):
    """Gaussian function for curve fitting."""
    return amplitude * np.exp(-0.5 * ((x - mean) / sigma) ** 2)


class GaussianDenoiserModel(LUMEModel):
    """
    Classical denoiser: estimates Gaussian parameters from noisy signal
    using nonlinear least-squares curve fitting.

    Inputs:
     - noisy_signal (256-point array from simulator)
     - x_axis (256-point array from simulator)

    Outputs:
     - est_mean (estimated center position)
     - est_sigma (estimated width)
     - est_amplitude (estimated peak height)
     - denoised_signal (reconstructed clean Gaussian from fit)
     - fit_quality (R-squared goodness of fit, 0 to 1)
    """

    def __init__(self):
        self._initial_state = {
            "noisy_signal": np.zeros(NUM_POINTS, dtype=np.float64),
            "x_axis": np.linspace(-10.0, 10.0, NUM_POINTS, dtype=np.float64),
            "est_mean": 0.0,
            "est_sigma": 2.0,
            "est_amplitude": 5.0,
            "denoised_signal": np.zeros(NUM_POINTS, dtype=np.float64),
            "fit_quality": 0.0,
        }
        self._state = self._initial_state.copy()

        self._variables = {
            "noisy_signal": NDVariable(
                name="noisy_signal",
                unit="a.u.",
                read_only=False,
                shape=(NUM_POINTS,),
                dtype=np.float64,
            ),
            "x_axis": NDVariable(
                name="x_axis",
                unit="mm",
                read_only=False,
                shape=(NUM_POINTS,),
                dtype=np.float64,
            ),
            "est_mean": ScalarVariable(
                name="est_mean",
                default_value=0.0,
                unit="mm",
                read_only=True,
            ),
            "est_sigma": ScalarVariable(
                name="est_sigma",
                default_value=2.0,
                unit="mm",
                read_only=True,
            ),
            "est_amplitude": ScalarVariable(
                name="est_amplitude",
                default_value=5.0,
                unit="a.u.",
                read_only=True,
            ),
            "denoised_signal": NDVariable(
                name="denoised_signal",
                unit="a.u.",
                shape=(NUM_POINTS,),
                dtype=np.float64,
                read_only=True,
            ),
            "fit_quality": ScalarVariable(
                name="fit_quality",
                default_value=0.0,
                unit="dimensionless",
                read_only=True,
            ),
        }

    @property
    def supported_variables(self) -> dict:
        return self._variables

    def _get(self, names: list[str]) -> dict[str, Any]:
        return {name: self._state[name] for name in names}

    def _set(self, values: dict[str, Any]) -> None:
        for name, value in values.items():
            self._state[name] = value

        # Perform curve fit
        x = self._state["x_axis"]
        y = self._state["noisy_signal"]

        try:
            # Initial guess: amplitude=max(y), mean=weighted center, sigma=1.0
            p0 = [float(np.max(y)), float(np.sum(x * y) / (np.sum(y) + 1e-8)), 1.0]
            popt, _ = curve_fit(gaussian, x, y, p0=p0, maxfev=5000)
            est_amp, est_mean, est_sigma = popt

            # Force sigma positive
            est_sigma = abs(est_sigma)

            # Reconstruct clean signal from fit
            denoised = gaussian(x, est_amp, est_mean, est_sigma)

            # R-squared
            ss_res = np.sum((y - denoised) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1.0 - (ss_res / (ss_tot + 1e-8))

            self._state["est_mean"] = float(est_mean)
            self._state["est_sigma"] = float(est_sigma)
            self._state["est_amplitude"] = float(est_amp)
            self._state["denoised_signal"] = denoised
            self._state["fit_quality"] = float(r_squared)

        except Exception:
            # If fit fails, keep previous values
            pass

    def reset(self) -> None:
        self._state = self._initial_state.copy()


if __name__ == "__main__":
    import argparse
    import logging
    from examples import add_common_test_args

    parser = argparse.ArgumentParser(description="Classical Gaussian denoiser (curve fit)")
    add_common_test_args(parser)
    parser.add_argument("--sim-prefix", dest="sim_prefix", default="SIM:", type=str,
                        help="Prefix of the simulator PVs to subscribe to (default: SIM:)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.v else logging.INFO)

    model = GaussianDenoiserModel()
    config = Runner.generate_config(model, put_mode=args.put_mode, prefix=args.pv_prefix)

    # Which protocol(s) the Runner SERVER uses to SERVE output PVs.
    # ["pva"] = PVA only (no pcaspy CA server, no CA port listeners)
    # ["ca", "pva"] = both (default)
    # ONLY affects serving. Client side (pvua subscribing to remote PVs) is unaffected.
    config["protocol"] = args.pv_server_protocol

    config["remote_model_mode"] = "continuous"
    config["description"] = "Classical Gaussian denoiser (curve fit)"

    # Subscribe to simulator PVs (using sim-prefix)
    config["variables"]["noisy_signal"]["mode"] = "remote"
    config["variables"]["noisy_signal"]["pv"] = f"{args.sim_prefix}noisy_signal"
    config["variables"]["x_axis"]["mode"] = "remote"
    config["variables"]["x_axis"]["pv"] = f"{args.sim_prefix}x_axis"

    runner = Runner(model=model, config=config)
    print(f"Classical Denoiser running (prefix={args.pv_prefix})")
    print(f"  Subscribes to: {args.sim_prefix}noisy_signal, {args.sim_prefix}x_axis")
    print(f"  Serves: {args.pv_prefix}est_mean, {args.pv_prefix}est_sigma, {args.pv_prefix}est_amplitude")
    print(f"          {args.pv_prefix}denoised_signal, {args.pv_prefix}fit_quality")
    print(f"  Try:  pvmonitor {args.pv_prefix}est_mean {args.pv_prefix}est_sigma {args.pv_prefix}fit_quality")
    runner.run()


