#!/usr/bin/env python3
"""
gaussian_ml.py — ML-based Gaussian Denoiser (Neural Network Inference)

=== PURPOSE ===
Receives a noisy Gaussian signal from the simulator and uses a pre-trained
neural network to estimate beam parameters (mean, sigma, amplitude).

=== COMPARISON TO CLASSICAL ===
| Aspect          | Classical (curve_fit)      | ML (this file)           |
|-----------------|----------------------------|--------------------------|
| Method          | Nonlinear least-squares    | Forward pass through MLP |
| Speed           | ~2 ms per cycle            | ~0.1 ms per cycle        |
| Accuracy        | Excellent (optimal for     | Good (depends on         |
|                 |  Gaussian-shaped data)     |  training data quality)  |
| Generalization  | Only works for Gaussians   | Can learn ANY shape      |
| Setup cost      | Zero (no training)         | Must train first         |
| Dependencies    | scipy                      | torch (larger)           |

=== PREREQUISITES ===
1. Train the model first:  python examples/gaussian_train.py
2. Start the simulator:    python examples/gaussian_sim.py
3. Then run this:          python examples/gaussian_ml.py

=== WHAT IT PUBLISHES ===
  ml_est_mean       — estimated beam center (from neural network)
  ml_est_sigma      — estimated beam width (from neural network)
  ml_est_amplitude  — estimated peak height (from neural network)
  ml_denoised       — reconstructed clean signal (from estimated params)
  ml_fit_quality    — R² goodness of fit
  ml_infer_time     — time for the neural network forward pass (seconds)
"""

# ═══════════════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════════════

from typing import Any
from pathlib import Path
import time as time_module

import numpy as np
import torch
import torch.nn as nn

from lume.model import LUMEModel
from lume.variables import NDVariable, ScalarVariable
from lume_pva.runner import Runner

# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

NUM_POINTS = 256
X_AXIS = np.linspace(-10.0, 10.0, NUM_POINTS, dtype=np.float64)

# Path to trained model (relative to this file)
MODEL_PATH = Path(__file__).parent / "gaussian_model.pt"


# ═══════════════════════════════════════════════════════════════════════════
# NEURAL NETWORK (same architecture as training — MUST match exactly)
# ═══════════════════════════════════════════════════════════════════════════

class GaussianEstimatorNet(nn.Module):
    """Same architecture as in gaussian_train.py. Must be identical."""

    def __init__(self):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(NUM_POINTS, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 3),
        )

    def forward(self, x):
        return self.network(x)


# ═══════════════════════════════════════════════════════════════════════════
# THE MODEL CLASS
# ═══════════════════════════════════════════════════════════════════════════

class GaussianMLDenoiserModel(LUMEModel):
    """
    ML-based denoiser: estimates Gaussian parameters using a trained neural network.

    The neural network was trained offline (gaussian_train.py) on thousands of
    synthetic noisy Gaussian signals with known parameters.

    At runtime, it:
      1. Receives a noisy signal (from the simulator via EPICS)
      2. Normalizes it (same normalization as training)
      3. Feeds it through the neural network (one forward pass)
      4. Denormalizes the output (converts back to physical units)
      5. Reconstructs the clean signal from estimated parameters
      6. Publishes results as EPICS PVs
    """

    def __init__(self):
        # ─── LOAD THE TRAINED MODEL ──────────────────────────────────
        if not MODEL_PATH.exists():
            raise FileNotFoundError(
                f"Trained model not found at {MODEL_PATH}\n"
                f"Run 'python examples/gaussian_train.py' first!"
            )

        checkpoint = torch.load(MODEL_PATH, map_location="cpu", weights_only=True)

        # Load normalization parameters (MUST match training)
        self._signal_max = checkpoint["signal_max"]
        self._param_scales = np.array(checkpoint["param_scales"], dtype=np.float32)

        # Create and load the network
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._net = GaussianEstimatorNet().to(self._device)
        self._net.load_state_dict(checkpoint["model_state_dict"])
        self._net.eval()  # Set to inference mode (disables dropout, etc.)

        print(f"Loaded trained model from {MODEL_PATH}")
        print(f"  Running on: {self._device}")
        print(f"  Signal normalization: ÷{self._signal_max:.4f}")
        print(f"  Param scales: {self._param_scales}")

        # ─── INITIAL STATE ────────────────────────────────────────────
        self._initial_state = {
            # Inputs
            "noisy_signal": np.zeros(NUM_POINTS, dtype=np.float64),
            "x_axis": np.linspace(-10.0, 10.0, NUM_POINTS, dtype=np.float64),
            # Outputs
            "ml_est_mean": 0.0,
            "ml_est_sigma": 2.0,
            "ml_est_amplitude": 5.0,
            "ml_denoised": np.zeros(NUM_POINTS, dtype=np.float64),
            "ml_fit_quality": 0.0,
            "ml_infer_time": 0.0,
        }
        self._state = self._initial_state.copy()

        # ─── VARIABLE DEFINITIONS ─────────────────────────────────────
        self._variables = {
            # --- INPUTS (from simulator, mode="remote") ---
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
            # --- OUTPUTS (served as PVs, mode="ro") ---
            "ml_est_mean": ScalarVariable(
                name="ml_est_mean",
                default_value=0.0,
                unit="mm",
                read_only=True,
            ),
            "ml_est_sigma": ScalarVariable(
                name="ml_est_sigma",
                default_value=2.0,
                unit="mm",
                read_only=True,
            ),
            "ml_est_amplitude": ScalarVariable(
                name="ml_est_amplitude",
                default_value=5.0,
                unit="a.u.",
                read_only=True,
            ),
            "ml_denoised": NDVariable(
                name="ml_denoised",
                unit="a.u.",
                shape=(NUM_POINTS,),
                dtype=np.float64,
                read_only=True,
            ),
            "ml_fit_quality": ScalarVariable(
                name="ml_fit_quality",
                default_value=0.0,
                unit="dimensionless",
                read_only=True,
            ),
            "ml_infer_time": ScalarVariable(
                name="ml_infer_time",
                default_value=0.0,
                unit="seconds",
                read_only=True,
            ),
        }

    @property
    def supported_variables(self) -> dict:
        return self._variables

    def _get(self, names: list[str]) -> dict[str, Any]:
        return {name: self._state[name] for name in names}

    def _set(self, values: dict[str, Any]) -> None:
        """
        ╔══════════════════════════════════════════════════════════════╗
        ║  NEURAL NETWORK INFERENCE                                    ║
        ║                                                              ║
        ║  Instead of curve_fit (iterative optimization, ~2ms),        ║
        ║  we do ONE forward pass through a trained network (~0.1ms).  ║
        ║                                                              ║
        ║  The network has already "memorized" the mapping from        ║
        ║  noisy signals to parameters during training.                ║
        ╚══════════════════════════════════════════════════════════════╝
        """
        # Step 1: Update state with new inputs
        for name, value in values.items():
            self._state[name] = value

        # Step 2: Read inputs
        x = self._state["x_axis"]
        y = self._state["noisy_signal"]

        # Step 3: Neural network inference
        try:
            t_start = time_module.perf_counter()

            # ─── PREPROCESSING ────────────────────────────────────────
            # Must match exactly what we did during training:
            #   1. Convert to float32 (network expects 32-bit)
            #   2. Normalize by signal_max (same factor as training)
            #   3. Reshape to (1, 256) — batch of 1 sample
            signal_f32 = y.astype(np.float32)
            signal_normalized = signal_f32 / self._signal_max

            # Convert to PyTorch tensor and move to device (GPU/CPU)
            input_tensor = torch.tensor(
                signal_normalized, dtype=torch.float32
            ).unsqueeze(0).to(self._device)
            # unsqueeze(0) adds batch dimension: shape (256,) → (1, 256)

            # ─── FORWARD PASS ─────────────────────────────────────────
            # This is the ENTIRE computation: one matrix multiply chain.
            # No iteration, no optimization, just: input → output.
            with torch.no_grad():  # No gradient computation needed for inference
                output = self._net(input_tensor)  # shape: (1, 3)

            # ─── POSTPROCESSING ───────────────────────────────────────
            # Convert back to numpy and denormalize
            predictions = output.cpu().numpy()[0]  # shape: (3,) = [mean, sigma, amp]
            predictions = predictions * self._param_scales  # Undo normalization

            est_mean = float(predictions[0])
            est_sigma = float(abs(predictions[1]))  # Force positive
            est_amplitude = float(predictions[2])

            t_end = time_module.perf_counter()
            infer_time = t_end - t_start

            # ─── RECONSTRUCT CLEAN SIGNAL ─────────────────────────────
            # Use estimated parameters to generate the "denoised" Gaussian
            denoised = est_amplitude * np.exp(
                -0.5 * ((x - est_mean) / est_sigma) ** 2
            )

            # ─── COMPUTE R-SQUARED (same as classical, for comparison) ─
            ss_res = np.sum((y - denoised) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1.0 - (ss_res / (ss_tot + 1e-8))

            # Step 4: Store results
            self._state["ml_est_mean"] = est_mean
            self._state["ml_est_sigma"] = est_sigma
            self._state["ml_est_amplitude"] = est_amplitude
            self._state["ml_denoised"] = denoised
            self._state["ml_fit_quality"] = float(r_squared)
            self._state["ml_infer_time"] = float(infer_time)

        except Exception:
            # If inference fails, keep previous values
            pass

    def reset(self) -> None:
        self._state = self._initial_state.copy()


# ═══════════════════════════════════════════════════════════════════════════
# MAIN: Configure and start the Runner
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import logging
    from examples import add_common_test_args

    parser = argparse.ArgumentParser(
        description="ML Gaussian Denoiser — Neural network inference over EPICS"
    )
    add_common_test_args(parser)
    parser.add_argument("--sim-prefix", dest="sim_prefix", default="SIM:", type=str,
                        help="Prefix of the simulator PVs to subscribe to (default: SIM:)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.v else logging.INFO)

    # Create model (loads trained network from .pt file)
    model = GaussianMLDenoiserModel()

    # Generate Runner configuration
    config = Runner.generate_config(model, put_mode=args.put_mode, prefix=args.pv_prefix)

    # Which protocol(s) the Runner SERVER uses to SERVE output PVs.
    # ["pva"] = PVA only (no pcaspy CA server, no CA port listeners)
    # ["ca", "pva"] = both (default)
    # ONLY affects serving. Client side (pvua subscribing to remote PVs) is unaffected.
    config["protocol"] = args.pv_server_protocol

    # Set continuous mode (subscribe to remote PVs with monitors)
    config["remote_model_mode"] = "continuous"
    config["description"] = "ML Gaussian denoiser (trained MLP, PyTorch)"

    # Wire inputs to simulator PVs (same PVs as the classical denoiser)
    config["variables"]["noisy_signal"]["mode"] = "remote"
    config["variables"]["noisy_signal"]["pv"] = f"{args.sim_prefix}noisy_signal"
    config["variables"]["x_axis"]["mode"] = "remote"
    config["variables"]["x_axis"]["pv"] = f"{args.sim_prefix}x_axis"

    # Create Runner and start
    runner = Runner(model=model, config=config)

    print("\n" + "═" * 60)
    print(f"ML Denoiser running (prefix={args.pv_prefix})")
    print(f"  Subscribes to: {args.sim_prefix}noisy_signal, {args.sim_prefix}x_axis")
    print(f"  Serves: {args.pv_prefix}ml_est_mean, {args.pv_prefix}ml_est_sigma, {args.pv_prefix}ml_est_amplitude")
    print(f"          {args.pv_prefix}ml_denoised, {args.pv_prefix}ml_fit_quality, {args.pv_prefix}ml_infer_time")
    print(f"  Try:  pvmonitor {args.pv_prefix}ml_est_mean {args.pv_prefix}ml_est_sigma {args.pv_prefix}ml_infer_time")
    print("═" * 60 + "\n")

    runner.run()


