#!/usr/bin/env python3
"""
gaussian_sim.py — Noisy Gaussian Signal Simulator

Generates a noisy 1D Gaussian beam profile signal over EPICS PVs.
Uses SimpleSimulator expressions - no LUMEModel needed for the simulator.

The denoiser model (separate process) subscribes to these PVs.

Usage:
    python examples/gaussian_sim.py
"""
import time

import numpy as np
from p4p.nt import NTNDArray, NTScalar
from p4p.server import Server
from p4p.server.thread import SharedPV

NUM_POINTS = 256
X_AXIS = np.linspace(-10.0, 10.0, NUM_POINTS, dtype=np.float64)


def generate_gaussian(mean, sigma, amplitude, noise_level):
    """Generate a noisy Gaussian signal."""
    clean = amplitude * np.exp(-0.5 * ((X_AXIS - mean) / sigma) ** 2)
    noise = np.random.normal(0, noise_level, NUM_POINTS)
    noisy = clean + noise
    return clean, noisy


if __name__ == "__main__":
    # Create PVs
    pv_noisy = SharedPV(nt=NTNDArray(), initial=np.zeros(NUM_POINTS, dtype=np.float64))
    pv_clean = SharedPV(nt=NTNDArray(), initial=np.zeros(NUM_POINTS, dtype=np.float64))
    pv_x_axis = SharedPV(nt=NTNDArray(), initial=X_AXIS)
    pv_mean = SharedPV(nt=NTScalar("d"), initial=0.0)
    pv_sigma = SharedPV(nt=NTScalar("d"), initial=2.0)
    pv_snr = SharedPV(nt=NTScalar("d"), initial=0.0)

    providers = {
        "SIM:noisy_signal": pv_noisy,
        "SIM:clean_signal": pv_clean,
        "SIM:x_axis": pv_x_axis,
        "SIM:mean": pv_mean,
        "SIM:sigma": pv_sigma,
        "SIM:snr": pv_snr,
    }

    with Server(providers=[providers]):
        print("Gaussian Simulator running")
        print("  PVs: SIM:noisy_signal, SIM:clean_signal, SIM:x_axis")
        print("        SIM:mean, SIM:sigma, SIM:snr")
        print("  Try:  pvmonitor SIM:mean SIM:sigma SIM:snr")

        t = 0.0
        try:
            while True:
                t += 0.1

                # Wandering parameters
                mean = 3.0 * np.sin(0.2 * t)
                sigma = 2.0 + 1.0 * np.sin(0.05 * t)
                amplitude = 5.0
                noise_level = 0.5

                # Generate signal
                clean, noisy = generate_gaussian(mean, sigma, amplitude, noise_level)

                # Compute SNR
                signal_power = np.sum(clean ** 2)
                noise_power = np.sum((noisy - clean) ** 2)
                snr = 10.0 * np.log10(signal_power / noise_power) if noise_power > 0 else 100.0

                # Post values
                pv_noisy.post(noisy, timestamp=time.time())
                pv_clean.post(clean, timestamp=time.time())
                pv_mean.post(float(mean), timestamp=time.time())
                pv_sigma.post(float(sigma), timestamp=time.time())
                pv_snr.post(float(snr), timestamp=time.time())

                time.sleep(0.5)

        except KeyboardInterrupt:
            pass


