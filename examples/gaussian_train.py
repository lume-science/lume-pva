#!/usr/bin/env python3
"""
gaussian_train.py — Train a neural network to estimate Gaussian parameters

This script:
  1. Generates thousands of noisy Gaussian signals (synthetic training data)
  2. Trains a simple MLP (Multi-Layer Perceptron) to predict mean, sigma, amplitude
  3. Saves the trained model to a .pt file

Run ONCE before using gaussian_ml.py:
    python examples/gaussian_train.py

The trained model is saved to:
    examples/gaussian_model.pt
"""

import numpy as np
import torch
import torch.nn as nn
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS (must match the simulator and denoiser)
# ═══════════════════════════════════════════════════════════════════════════

NUM_POINTS = 256
X_AXIS = np.linspace(-10.0, 10.0, NUM_POINTS, dtype=np.float64)

# Training parameters
NUM_SAMPLES = 50000       # How many training examples to generate
BATCH_SIZE = 256          # How many samples per training step
EPOCHS = 50               # How many passes through the full dataset
LEARNING_RATE = 0.001     # How fast the network adjusts (smaller = more stable)

# ═══════════════════════════════════════════════════════════════════════════
# THE NEURAL NETWORK ARCHITECTURE
# ═══════════════════════════════════════════════════════════════════════════

class GaussianEstimatorNet(nn.Module):
    """
    A simple Multi-Layer Perceptron (MLP) that takes a 256-point noisy signal
    and predicts 3 numbers: mean, sigma, amplitude.

    Architecture:
        Input (256) → Hidden (128) → Hidden (64) → Hidden (32) → Output (3)

    Each arrow is a "linear layer" (matrix multiplication + bias)
    followed by a "ReLU activation" (negative values become zero).

    This is the simplest possible neural network for this task.
    A real production model might use 1D convolutions or attention,
    but for demonstrating lume-pva, an MLP is perfect.
    """

    def __init__(self):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(NUM_POINTS, 128),  # 256 inputs → 128 neurons
            nn.ReLU(),                   # Activation function
            nn.Linear(128, 64),          # 128 → 64
            nn.ReLU(),
            nn.Linear(64, 32),           # 64 → 32
            nn.ReLU(),
            nn.Linear(32, 3),            # 32 → 3 outputs (mean, sigma, amplitude)
        )

    def forward(self, x):
        """
        Forward pass: given input signal x, predict parameters.

        Parameters
        ----------
        x : torch.Tensor, shape (batch_size, 256)
            Batch of noisy signals

        Returns
        -------
        torch.Tensor, shape (batch_size, 3)
            Predicted [mean, sigma, amplitude] for each signal
        """
        return self.network(x)


# ═══════════════════════════════════════════════════════════════════════════
# DATA GENERATION
# ═══════════════════════════════════════════════════════════════════════════

def generate_training_data(num_samples):
    """
    Generate synthetic training data: noisy Gaussians with known parameters.

    This is the beauty of simulation-based ML:
      - We KNOW the ground truth (we generated it)
      - We can make as much data as we want
      - The network learns the inverse mapping: signal → parameters

    Strategy:
      - Random mean in [-5, +5] (covers the interesting part of our x range)
      - Random sigma in [0.5, 4.0] (realistic beam sizes)
      - Random amplitude in [2.0, 8.0] (realistic intensities)
      - Fixed noise level 0.5 (matching the simulator)

    Returns
    -------
    signals : numpy array, shape (num_samples, 256)
        The noisy signals (network INPUT)
    params : numpy array, shape (num_samples, 3)
        The true parameters [mean, sigma, amplitude] (network TARGET)
    """
    signals = np.zeros((num_samples, NUM_POINTS), dtype=np.float32)
    params = np.zeros((num_samples, 3), dtype=np.float32)

    for i in range(num_samples):
        # Random ground truth parameters
        mean = np.random.uniform(-5.0, 5.0)
        sigma = np.random.uniform(0.5, 4.0)
        amplitude = np.random.uniform(2.0, 8.0)
        noise_level = 0.5  # Fixed, matching simulator

        # Generate noisy Gaussian
        clean = amplitude * np.exp(-0.5 * ((X_AXIS - mean) / sigma) ** 2)
        noise = np.random.normal(0, noise_level, NUM_POINTS)
        noisy = clean + noise

        # Store
        signals[i] = noisy.astype(np.float32)
        params[i] = [mean, sigma, amplitude]

    return signals, params


# ═══════════════════════════════════════════════════════════════════════════
# TRAINING LOOP
# ═══════════════════════════════════════════════════════════════════════════

def train_model():
    """
    The complete training pipeline:
      1. Generate data
      2. Split into train/validation
      3. Train for N epochs
      4. Report final accuracy
      5. Save the trained model

    This is a standard PyTorch training loop. If you've never seen one before:
      - Each "epoch" processes all training data once
      - Data is processed in "batches" (256 samples at a time)
      - The "loss" measures how wrong the predictions are
      - The "optimizer" adjusts weights to reduce the loss
      - We check "validation loss" to make sure we're not overfitting
    """

    # ─── DEVICE SELECTION ─────────────────────────────────────────────
    # Use GPU if available (much faster), otherwise CPU
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Training on: {device}")
    if device.type == "cuda":
        print(f"  GPU: {torch.cuda.get_device_name(0)}")

    # ─── GENERATE DATA ────────────────────────────────────────────────
    print(f"Generating {NUM_SAMPLES} training samples...")
    signals, params = generate_training_data(NUM_SAMPLES)

    # ─── NORMALIZE INPUTS ─────────────────────────────────────────────
    # Neural networks work best when inputs are roughly in [-1, 1] range.
    # We normalize by the maximum absolute value across the dataset.
    # IMPORTANT: We must save this normalization factor and apply it
    # during inference too!
    signal_max = np.max(np.abs(signals))
    signals_normalized = signals / signal_max
    print(f"  Signal normalization factor: {signal_max:.4f}")

    # ─── NORMALIZE OUTPUTS ────────────────────────────────────────────
    # Similarly, normalize the target parameters so the network
    # doesn't have to learn vastly different scales.
    # mean: range [-5, 5] → divide by 5
    # sigma: range [0.5, 4] → divide by 4
    # amplitude: range [2, 8] → divide by 8
    param_scales = np.array([5.0, 4.0, 8.0], dtype=np.float32)
    params_normalized = params / param_scales

    # ─── TRAIN/VALIDATION SPLIT ───────────────────────────────────────
    # Use 80% for training, 20% for validation (checking generalization)
    split_idx = int(0.8 * NUM_SAMPLES)

    train_signals = torch.tensor(signals_normalized[:split_idx]).to(device)
    train_params = torch.tensor(params_normalized[:split_idx]).to(device)
    val_signals = torch.tensor(signals_normalized[split_idx:]).to(device)
    val_params = torch.tensor(params_normalized[split_idx:]).to(device)

    print(f"  Training samples: {split_idx}")
    print(f"  Validation samples: {NUM_SAMPLES - split_idx}")

    # ─── CREATE MODEL + OPTIMIZER ─────────────────────────────────────
    model = GaussianEstimatorNet().to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)
    # Adam = Adaptive Moment estimation. A smart optimizer that adjusts
    # learning rate per-parameter. Almost always the right default choice.

    loss_fn = nn.MSELoss()
    # MSE = Mean Squared Error: average of (prediction - truth)²
    # The network learns to minimize this.

    # ─── TRAINING LOOP ────────────────────────────────────────────────
    print(f"\nTraining for {EPOCHS} epochs...")
    print(f"{'Epoch':>6} {'Train Loss':>12} {'Val Loss':>12} {'Val MAE mean':>14} {'Val MAE sigma':>14}")
    print("─" * 62)

    for epoch in range(EPOCHS):
        model.train()  # Set to training mode (enables dropout, etc. — not used here but good practice)
        epoch_loss = 0.0
        num_batches = 0

        # Shuffle training data each epoch (prevents learning order-dependent patterns)
        indices = torch.randperm(split_idx, device=device)

        for start in range(0, split_idx, BATCH_SIZE):
            end = min(start + BATCH_SIZE, split_idx)
            batch_idx = indices[start:end]

            batch_x = train_signals[batch_idx]
            batch_y = train_params[batch_idx]

            # Forward pass: signal → predicted parameters
            predictions = model(batch_x)

            # Compute loss (how wrong are we?)
            loss = loss_fn(predictions, batch_y)

            # Backward pass: compute gradients (how to adjust weights)
            optimizer.zero_grad()  # Reset gradients from previous step
            loss.backward()        # Compute gradients via backpropagation
            optimizer.step()       # Adjust weights using gradients

            epoch_loss += loss.item()
            num_batches += 1

        # ─── VALIDATION ───────────────────────────────────────────────
        model.eval()  # Set to evaluation mode
        with torch.no_grad():  # Don't compute gradients (saves memory)
            val_predictions = model(val_signals)
            val_loss = loss_fn(val_predictions, val_params).item()

            # Convert back to original scale for interpretable errors
            val_pred_original = val_predictions.cpu().numpy() * param_scales
            val_true_original = val_params.cpu().numpy() * param_scales

            # MAE = Mean Absolute Error (average |prediction - truth|)
            mae = np.mean(np.abs(val_pred_original - val_true_original), axis=0)
            # mae[0] = mean error in mm, mae[1] = sigma error in mm, mae[2] = amplitude error

        avg_train_loss = epoch_loss / num_batches

        # Print progress every 5 epochs
        if epoch % 5 == 0 or epoch == EPOCHS - 1:
            print(f"{epoch:>6} {avg_train_loss:>12.6f} {val_loss:>12.6f} {mae[0]:>12.4f} mm {mae[1]:>12.4f} mm")

    # ─── FINAL EVALUATION ─────────────────────────────────────────────
    print("\n" + "═" * 62)
    print("FINAL VALIDATION RESULTS (original scale):")
    print(f"  Mean estimation error:      {mae[0]:.4f} mm")
    print(f"  Sigma estimation error:     {mae[1]:.4f} mm")
    print(f"  Amplitude estimation error: {mae[2]:.4f} a.u.")
    print(f"\n  For context:")
    print(f"    Beam wanders over ±5 mm → error is {mae[0]/5*100:.1f}% of range")
    print(f"    Beam size 0.5–4.0 mm → error is {mae[1]/3.5*100:.1f}% of range")

    # ─── SAVE THE MODEL ───────────────────────────────────────────────
    # We save:
    #   1. The network weights (what the network learned)
    #   2. The normalization factors (needed to preprocess inputs at inference time)
    #   3. The parameter scales (needed to convert outputs back to physical units)
    #
    # All packed into one .pt file using torch.save()

    save_path = Path(__file__).parent / "gaussian_model.pt"
    torch.save({
        "model_state_dict": model.state_dict(),   # Trained weights
        "signal_max": float(signal_max),           # Input normalization
        "param_scales": param_scales.tolist(),     # Output denormalization
        "num_points": NUM_POINTS,                  # Architecture info
        "architecture": "MLP_256_128_64_32_3",    # For documentation
    }, save_path)

    print(f"\n✅ Model saved to: {save_path}")
    print(f"   File size: {save_path.stat().st_size / 1024:.1f} KB")
    print(f"\nNext: python examples/gaussian_ml.py")


if __name__ == "__main__":
    train_model()


