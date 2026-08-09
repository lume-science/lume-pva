#!/bin/bash
# Start the ML Gaussian denoiser (PyTorch neural network) in a tmux session
# Subscribes to: SIM:noisy_signal, SIM:x_axis
# PVs served: ML:ml_est_mean, ML:ml_est_sigma, ML:ml_est_amplitude,
#             ML:ml_denoised, ML:ml_fit_quality, ML:ml_infer_time, ML:STATUS
# Protocol: PVA only (Runner with --pv-server-protocol pva, no pcaspy CA server)
# Prerequisite: examples/gaussian_model.pt must exist (run gaussian_train.py first)
SRCDIR="$HOME/controls/lume-pva/lume-pva-src"
ENVSCRIPT="$SRCDIR/examples/epics-env-localhost.sh"
SESSION="gauss-ml"
PREFIX="ML:"
SIM_PREFIX="SIM:"

if [ ! -f "$SRCDIR/examples/gaussian_model.pt" ]; then
    echo "ERROR: gaussian_model.pt not found. Run 'python -m examples.gaussian_train' first."
    exit 1
fi

tmux kill-session -t $SESSION 2>/dev/null
tmux new-session -d -s $SESSION
tmux send-keys -t $SESSION "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.gaussian_ml --pv-prefix '$PREFIX' --sim-prefix '$SIM_PREFIX' --pv-server-protocol pva" Enter

echo "Started: $SESSION (prefix=$PREFIX, subscribes to ${SIM_PREFIX}*)"
echo "  PVs: ${PREFIX}ml_est_mean, ${PREFIX}ml_est_sigma, ${PREFIX}ml_infer_time, ${PREFIX}STATUS"
echo "  Protocol: PVA only (no CA server)"
echo "  Attach: tmux attach -t $SESSION"
