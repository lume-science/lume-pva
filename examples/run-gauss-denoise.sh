#!/bin/bash
# Start the classical Gaussian denoiser (scipy curve_fit) in a tmux session
# Subscribes to: SIM:noisy_signal, SIM:x_axis
# PVs served: DENOISE:est_mean, DENOISE:est_sigma, DENOISE:est_amplitude,
#             DENOISE:denoised_signal, DENOISE:fit_quality, DENOISE:STATUS
# Protocol: PVA only (Runner with --pv-server-protocol pva, no pcaspy CA server)
SRCDIR="$HOME/controls/lume-pva/lume-pva-src"
ENVSCRIPT="$SRCDIR/examples/epics-env-localhost.sh"
SESSION="gauss-denoise"
PREFIX="DENOISE:"
SIM_PREFIX="SIM:"

tmux kill-session -t $SESSION 2>/dev/null
tmux new-session -d -s $SESSION
tmux send-keys -t $SESSION "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.gaussian_classical --pv-prefix '$PREFIX' --sim-prefix '$SIM_PREFIX' --pv-server-protocol pva" Enter

echo "Started: $SESSION (prefix=$PREFIX, subscribes to ${SIM_PREFIX}*)"
echo "  PVs: ${PREFIX}est_mean, ${PREFIX}est_sigma, ${PREFIX}fit_quality, ${PREFIX}STATUS"
echo "  Protocol: PVA only (no CA server)"
echo "  Attach: tmux attach -t $SESSION"
