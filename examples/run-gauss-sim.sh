#!/bin/bash
# Start the Gaussian simulator in a tmux session
# PVs served: SIM:mean, SIM:sigma, SIM:snr, SIM:noisy_signal, SIM:clean_signal, SIM:x_axis
# Protocol: PVA only (no CA — uses p4p directly)
SRCDIR="$HOME/controls/lume-pva/lume-pva-src"
ENVSCRIPT="$HOME/controls/lume-pva/epics-env.sh"
SESSION="gauss-sim"
PREFIX="SIM:"

tmux kill-session -t $SESSION 2>/dev/null
tmux new-session -d -s $SESSION
tmux send-keys -t $SESSION "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.gaussian_sim --pv-prefix '$PREFIX'" Enter

echo "Started: $SESSION (prefix=$PREFIX)"
echo "  PVs: ${PREFIX}mean, ${PREFIX}sigma, ${PREFIX}snr, ${PREFIX}noisy_signal"
echo "  Attach: tmux attach -t $SESSION"
