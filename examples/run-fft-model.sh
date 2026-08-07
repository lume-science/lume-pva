#!/bin/bash
# Start the FFT model (embedded simulator + FFT computation) in a tmux session
# PVs served: FFT:fft_real, FFT:fft_imag, FFT:signal_a, FFT:signal_b, FFT:signal_c,
#             FFT:string_array, FFT:2d_array, FFT:STATUS
# Protocol: CA + PVA (uses Runner + embedded SimpleSimulator)
SRCDIR="$HOME/controls/lume-pva/lume-pva-src"
ENVSCRIPT="$HOME/controls/lume-pva/epics-env.sh"
SESSION="fft-model"
PREFIX="FFT:"

tmux kill-session -t $SESSION 2>/dev/null
tmux new-session -d -s $SESSION
tmux send-keys -t $SESSION "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.fft_model --pv-prefix '$PREFIX'" Enter

echo "Started: $SESSION (prefix=$PREFIX)"
echo "  PVs: ${PREFIX}fft_real, ${PREFIX}fft_imag, ${PREFIX}STATUS"
echo "  Attach: tmux attach -t $SESSION"
