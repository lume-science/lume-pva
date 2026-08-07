#!/bin/bash
# =============================================================================
# start-all.sh — Start all lume-pva example servers in tmux sessions
# =============================================================================
# Usage: ./examples/start-all.sh
# Kill:  tmux kill-server

SRCDIR="$HOME/controls/lume-pva/lume-pva-src"
ENVSCRIPT="$HOME/controls/lume-pva/epics-env.sh"

# Kill any existing sessions
tmux kill-server 2>/dev/null
sleep 1

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Starting lume-pva example servers"
echo "═══════════════════════════════════════════════════════════════════"
echo ""

# --- Server 1: Gaussian Simulator ---
tmux new-session -d -s gauss-sim
tmux send-keys -t gauss-sim "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.gaussian_sim --pv-prefix 'SIM:'" Enter
echo "  [1/5] gauss-sim        prefix=SIM:       PVs: SIM:mean, SIM:sigma, SIM:noisy_signal"

sleep 2  # Let simulator start before denoisers subscribe

# --- Server 2: Classical Denoiser ---
tmux new-session -d -s gauss-denoise
tmux send-keys -t gauss-denoise "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.gaussian_classical --pv-prefix 'DENOISE:' --sim-prefix 'SIM:'" Enter
echo "  [2/5] gauss-denoise    prefix=DENOISE:   PVs: DENOISE:est_mean, DENOISE:est_sigma, DENOISE:STATUS"

# --- Server 3: ML Denoiser ---
tmux new-session -d -s gauss-ml
tmux send-keys -t gauss-ml "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.gaussian_ml --pv-prefix 'ML:' --sim-prefix 'SIM:'" Enter
echo "  [3/5] gauss-ml         prefix=ML:        PVs: ML:ml_est_mean, ML:ml_est_sigma, ML:STATUS"

# --- Server 4: FFT Model ---
tmux new-session -d -s fft-model
tmux send-keys -t fft-model "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.fft_model --pv-prefix 'FFT:'" Enter
echo "  [4/5] fft-model        prefix=FFT:       PVs: FFT:fft_real, FFT:fft_imag, FFT:STATUS"

# --- Server 5: Math Model (put-complete mode) ---
tmux new-session -d -s math-model
tmux send-keys -t math-model "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.math_model --pv-prefix 'MATH:' --put-mode complete" Enter
echo "  [5/5] math-model       prefix=MATH:      PVs: MATH:sum_output, MATH:input_a, MATH:STATUS"

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  All servers launched. Waiting 5s for startup..."
echo "═══════════════════════════════════════════════════════════════════"

sleep 5

# --- Verify ---
echo ""
echo "  Verifying..."
echo ""

# Source env for this shell too
source "$ENVSCRIPT"

# Quick PV check
PASS=0
FAIL=0

for pv in SIM:mean DENOISE:est_mean ML:ml_est_mean FFT:fft_real MATH:sum_output; do
    val=$(pvxget "$pv" 2>/dev/null | grep "value" | head -1 | awk '{print $NF}')
    if [ -n "$val" ]; then
        printf "    ✅ %-20s = %s\n" "$pv" "$val"
        PASS=$((PASS + 1))
    else
        printf "    ❌ %-20s TIMEOUT\n" "$pv"
        FAIL=$((FAIL + 1))
    fi
done

echo ""

# STATUS check
for pv in DENOISE:STATUS ML:STATUS FFT:STATUS MATH:STATUS; do
    val=$(pvxget "$pv" 2>/dev/null | grep "index" | awk '{print $NF}')
    if [ "$val" = "0" ]; then
        printf "    ✅ %-20s = Idle\n" "$pv"
        PASS=$((PASS + 1))
    elif [ "$val" = "1" ]; then
        printf "    ✅ %-20s = Simulating\n" "$pv"
        PASS=$((PASS + 1))
    else
        printf "    ❌ %-20s TIMEOUT\n" "$pv"
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "  Sessions: $(tmux ls 2>/dev/null | wc -l) tmux sessions running"
echo ""
echo "  Demo commands:"
echo "    pvxmonitor SIM:mean DENOISE:est_mean ML:ml_est_mean"
echo "    pvxmonitor DENOISE:STATUS DENOISE:est_mean"
echo "    time pvxput MATH:input_a 5.0"
echo "    pvxput MATH:input_a 50.0   # (rejected — out of range)"
echo ""
echo "  Management:"
echo "    tmux ls                     # list sessions"
echo "    tmux attach -t gauss-sim    # attach to a session"
echo "    tmux kill-server            # stop everything"
echo "═══════════════════════════════════════════════════════════════════"
