#!/bin/bash
# =============================================================================
# start-all-pva.sh — Start the Gaussian pipeline in PVA-ONLY mode
# =============================================================================
# Usage: ./examples/start-all-pva.sh
# Kill:  tmux kill-server
#
# PVA-ONLY means:
#   - Servers create PVA listeners only (p4p SharedPVs)
#   - No pcaspy CA server is started — zero CA TCP port listeners
#   - Clients must use pvxget/pvxput/pvxmonitor (PVA tools)
#   - caget/caput/camonitor will NOT find these PVs
#
# This script launches only the Gaussian pipeline (3 servers):
#   1. gaussian_sim.py       — signal generator (p4p native, always PVA-only)
#   2. gaussian_classical.py — scipy curve_fit denoiser (Runner, PVA-only)
#   3. gaussian_ml.py        — PyTorch MLP denoiser (Runner, PVA-only)
#
# The other examples (fft_model, math_model, simulator) use pcaspy and are
# NOT included here. Use start-all.sh for the full CA+PVA suite.

SRCDIR="$HOME/controls/lume-pva/lume-pva-src"
ENVSCRIPT="$HOME/controls/lume-pva/epics-env.sh"
PROTO="--pv-server-protocol pva"

# Kill any existing sessions
tmux kill-server 2>/dev/null
sleep 1

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Starting Gaussian pipeline (PVA-ONLY mode)"
echo "═══════════════════════════════════════════════════════════════════"
echo ""

# --- Server 1: Gaussian Simulator (already PVA-only, uses p4p directly) ---
tmux new-session -d -s gauss-sim
tmux send-keys -t gauss-sim "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.gaussian_sim --pv-prefix 'SIM:'" Enter
echo "  [1/3] gauss-sim        prefix=SIM:       (PVA-only, p4p native)"

sleep 2  # Let simulator start before denoisers subscribe

# --- Server 2: Classical Denoiser ---
tmux new-session -d -s gauss-denoise
tmux send-keys -t gauss-denoise "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.gaussian_classical --pv-prefix 'DENOISE:' --sim-prefix 'SIM:' $PROTO" Enter
echo "  [2/3] gauss-denoise    prefix=DENOISE:   (PVA-only server, subscribes via pvua)"

# --- Server 3: ML Denoiser ---
tmux new-session -d -s gauss-ml
tmux send-keys -t gauss-ml "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.gaussian_ml --pv-prefix 'ML:' --sim-prefix 'SIM:' $PROTO" Enter
echo "  [3/3] gauss-ml         prefix=ML:        (PVA-only server, subscribes via pvua)"

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  All servers launched. Waiting 5s for startup..."
echo "═══════════════════════════════════════════════════════════════════"

sleep 5

# --- Verify ---
echo ""
echo "  Verifying (PVA only — using pvxget)..."
echo ""

source "$ENVSCRIPT"

PASS=0
FAIL=0

for pv in SIM:mean SIM:sigma SIM:snr DENOISE:est_mean DENOISE:est_sigma DENOISE:fit_quality ML:ml_est_mean ML:ml_est_sigma ML:ml_infer_time; do
    val=$(pvxget "$pv" 2>/dev/null | grep "value" | head -1 | awk '{print $NF}')
    if [ -n "$val" ]; then
        printf "    ✅ %-25s = %s\n" "$pv" "$val"
        PASS=$((PASS + 1))
    else
        printf "    ❌ %-25s TIMEOUT\n" "$pv"
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "  Checking for CA listeners (should be ZERO)..."
CA_COUNT=$(ss -tlnp 2>/dev/null | grep python | grep ":5064" | wc -l)

if [ "$CA_COUNT" -eq 0 ]; then
    printf "    ✅ CA listeners on port 5064: 0 (correct — PVA-only mode)\n"
    PASS=$((PASS + 1))
else
    printf "    ❌ CA listeners on port 5064: %d (unexpected — pcaspy should not be running)\n" "$CA_COUNT"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "  Sessions: $(tmux ls 2>/dev/null | wc -l) tmux sessions running"
echo ""
echo "  Demo commands:"
echo "    pvxmonitor SIM:mean DENOISE:est_mean ML:ml_est_mean"
echo "    pvxmonitor DENOISE:STATUS ML:STATUS"
echo "    caget SIM:mean              # (should FAIL — no CA server)"
echo ""
echo "  Management:"
echo "    tmux ls                     # list sessions"
echo "    tmux attach -t gauss-sim    # attach to a session"
echo "    tmux kill-server            # stop everything"
echo "    ~/show_servers.sh           # show ports"
echo "═══════════════════════════════════════════════════════════════════"
