#!/bin/bash
# Start the math model (a+b+c+d with put-complete mode) in a tmux session
# PVs served: MATH:input_a, MATH:input_b, MATH:input_c, MATH:input_d,
#             MATH:invert, MATH:sum_output, MATH:desc, MATH:my_enum, MATH:STATUS
# Protocol: CA + PVA (uses Runner)
# Put mode: complete (pvput blocks until simulation finishes)
SRCDIR="$HOME/controls/lume-pva/lume-pva-src"
ENVSCRIPT="$HOME/controls/lume-pva/epics-env.sh"
SESSION="math-model"
PREFIX="MATH:"
PUT_MODE="complete"

tmux kill-session -t $SESSION 2>/dev/null
tmux new-session -d -s $SESSION
tmux send-keys -t $SESSION "conda activate lume-pva && source $ENVSCRIPT && cd $SRCDIR && python -m examples.math_model --pv-prefix '$PREFIX' --put-mode $PUT_MODE" Enter

echo "Started: $SESSION (prefix=$PREFIX, put-mode=$PUT_MODE)"
echo "  PVs: ${PREFIX}sum_output, ${PREFIX}input_a, ${PREFIX}STATUS"
echo "  Attach: tmux attach -t $SESSION"
echo "  Demo: time pvxput ${PREFIX}input_a 5.0  (blocks ~1s in complete mode)"
