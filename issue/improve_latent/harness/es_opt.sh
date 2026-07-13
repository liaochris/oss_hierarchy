#!/bin/bash
# es_opt.sh <tag> <restrict=full> <dropK=0>  -> run the cyborg ES on es_input/<tag>_ctrliso with window
# alignment / top-K drop; print one RESULT line.
set -e
cd "$(git rev-parse --show-toplevel)"
source activate org_resilience >/dev/null 2>&1
export PYTHONPATH=.
tag="$1"; restrict="${2:-full}"; dropK="${3:-0}"
out=$(Rscript issue/improve_latent/harness/run_es_opt.R "issue/improve_latent/es_input/${tag}_ctrliso" \
      "${tag}_${restrict}_d${dropK}" "$restrict" "$dropK" 2>&1)
raw=$(echo "$out"  | grep "opened raw"  | sed 's/.*gap=//')
norm=$(echo "$out" | grep "opened norm" | sed 's/.*gap=//')
dropped=$(echo "$out" | grep "dropped top" || true)
echo "RESULT ${tag} restrict=${restrict} dropK=${dropK} | raw ${raw} | norm ${norm} | ${dropped}"
