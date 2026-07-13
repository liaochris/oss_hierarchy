#!/bin/bash
# score_variant.sh <method> <tag>  -> build 100-draw draws, run the CYBORG event study + opened scatter,
# print ONE parseable RESULT line. method e.g. "nbglm:glm_eqw" or "table:pooled_decay".
set -e
cd "$(git rev-parse --show-toplevel)"
source activate org_resilience >/dev/null 2>&1
export PYTHONPATH=.
method="$1"; tag="$2"; H=issue/improve_latent/harness
python "$H/build_draws.py" "$method" 100 exact1 tag="$tag" >/dev/null 2>&1
python "$H/make_control_isolation.py" "$tag" >/dev/null 2>&1
d="issue/improve_latent/es_input/${tag}_ctrliso/opened_cohort/adaptive/draws/pooled/important_degree_top3/exact1/nevertreated"
mkdir -p "$d"; ln -sf "$(pwd)/issue/improve_latent/draws/${tag}_ctrliso/exact1/raw_draws.parquet" "$d/raw_draws.parquet"
es=$(Rscript "$H/run_es_exact1.R" "issue/improve_latent/es_input/${tag}_ctrliso" "${tag}_ctrliso" 2>/dev/null | grep -E "opened")
sc=$(python "$H/gen_scatter.py" "$tag" 2>/dev/null | grep "control ")
raw=$(echo "$es" | grep "raw" | sed 's/.*gap(actual-p50)=//;s/  coverage=/ cov=/')
norm=$(echo "$es" | grep "norm" | sed 's/.*gap(actual-p50)=//;s/  coverage=/ cov=/')
beta=$(echo "$sc" | sed 's/.*control  *//')
echo "RESULT ${tag} | raw ${raw} | norm ${norm} | ${beta}"
