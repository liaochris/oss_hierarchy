#!/bin/bash
# score.sh <method> [tag] [n_draws]  -> build exact1 draws, run the CYBORG event study (actual treated
# + modeled control) and the scatter, print the opened raw/norm gap+coverage and control/treated/pooled
# beta. baseline: method="baseline".
set -e
cd "$(git rev-parse --show-toplevel)"
method="$1"; tag="${2:-$(echo "$method" | tr ':' '_')}"; ndraws="${3:-100}"
H=issue/improve_latent/harness

python "$H/build_draws.py" "$method" "$ndraws" exact1 tag="$tag" >/dev/null 2>&1

link() {
  local t="$1"
  local d="issue/improve_latent/es_input/$t/opened_cohort/adaptive/draws/pooled/important_degree_top3/exact1/nevertreated"
  mkdir -p "$d"
  ln -sf "$(pwd)/issue/improve_latent/draws/$t/exact1/raw_draws.parquet" "$d/raw_draws.parquet"
}

# Cyborg: overwrite treated with actual opened, then run the real SA estimator on it.
python "$H/make_control_isolation.py" "$tag" >/dev/null 2>&1
link "${tag}_ctrliso"
cyborg=$(Rscript "$H/run_es_exact1.R" "issue/improve_latent/es_input/${tag}_ctrliso" "${tag}_ctrliso" 2>&1 | grep -E "opened")
echo "CYBORG $tag:"; echo "$cyborg"

# Scatter (opened panel) on the modeled draws.
link "$tag"
python "$H/gen_scatter.py" "$tag" 2>&1 | grep -E "opened|beta|R2|control|treated|pooled" || true
