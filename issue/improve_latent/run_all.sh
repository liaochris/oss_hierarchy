#!/bin/bash
# run_all.sh -- regenerate the issue/improve_latent analysis and improve_latent.pdf end-to-end.
#
#   bash issue/improve_latent/run_all.sh            # full rerun (~20-30 min; dominated by the ES runs)
#   bash issue/improve_latent/run_all.sh --pdf-only # skip re-analysis: just rebuild figures from the
#                                                     existing band CSVs/draws and recompile the PDF
#
# Stages: exports -> pre-period features -> fit all specs -> score the figure specs (cyborg event study
# at full window + [-5,5]-aligned + [-5,5]+drop-top-3) -> band & scatter figures -> scoreboard -> PDF.
# NOTE: the tex table numbers are typed in; if a rerun shifts results, update the tables in
# tex/improve_latent.tex (the figures and scoreboard.csv regenerate automatically).
set -e
cd "$(git rev-parse --show-toplevel)"
source activate org_resilience
export PYTHONPATH=.
H=issue/improve_latent/harness
FIG_SPECS="glm_flatpost glm_eqw nbrf glm_offset glm_nb"

if [ "$1" != "--pdf-only" ]; then
  echo "==[1/6]== exports (opened-cohort actual series + trimmed repo set, via the real estimator)"
  Rscript $H/export_actual.R
  Rscript $H/export_trimmed_repos.R

  echo "==[2/6]== build pre-period-outcome features"
  python $H/build_features.py

  echo "==[3/6]== fit all specifications"
  python $H/fit_glm_forecast.py          # glm_pois, glm_pois_trend, glm_nb
  python $H/fit_ml_forecast.py rf        # ml_rf (and the mean nbrf reuses)
  python $H/fit_ml_forecast.py gbm       # ml_gbm
  python $H/fit_glm_variants.py          # nbrf, glm_offset, glm_eqw, glm_flatpost, glm_norm*, twopart, ...
  for m in pooled_decay pooled_decay_med bin_decay ar1_partial decay_last cre_proj cre_last; do
    python $H/fit_cv_forecasters.py "$m"
  done

  echo "==[4/6]== score figure specs (cyborg ES: full window + [-5,5] + [-5,5]+drop-top-3)"
  for tag in $FIG_SPECS; do
    echo "  -- $tag"
    bash $H/score_variant.sh "nbglm:$tag" "$tag"   # build draws + cyborg full -> es_bands_<tag>_ctrliso.csv
    bash $H/es_opt.sh "$tag" win 0                 # -> es_bands_<tag>_win_d0.csv
    bash $H/es_opt.sh "$tag" win 3                 # -> es_bands_<tag>_win_d3.csv
  done
fi

echo "==[5/6]== figures (2x3 band: full/[-5,5]/+drop3; symlog scatter incl. zeros)"
python $H/plot_bands.py   $FIG_SPECS
python $H/plot_scatter.py $FIG_SPECS

echo "==[6/6]== scoreboard + compile improve_latent.pdf"
python $H/compile_scoreboard.py
cd issue/improve_latent/tex
pdflatex -interaction=nonstopmode -halt-on-error improve_latent.tex >/dev/null
pdflatex -interaction=nonstopmode -halt-on-error improve_latent.tex >/dev/null
rm -f improve_latent.aux improve_latent.log improve_latent.out improve_latent.toc
echo "DONE -> issue/improve_latent/tex/improve_latent.pdf"
