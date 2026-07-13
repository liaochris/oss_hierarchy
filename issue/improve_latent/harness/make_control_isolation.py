"""Cyborg / control-arm isolation via the REAL estimator. Build a modified draws parquet in which
every TREATED repo's opened count is overwritten with its ACTUAL observed opened (identical across
draws), so the treated arm contributes zero model error. Running the real event study on this
isolates the CONTROL-arm model-vs-actual gap in the estimator's exact raw/norm units (SA + repo/time
FE + trim). The modeled control here IS the latent-distribution forecast, so the cyborg gap equals
(modeled-control - actual-control).

Usage: python make_control_isolation.py <tag>  -> writes draws/<tag>_ctrliso/exact1/raw_draws.parquet
Then:  Rscript run_es_exact1.R es_input/<tag>_ctrliso <tag>_ctrliso  (gap = control-arm error)
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

DRAWS  = Path("issue/improve_latent/draws")
ACTUAL = Path("issue/improve_latent/results/actual_opened_exact1.csv")


def main():
    tag = sys.argv[1]
    draws = pd.read_parquet(DRAWS / tag / "exact1" / "raw_draws.parquet")
    treated_repos = set(draws[draws["is_treated"]]["repo_name"])
    # Use the ESTIMATOR's OPENED-COHORT actual (what the event study scores), NOT the analysis panel
    # (which counts outside-contributor opens the model never predicts -- 19% more volume).
    actual = pd.read_csv(ACTUAL)[["repo_name", "quasi_event_time", "pull_request_opened"]]
    cyborg = draws.copy()
    treated_mask = cyborg["is_treated"]
    actual_opened_for_treated = cyborg.loc[treated_mask, ["repo_name", "quasi_event_time"]].merge(
        actual, on=["repo_name", "quasi_event_time"], how="left")["pull_request_opened"].to_numpy()
    cyborg.loc[treated_mask, "pull_request_opened"] = np.where(
        np.isfinite(actual_opened_for_treated), actual_opened_for_treated,
        cyborg.loc[treated_mask, "pull_request_opened"])
    outdir = DRAWS / f"{tag}_ctrliso" / "exact1"
    outdir.mkdir(parents=True, exist_ok=True)
    cyborg.to_parquet(outdir / "raw_draws.parquet")
    print(f"wrote {outdir/'raw_draws.parquet'}  (treated repos set to actual opened: {len(treated_repos)})")


if __name__ == "__main__":
    main()
