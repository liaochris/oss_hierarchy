import numpy as np


def FilterControlGroup(panel, control_group):
    if control_group == "nevertreated":
        return panel[panel["num_departures"] <= 1].copy()
    if control_group == "notyettreated":
        return panel[panel["num_departures"] == 1].copy()
    raise ValueError(f"Unsupported control group: {control_group}")


def ParseQualifiedSample(qualified_sample):
    if not qualified_sample.startswith("exact"):
        raise ValueError(f"Unsupported qualified sample: {qualified_sample}")
    parts = qualified_sample.replace("exact", "").lstrip("_").split("_")
    return {int(part) for part in parts}


def FilterQualifiedSample(panel, qualified_sample, extra_counts=frozenset()):
    qualified_counts = ParseQualifiedSample(qualified_sample) | set(extra_counts)
    at_event = panel[panel["time_index"] == panel["quasi_treatment_group"]]
    keep_repos = at_event.loc[at_event["num_important_qualified"].isin(qualified_counts), "repo_name"].unique()
    return panel[panel["repo_name"].isin(keep_repos)].copy()


def CreateCompletePanel(panel, active_outcomes, min_event_time, max_event_time):
    if panel.empty:
        return panel.copy()

    present_outcomes = [outcome for outcome in active_outcomes if outcome in panel.columns]
    keep_repos = set.intersection(*[set(OutcomeValidRepos(panel, outcome, max_event_time)) for outcome in present_outcomes])
    filtered = panel[panel["repo_name"].isin(keep_repos)].copy()
    event_time = filtered["time_index"] - filtered["quasi_treatment_group"]
    keep_window = (
        filtered.assign(quasi_event_time=event_time)
        .groupby("repo_name")["quasi_event_time"]
        .apply(lambda times: set(range(min_event_time, max_event_time + 1)).issubset(set(times.tolist())))
    )
    return filtered[filtered["repo_name"].isin(keep_window[keep_window].index)].copy()


def OutcomeValidRepos(panel, outcome, max_event_time):
    baseline_mask = (
        (panel["time_index"] < panel["quasi_treatment_group"])
        & (panel["time_index"] >= panel["quasi_treatment_group"] - max_event_time)
    )
    baseline_values = panel.loc[baseline_mask, ["repo_name", outcome]]
    baseline_stats = baseline_values.groupby("repo_name")[outcome].agg(["mean", "std"])
    joined = panel[["repo_name", outcome]].join(baseline_stats, on="repo_name")
    normalized = (joined[outcome] - joined["mean"]) / joined["std"]
    return joined.loc[np.isfinite(normalized), "repo_name"].unique()
