
import networkx as nx
import pandas as pd
import numpy as np
from pathlib import Path
from joblib import Parallel, delayed
import random

def Main():
    indir_interactions = Path("drive/output/derived/graph_structure/interactions")
    indir_graphs = Path("drive/output/derived/graph_structure/graphs")
    outdir = Path("drive/output/derived/graph_structure/graph_degrees")

    ProcessRepos(indir_interactions, indir_graphs, outdir)


def ProcessSingleRepo(repo_file: Path, indir_graphs: Path, outdir: Path):
    repo = repo_file.stem
    out_path = outdir / f"{repo}.parquet"

    if out_path.exists():
        return

    dfs = []

    for folder in indir_graphs.iterdir():
        if not folder.is_dir():
            continue

        gexf_path = folder / f"{repo}.gexf"
        if not gexf_path.exists():
            continue

        try:
            print(f"Processing {repo}...")
            dfs.append(
                BuildDegreeTable(gexf_path).assign(
                    time_period=pd.to_datetime(folder.stem, format="%Y%m")
                )
            )
        except Exception as e:
            print(f"Skipping {gexf_path}: {e}")
            continue

    if not dfs:
        return

    df_all = pd.concat(dfs, ignore_index=True)
    df_all.to_parquet(out_path, index=False)
    print(f"Saved {out_path}")


def ProcessRepos(indir_interactions: Path, indir_graphs: Path, outdir: Path, n_jobs: int = -1):
    outdir.mkdir(parents=True, exist_ok=True)
    repo_files = list(indir_interactions.glob("*.parquet"))
    random.shuffle(repo_files)
    repo_files = [repo for repo in repo_files if not (outdir / f"{repo.stem}.parquet").exists()]
    print(len(repo_files))

    Parallel(n_jobs=n_jobs)(
        delayed(ProcessSingleRepo)(repo_file, indir_graphs, outdir)
        for repo_file in repo_files
    )


def CleanNodes(graph: nx.Graph) -> nx.Graph:
    nan_nodes = [
        node for node in graph.nodes
        if str(node).lower() == "nan" or (isinstance(node, float) and math.isnan(node))
    ]
    graph.remove_nodes_from(nan_nodes)
    
    mapping = {node: int(float(node)) for node in graph.nodes}
    graph = nx.relabel_nodes(graph, mapping)
    
    zero_degree_nodes = [node for node, degree in dict(graph.degree()).items() if degree == 0]
    graph.remove_nodes_from(zero_degree_nodes)
    
    return graph
def BuildDegreeTable(gexf_path: Path) -> pd.DataFrame:
    G = nx.read_gexf(gexf_path)
    G = CleanNodes(G)

    if G.number_of_nodes() == 0:
        return pd.DataFrame(columns=[
            "actor_id", "degree",
            "degree_centrality", "degree_centrality_z", "degree_centrality_rank",
            "weighted_degree_centrality", "weighted_degree_centrality_z", "weighted_degree_centrality_rank",
            "betweenness_centrality", "betweenness_centrality_z", "betweenness_centrality_rank"
        ])

    # Base degree (raw counts only)
    deg_data = dict(G.degree())
    df = pd.DataFrame({"actor_id": list(deg_data.keys()), "degree": list(deg_data.values())})

    n = G.number_of_nodes()

    # Compute metrics
    metrics = {
        "degree_centrality": {} if n <= 1 else nx.degree_centrality(G),
        "betweenness_centrality": {} if n <= 1 else nx.betweenness_centrality(G, weight="weight", normalized=True),
        "weighted_degree_centrality": {}
    }

    # Weighted degree centrality normalized by max observed strength
    weighted_degree = dict(G.degree(weight="weight"))
    max_strength = max(weighted_degree.values()) if weighted_degree else 0.0
    if max_strength > 0:
        metrics["weighted_degree_centrality"] = {node: val / max_strength for node, val in weighted_degree.items()}
    else:
        metrics["weighted_degree_centrality"] = {node: np.nan for node in G.nodes()}

    # Attach metrics + z-scores + ranks (rank based on z)
    for name, values in metrics.items():
        df[name] = df["actor_id"].map(values).astype(float)

        if df[name].nunique(dropna=True) <= 1:  # constant or all nan
            df[f"{name}_z"] = np.nan
        else:
            mean = df[name].mean(skipna=True)
            std = df[name].std(skipna=True)
            df[f"{name}_z"] = (df[name] - mean) / std if std > 0 else np.nan

        # Rank from z-score (descending); NaNs stay NaN
        df[f"{name}_rank"] = df[f"{name}_z"].rank(method="dense", ascending=False, na_option="keep").astype("Int64")

    return df

if __name__ == "__main__":
    Main()