#!/usr/bin/env python
# coding: utf-8
import os, sys, itertools
import pandas as pd, networkx as nx, ot, numpy as np
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pylab as pl
from sklearn.manifold import MDS

spec_cov = {
    "imp_contr": ["total_important"],
    "more_imp": ["normalized_degree"],
    "imp_contr_more_imp": ["total_important", "normalized_degree"],
    "imp_ratio": ["prop_important"],
    "indiv_clus": ["overall_overlap"],
    "project_clus_ov": ["mean_cluster_overlap"],
    "project_clus_node": ["avg_clusters_per_node"],
    "project_clus_pct_one": ["pct_nodes_one_cluster"],
    "indiv_cluster_size": ["overall_overlap", "total_important"],
    "indiv_cluster_impo": ["overall_overlap", "normalized_degree"],
    "imp_imp_comm": ["imp_to_imp_avg_edge_weight"],
    "imp_other_comm": ["imp_to_other_avg_edge_weight"],
    "both_comm": ["imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight"],
    "comm_imp_more_imp": ["normalized_degree", "imp_to_imp_avg_edge_weight"],
    "comm_within_more_imp": ["normalized_degree", "imp_to_other_avg_edge_weight"],
    "comm_cluster": ["imp_to_imp_avg_edge_weight", "overall_overlap"],
    "comm_within_cluster": ["imp_to_other_avg_edge_weight", "overall_overlap"]
}

def Main():
    sizebary = int(sys.argv[1])
    # Loop over each dep and spec as needed.
    for dep in ["graphs"]:
        for spec in spec_cov.keys():
            RepresentativeGraph(dep, spec, sizebary)

def PlotGraph(x, C, color="C0", s=None):
    for j in range(C.shape[0]):
        for i in range(j):
            pl.plot([x[i, 0], x[j, 0]], [x[i, 1], x[j, 1]], alpha=C[i, j], color="k")
    pl.scatter(x[:, 0], x[:, 1], c=color, s=s, zorder=10, edgecolors="k", cmap="tab10", vmax=9)

def ProcessFile(row):
    repo = row['repo_name'].replace("/", "_")
    tp = pd.to_datetime(row['time_period']).strftime('%Y%m')
    try:
        G = nx.read_gexf(f"drive/output/derived/graph_structure/graphs/{tp}/{repo}.gexf")
    except Exception as e:
        print(f"Error reading graph for {repo} at {tp}: {e}")
        return repo, row['relative_time'], None
    return repo, row['relative_time'], G

def ComputeBarycenter(graph_list):
    mats = [nx.to_numpy_array(G) for G in graph_list]
    Cdict, _ = ot.gromov.gromov_wasserstein_dictionary_learning(mats, 1, 25, reg=0.01)
    return Cdict[0]

def PlotCombinedGraph(barys, labels, event_time, dep, spec, sizebary):
    n = len(barys)
    pl.figure(figsize=(4 * n, 4))
    # Title now includes the sizebary value.
    title_str = (f"Representative Graphs (size={sizebary}): Combined Event Times" 
                 if event_time == "Combined" 
                 else f"Representative Graphs (size={sizebary}): Event Time: {event_time}")
    pl.suptitle(title_str, fontsize=16)
    for i, atom in enumerate(barys):
        scaled = (atom - atom.min()) / (atom.max() - atom.min())
        x = MDS(dissimilarity="precomputed", random_state=0).fit_transform(1 - scaled)
        pl.subplot(1, n, i + 1)
        pl.title(labels[i], fontsize=10)
        PlotGraph(x, atom / atom.max(), color="C0", s=100)
        pl.axis("off")
    pl.tight_layout(rect=[0, 0, 1, 0.95])
    out_dir = f"issue/event_study/{dep}/{spec}/representative_graphs"
    os.makedirs(out_dir, exist_ok=True)
    file_name = f"{out_dir}/rep_graphs_size{sizebary}_event_{event_time}.png"
    pl.savefig(file_name, dpi=300)

def CalculateCombinedGraph(bary_list, spec, dep, sizebary, labels, event_time):
    barys = [ComputeBarycenter(gl) for gl in bary_list]
    PlotCombinedGraph(barys, labels, event_time, dep, spec, sizebary)

def load_graph_dict(dep, spec):
    df = pd.read_csv(f"issue/event_study/{dep}/early_sample.csv")
    df['time_period'] = pd.to_datetime(df['time_period'])
    df['relative_time'] = df['time_index'] - df['treatment_group']
    cols = ['repo_name', 'time_period', 'relative_time'] + spec_cov[spec]
    df = df.query('relative_time >= -4 & relative_time <= 4')[cols]
    graph_dict = {}
    with ThreadPoolExecutor() as ex:
        for repo, rt, G in ex.map(ProcessFile, df.to_dict('records')):
            if G:
                graph_dict.setdefault(rt, {})[repo] = G
    return graph_dict

def PrepareCalculationData(event_graphs, spec):
    bary_list = [list(event_graphs.values())]
    labels = ["All"]
    for combo in itertools.product([0, 1], repeat=len(spec_cov[spec])):
        fname = "_".join(f"{spec_cov[spec][i]}_2p_back_bin_median{combo[i]}" 
                         for i in range(len(spec_cov[spec])))
        df_combo = pd.read_csv(f'issue/event_study/graphs/{spec}/{fname}.csv')
        repos = df_combo['repo_name'].str.replace("/", "_").unique()
        combo_graphs = [event_graphs[r] for r in repos if r in event_graphs]
        if combo_graphs:
            bary_list.append(combo_graphs)
            combo_label = "Combo: " + ", ".join(f"{name}={combo[i]}" 
                                for i, name in enumerate(spec_cov[spec]))
            labels.append(combo_label)
    return bary_list, labels

def RepresentativeGraph(dep, spec, sizebary=20):
    graph_dict = load_graph_dict(dep, spec)
    event_res = {}
    for rt in sorted(graph_dict.keys()):
        bary_list, labs = PrepareCalculationData(graph_dict[rt], spec)
        event_res[rt] = (bary_list, labs)
        CalculateCombinedGraph(bary_list, spec, dep, sizebary, labs, event_time=rt)
    all_bary_list, all_labs = [], []
    for rt in sorted(event_res.keys()):
        bl, l = event_res[rt]
        all_bary_list.extend(bl)
        all_labs.extend([f"{rt}: {lab}" for lab in l])
    CalculateCombinedGraph(all_bary_list, spec, dep, sizebary, all_labs, event_time="Combined")

if __name__ == '__main__':
    Main()
