#!/usr/bin/env python
# coding: utf-8
import os, sys, itertools
import pandas as pd, networkx as nx, ot, numpy as np
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import matplotlib.pylab as pl
from sklearn.manifold import MDS
 
# WHEN DOING SCONSCRIPT, make spec an argument into the script
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

def ProcessFile(row):
    """Process a row and return (repo, relative_time, G)."""
    repo = row['repo_name'].replace("/", "_")
    tp = pd.to_datetime(row['time_period']).strftime('%Y%m')
    try:
        G = nx.read_gexf(f"drive/output/derived/graph_structure/graphs/{tp}/{repo}.gexf")
    except Exception as e:
        print(f"Error reading graph for {repo} at {tp}: {e}")
        return repo, row['relative_time'], None
    return repo, row['relative_time'], G

def ComputeBarycenter(graph_list, sizebary):
    """Compute the barycenter from a list of graphs using sizebary iterations."""
    mats = [nx.to_numpy_array(G) for G in graph_list]
    Cdict, _ = ot.gromov.gromov_wasserstein_dictionary_learning(mats, 1, sizebary, reg=0.01, random_state = 0)
    return Cdict[0]

def LoadGraphDict(dep, spec):
    """Load a graph dictionary from CSV data."""
    df = pd.read_csv(f"issue/event_study/{dep}/early_sample.csv")
    df['time_period'] = pd.to_datetime(df['time_period'])
    df['relative_time'] = df['time_index'] - df['treatment_group']
    cols = ['repo_name', 'time_period', 'relative_time'] + spec_cov[spec]
    df = df.query('relative_time >= -4 & relative_time <= 4')[cols]
    graph_dict = {}
    with ThreadPoolExecutor() as ex:
        for repo, event_time, G in ex.map(ProcessFile, df.to_dict('records')):
            if G:
                graph_dict.setdefault(event_time, {})[repo] = G
    return graph_dict

def PrepareCalcData(event_graphs, spec):
    """Prepare calculation data for one event time."""
    bary_list = [list(event_graphs.values())]
    labels = ["All"]
    for combo in itertools.product([0, 1], repeat=len(spec_cov[spec])):
        fname = "_".join(f"{spec_cov[spec][i]}_2p_back_bin_median{combo[i]}" for i in range(len(spec_cov[spec])))
        df_combo = pd.read_csv(f'issue/event_study/graphs/{spec}/{fname}.csv')
        repos = df_combo['repo_name'].str.replace("/", "_").unique()
        combo_graphs = [event_graphs[r] for r in repos if r in event_graphs]
        if combo_graphs:
            bary_list.append(combo_graphs)
            combo_label = "Combo: " + "\n".join(f"{spec_cov[spec][i]}={combo[i]}" for i in range(len(spec_cov[spec])))
            labels.append(combo_label)
    return bary_list, labels

def PlotGraph(x, C, color="C0", s=100):
    """Plot graph edges and nodes on the current axis."""
    for j in range(C.shape[0]):
        for i in range(j):
            pl.plot([x[i, 0], x[j, 0]], [x[i, 1], x[j, 1]], alpha=C[i, j], color="k")
    pl.scatter(x[:, 0], x[:, 1], c=color, s=s, zorder=10, edgecolors="k", cmap="tab10", vmax=9)

def PlotSingleGraph(ax, atom, label, global_max):
    """Plot one graph cell on a given axis using global_max for normalization."""
    scaled = (atom - atom.min()) / (atom.max() - atom.min())
    x = MDS(dissimilarity="precomputed", random_state=0).fit_transform(1 - scaled)
    ax.set_title(label, fontsize=10)
    for i in range(scaled.shape[0]):
        for j in range(i):
            ax.plot([x[i, 0], x[j, 0]], [x[i, 1], x[j, 1]],
                    alpha=(atom/global_max)[i, j], color="k")
    ax.scatter(x[:, 0], x[:, 1], c="C0", s=100, zorder=10, edgecolors="k", cmap="tab10", vmax=9)
    ax.axis("off")

def PlotCombinedGraph(barys, labels, event_time, dep, spec, sizebary):
    """Plot a horizontal row of graphs for a single event time."""
    n = len(barys)
    pl.figure(figsize=(4 * n, 4))
    title_str = (f"Representative Graphs (size={sizebary}): Combined Event Times" 
                 if event_time == "Combined" 
                 else f"Representative Graphs (size={sizebary}): Event Time: {event_time}")
    pl.suptitle(title_str, fontsize=16)
    global_max = max([bary.max() for bary in barys])
    for i, atom in enumerate(barys):
        scaled = (atom - atom.min()) / (atom.max() - atom.min())
        x = MDS(dissimilarity="precomputed", random_state=0).fit_transform(1 - scaled)
        pl.subplot(1, n, i + 1)
        pl.title(labels[i], fontsize=10)
        PlotGraph(x, atom/global_max, color="C0")
        pl.axis("off")
    pl.tight_layout(rect=[0, 0, 1, 0.95])
    out_dir = f"issue/event_study/{dep}/{spec}/representative_graphs"
    os.makedirs(out_dir, exist_ok=True)
    file_name = f"{out_dir}/rep_graphs_size{sizebary}_event_{event_time}.png"
    pl.savefig(file_name, dpi=300)

def PlotCombinedVerticalGraph(event_res, dep, spec, sizebary):
    """
    For each event time in event_res, create a subfigure (with one row and as many columns as graphs)
    titled with the event time, and vertically combine all subfigures into one final figure.
    """
    event_times = sorted(event_res.keys())
    n_events = len(event_times)
    fig = pl.figure(figsize=(8, 4 * n_events))
    subfigs = fig.subfigures(nrows=n_events, ncols=1)
    if n_events == 1:
        subfigs = [subfigs]

    for i, et in enumerate(event_times):
        labs, barys = event_res[et]
        n_graphs = len(barys)
        subfig = subfigs[i]
        subfig.suptitle(f"Event time: {et}", fontsize=12)
        axes = subfig.subplots(nrows=1, ncols=n_graphs)
        if n_graphs == 1:
            axes = [axes]
        global_max = max(bary.max() for bary in barys)
        for j in range(n_graphs):
            ax = axes[j]
            PlotSingleGraph(ax, barys[j], labs[j], global_max)
    
    fig.suptitle(f"Representative Graphs (size={sizebary}): Combined Event Times", fontsize=16)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out_dir = f"issue/event_study/{dep}/{spec}/representative_graphs"
    os.makedirs(out_dir, exist_ok=True)
    file_name = f"{out_dir}/rep_graphs_size{sizebary}_combined_vertical.png"
    fig.savefig(file_name, dpi=300)

def ProcessEventTime(rt, event_graphs, spec, sizebary, dep):
    """Process a single event time: compute barycenters, plot, and return results."""
    bary_list, labs = PrepareCalcData(event_graphs, spec)
    barys = [ComputeBarycenter(gl, sizebary) for gl in bary_list]
    PlotCombinedGraph(barys, labs, event_time=rt, dep=dep, spec=spec, sizebary=sizebary)
    return rt, (labs, barys)

def RepresentativeGraph(dep, spec, sizebary=20):
    """Load data, compute barycenters sequentially over event times, and produce plots."""
    graph_dict = LoadGraphDict(dep, spec)
    event_res = {}
    event_times = sorted(graph_dict.keys())
    
    for rt in event_times:
        rt, result = ProcessEventTime(rt, graph_dict[rt], spec, sizebary, dep)
        event_res[rt] = result
    
    PlotCombinedVerticalGraph(event_res, dep, spec, sizebary)

def Main():
    sizebary = int(sys.argv[1])
    for dep in ["graphs"]:
        for spec in spec_cov.keys():
            RepresentativeGraph(dep, spec, sizebary)

if __name__ == '__main__':
    Main()
