#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import matplotlib.pyplot as plt
import random
import numpy as np
import networkx as nx
from glob import glob
from ot.gromov import gromov_barycenters
import scipy as sp
from scipy.sparse.csgraph import shortest_path
import sys
import itertools

specification_covariates = {
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
    spec_list = list(specification_covariates.keys())
    departure_type_list = ["graphs"]

    tasks = [(departure_type, spec, sizebary) 
            for departure_type in departure_type_list 
            for spec in spec_list]

    for dt, spc, sizebary in tasks:
        RepresentativeGraph(dt, spc, sizebary)

def RepresentativeGraph(departure_type, spec, sizebary=20):
    df = pd.read_csv(f"issue/event_study/{departure_type}/early_sample.csv")
    read_data = df.assign(relative_time=lambda x: x.time_index - x.treatment_group)[['repo_name', 'time_period', 'relative_time'] + specification_covariates[spec]]
    read_data['time_period'] = pd.to_datetime(read_data['time_period'])
    read_data = read_data.query('relative_time > -4 & relative_time <= 4')
    
    graph_dict = {}
    with ThreadPoolExecutor() as executor:
        for repo, relative_time, G in executor.map(ProcessFile, read_data.to_dict('records')):
            if G:
                graph_dict.setdefault(relative_time, {})[repo] = G
                
    for event_time in sorted(graph_dict.keys()):
        graph_name = f"pre{abs(event_time)}" if event_time < 0 else f"post{abs(event_time)}"
        all_repos_graphs = graph_dict[event_time].values()
        GromovBarycenter(list(all_repos_graphs), event_time, spec, departure_type, sizebary=sizebary, filename = f"{graph_name}_barycenter_size{sizebary}.txt")
        print("Barycenter for", event_time, "computed", spec)
        num_vars = specification_covariates[spec]
        for combo in list(itertools.product([0, 1], repeat=len(num_vars))):
            combo_filename = "_".join(f"{num_vars[i]}_2p_back_bin_median{combo[i]}" for i in range(len(num_vars)))
            df_combo = pd.read_csv(f'issue/event_study/graphs/{spec}/{combo_filename}.csv')
            combo_repos = list(df_combo['repo_name'].apply(lambda x: x.replace("/","_")).unique())
            combo_repos_graphs = [graph_dict[event_time][combo_repo] for combo_repo in combo_repos if combo_repo in graph_dict[event_time].keys()]
            GromovBarycenter(combo_repos_graphs, event_time, spec, departure_type, sizebary=sizebary, filename = f"{graph_name}_barycenter_size{sizebary}_{combo_filename}.txt")
            print("Barycenter for", event_time, "computed", spec, combo_filename)

def ProcessFile(read_data_row):
    repo = read_data_row['repo_name'].replace("/", "_")
    time_period = read_data_row['time_period']
    time_period_str = time_period.strftime('%Y%m')
    relative_time = read_data_row['relative_time']
    try:
        G = nx.read_gexf(f"drive/output/derived/graph_structure/graphs/{time_period_str}/{repo}.gexf")
    except Exception as e:
        print(f"Error reading graph for {repo} at {time_period_str}: {e}")
        G = False
    return repo, relative_time, G

def ComputeCostMatrix(G):
    nodes = list(G.nodes())
    lengths = dict(nx.all_pairs_shortest_path_length(G))
    LARGE_VALUE = 1e6
    cost = np.array([[lengths[u].get(v, LARGE_VALUE) for v in nodes] for u in nodes])
    return cost 
    
def GromovBarycenter(graph_list, event_time, spec, departure_type, sizebary, filename):
    Cs = [ComputeCostMatrix(G) for G in graph_list]
    C = gromov_barycenters(sizebary, Cs, tol=1e-3, max_iter=100, verbose = False)
    
    output_dir = Path(f"issue/event_study/{departure_type}/{spec}/representative_graphs")
    output_dir.mkdir(parents=True, exist_ok=True)
    np.savetxt(output_dir / filename, C)

if __name__ == '__main__':
    Main()
