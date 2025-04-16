#!/usr/bin/env python

import os
import pandas as pd
from pathlib import Path
from source.lib.helpers import *
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import networkx as nx
from source.derived.contributor_stats.calculate_contributions import *
from glob import glob
import json

def Main():
    indir = "drive/output/derived/graph_structure/graphs"
    outdir = Path("drive/output/derived/graph_structure")
    graph_dict = CreateGraphDict(indir)
    degree_graph_dict = CreateDegreeGraphDict(graph_dict)
    assert(len(graph_dict) == len(degree_graph_dict))
    graph_metrics = ComputeAllGraphMetrics(degree_graph_dict)
    with open(outdir / "graph_metrics.json", "w") as f:
        json.dump(graph_metrics, f, indent=4)

def CreateGraphDict(indir):
    filepaths = glob(f"{indir}/**/**", recursive=True)
    valid_paths = [fp for fp in filepaths if fp.endswith(".gexf") and not (fp.endswith("_pr.gexf") or fp.endswith("_issue.gexf") or fp.endswith("_pr_review.gexf"))]
    graph_dict = {}
    with ThreadPoolExecutor() as executor:
        for repo, time_period, G in executor.map(ProcessFile, valid_paths):
            graph_dict.setdefault(repo, {})[time_period] = G
    return graph_dict

def ProcessFile(filepath):
    filename = os.path.basename(filepath)
    repo = filename.replace("_", "/").replace(".gexf", "")
    time_period = filepath.split(os.sep)[-2]
    G = nx.read_gexf(filepath)
    print(repo, time_period)
    return repo, time_period, G

def CreateDegreeGraphDict(graph_dict):
    degree_graph_dict = {}
    for repo, repo_dict in graph_dict.items():
        for time_period, G in repo_dict.items():
            original_G = G.copy()
            deg_data = list(original_G.degree())
            if not deg_data:
                continue
            nodes, deg_values = zip(*deg_data)
            deg_values = np.array(deg_values, dtype=float)
            mean_deg = deg_values.mean()
            std_deg = deg_values.std()
            if std_deg == 0:
                important_nodes = {nodes[np.argmax(deg_values)]}
            else:
                z_scores = (deg_values - mean_deg) / std_deg
                important_nodes = set(np.array(nodes)[z_scores >= 1])
                if not important_nodes:
                    important_nodes = {np.array(nodes)[np.argmax(deg_values)]}
            modified_G = original_G.copy()
            nx.set_node_attributes(modified_G, {node: (node in important_nodes) for node in modified_G.nodes()}, 'important')
            edges_to_remove = [(u, v) for u, v in modified_G.edges() if u in important_nodes and v in important_nodes]
            modified_G.remove_edges_from(edges_to_remove)
            degree_graph_dict.setdefault(repo, {})[time_period] = {
                'original': original_G,
                'important_edges_removed': modified_G
            }
        print(repo)
    return degree_graph_dict

def GetImportantContributors(G):
    return {node for node, attr in G.nodes(data=True) if attr.get('important', False)}

def ComputeNeighborhoods(G):
    return {node: set(G.neighbors(node)) for node in G.nodes()}

def ComputeImportanceMetrics(G):
    importance_metrics = {}
    n_nodes = G.number_of_nodes()
    betweenness = nx.betweenness_centrality(G)
    for node in G.nodes():
        degree = G.degree(node)
        normalized_degree = degree / (n_nodes - 1) if n_nodes > 1 else 0
        importance_metrics[node] = {
            'degree': degree,
            'normalized_degree': normalized_degree,
            'betweenness': betweenness.get(node, 0)
        }
    return importance_metrics

def ComputeOverlapMetrics(G, important_contributors, neighborhoods, weight='weight'):
    metrics_results = {}
    for i in important_contributors:
        neighbors_i = neighborhoods.get(i, set())
        degree_i = len(neighbors_i)
        weighted_degree_i = sum(G[i][nbr].get(weight, 1) for nbr in neighbors_i)
        pairwise_overlap = {}
        weighted_pairwise_overlap = {}
        for j in important_contributors:
            if i == j:
                continue
            neighbors_j = neighborhoods.get(j, set())
            common_neighbors = neighbors_i.intersection(neighbors_j)
            overlap = (len(common_neighbors) / degree_i * 100) if degree_i > 0 else 0
            pairwise_overlap[j] = overlap
            weighted_common = sum(G[i][v].get(weight, 1) for v in common_neighbors)
            weighted_overlap = (weighted_common / weighted_degree_i * 100) if weighted_degree_i > 0 else 0
            weighted_pairwise_overlap[j] = weighted_overlap
        union_neighbors = set()
        for j in important_contributors:
            if i == j:
                continue
            union_neighbors |= neighborhoods.get(j, set())
        overall_overlap = (len(neighbors_i.intersection(union_neighbors)) / degree_i * 100) if degree_i > 0 else 0
        weighted_common_total = sum(G[i][v].get(weight, 1) for v in neighbors_i.intersection(union_neighbors))
        overall_weighted_overlap = (weighted_common_total / weighted_degree_i * 100) if weighted_degree_i > 0 else 0
        metrics_results[i] = {
            "pairwise": pairwise_overlap,
            "weighted_pairwise": weighted_pairwise_overlap,
            "any_other": overall_overlap,
            "weighted_any_other": overall_weighted_overlap
        }
    return metrics_results

def ComputeClusteringMetrics(G_modified, important_contributors, overlap_metrics):
    clustering_metrics = {}
    total_nodes = G_modified.number_of_nodes()
    ego_graphs = {}
    U = set()
    for i in important_contributors:
        ego = nx.ego_graph(G_modified, i, radius=1)
        ego_graphs[i] = ego
        U.update(ego.nodes())
    U_size = len(U)
    for i in important_contributors:
        ego = ego_graphs[i]
        ind_cov = (ego.number_of_nodes() / total_nodes * 100) if total_nodes > 0 else 0
        ind_cov_cluster = (ego.number_of_nodes() / U_size * 100) if U_size > 0 else 0
        clustering_metrics[i] = {
            'individual_coverage': ind_cov,
            'individual_coverage_cluster': ind_cov_cluster,
            'overall_overlap': overlap_metrics.get(i, {}).get('any_other', None),
            'weighted_overall_overlap': overlap_metrics.get(i, {}).get('weighted_any_other', None)
        }
    components = list(nx.connected_components(G_modified))
    clusters = {}
    for i in important_contributors:
        comp = next((comp for comp in components if i in comp), {i})
        comp_key = tuple(sorted(comp))
        clusters.setdefault(comp_key, []).append(i)
    total_overlap = 0
    total_weight_overlap = 0
    total_cluster_nodes = 0
    for comp_key, nodes in clusters.items():
        total_weight = sum(G_modified.degree(n) for n in nodes)
        if total_weight > 0:
            mean_overlap = sum(overlap_metrics[n]['any_other'] * G_modified.degree(n) for n in nodes) / total_weight
            mean_weighted_overlap = sum(overlap_metrics[n]['weighted_any_other'] * G_modified.degree(n) for n in nodes) / total_weight
        else:
            mean_overlap = 0
            mean_weighted_overlap = 0
        total_overlap += mean_overlap * len(comp_key)
        total_weight_overlap += mean_weighted_overlap * len(comp_key)
        total_cluster_nodes += len(comp_key)
    if total_cluster_nodes > 0:
        agg_mean_overlap = total_overlap / total_cluster_nodes
        agg_mean_weighted_overlap = total_weight_overlap / total_cluster_nodes
    else:
        agg_mean_overlap = agg_mean_weighted_overlap = 0
    cluster_aggregates = {
        'mean_overlap': agg_mean_overlap,
        'mean_weighted_overlap': agg_mean_weighted_overlap,
        'total_clusters': len(clusters)
    }
    cluster_counts = {}
    for v in U:
        count = sum(1 for ego in ego_graphs.values() if v in ego)
        cluster_counts[v] = count
    if U:
        avg_clusters = sum(cluster_counts.values()) / len(U)
        one_cluster = sum(1 for count in cluster_counts.values() if count == 1)
        pct_one_cluster = (one_cluster / len(U)) * 100
    else:
        avg_clusters = 0
        pct_one_cluster = 0
    cluster_aggregates['avg_clusters_per_node'] = avg_clusters
    cluster_aggregates['pct_nodes_one_cluster'] = pct_one_cluster
    return clustering_metrics, cluster_aggregates

def ComputeAggregateEgoClusterCoverage(G_modified, important_contributors, radius=1):
    nodes_in_ego_clusters = set()
    for node in important_contributors:
        if node in G_modified:
            ego_cluster = nx.ego_graph(G_modified, node, radius=radius)
            nodes_in_ego_clusters.update(ego_cluster.nodes())
    total_nodes = G_modified.number_of_nodes()
    coverage = (len(nodes_in_ego_clusters) / total_nodes * 100) if total_nodes > 0 else 0
    return coverage

def ComputeAggregateConnectedComponentCoverage(G_modified, important_contributors):
    components = list(nx.connected_components(G_modified))
    nodes_in_important_clusters = set()
    for comp in components:
        if comp.intersection(important_contributors):
            nodes_in_important_clusters.update(comp)
    total_nodes = G_modified.number_of_nodes()
    coverage = (len(nodes_in_important_clusters) / total_nodes * 100) if total_nodes > 0 else 0
    return coverage

def ComputeCommunicationMetrics(G_modified, G_original, important_contributors):
    comm_metrics = {}
    components = list(nx.connected_components(G_modified))
    comp_map = {}
    for comp in components:
        for node in comp:
            comp_map[node] = comp
    for node in important_contributors:
        comp = comp_map.get(node, {node})
        weights = [G_modified[node][nbr]['weight'] for nbr in G_modified.neighbors(node) if nbr in comp]
        if weights:
            arr = np.array(weights)
            mean_val = np.mean(arr)
            se_val = np.std(arr, ddof=1) / np.sqrt(len(arr)) if len(arr) > 1 else 0
            percentiles = np.percentile(arr, [10, 25, 50, 75, 90])
        else:
            mean_val = se_val = 0
            percentiles = [0, 0, 0, 0, 0]
        comm_metrics[node] = {
            'avg_edge_weight': mean_val,
            'se_edge_weight': se_val,
            'percentiles': {
                '10': percentiles[0],
                '25': percentiles[1],
                '50': percentiles[2],
                '75': percentiles[3],
                '90': percentiles[4]
            }
        }
    all_nodes = set(G_original.nodes())
    other_contributors = all_nodes - set(important_contributors)
    for node in other_contributors:
        ego = nx.ego_graph(G_modified, node, radius=1)
        nonimportant_neighbors = [nbr for nbr in ego.nodes() if nbr != node and nbr not in important_contributors]
        weights = [G_modified[node][nbr]['weight'] for nbr in G_modified.neighbors(node) if nbr in nonimportant_neighbors]
        if weights:
            arr = np.array(weights)
            mean_val = np.mean(arr)
            se_val = np.std(arr, ddof=1) / np.sqrt(len(arr)) if len(arr) > 1 else 0
            percentiles = np.percentile(arr, [10, 25, 50, 75, 90])
        else:
            mean_val = se_val = 0
            percentiles = [0, 0, 0, 0, 0]
        comm_metrics[node] = {
            'avg_edge_weight': mean_val,
            'se_edge_weight': se_val,
            'percentiles': {
                '10': percentiles[0],
                '25': percentiles[1],
                '50': percentiles[2],
                '75': percentiles[3],
                '90': percentiles[4]
            }
        }
    imp_comm_metrics = {}
    subG = G_original.subgraph(important_contributors)
    for node in important_contributors:
        weights = [subG[node][nbr]['weight'] for nbr in subG.neighbors(node)]
        if weights:
            arr = np.array(weights)
            mean_val = np.mean(arr)
            se_val = np.std(arr, ddof=1) / np.sqrt(len(arr)) if len(arr) > 1 else 0
            percentiles = np.percentile(arr, [10, 25, 50, 75, 90])
        else:
            mean_val = se_val = 0
            percentiles = [0, 0, 0, 0, 0]
        imp_comm_metrics[node] = {
            'avg_edge_weight': mean_val,
            'se_edge_weight': se_val,
            'percentiles': {
                '10': percentiles[0],
                '25': percentiles[1],
                '50': percentiles[2],
                '75': percentiles[3],
                '90': percentiles[4]
            }
        }
    all_nodes = set(G_original.nodes())
    important_set = set(important_contributors)
    other_contributors = all_nodes - important_set
    for node in other_contributors:
        nbrs = [nbr for nbr in G_original.neighbors(node) if nbr in important_set]
        weights = [G_original[node][nbr]['weight'] for nbr in nbrs]
        if weights:
            arr = np.array(weights)
            mean_val = np.mean(arr)
            se_val = np.std(arr, ddof=1) / np.sqrt(len(arr)) if len(arr) > 1 else 0
            percentiles = np.percentile(arr, [10, 25, 50, 75, 90])
        else:
            mean_val = se_val = 0
            percentiles = [0, 0, 0, 0, 0]
        imp_comm_metrics[node] = {
            'avg_edge_weight': mean_val,
            'se_edge_weight': se_val,
            'percentiles': {
                '10': percentiles[0],
                '25': percentiles[1],
                '50': percentiles[2],
                '75': percentiles[3],
                '90': percentiles[4]
            }
        }
    return comm_metrics, imp_comm_metrics

def GetGraphSize(G):
    return G.number_of_nodes(), G.number_of_edges()

def ComputeRepoGraphMetrics(repo, time_period, data):
    G_mod = data.get('important_edges_removed')
    G_orig = data.get('original')
    if G_mod is None or G_orig is None:
        print(f"{repo} is not available in {time_period}")
        return None
    important_contributors = GetImportantContributors(G_mod)
    if not important_contributors:
        return None
    neighborhoods = ComputeNeighborhoods(G_mod)
    importance = ComputeImportanceMetrics(G_orig)
    overlap_metrics = ComputeOverlapMetrics(G_mod, important_contributors, neighborhoods)
    clustering_metrics, cluster_aggregates = ComputeClusteringMetrics(G_mod, important_contributors, overlap_metrics)
    agg_cluster_cov = ComputeAggregateEgoClusterCoverage(G_mod, important_contributors, radius=1)
    agg_cc_cov = ComputeAggregateConnectedComponentCoverage(G_mod, important_contributors)
    comm_metrics, imp_comm_metrics = ComputeCommunicationMetrics(G_mod, G_orig, important_contributors)
    subG = G_orig.subgraph(important_contributors)
    imp_to_imp_edge_weights = {
        node: {nbr: d['weight'] for nbr, d in subG[node].items()}
        for node in important_contributors
    }
    weights_all = [d['weight'] for u, v, d in subG.edges(data=True)]
    if weights_all:
        mean_all = np.mean(weights_all)
        se_all = np.std(weights_all, ddof=1) / np.sqrt(len(weights_all)) if len(weights_all) > 1 else 0
        perc_all = np.percentile(np.array(weights_all), [10, 25, 50, 75, 90])
        imp_to_imp_overall = {
            'avg_edge_weight': mean_all,
            'se_edge_weight': se_all,
            'percentiles': {
                '10': perc_all[0],
                '25': perc_all[1],
                '50': perc_all[2],
                '75': perc_all[3],
                '90': perc_all[4]
            }
        }
    else:
        imp_to_imp_overall = {}
    other_contributors = set(G_orig.nodes()) - set(important_contributors)
    imp_to_other_weights = [
        d['weight']
        for u, v, d in G_orig.edges(data=True)
        if (u in important_contributors and v not in important_contributors) or 
        (v in important_contributors and u not in important_contributors)
    ]
    if imp_to_other_weights:
        mean_other = np.mean(imp_to_other_weights)
        se_other = np.std(imp_to_other_weights, ddof=1) / np.sqrt(len(imp_to_other_weights)) if len(imp_to_other_weights) > 1 else 0
        perc_other = np.percentile(np.array(imp_to_other_weights), [10, 25, 50, 75, 90])
        imp_to_other_overall = {
            'avg_edge_weight': mean_other,
            'se_edge_weight': se_other,
            'percentiles': {
                '10': perc_other[0],
                '25': perc_other[1],
                '50': perc_other[2],
                '75': perc_other[3],
                '90': perc_other[4]
            }
        }
    else:
        imp_to_other_overall = {}
    node_metrics = {
        contributor: {
            **importance.get(contributor, {}),
            'pairwise_overlap': overlap_metrics.get(contributor, {}).get('pairwise', {}),
            'weighted_pairwise_overlap': overlap_metrics.get(contributor, {}).get('weighted_pairwise', {}),
            'overall_overlap': overlap_metrics.get(contributor, {}).get('any_other'),
            'weighted_overall_overlap': overlap_metrics.get(contributor, {}).get('weighted_any_other'),
            'individual_coverage': clustering_metrics.get(contributor, {}).get('individual_coverage'),
            'individual_coverage_cluster': clustering_metrics.get(contributor, {}).get('individual_coverage_cluster'),
            **comm_metrics.get(contributor, {}),
            'imp_to_imp_comm': imp_comm_metrics.get(contributor, {}),
            'imp_to_imp_edge_weights': imp_to_imp_edge_weights.get(contributor, {})
        }
        for contributor in important_contributors
    }
    all_nodes = set(G_orig.nodes())
    important_set = set(important_contributors)
    other_contributors = all_nodes - important_set
    for contributor in other_contributors:
        node_metrics[contributor] = {
            **importance.get(contributor, {}),
            **comm_metrics.get(contributor, {}),
            'imp_to_imp_comm': imp_comm_metrics.get(contributor, {}),
        }
    total_nodes, total_edges = GetGraphSize(G_orig)
    overall_metrics = {
        'total_nodes': total_nodes,
        'total_edges': total_edges,
        'aggregate_connected_component_coverage': agg_cc_cov,
        'aggregate_cluster_coverage': agg_cluster_cov,
        'cluster_averages': cluster_aggregates,
        'total_important': len(important_contributors),
        'imp_to_imp_comm_overall': imp_to_imp_overall,
        'imp_to_other_comm_overall': imp_to_other_overall
    }
    repo_time_metrics = {**node_metrics, 'repo_overall': overall_metrics}
    return repo, time_period, repo_time_metrics

def ComputeAllGraphMetrics(degree_graph_dict):
    tasks = [(repo, tp, data)
             for repo, repo_dict in degree_graph_dict.items()
             for tp, data in repo_dict.items()]
    all_metrics = {}
    with ThreadPoolExecutor() as executor:
        for result in executor.map(lambda args: ComputeRepoGraphMetrics(*args), tasks):
            if result is not None:
                repo, time_period, metrics = result
                all_metrics.setdefault(repo, {})[time_period] = metrics
                print(repo, time_period)
    return all_metrics


if __name__ == '__main__':
    Main()
