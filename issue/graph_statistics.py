#!/usr/bin/env python
# coding: utf-8


# In[3]:


import pandas as pd
from pathlib import Path
from source.lib.helpers import *
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import matplotlib.pyplot as plt
import random
import numpy as np
import networkx as nx
from source.derived.contributor_stats.calculate_contributions import *


# In[4]:


from glob import glob
filepaths = glob("issue/graphs/**/**", recursive=True)


# In[ ]:


def process_file(filepath):
    filename = os.path.basename(filepath)
    repo = filename.replace("_", "/").replace(".gexf", "")
    time_period = filepath.split(os.sep)[-2]
    G = nx.read_gexf(filepath)
    print(repo, time_period)
    return repo, time_period, G

valid_paths = [fp for fp in filepaths if fp.endswith(".gexf") and 
               not (fp.endswith("_pr.gexf") or fp.endswith("_issue.gexf") or fp.endswith("_pr_review.gexf"))]
graph_dict = {}

with ThreadPoolExecutor() as executor:
    for repo, time_period, G in executor.map(process_file, valid_paths):
        graph_dict.setdefault(repo, {})[time_period] = G


# In[8]:


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
            # All nodes have the same degree; pick the one with highest degree (they're all equal).
            important_nodes = {nodes[np.argmax(deg_values)]}
        else:
            z_scores = (deg_values - mean_deg) / std_deg
            important_nodes = set(np.array(nodes)[z_scores >= 3])
            # If none meets the threshold, pick the node with the highest degree.
            if not important_nodes:
                important_nodes = {np.array(nodes)[np.argmax(deg_values)]}
            
        modified_G = original_G.copy()
        nx.set_node_attributes(modified_G,
                               {node: (node in important_nodes) for node in modified_G.nodes()},
                               'important')

        edges_to_remove = [(u, v) for u, v in modified_G.edges()
                           if u in important_nodes and v in important_nodes]
        modified_G.remove_edges_from(edges_to_remove)

        degree_graph_dict.setdefault(repo, {})[time_period] = {
            'original': original_G,
            'important_edges_removed': modified_G
        }
    print(repo)


# In[10]:


assert(len(graph_dict) == len(degree_graph_dict))


# ## Mathematical Formulations
# 
# Let $ G = (V, E) $ be a graph with $|V|$ nodes, and let $ I \subseteq V $ be the set of important contributors.
# 
# 
# ### Importance Metrics
# For each $ i \in I $:
# 1. **Unnormalized Degree:**
#    $$
#    d_i = \text{degree}(i) \quad (\text{computed on } G')
#    $$
# 2. **Normalized Degree Centrality:**
#    $$
#    C_D(i) = \frac{d_i}{|V| - 1}
#    $$
# 3. **Betweenness Centrality:**
#    $$
#    C_B(i) = \text{betweenness}(i)
#    $$
# 
# Total number of important contributors:
# $$
# |I|
# $$
# 
# ### Defining Important Contributors
# 
# **1. Degree and Z-Score Calculation**
# 
# Let $G = (V, E)$ be an undirected graph, and for each node $v \in V$, let $d(v)$ denote its degree. Define the mean and standard deviation of the degrees as follows:
# 
# $$
# \mu = \frac{1}{|V|} \sum_{v \in V} d(v), \quad \sigma = \sqrt{\frac{1}{|V|} \sum_{v \in V} \left(d(v) - \mu\right)^2}.
# $$
# 
# Then, the z-score for node $v$ is given by:
# 
# $$
# z(v) = \frac{d(v) - \mu}{\sigma}.
# $$
# 
# **2. Important Nodes**
# 
# A node is considered an important contributor if its degree z-score exceeds $3.5$. Hence, the set of important nodes is defined as:
# 
# $$
# I = \{ v \in V \mid z(v) > 3.5 \}.
# $$
# 
# 3. **Modified Graph Construction**
# 
# Construct a modified graph $ G' = (V, E') $ by removing all edges connecting two important nodes:
# $$
# E' = E \setminus \{ (u, v) \in E \mid u \in I \text{ and } v \in I \}.
# $$
# For each node $ v \in V $, define the attribute:
# $$
# \text{important}(v) =
# \begin{cases}
# \text{True} & \text{if } v \in I, \\
# \text{False} & \text{otherwise}.
# \end{cases}
# $$
# 
# 
# ### Clustering Metrics
# 
# Let $ G' $ be the modified graph, and let $ C(i) $ be the connected component of $ G' $ that contains $ i $.
# 
# 1. **Individual Coverage:**  
#     For each important contributor $i$, let $E(i)$ denote its ego graph (i.e. the set of nodes consisting of $i$ and its immediate neighbors). Then the individual coverage is defined as:
#     $$
#     \text{Individual Coverage}(i) = \frac{|E(i)|}{|V|} \times 100\%,
#     $$
#     where $|V|$ is the total number of nodes in the graph.
# 
# 2. **Individual Coverage Cluster:**  
#     Let $U$ be the union of the ego graphs of all important contributors:
#     $$U = \bigcup_{j \in I} E(j).$$
#     Then, for each important contributor $i$, the individual coverage cluster is defined as:
#     $$\text{Individual Coverage Cluster}(i) = \frac{|E(i)|}{|U|} \times 100\%$$
# 
# 2. **Overall Unweighted Overlap:**  
#    Let $ N(i) $ be the set of neighbors of $ i $ in $ G' $ and define
#    $$
#    U_i = \bigcup_{\substack{j \in I \\ j \neq i}} N(j)
#    $$
#    Then:
#    $$
#    O(i) = \frac{|N(i) \cap U_i|}{|N(i)|} \times 100\%
#    $$
# 3. **Weighted Overall Overlap:**  
#    Let 
#    $$
#    w_i = \sum_{v \in N(i)} w(i,v)
#    $$
#    Then:
#    $$
#    W(i) = \frac{\sum_{v \in N(i) \cap U_i} w(i,v)}{w_i} \times 100\%
#    $$
# 4. **Aggregate Cluster Coverage:**  
#    Let $ \mathcal{C} $ be the set of connected components in $ G' $ that contain at least one important contributor.
#    $$
#    \text{Coverage}_{\text{agg}} = \frac{\left|\bigcup_{C \in \mathcal{C}} C\right|}{|V|} \times 100\%
#    $$
# 5. **Cluster-Level Weighted Averages:**  
#    For a cluster $ C $ with important contributors $ I_C $,
#    $$
#    \overline{O}_C = \frac{\sum_{i \in I_C} O(i) \, d_i}{\sum_{i \in I_C} d_i}, \quad
#    \overline{W}_C = \frac{\sum_{i \in I_C} W(i) \, d_i}{\sum_{i \in I_C} d_i}
#    $$
# 6. **Ego-Cluster Overlap Metrics:**  
#    For each $ i \in I $, define its ego cluster as:
#    $$
#    E_i = \{ i \} \cup N(i)
#    $$
#    Let 
#    $$
#    U = \bigcup_{i \in I} E_i
#    $$
#    and for each node $ v \in U $, let $ c(v) $ be the number of ego clusters that contain $ v $. Then:
#    - **Average Number of Ego Clusters per Node:**
#      $$
#      \text{AvgClusters} = \frac{1}{|U|} \sum_{v \in U} c(v)
#      $$
#    - **Percentage of Nodes in Exactly One Ego Cluster:**
#      $$
#      \text{PctOneCluster} = \frac{|\{ v \in U : c(v) = 1 \}|}{|U|} \times 100\%
#      $$
# 
# ### Communication Metrics
# 
# For each $ i \in I $, define:
# - $ E_i $: the set of edges incident on $ i $ in $ G' $ (with the other endpoint in the same connected component).
# - $ E^I_i $: the set of edges in the original graph $ G $ between $ i $ and other important contributors.
# 
# Then:
# 1. **Mean Edge Weight:**
#    $$
#    \bar{w}_i = \frac{1}{|E_i|} \sum_{e \in E_i} w(e), \quad \bar{w}^I_i = \frac{1}{|E^I_i|} \sum_{e \in E^I_i} w(e)
#    $$
# 2. **Standard Error (SE):**
#    $$
#    \text{SE}_i = \frac{\sigma_i}{\sqrt{|E_i|}}, \quad \sigma_i = \sqrt{\frac{1}{|E_i| - 1} \sum_{e \in E_i} \left(w(e) - \bar{w}_i\right)^2}
#    $$
# 3. **Percentiles:**  
#    The 10th, 25th, 50th, 75th, and 90th percentiles are computed from 
#    $$
#    \{ w(e) : e \in E_i \} \quad \text{(or from } \{ w(e) : e \in E^I_i \}\text{)}
#    $$
# 4. Additionally, for each important contributor, we record the weight of the edges connecting them to each other important contributor (from the original graph $ G $).
# 
# ### Graph Size
# 
# For the original graph $ G $:
# - **Total Nodes:** $|V|$
# - **Total Edges:** $|E|$

# In[9]:


def GetImportantContributors(G):
    """
    Return a set of nodes that are marked as important.
    Each node is expected to have an attribute 'important' that is True.
    """
    return {node for node, attr in G.nodes(data=True) if attr.get('important', False)}

def ComputeNeighborhoods(G):
    """
    Precompute and return the neighborhood (set of direct neighbors) for each node in G.
    """
    return {node: set(G.neighbors(node)) for node in G.nodes()}

def ComputeImportanceMetrics(G, important_contributors):
    """
    Compute basic importance statistics for each important contributor in G:
      - Unnormalized degree: d_i
      - Normalized degree centrality: C_D(i) = d_i / (|V| - 1)
      - Betweenness centrality: C_B(i)
    
    Returns a dictionary keyed by each important contributor.
    *Note: These metrics are computed on the modified graph G where edges between important contributors are removed.*
    """
    importance_metrics = {}
    n_nodes = G.number_of_nodes()
    betweenness = nx.betweenness_centrality(G)
    for node in G.nodes():#important_contributors:
        degree = G.degree(node) 
        normalized_degree = degree / (n_nodes - 1) if n_nodes > 1 else 0
        importance_metrics[node] = {
            'degree': degree,
            'normalized_degree': normalized_degree,
            'betweenness': betweenness.get(node, 0)
        }
    return importance_metrics

def ComputeOverlapMetrics(G, important_contributors, neighborhoods, weight='weight'):
    """
    For each important contributor, compute overlap metrics:
      - Pairwise unweighted overlap: for each other important contributor j, 
        the percentage of i's neighbors that are also neighbors of j.
      - Pairwise weighted overlap: the percentage of i's weighted degree contributed by common neighbors.
      - Overall unweighted overlap: percentage of i's neighbors shared with any other important contributor.
      - Overall weighted overlap: as above, but weighted by the sum of weights.
    
    Returns a dictionary keyed by each important contributor with keys:
       "pairwise", "weighted_pairwise", "any_other", and "weighted_any_other".
    """
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
    """
    Compute clustering metrics based on the modified graph G_modified.
    
    For each important contributor i, define its ego graph E(i) (i.e., i and its immediate neighbors).
    
    Then compute:
      - Individual Coverage:
          individual_coverage(i) = (|E(i)| / |V|) * 100,
        where |V| is the total number of nodes in G_modified.
      
      - Individual Coverage Cluster:
          individual_coverage_cluster(i) = (|E(i)| / |U|) * 100,
        where U = ∪_{j ∈ I} E(j) is the union of the ego graphs of all important contributors.
      
      - Overall overlap metrics are computed as before using overlap_metrics.
    
    Additionally, important contributors are grouped by connected components and cluster-level weighted averages are computed:
    
          For a cluster C with important contributors I_C:
          mean_overlap_C = (∑_{i ∈ I_C} O(i) · d_i) / (∑_{i ∈ I_C} d_i)
          mean_weighted_overlap_C = (∑_{i ∈ I_C} W(i) · d_i) / (∑_{i ∈ I_C} d_i)
    
    Returns a tuple:
      (clustering_metrics, cluster_aggregates)
      
      - clustering_metrics: dict mapping each important contributor to:
         {'individual_coverage': value, 'individual_coverage_cluster': value,
          'overall_overlap': value, 'weighted_overall_overlap': value}
      - cluster_aggregates: a dictionary with aggregated cluster-level statistics:
         {'mean_overlap': value, 'mean_weighted_overlap': value, 'total_clusters': value,
          'avg_clusters_per_node': value, 'pct_nodes_one_cluster': value}
    """
    clustering_metrics = {}
    total_nodes = G_modified.number_of_nodes()
    
    # Compute ego graphs for each important contributor and build the union U.
    ego_graphs = {}
    U = set()
    for i in important_contributors:
        ego = nx.ego_graph(G_modified, i, radius=1)
        ego_graphs[i] = ego
        U.update(ego.nodes())
    U_size = len(U)
    
    # For each important contributor, compute the new individual coverage metrics.
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
    
    # Group important contributors by their connected component.
    components = list(nx.connected_components(G_modified))
    clusters = {}
    for i in important_contributors:
        comp = next((comp for comp in components if i in comp), {i})
        comp_key = tuple(sorted(comp))
        clusters.setdefault(comp_key, []).append(i)
    
    # Aggregate cluster-level weighted averages.
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
    
    # Compute ego-cluster metrics.
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
    """
    Compute the aggregate ego cluster coverage: the percentage of all nodes in G_modified
    that are contained in the ego clusters (ego graphs) of the important contributors.
    
    For each important contributor in G_modified, compute its ego graph (neighbors within the given radius)
    and take the union of these nodes. Coverage is calculated as:
      Coverage_agg = (|∪_{i in important_contributors} (ego_graph(i).nodes())| / |V|) * 100%
    
    Args:
      G_modified: A NetworkX graph.
      important_contributors: An iterable of nodes considered important.
      radius: The radius for the ego graph (default is 1).
      
    Returns:
      The aggregate coverage as a percentage.
    """
    nodes_in_ego_clusters = set()
    for node in important_contributors:
        if node in G_modified:
            ego_cluster = nx.ego_graph(G_modified, node, radius=radius)
            nodes_in_ego_clusters.update(ego_cluster.nodes())
    
    total_nodes = G_modified.number_of_nodes()
    coverage = (len(nodes_in_ego_clusters) / total_nodes * 100) if total_nodes > 0 else 0
    return coverage


def ComputeAggregateConnectedComponentCoverage(G_modified, important_contributors):
    """
    Compute the aggregate cluster coverage: the percentage of all nodes in G_modified that are
    contained in clusters (connected components) that include at least one important contributor.
    
    Returns:
      Coverage_agg = (|∪{ C in components with I ∩ C ≠ ∅ }| / |V|) * 100%
    """
    components = list(nx.connected_components(G_modified))
    nodes_in_important_clusters = set()
    for comp in components:
        if comp.intersection(important_contributors):
            nodes_in_important_clusters.update(comp)
    total_nodes = G_modified.number_of_nodes()
    coverage = (len(nodes_in_important_clusters) / total_nodes * 100) if total_nodes > 0 else 0
    return coverage

def ComputeCommunicationMetrics(G_modified, G_original, important_contributors):
    """
    Compute communication metrics.
    
    Using the modified graph (G_modified) that has important-to-important edges removed, for each important contributor,
    compute:
      - Mean edge weight, standard error (SE), and percentiles (10th, 25th, 50th, 75th, 90th)
        for edges connecting the contributor with others in the same connected component.
    
    Using the original graph (G_original), for the subgraph induced by important contributors, compute similar metrics.
    
    Returns a tuple:
       (comm_metrics, imp_comm_metrics)
       
       comm_metrics: dict keyed by important contributor with:
           {'avg_edge_weight': value, 'se_edge_weight': value, 'percentiles': {'10': ..., '25': ..., '50': ..., '75': ..., '90': ...}}
       imp_comm_metrics: similar dictionary for the subgraph of important contributors.
    """
    
    comm_metrics = {}
    components = list(nx.connected_components(G_modified))
    comp_map = {}
    for comp in components:
        for node in comp:
            comp_map[node] = comp
    #### ANYONE -> NOT IMPORTANT CONTRIBUTORS 
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
    # Now compute ego-based communication metrics for all other contributors
    all_nodes = set(G_original.nodes())
    other_contributors = all_nodes - set(important_contributors)
    other_comm_metrics_ego = {}
    
    for node in other_contributors:
        # Get the ego graph of the node (neighbors within radius 1)
        ego = nx.ego_graph(G_modified, node, radius=1)
        # Among the ego graph, select only those neighbors that are NOT important (and exclude self)
        nonimportant_neighbors = [nbr for nbr in ego.nodes() if nbr != node and nbr not in important_contributors]
        # For the node, get weights of edges to these non-important neighbors from the original graph.
        # (Alternatively, you could use G_modified if that makes more sense.)
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
    
    #### ANYONE -> IMPORTANT CONTRIBUTORS 
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
        # Consider only edges from 'node' that connect it to an important contributor.
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
    """
    Return a tuple (n, m) where n is the number of nodes and m is the number of edges in graph G.
    """
    return G.number_of_nodes(), G.number_of_edges()


def ComputeRepoGraphMetrics(repo, time_period, data):
    G_mod = data.get('important_edges_removed')
    G_orig = data.get('original')
    if G_mod is None or G_orig is None:
        # If one of the graphs is missing, skip this repo/time.
        print(f"{repo} is not available in {time_period}")
        return None
    important_contributors = GetImportantContributors(G_mod)
    if not important_contributors:
        return None

    # Precompute common metrics.
    neighborhoods = ComputeNeighborhoods(G_mod)
    importance = ComputeImportanceMetrics(G_orig, important_contributors)
    overlap_metrics = ComputeOverlapMetrics(G_mod, important_contributors, neighborhoods)
    clustering_metrics, cluster_aggregates = ComputeClusteringMetrics(G_mod, important_contributors, overlap_metrics)
    agg_cluster_cov = ComputeAggregateEgoClusterCoverage(G_mod, important_contributors, radius=1)
    agg_cc_cov = ComputeAggregateConnectedComponentCoverage(G_mod, important_contributors)
    comm_metrics, imp_comm_metrics = ComputeCommunicationMetrics(G_mod, G_orig, important_contributors)
    
    # Compute important-to-important edge weights (across all important contributors)
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
        imp_to_imp_overall = {
            'avg_edge_weight': 0,
            'se_edge_weight': 0,
            'percentiles': {'10': 0, '25': 0, '50': 0, '75': 0, '90': 0}
        }
    
    # Build node-level metrics with a dictionary comprehension.
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
        'imp_to_imp_comm_overall': imp_to_imp_overall
    }
    
    repo_time_metrics = {**node_metrics, 'repo_overall': overall_metrics}
    return repo, time_period, repo_time_metrics

def ComputeAllGraphMetrics(degree_graph_dict):
    # Build list of tasks: each task is a tuple (repo, time_period, data).
    tasks = [(repo, tp, data)
             for repo, repo_dict in degree_graph_dict.items()
             for tp, data in repo_dict.items()]
    
    all_metrics = {}
    with ThreadPoolExecutor() as executor:
        # Process all repo-time pairs in parallel.
        for result in executor.map(lambda args: ComputeRepoGraphMetrics(*args), tasks):
            if result is not None:
                repo, time_period, metrics = result
                all_metrics.setdefault(repo, {})[time_period] = metrics
                print(repo, time_period)
    return all_metrics


# In[ ]:


graph_metrics = ComputeAllGraphMetrics(degree_graph_dict)


# In[ ]:


import json
with open("issue/graph_metrics.json", "w") as f:
    json.dump(graph_metrics, f, indent=4)

