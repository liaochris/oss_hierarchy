import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import Normalize, LinearSegmentedColormap
from fa2 import ForceAtlas2  # pip install fa2

def plot_gephi_style(G, output_image='gephi_plot.png', repulsion_strength=1000):
    """
    Reads a GEXF file, computes a ForceAtlas2 layout with given repulsion strength,
    and creates a plot with:
      1. A ForceAtlas2 layout using outbound attraction distribution (so hubs are less separated).
      2. Node colors on a light-to-dark teal scale based on weighted degree.
      3. Node sizes scaled by weighted degree.
      4. Edge colors on a light-to-dark greyscale (based on edge weight).

    Parameters:
      gexf_file (str): Path to the input GEXF file.
      output_image (str): Filename for the saved PNG image.
      repulsion_strength (float): Scaling ratio for the ForceAtlas2 layout.
    """    
    # Compute ForceAtlas2 layout with hubs being less separated.
    forceatlas2 = ForceAtlas2(
        scalingRatio=repulsion_strength,
        gravity=1.0,
        jitterTolerance=1.0,
        barnesHutOptimize=False,
        outboundAttractionDistribution=True  # This reduces repulsion for high-degree hubs.
    )
    positions = forceatlas2.forceatlas2_networkx_layout(G, pos=None, iterations=2000)
    
    # --- Node Visualization Setup ---
    # Compute weighted degree for each node (use default weight=1 if missing).
    weighted_degrees = np.array([
        sum(data.get("weight", 1) for _, _, data in G.edges(nbunch=[n], data=True))
        for n in G.nodes()
    ])
    norm_node = Normalize(vmin=weighted_degrees.min(), vmax=weighted_degrees.max())
    
    # Create a custom colormap from light teal to dark teal.
    teal_cmap = LinearSegmentedColormap.from_list("teal_scale", ["#B2DFDB", "#00695C"])
    node_colors = [teal_cmap(norm_node(wd)) for wd in weighted_degrees]
    
    # Optionally scale node sizes by weighted degree.
    min_size = 100
    max_size = 300
    if weighted_degrees.max() != weighted_degrees.min():
        node_sizes = min_size + (weighted_degrees - weighted_degrees.min()) / (weighted_degrees.max() - weighted_degrees.min()) * (max_size - min_size)
    else:
        node_sizes = np.full_like(weighted_degrees, min_size)
    
    # --- Edge Visualization Setup ---
    # Get edge weights (default to 1 if no weight attribute).
    edge_weights = np.array([data.get("weight", 1) for _, _, data in G.edges(data=True)])
    norm_edge = Normalize(vmin=edge_weights.min(), vmax=edge_weights.max())
    grey_cmap = plt.cm.Greys
    edge_colors = [grey_cmap(norm_edge(data.get("weight", 1))) for _, _, data in G.edges(data=True)]
    
    # --- Plotting ---
    plt.figure(figsize=(12, 12))
    nx.draw_networkx_edges(G, pos=positions, edge_color=edge_colors, alpha=0.7)
    nx.draw_networkx_nodes(G, pos=positions, node_color=node_colors, node_size=node_sizes)
    plt.axis('off')
    plt.title("ForceAtlas2 Layout with Light-to-Dark Teal Node Colors")
    plt.savefig(output_image, format='PNG', dpi=300)
    plt.show()


def calculate_empirical_distribution(graph_list, num_points=101):
    """
    Given a list of graphs, calculates for each percentile (0, 1, ..., 100) 
    the average relative weighted degree and connection ratio across all graphs.
    
    For each graph:
      - Compute each node's weighted degree (sum of edge weights, default weight=1).
      - Normalize by dividing by the maximum weighted degree (relative weighted degree).
      - Compute connection ratio = degree / (n - 1).
      - Use np.percentile to get the value at each percentile.
      
    Returns:
      A numpy array of shape (num_points, 3) where each row is:
      [percentile, mean_relative_weighted_degree, mean_connection_ratio]
    """
    import numpy as np
    percentiles = np.linspace(0, 100, num_points)
    rel_wd_values = []
    conn_ratio_values = []
    
    for G in graph_list:
        n = G.number_of_nodes()
        if n <= 1:
            continue
        # Compute weighted degree for each node (default weight=1 if missing).
        wd = np.array([sum(data.get("weight", 1) for _, _, data in G.edges(nbunch=[node], data=True))
                       for node in G.nodes()])
        max_wd = wd.max() if wd.max() > 0 else 1
        rel_wd = wd / max_wd
        
        # Compute connection ratio = degree/(n-1)
        deg = np.array([G.degree(node) for node in G.nodes()])
        conn_ratio = deg / (n - 1)
        
        # Get percentile values for this graph.
        rel_wd_percentiles = np.percentile(rel_wd, percentiles)
        conn_ratio_percentiles = np.percentile(conn_ratio, percentiles)
        
        rel_wd_values.append(rel_wd_percentiles)
        conn_ratio_values.append(conn_ratio_percentiles)
    
    rel_wd_values = np.array(rel_wd_values)
    conn_ratio_values = np.array(conn_ratio_values)
    
    mean_rel_wd = np.mean(rel_wd_values, axis=0)
    mean_conn_ratio = np.mean(conn_ratio_values, axis=0)
    
    return np.column_stack((percentiles, mean_rel_wd, mean_conn_ratio))


def modify_graph_to_match_empirical(G, graph_list, sigma_wd=0.1, sigma_conn=0.1, max_iterations=1000):
    """
    For each node in G, adjust its metrics by removing edges (without disconnecting nodes)
    so that its normalized weighted degree and connection ratio (degree/(n-1)) match the 
    empirical target values for its percentile rank (based on weighted degree) across all graphs.
    
    Parameters:
      G : networkx.Graph
         The input graph.
      empirical_data : array-like of shape (N,3)
         Empirical distribution data with columns: [percentile, target_rel_wd, target_conn_ratio],
         where percentiles range from 0 to 100.
      sigma_wd : float
         Standard deviation used to normalize weighted degree error.
      sigma_conn : float
         Standard deviation used to normalize connection ratio error.
      max_iterations : int
         Maximum number of edge removals.
    
    Returns:
      networkx.Graph
         The modified graph.
    """
    import networkx as nx
    import numpy as np

    empirical_data = calculate_empirical_distribution(graph_list)
    percentiles_emp = empirical_data[:, 0]
    target_rel_wd_emp = empirical_data[:, 1]
    target_conn_emp = empirical_data[:, 2]

    def compute_node_metrics(G):
        n = G.number_of_nodes()
        wd = np.array([sum(data.get("weight", 1) for _, _, data in G.edges(nbunch=[node], data=True))
                       for node in G.nodes()])
        max_wd = wd.max() if len(wd) > 0 else 1
        norm_wd = wd / max_wd
        degrees = np.array([G.degree(node) for node in G.nodes()])
        conn_ratio = degrees / (n - 1) if n > 1 else np.zeros_like(degrees)
        return norm_wd, conn_ratio

    def compute_percentile_ranks(norm_wd):
        sorted_vals = np.sort(norm_wd)
        return np.array([100 * (np.searchsorted(sorted_vals, val, side='right') / len(sorted_vals))
                         for val in norm_wd])

    def compute_total_error(G):
        norm_wd, conn_ratio = compute_node_metrics(G)
        percentiles = compute_percentile_ranks(norm_wd)
        target_wd = np.interp(percentiles, percentiles_emp, target_rel_wd_emp)
        target_conn = np.interp(percentiles, percentiles_emp, target_conn_emp)
        err_wd = ((norm_wd - target_wd) / sigma_wd) ** 2
        err_conn = ((conn_ratio - target_conn) / sigma_conn) ** 2
        return np.sum(err_wd + err_conn), percentiles

    current_error, _ = compute_total_error(G)
    iteration = 0
    improved = True

    while improved and iteration < max_iterations and G.number_of_edges() > 0:
        improved = False
        best_edge = None
        best_error = current_error
        candidates = []
        for u, v, data in G.edges(data=True):
            if G.degree(u) > 1 and G.degree(v) > 1:
                candidates.append((u, v, data))
        if not candidates:
            break
        for (u, v, data) in candidates:
            G_temp = G.copy()
            G_temp.remove_edge(u, v)
            new_error, _ = compute_total_error(G_temp)
            if new_error < best_error:
                best_error = new_error
                best_edge = (u, v)
        if best_edge is not None and best_error < current_error:
            G.remove_edge(*best_edge)
            current_error = best_error
            improved = True
        iteration += 1
    return G



n_graph_dict = dict()
n_graph_dict['test'] = dict()

from issue.barycenter_calculation import *
dep = "graphs"
spec = "project_clus_ov"

graph_dict = LoadGraphDict(dep, spec)
event_res = {}
event_times = sorted(graph_dict.keys())

rt = -2
event_graphs = graph_dict[rt]
sizebary = 25
bary_list, labs = PrepareCalcData(event_graphs, spec)

for graph_ind in [0, 1, 2]:
    graph_list = bary_list[graph_ind] #bary_list[1]
    mats = [nx.to_numpy_array(G) for G in graph_list]
    Cdict, _ = ot.gromov.gromov_wasserstein_dictionary_learning(mats, 1, sizebary, reg=2, random_state = 0)
    G = nx.from_numpy_array(Cdict[0].astype(int))
    G.remove_edges_from(nx.selfloop_edges(G))

    G = modify_graph_to_match_empirical(G, graph_list)

    if graph_ind == 0:
        nx.write_gexf(G, "issue/test.gexf")
        plot_gephi_style(G, "issue/test.png")
    elif graph_ind == 1:
        nx.write_gexf(G, "issue/test_unclustered.gexf")
        plot_gephi_style(G, "issue/test_unclustered.png")
    elif graph_ind == 2:
        nx.write_gexf(G, "issue/test_clustered.gexf")
        plot_gephi_style(G, "issue/test_clustered.png")
    n_graph_dict['test'][graph_ind] = G

barys = [ComputeBarycenter(graph_list, sizebary) for graph_list in bary_list]
PlotCombinedGraph(barys, labs, event_time=rt, dep=dep, spec=spec, sizebary=sizebary)

graph_dict = n_graph_dict

for graph_ind in [0, 1, 2]:
    G = degree_graph_dict['test'][graph_ind]['important_edges_removed']
    if graph_ind == 0:
        nx.write_gexf(G, "issue/test.gexf")
        plot_gephi_style(G, "issue/test.png")
    elif graph_ind == 1:
        nx.write_gexf(G, "issue/test_unclustered.gexf")
        plot_gephi_style(G, "issue/test_unclustered.png")
    elif graph_ind == 2:
        nx.write_gexf(G, "issue/test_clustered.gexf")
        plot_gephi_style(G, "issue/test_clustered.png")
