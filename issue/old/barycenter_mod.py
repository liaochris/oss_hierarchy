import networkx as nx
import numpy as np

def modify_graph_minimize_squared_error(G, graph_list, max_iterations=1000):
    """
    Adjusts graph G by removing edges so that its connection ratio and average weighted degree
    approach the empirical targets computed from graph_list. The squared error for each metric
    is normalized by its standard deviation so that both are valued equally.
    An edge is removed only if its removal does not disconnect the graph.
    
    Parameters:
      G (networkx.Graph): The graph to modify.
      graph_list (list): A list of graphs used to compute empirical target metrics.
      max_iterations (int): Maximum number of removal iterations.
      
    Returns:
      networkx.Graph: The modified graph.
    """
    # Compute empirical targets and standard deviations over graph_list.
    conn_ratios = []
    weighted_degrees = []
    for H in graph_list:
        n = H.number_of_nodes()
        if n == 0:
            continue
        conn_ratios.append(H.number_of_edges() / n)
        total_weight = sum(data.get("weight", 1) for _, _, data in H.edges(data=True))
        weighted_degrees.append(total_weight / n)
    target_conn_ratio = np.mean(conn_ratios)
    target_weighted_degree = np.mean(weighted_degrees)
    
    conn_std = np.std(conn_ratios) if np.std(conn_ratios) > 0 else 1
    weighted_std = np.std(weighted_degrees) if np.std(weighted_degrees) > 0 else 1

    def current_metrics(G):
        n = G.number_of_nodes() or 1  # avoid division by zero
        conn = G.number_of_edges() / n
        total_w = sum(data.get("weight", 1) for _, _, data in G.edges(data=True))
        weighted = total_w / n
        return conn, weighted

    def error(G):
        conn, weighted = current_metrics(G)
        # Normalize each difference by its standard deviation so both metrics contribute equally.
        err_conn = (conn - target_conn_ratio) / conn_std
        err_weighted = (weighted - target_weighted_degree) / weighted_std
        return err_conn**2 + err_weighted**2

    current_error = error(G)
    
    for iteration in range(max_iterations):
        # Gather candidate edges that can be removed without disconnecting the graph.
        removable_edges = []
        for u, v, data in G.edges(data=True):
            G_temp = G.copy()
            G_temp.remove_edge(u, v)
            if nx.is_connected(G_temp):
                removable_edges.append((u, v, data))
                
        best_edge = None
        best_error = current_error
        
        # Evaluate each candidate removal.
        for u, v, data in removable_edges:
            G_temp = G.copy()
            G_temp.remove_edge(u, v)
            new_error = error(G_temp)
            if new_error < best_error:
                best_error = new_error
                best_edge = (u, v)
                
        if best_edge is None:
            # No removal improves the error.
            break
        else:
            G.remove_edge(*best_edge)
            current_error = best_error
    
    return G

# Example usage:
# modified_G = modify_graph_minimize_squared_error(G, graph_list)
