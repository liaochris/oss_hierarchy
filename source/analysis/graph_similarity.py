from joblib import Parallel, delayed
import networkx as nx
import numpy as np
#from grakel import Graph
#from grakel.kernels import WeisfeilerLehman, VertexHistogram
from scipy.stats import entropy, wasserstein_distance, ks_2samp
import math
import argparse
from source.derived.graph_structure.calculate_degree import CleanNodes
import os
import itertools
import pandas as pd
from pathlib import Path

def ConvertNxToGrakel(graph, use_node_labels=False):
    if use_node_labels:
        node_labels = {n: str(graph.degree[n]) for n in graph.nodes()}
    else:
        node_labels = {n: None for n in graph.nodes()}
    edges = [(u, v) for u, v in graph.edges()]
    return Graph(node_labels, edges)

def WeisfeilerLehmanSimilarity(graph1, graph2, n_iter=3):
    g1 = ConvertNxToGrakel(graph1, use_node_labels=True)
    g2 = ConvertNxToGrakel(graph2, use_node_labels=True)
    wl_kernel = WeisfeilerLehman(n_iter=n_iter, base_kernel=VertexHistogram)
    K = wl_kernel.fit_transform([g1, g2])
    # Normalize kernel to [0,1]
    return K[0, 1] / np.sqrt(K[0, 0] * K[1, 1])

def HistogramSimilarity(graph1, graph2, bins=20, range=None):
    w1 = [d.get("weight", 1.0) for _, _, d in graph1.edges(data=True)]
    w2 = [d.get("weight", 1.0) for _, _, d in graph2.edges(data=True)]

    hist1, bin_edges = np.histogram(w1, bins=bins, range=range, density=True)
    hist2, _ = np.histogram(w2, bins=bin_edges, density=True)

    hist1 = hist1 / (hist1.sum() + 1e-12)
    hist2 = hist2 / (hist2.sum() + 1e-12)

    # JS Divergence → similarity
    js_div = 0.5 * (entropy(hist1, (hist1 + hist2) / 2) + entropy(hist2, (hist1 + hist2) / 2))
    js_sim = 1 - js_div / math.log(2)

    # Chi-Square → similarity
    chi_sq = 0.5 * np.sum(((hist1 - hist2) ** 2) / (hist1 + hist2 + 1e-12))
    chi_sim = 1 / (1 + chi_sq)

    # Histogram Intersection (already in [0,1])
    intersection = np.sum(np.minimum(hist1, hist2))

    # Wasserstein → similarity (scaled by range)
    if range is None:
        wmin = min(min(w1), min(w2))
        wmax = max(max(w1), max(w2))
    else:
        wmin, wmax = range
    w_range = max(wmax - wmin, 1e-12)
    wass = wasserstein_distance(w1, w2)
    wass_sim = max(0.0, 1 - wass / w_range)

    # Kolmogorov–Smirnov
    ks_stat, _ = ks_2samp(w1, w2)
    ks_sim = 1 - ks_stat

    return {
        "JensenShannon": js_sim,
        "ChiSquare": chi_sim,
        "HistogramIntersection": intersection,
        "Wasserstein": wass_sim,
        "KolmogorovSmirnov": ks_sim
    }


def ComputePairwiseSimilarity(repo1_file, repo2_file, folder_path, folder):
    repo1, repo2 = Path(repo1_file).stem, Path(repo2_file).stem
    G1 = CleanNodes(nx.read_gexf(os.path.join(folder_path, repo1_file)))
    G2 = CleanNodes(nx.read_gexf(os.path.join(folder_path, repo2_file)))
    sim_scores = HistogramSimilarity(G1, G2, bins=10)
    row = {"repo1": repo1, "repo2": repo2, "folder": folder}
    row.update(sim_scores)
    return row

def ComputeGraphSimilarities(graphs_root, output_file, n_jobs=4, chunk_size=10000,
                             nparts=1, part=1):  # <-- part starts at 1 now
    if part < 1 or part > nparts:
        raise ValueError(f"part must be between 1 and {nparts}, got {part}")

    output_path = Path(output_file)
    writer = None

    # All available folders
    all_folders = sorted([f for f in os.listdir(graphs_root) if os.path.isdir(os.path.join(graphs_root, f))])
    
    # Convert 1-based part to 0-based index for slicing
    folders = all_folders[(part-1)::nparts]
    print(f"Machine {part}/{nparts} handling {len(folders)} folders: {folders}")

    for folder in folders:
        print(f"Processing folder {folder}")
        folder_path = os.path.join(graphs_root, folder)

        graph_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".gexf")])
        pairs = list(itertools.combinations(graph_files, 2))

        for start in range(0, len(pairs), chunk_size):
            batch = pairs[start:start+chunk_size]

            results = Parallel(n_jobs=n_jobs, backend="loky", verbose=0)(
                delayed(ComputePairwiseSimilarity)(repo1, repo2, folder_path, folder)
                for repo1, repo2 in batch
            )

            df = pd.DataFrame(results)
            table = pa.Table.from_pandas(df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)
            writer.write_table(table)

    if writer:
        writer.close()

    return pd.read_parquet(output_path)

def Main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nparts", type=int, required=True, help="total number of machines")
    parser.add_argument("--part", type=int, required=True, help="this machine's part index (1-based)")
    args = parser.parse_args()

    graphs_root = "drive/output/derived/graph_structure/graphs"
    output_file = f"drive/output/analysis/graph_similarity/graph_similarity_part{args.part}.parquet"

    ComputeGraphSimilarities(
        graphs_root=graphs_root,
        output_file=output_file,
        nparts=args.nparts,
        part=args.part,
    )

    print(f"Machine {args.part}/{args.nparts} finished. Output written to {output_file}")


if __name__ == "__main__":
    Main()