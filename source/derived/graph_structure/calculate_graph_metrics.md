This document contains math definition for features calculated in `calculate_graph_metrics.py`

## Mathematical Formulations

Let $ G = (V, E) $ be a graph with $|V|$ nodes, and let $ I \subseteq V $ be the set of important contributors.


### Importance Metrics
For each $ i \in I $:
1. **Unnormalized Degree:**
   $$
   d_i = \text{degree}(i) \quad (\text{computed on } G')
   $$
2. **Normalized Degree Centrality:**
   $$
   C_D(i) = \frac{d_i}{|V| - 1}
   $$
3. **Betweenness Centrality:**
   $$
   C_B(i) = \text{betweenness}(i)
   $$

Total number of important contributors:
$$
|I|
$$

### Defining Important Contributors

**1. Degree and Z-Score Calculation**

Let $G = (V, E)$ be an undirected graph, and for each node $v \in V$, let $d(v)$ denote its degree. Define the mean and standard deviation of the degrees as follows:

$$
\mu = \frac{1}{|V|} \sum_{v \in V} d(v), \quad \sigma = \sqrt{\frac{1}{|V|} \sum_{v \in V} \left(d(v) - \mu\right)^2}.
$$

Then, the z-score for node $v$ is given by:

$$
z(v) = \frac{d(v) - \mu}{\sigma}.
$$

**2. Important Nodes**

A node is considered an important contributor if its degree z-score exceeds $3.5$. Hence, the set of important nodes is defined as:

$$
I = \{ v \in V \mid z(v) > 3.5 \}.
$$

3. **Modified Graph Construction**

Construct a modified graph $ G' = (V, E') $ by removing all edges connecting two important nodes:
$$
E' = E \setminus \{ (u, v) \in E \mid u \in I \text{ and } v \in I \}.
$$
For each node $ v \in V $, define the attribute:
$$
\text{important}(v) =
\begin{cases}
\text{True} & \text{if } v \in I, \\
\text{False} & \text{otherwise}.
\end{cases}
$$


### Clustering Metrics

Let $ G' $ be the modified graph, and let $ C(i) $ be the connected component of $ G' $ that contains $ i $.

1. **Individual Coverage:**  
    For each important contributor $i$, let $E(i)$ denote its ego graph (i.e. the set of nodes consisting of $i$ and its immediate neighbors). Then the individual coverage is defined as:
    $$
    \text{Individual Coverage}(i) = \frac{|E(i)|}{|V|} \times 100\%,
    $$
    where $|V|$ is the total number of nodes in the graph.

2. **Individual Coverage Cluster:**  
    Let $U$ be the union of the ego graphs of all important contributors:
    $$U = \bigcup_{j \in I} E(j).$$
    Then, for each important contributor $i$, the individual coverage cluster is defined as:
    $$\text{Individual Coverage Cluster}(i) = \frac{|E(i)|}{|U|} \times 100\%$$

2. **Overall Unweighted Overlap:**  
   Let $ N(i) $ be the set of neighbors of $ i $ in $ G' $ and define
   $$
   U_i = \bigcup_{\substack{j \in I \\ j \neq i}} N(j)
   $$
   Then:
   $$
   O(i) = \frac{|N(i) \cap U_i|}{|N(i)|} \times 100\%
   $$
3. **Weighted Overall Overlap:**  
   Let 
   $$
   w_i = \sum_{v \in N(i)} w(i,v)
   $$
   Then:
   $$
   W(i) = \frac{\sum_{v \in N(i) \cap U_i} w(i,v)}{w_i} \times 100\%
   $$
4. **Aggregate Cluster Coverage:**  
   Let $ \mathcal{C} $ be the set of connected components in $ G' $ that contain at least one important contributor.
   $$
   \text{Coverage}_{\text{agg}} = \frac{\left|\bigcup_{C \in \mathcal{C}} C\right|}{|V|} \times 100\%
   $$
5. **Cluster-Level Weighted Averages:**  
   For a cluster $ C $ with important contributors $ I_C $,
   $$
   \overline{O}_C = \frac{\sum_{i \in I_C} O(i) \, d_i}{\sum_{i \in I_C} d_i}, \quad
   \overline{W}_C = \frac{\sum_{i \in I_C} W(i) \, d_i}{\sum_{i \in I_C} d_i}
   $$
6. **Ego-Cluster Overlap Metrics:**  
   For each $ i \in I $, define its ego cluster as:
   $$
   E_i = \{ i \} \cup N(i)
   $$
   Let 
   $$
   U = \bigcup_{i \in I} E_i
   $$
   and for each node $ v \in U $, let $ c(v) $ be the number of ego clusters that contain $ v $. Then:
   - **Average Number of Ego Clusters per Node:**
     $$
     \text{AvgClusters} = \frac{1}{|U|} \sum_{v \in U} c(v)
     $$
   - **Percentage of Nodes in Exactly One Ego Cluster:**
     $$
     \text{PctOneCluster} = \frac{|\{ v \in U : c(v) = 1 \}|}{|U|} \times 100\%
     $$

### Communication Metrics

For each $ i \in I $, define:
- $ E_i $: the set of edges incident on $ i $ in $ G' $ (with the other endpoint in the same connected component).
- $ E^I_i $: the set of edges in the original graph $ G $ between $ i $ and other important contributors.

Then:
1. **Mean Edge Weight:**
   $$
   \bar{w}_i = \frac{1}{|E_i|} \sum_{e \in E_i} w(e), \quad \bar{w}^I_i = \frac{1}{|E^I_i|} \sum_{e \in E^I_i} w(e)
   $$
2. **Standard Error (SE):**
   $$
   \text{SE}_i = \frac{\sigma_i}{\sqrt{|E_i|}}, \quad \sigma_i = \sqrt{\frac{1}{|E_i| - 1} \sum_{e \in E_i} \left(w(e) - \bar{w}_i\right)^2}
   $$
3. **Percentiles:**  
   The 10th, 25th, 50th, 75th, and 90th percentiles are computed from 
   $$
   \{ w(e) : e \in E_i \} \quad \text{(or from } \{ w(e) : e \in E^I_i \}\text{)}
   $$
4. Additionally, for each important contributor, we record the weight of the edges connecting them to each other important contributor (from the original graph $ G $).

### Graph Size

For the original graph $ G $:
- **Total Nodes:** $|V|$
- **Total Edges:** $|E|$