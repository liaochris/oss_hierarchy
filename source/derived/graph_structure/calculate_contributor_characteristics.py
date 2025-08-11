import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
from datetime import datetime
import json
from source.lib.JMSLab.SaveData import SaveData

def Main():
    indir = Path('drive/output/derived/graph_structure')
    outdir = indir
    logdir =  Path('output/derived/graph_structure')
    with open(indir / f"graph_metrics.json", "r") as f:
        graph_metrics = json.load(f)

    rows = []

    for repo, time_dict in graph_metrics.items():
        for t_str, project_time in time_dict.items():
            year = int(t_str[:4])
            month = int(t_str[4:6])
            time_period = datetime(year, month, 1)
            
            # Extract project-level metrics
            overall = project_time.get('repo_overall', {})
            total_important = overall.get('total_important', None)
            total_nodes = overall.get('total_nodes', None)
            cluster_averages = overall.get('cluster_averages', {})
            mean_overlap = cluster_averages.get('mean_overlap', None)
            avg_clusters_per_node = cluster_averages.get('avg_clusters_per_node', None)
            pct_nodes_one_cluster = cluster_averages.get('pct_nodes_one_cluster', None)
            imp_to_imp_comm_overall = overall.get('imp_to_imp_comm_overall', None)
            if imp_to_imp_comm_overall is not None:
                imp_to_imp_comm_overall = imp_to_imp_comm_overall.get('avg_edge_weight', None)   
            imp_to_other_comm_overall = overall.get('imp_to_other_comm_overall', None)
            if imp_to_other_comm_overall is not None:
                imp_to_other_comm_overall = imp_to_other_comm_overall.get('avg_edge_weight', None)   
                
            # important contributors
            contributor_keys = [k for k in project_time.keys() if k != 'repo_overall' and 'overall_overlap' not in project_time[k]]
            important_contributor_keys = [k for k in project_time.keys() if k != 'repo_overall' and 'overall_overlap' in project_time[k]]

            # Compute HHI values for three metrics across actors in this project_time.
            total_norm = sum(project_time[actor].get('normalized_degree', 0) for actor in important_contributor_keys)
            total_ind_cov = sum(project_time[actor].get('individual_coverage', 0) for actor in important_contributor_keys)
            total_ind_cov_cluster = sum(project_time[actor].get('individual_coverage_cluster', 0) for actor in important_contributor_keys)
            
            hhi_norm = sum((project_time[actor].get('normalized_degree', 0)/total_norm)**2 for actor in important_contributor_keys) if total_norm > 0 else None
            hhi_ind_cov = sum((project_time[actor].get('individual_coverage', 0)/total_ind_cov)**2 for actor in important_contributor_keys) if total_ind_cov > 0 else None
            hhi_ind_cov_cluster = sum((project_time[actor].get('individual_coverage_cluster', 0)/total_ind_cov_cluster)**2 for actor in important_contributor_keys) if total_ind_cov_cluster > 0 else None

            contributor_keys = [k for k in project_time.keys() if k != 'repo_overall']
            for actor in important_contributor_keys + contributor_keys:
                actor_data = project_time[actor]
                row = {
                    'repo_name': repo,
                    'time_period': time_period,
                    'actor_id': actor,
                    'total_important': total_important,
                    'total_nodes': total_nodes,
                    'mean_cluster_overlap': mean_overlap,
                    'avg_clusters_per_node': avg_clusters_per_node,
                    'pct_nodes_one_cluster': pct_nodes_one_cluster,
                    'HHI_normalized_degree': hhi_norm,
                    'HHI_individual_coverage': hhi_ind_cov,
                    'HHI_individual_coverage_cluster': hhi_ind_cov_cluster,
                    'imp_to_imp_overall': imp_to_imp_comm_overall,
                    'imp_to_other_overall': imp_to_other_comm_overall,
                    'normalized_degree': actor_data.get('normalized_degree', None),
                    'imp_to_other_avg_edge_weight': actor_data.get('avg_edge_weight', None),
                    'communication_log': str(actor_data.get('communication_log', None)),
                }
                if actor in important_contributor_keys:
                    row['individual_node_coverage'] = actor_data.get('individual_coverage', None)
                    row['individual_coverage_cluster'] = actor_data.get('individual_coverage_cluster', None)
                    row['overall_overlap'] = actor_data.get('overall_overlap', None)
                    row['weighted_overall_overlap'] = actor_data.get('weighted_overall_overlap', None)
                    row['important'] = 1
                    
                # Add actor-level percentiles
                perc = actor_data.get('percentiles', {})
                row['imp_to_other_perc_10'] = perc.get('10', None)
                row['imp_to_other_perc_25'] = perc.get('25', None)
                row['imp_to_other_perc_50'] = perc.get('50', None)
                row['imp_to_other_perc_75'] = perc.get('75', None)
                row['imp_to_other_perc_90'] = perc.get('90', None)
                
                # Add important-to-important communication metrics
                imp_comm = actor_data.get('imp_to_imp_comm', {})
                row['imp_to_imp_avg_edge_weight'] = imp_comm.get('avg_edge_weight', None)
                imp_comm_perc = imp_comm.get('percentiles', {})
                row['imp_to_imp_perc_10'] = imp_comm_perc.get('10', None)
                row['imp_to_imp_perc_25'] = imp_comm_perc.get('25', None)
                row['imp_to_imp_perc_50'] = imp_comm_perc.get('50', None)
                row['imp_to_imp_perc_75'] = imp_comm_perc.get('75', None)
                row['imp_to_imp_perc_90'] = imp_comm_perc.get('90', None)
                
                rows.append(row)

    df = pd.DataFrame(rows)
    df['time_period'] = pd.to_datetime(df['time_period'])
    df['prop_important'] = df['total_important']/df['total_nodes']
    df = df.drop_duplicates()

    SaveData(df, ['repo_name','time_period','actor_id'],
             outdir / 'contributor_characteristics.parquet',
             logdir / 'contributor_characteristics.log')
    

if __name__ == '__main__':
    Main()
