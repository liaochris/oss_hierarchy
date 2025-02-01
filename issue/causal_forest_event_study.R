# RENAME TO CAUSAL FOREST

library(eventstudyr)
library(tidyverse)
library(ggplot2)
library(egg)
library(gtable)
library(foreach)
library(data.table)
library(grid)
library(arrow)
library(scales)
library(grf)
library(doParallel)
library(future.apply)
library(Matrix)

plan(multisession, workers = parallel::detectCores() - 1)
`%ni%` = Negate(`%in%`)

# import outcomes?
indir <- "drive/output/derived/project_outcomes"
indir_departed <- "drive/output/derived/contributor_stats/filtered_departed_contributors"
issue_tempdir <- "issue"
outdir <- "issue/event_study"

time_period <- 6
rolling_window <- 732
criteria_pct <- 75
consecutive_periods <- 3
post_periods <- 2

df_project_outcomes <- read_parquet(file.path(indir, paste0("project_outcomes_major_months",time_period,".parquet"))) %>% 
  mutate(time_period = as.Date(time_period)) 
df_departed_contributors <- read_parquet(
  file.path(indir_departed, paste0("filtered_departed_contributors_major_months",time_period,"_window",
                                   rolling_window,"D_criteria_commits_",criteria_pct,"pct_consecutive",consecutive_periods,
                                   "_post_period",post_periods,"_threshold_gap_qty_0.parquet")))
df_project_covariates <- read_parquet(file.path(issue_tempdir, "project_covariates.parquet")) %>%
  mutate(time_period = as.Date(time_period))
df_contributor_covariates <- read_parquet(file.path(issue_tempdir, "contributor_covariates.parquet")) %>%
  mutate(departed_actor_id = actor_id,
         time_period = as.Date(time_period)) %>%
  select(-actor_id)



df_departed <- df_departed_contributors %>% 
  filter(year(treatment_period)<2023 & !abandoned_scraped & !abandoned_consecutive_req3_permanentTrue) %>%
  group_by(repo_name) %>% 
  mutate(repo_count = n()) %>%
  ungroup() %>%
  mutate(departed_actor_id = actor_id,
         abandoned_date = as.Date(abandoned_date_consecutive_req2_permanentTrue)) %>%
  filter(repo_count == 1) %>%
  select(repo_name,departed_actor_id,last_pre_period,treatment_period,abandoned_date)

treatment_inelg <- df_departed_contributors %>% 
  filter(repo_name %ni% df_departed$repo_name)

treated_more_than_once <- df_departed_contributors %>% 
  filter(year(treatment_period)<2023 & !abandoned_scraped & !abandoned_consecutive_req3_permanentTrue) %>%
  group_by(repo_name) %>% 
  mutate(repo_count = n()) %>%
  filter(repo_count > 1) %>% pull(repo_name)

df_project <- df_project_outcomes %>%
  filter(year(time_period)<2023 | (year(time_period) == 2023 & month(time_period) == 1)) %>%
  filter(repo_name %ni% treated_more_than_once) %>%
  mutate(time_index = dense_rank(time_period))

df_project_departed <- df_project %>%
  left_join(df_departed) %>%
  mutate(treated_project = 1-as.numeric(is.na(last_pre_period)),
         treatment = as.numeric(treated_project & time_period>=treatment_period)) %>%
  left_join(df_project_covariates) %>%
  mutate(final_period = as.Date(final_period),
         last_pre_period = as.Date(last_pre_period)) %>%
  left_join(df_contributor_covariates)

df_project_departed <- df_project_departed %>% 
  mutate(project_age = (181 + as.numeric(time_period - as.Date(created_at)))) %>%
  group_by(repo_name) %>%
  mutate(smallest_age = min(project_age)) %>%
  ungroup() %>%
  mutate(project_age = ifelse(smallest_age < 0, NA, project_age)) %>%
  mutate(project_id = dense_rank(repo_name))  %>%
  mutate(row = row_number()) 

df_project_departed <- df_project_departed %>%
  group_by(repo_name) %>%
  mutate(treatment_time_index = case_when(
    any(treatment == 1) ~ min(time_index[treatment == 1], na.rm = TRUE),
    .default = NA
    )
  ) %>%
  ungroup()


all_projects <- df_project_departed %>% pull(repo_name) %>% unique()
treated_projects <- df_project_departed %>% filter(treatment == 1) %>% pull(repo_name) %>% unique()
treated_projects_count <- length(treated_projects)
control_projects <- df_project_departed %>%
  filter(repo_name %ni% treated_projects) %>% pull(repo_name) %>% unique()
control_projects_count <- length(control_projects)

print(paste("Project Count:", length(all_projects)))
print(paste("Control Projects:", control_projects_count))
print(paste("Treated Projects:", treated_projects_count))

org_covars <- c(
  "stars_accumulated",
  "forks_gained",
  "truckfactor",
  "contributors",
  "org_status",
  "project_age"
)
org_covars_long <- c(
  org_covars, 
  "corporate_pct",
  "educational_pct",
  "apache_software_license",
  "bsd_license",
  "gnu_general_public_license",
  "mit_license"
)

contributor_covars <- c(
  "truckfactor_member",
  "max_rank",
  "total_share",
  "comments_share_avg_wt",
  "comments_hhi_avg_wt",
  "pct_cooperation_comments"
)
contributor_covars_long <- c(
  contributor_covars,
  "contributor_email_educational",
  "contributor_email_corporate",
  "problem_identification_share",
  "problem_discussion_share",
  "coding_share",
  "problem_approval_share",
  "comments_share_avg_wt",
  "comments_hhi_avg_wt",
  "pct_cooperation_comments"
) 

org_structure <- c(
  "min_layer_count",
  "problem_discussion_higher_layer_work",
  "coding_higher_layer_work",
  "total_HHI",
  "contributors_comments_wt",
  "comments_cooperation_pct"
)

org_structure_long <- c(
  org_structure,
  "layer_count",
  "problem_identification_layer_contributor_pct",
  "problem_discussion_layer_contributor_pct",
  "coding_layer_contributor_pct",
  "problem_approval_layer_contributor_pct",
  "problem_discussion_higher_layer_contributor_overlap",
  "coding_higher_layer_contributor_overlap",
  "problem_identification_HHI",
  "problem_discussion_HHI",
  "coding_HHI",
  "hhi_comments",
  "problem_approval_HHI"
)


outcome <- "commits"
norm_outcome <- df_project_departed %>% 
  filter(time_period <= last_pre_period) %>% 
  summarise(mean(get(outcome), na.rm = T)) %>% 
  pull()
df_project_departed[[outcome]] <- df_project_departed[[outcome]]/norm_outcome


g_list <- sort(df_project_departed %>% filter(treatment == 1) %>% pull(time_index) %>% unique())
t_list <- sort(unique(df_project_departed$time_index))

X_space <- df_project_departed %>%
  select(all_of(c(org_covars, org_structure, contributor_covars, "time_index","project_id","row"))) %>%
  unique()
X_space_mat <- X_space %>%
  select(-c(time_index,project_id, row)) %>%
  as.matrix()


InputData <- function(data, g, t) {
  set.seed(123)
  
  data_gt <- data %>% group_by(repo_name) %>% 
    filter(treatment_time_index == g | is.na(treatment_time_index)) %>%
    mutate(project_appears = sum((time_index == g-1) | (time_index== t))) %>% 
    filter(project_appears == 2 & ((time_index == g-1) | (time_index == t))) %>%
    select(-project_appears) %>% 
    arrange(project_id) %>%
    ungroup()

  project_ids <- sort(unique(data_gt$project_id))
  sample1_ids <- sample(project_ids, size = floor(length(project_ids)/2))
  sample2_ids <- project_ids[project_ids %ni% sample1_ids]
  
  # for pre period filtering I'd want to change it to be only untreated and not yet treated
  data_gt_x <- data_gt %>%
    filter(time_index == g-1)  %>% 
    arrange(project_id) %>%
    select(all_of(c(org_covars, org_structure, contributor_covars,"time_index", "project_id", "row")))
  
  data_d <- data_gt %>%
    filter(time_index == t) %>% 
    arrange(project_id)
  if (t>=g) {
    data_d <- data_d  %>%
      select(treatment, project_id) 
  } else {
    data_d <- data_d  %>%
      select(treated_project, project_id)     
  }
 
  pre_treatment_y <- data_gt %>% filter(time_index == g-1) %>%
    select(all_of(c("repo_name", outcome))) %>%
    mutate(outcome_pre_treatment = get(outcome)) %>%
    select(-outcome)
  
  data_y <- data_gt %>% 
    filter(time_index == t) %>%
    left_join(pre_treatment_y) %>%
    mutate(outcome_diff = get(outcome) - outcome_pre_treatment) %>%
    arrange(project_id) %>%
    select(outcome_diff, project_id) 
  
  return(list(data_gt = data_gt, data_gt_x = data_gt_x, data_gt_d = data_d, data_gt_y = data_y,
              sample1_ids = sample1_ids, sample2_ids = sample2_ids))
}


# for (t in t_list) {
#   for (g in g_list) {
#   }
# }

t <- 3
g <- 6
num_trees <- 20000
tree_indices <- 1:num_trees

data_obj <- InputData(data = df_project_departed, g = g, t = t)
data_gt <- data_obj$data_gt
sample1_ids <- sort(data_obj$sample1_ids)
sample2_ids <- sort(data_obj$sample2_ids)

data_gt_x_sample1 <- data_obj$data_gt_x %>% 
  filter(project_id %in% sample1_ids)
X_sample1 <- data_gt_x_sample1 %>%
  select(-c(time_index,project_id, row)) %>%
  as.matrix()
Y_sample1 <- data_obj$data_gt_y %>% 
  filter(project_id %in% sample1_ids) %>%
  select(-project_id) %>% 
  as.matrix()
D_sample1 <- data_obj$data_gt_d %>% 
  filter(project_id %in% sample1_ids) %>%
  select(-project_id) %>% 
  as.matrix()


forest1 <- causal_forest(X = X_sample1, Y = Y_sample1, W = D_sample1, num.trees = num_trees, tune.parameters = "all")


data_gt_x_sample2 <- data_obj$data_gt_x %>% 
  filter(project_id %in% sample2_ids)
X_sample2 <- data_gt_x_sample2 %>%
  select(-c(time_index,project_id, row)) %>%
  as.matrix()
Y_sample2 <- data_obj$data_gt_y %>% 
  filter(project_id %in% sample2_ids) %>%
  select(-project_id) %>% 
  as.matrix()
D_sample2 <- data_obj$data_gt_d %>% 
  filter(project_id %in% sample2_ids) %>%
  select(-project_id) %>% 
  as.matrix()

forest2 <- causal_forest(X = X_sample2, Y = Y_sample2, W = D_sample2, num.trees = num_trees, tune.parameters = "all")

extract_leaf_nodes <- function(forest, X_matrix, tree_indices) {
  leaf_list <- future_lapply(tree_indices, function(i) {
    tree <- get_tree(forest, i)
    if (!tree$nodes[[1]]$is_leaf) {
      return(get_leaf_node(tree, X_matrix))
    } else {
      return(NULL) 
    }
  })
  leaf_list <- leaf_list[!sapply(leaf_list,is.null)]
  return(leaf_list)
}

leaf_nodes_sample1 <- extract_leaf_nodes(forest = forest2, 
                                         X_matrix = X_sample1, 
                                         tree_indices = tree_indices)
leaf_nodes_sample2 <- extract_leaf_nodes(forest = forest1, 
                                         X_matrix = X_sample2, 
                                         tree_indices = tree_indices)
forest1_X <- extract_leaf_nodes(forest = forest1, 
                                X_matrix = X_space_mat, 
                                tree_indices = tree_indices)
forest2_X <- extract_leaf_nodes(forest = forest2, 
                                X_matrix = X_space_mat, 
                                tree_indices = tree_indices)



gt_obs <- nrow(data_obj$data_gt_x)
x_obs <- nrow(X_space_mat)
matrix_list <- vector("list", length = num_trees)

ConvertProjectIDToRow <- function(data_gt, t, sample_ids) {
  project_id_vector <- data_gt %>% filter(time_index == t) %>% pull(project_id)
  return(match(sample_ids, project_id_vector))
}

row_sample1_ids <- ConvertProjectIDToRow(data_gt, t, sample1_ids)
row_sample2_ids <- ConvertProjectIDToRow(data_gt, t, sample2_ids)
matrix_list_vectorized <- vector("list", length = num_trees)
forest1_numtrees <- length(forest1_X)
forest2_numtrees <- length(forest2_X)

  
# https://chatgpt.com/share/e/679c6fc0-a358-800e-a903-edfef6721bd8 
# AMEND THE PROCESS SO X derived from SAMPLE 2 is 0 in the below example

print(paste("g:",g,"t:",t,"forest 2 tree count:",forest2_numtrees,
            "forest 1 tree count:",forest1_numtrees, "# trees:",num_trees))

GenerateTreeWeights <- function(gt_obs, x_obs, i, row_sample1_ids, row_sample2_ids,
                                leaf_nodes_sample1, leaf_nodes_sample2, forest2_X, forest1_X,
                                forest1_numtrees, forest2_numtrees) {
  M_i <- Matrix(0, nrow = gt_obs, ncol = x_obs, sparse = TRUE)
  
  if (length(row_sample1_ids) > 0 & i <= forest2_numtrees) {
    leaf_nodes1 <- leaf_nodes_sample1[[i]]
    forest2_X_i <- forest2_X[[i]]
    
    # Convert to character vectors for consistent naming in the map
    leaf_nodes1_char <- as.character(leaf_nodes1)
    forest2_X_i_char <- as.character(forest2_X_i)
    
    forest2_map <- split(seq_along(forest2_X_i_char), forest2_X_i_char)
    matched_cols_list <- forest2_map[leaf_nodes1_char]
    valid_matches <- !sapply(matched_cols_list, is.null)
    
    row_sample1_ids_valid <- row_sample1_ids[valid_matches]
    matched_cols_valid <- matched_cols_list[valid_matches]
    
    rows_to_set <- rep(as.integer(row_sample1_ids_valid), times = lengths(matched_cols_valid))
    cols_to_set <- as.integer(unlist(matched_cols_valid, use.names = FALSE))
    
    M_i[cbind(rows_to_set, cols_to_set)] <- 1/forest2_numtrees
  }
  if (length(row_sample2_ids) > 0 & i <= forest1_numtrees) {    
    leaf_nodes2 <- leaf_nodes_sample2[[i]]
    forest1_X_i <- forest1_X[[i]]
    
    # Convert to character vectors for consistent naming in the map
    leaf_nodes2_char <- as.character(leaf_nodes2)
    forest1_X_i_char <- as.character(forest1_X_i)
    
    forest1_map <- split(seq_along(forest1_X_i_char), forest1_X_i_char)
    matched_cols_list <- forest1_map[leaf_nodes2_char]
    valid_matches <- !sapply(matched_cols_list, is.null)
    
    row_sample2_ids_valid <- row_sample2_ids[valid_matches]
    matched_cols_valid <- matched_cols_list[valid_matches]
    
    rows_to_set <- rep(as.integer(row_sample2_ids_valid), times = lengths(matched_cols_valid))
    cols_to_set <- as.integer(unlist(matched_cols_valid, use.names = FALSE))
    
    M_i[cbind(rows_to_set, cols_to_set)] <- 1/forest1_numtrees
  }
  return(M_i)
}

num_cores <- parallel::detectCores() - 1
cl <- makeCluster(num_cores)
registerDoParallel(cl)

batch_size <- 10
total_trees <- max(forest1_numtrees, forest2_numtrees)
num_batches <- ceiling(total_trees / batch_size)
batches <- split(1:total_trees, ceiling(seq_along(1:total_trees) / batch_size))

matrix_list_vectorized <- foreach(batch = batches, .packages = "Matrix", .combine = 'c') %dopar% {
  batch_matrices <- lapply(batch, function(i) {
    GenerateTreeWeights(gt_obs, x_obs, i, row_sample1_ids, row_sample2_ids,
                        leaf_nodes_sample1, leaf_nodes_sample2, forest2_X, forest1_X,
                        forest1_numtrees, forest2_numtrees)
  })
  sum_batch_matrices <- Reduce(`+`, batch_matrices)
  print(paste("Batch", batch,"done"))
  sum_batch_matrices
}
stopCluster(cl)

sum_M <- Reduce(`+`, matrix_list_vectorized)
# rescaling by column sum
sum_M@x <- sum_M@x / rep.int(colSums(sum_M), diff(sum_M@p)) 

# Define the full file path with .rds extension
saveRDS(sum_M, file = file.path(issue_tempdir,paste0("g",g,"_","t",t,"_","trees",num_trees,".rds")))

