library(data.table)
library(Matrix)
library(grf)
library(doParallel)
library(foreach)
library(arrow)
library(progressr)
library(tidyverse)

`%ni%` <- Negate(`%in%`)
handlers(global = TRUE)
handlers("txtprogressbar")
num_cores <- parallel::detectCores() - 1


ConvertProjectIDToRow <- function(data_gt, t, sample_ids) {
  project_id_vector <- data_gt[time_index == t, project_id]
  match(sample_ids, project_id_vector)
}

GenerateTreeWeights <- function(gt_obs, x_obs, i, row_sample1_ids, row_sample2_ids,
                                leaf_nodes_sample1, leaf_nodes_sample2, forest2_X, forest1_X,
                                forest1_numtrees, forest2_numtrees) {
  M_i <- Matrix(0, nrow = gt_obs, ncol = x_obs, sparse = TRUE)
  
  if (length(row_sample1_ids) > 0 & i <= forest2_numtrees) {
    leaf_nodes1 <- leaf_nodes_sample1[[i]]
    forest2_X_i <- forest2_X[[i]]
    forest2_map <- split(seq_along(forest2_X_i), forest2_X_i)
    matched_cols_list <- forest2_map[as.character(leaf_nodes1)]
    valid_matches <- !sapply(matched_cols_list, is.null)
    
    rows_to_set <- rep(row_sample1_ids[valid_matches], lengths(matched_cols_list[valid_matches]))
    cols_to_set <- unlist(matched_cols_list[valid_matches], use.names = FALSE)
    
    M_i[cbind(rows_to_set, cols_to_set)] <- 1 / forest2_numtrees
  }
  
  if (length(row_sample2_ids) > 0 & i <= forest1_numtrees) {    
    leaf_nodes2 <- leaf_nodes_sample2[[i]]
    forest1_X_i <- forest1_X[[i]]
    forest1_map <- split(seq_along(forest1_X_i), forest1_X_i)
    matched_cols_list <- forest1_map[as.character(leaf_nodes2)]
    valid_matches <- !sapply(matched_cols_list, is.null)
    
    rows_to_set <- rep(row_sample2_ids[valid_matches], lengths(matched_cols_list[valid_matches]))
    cols_to_set <- unlist(matched_cols_list[valid_matches], use.names = FALSE)
    
    M_i[cbind(rows_to_set, cols_to_set)] <- 1 / forest1_numtrees
  }
  
  return(M_i)
}
SplitProjectIDs <- function(project_ids) {
  sample_size <- floor(length(project_ids) / 2)
  sample1 <- sample(project_ids, size = sample_size)
  sample2 <- project_ids[!project_ids %in% sample1]
  list(sample1 = sample1, sample2 = sample2)
}


InputData <- function(data, g, t, outcome) {
  set.seed(1234)
  
  data_gt <- data[treatment_time_index == g | is.na(treatment_time_index), ]
  data_gt <- data_gt[, .(project_appears = sum(time_index == (g - 1) | time_index == t)), by = repo_name]
  data_gt <- data_gt[project_appears == 2, ]
  data_gt <- merge(data_gt, data, by = "repo_name")[time_index == (g - 1) | time_index == t]
  setorder(data_gt, project_id)
  data_gt[, project_appears := NULL]
  
  unique_projects <- unique(data_gt[, .(project_id, treated_project)])
  control_project_ids <- unique_projects[treated_project == 0, project_id]
  treated_project_ids <- unique_projects[treated_project == 1, project_id]
  control_samples <- SplitProjectIDs(control_project_ids)
  treated_samples <- SplitProjectIDs(treated_project_ids)
  
  sample1_ids <- c(control_samples$sample1, treated_samples$sample1)
  sample2_ids <- c(control_samples$sample2, treated_samples$sample2)
  
  data_gt_x <- data_gt[time_index == (g - 1), .SD, .SDcols = c(org_covars, org_structure, contributor_covars, "time_index", "project_id", "row")]
  
  if (t>=g) {
    data_d <- data_gt %>% mutate(D = treatment)
  } else {
    data_d <- data_gt %>% mutate(D = treated_project)
  }
  data_d <- data_d[time_index == t,] %>%select(project_id, D)
  
  pre_treatment_y <- data_gt[time_index == (g - 1), .(repo_name, outcome_pre_treatment = get(outcome))]
  
  data_y <- merge(data_gt[time_index == t, .(outcome = get(outcome), repo_name, project_id)], pre_treatment_y, by = "repo_name", all.x = TRUE)
  data_y[, outcome_diff := outcome - outcome_pre_treatment]
  data_y <- data_y[, .(outcome_diff, project_id)]
  
  list(data_gt = data_gt, data_gt_x = data_gt_x, data_gt_d = data_d, data_gt_y = data_y,
       sample1_ids = sample1_ids, sample2_ids = sample2_ids)
}


indir <- "drive/output/derived/project_outcomes"
indir_departed <- "drive/output/derived/contributor_stats/filtered_departed_contributors"
issue_tempdir <- "issue"
outdir <- "issue/event_study"

time_period <- 6
rolling_window <- 732
criteria_pct <- 75
consecutive_periods <- 3
post_periods <- 2

fread_parquet <- function(path) {
  as.data.table(read_parquet(path))
}

df_project_outcomes <- fread_parquet(file.path(indir, paste0("project_outcomes_major_months", time_period, ".parquet")))[
  , time_period := as.Date(time_period)
]
df_departed_contributors <- fread_parquet(
  file.path(indir_departed, paste0("filtered_departed_contributors_major_months", time_period,
                                   "_window", rolling_window, "D_criteria_commits_", criteria_pct,
                                   "pct_consecutive", consecutive_periods,
                                   "_post_period", post_periods, "_threshold_gap_qty_0.parquet")))
df_project_covariates <- fread_parquet(file.path(issue_tempdir, "project_covariates.parquet"))[
  , time_period := as.Date(time_period)
]
df_contributor_covariates <- fread_parquet(file.path(issue_tempdir, "contributor_covariates.parquet"))[
  , `:=`(departed_actor_id = actor_id, time_period = as.Date(time_period))
][, actor_id := NULL]
df_departed <- df_departed_contributors[
  year(treatment_period) < 2023 & !abandoned_scraped & !abandoned_consecutive_req3_permanentTrue
][
  , repo_count := .N, by = repo_name
][
  repo_count == 1, .(repo_name, departed_actor_id = actor_id,
                     last_pre_period, treatment_period,
                     abandoned_date = as.Date(abandoned_date_consecutive_req2_permanentTrue))
]
treated_more_than_once <- df_departed_contributors[
  year(treatment_period) < 2023 & !abandoned_scraped & !abandoned_consecutive_req3_permanentTrue
][
  , .N, by = repo_name
][N > 1, repo_name]

df_project <- df_project_outcomes[
  year(time_period) < 2023 | (year(time_period) == 2023 & month(time_period) == 1)
][
  repo_name %ni% treated_more_than_once
][
  , time_index := frank(time_period, ties.method = "dense")
]

df_project_departed <- merge(df_project, df_departed, by = "repo_name", all.x = TRUE)
df_project_departed <- merge(df_project_departed, df_project_covariates, by = c("repo_name", "time_period"), all.x = TRUE)
df_project_departed <- merge(df_project_departed, df_contributor_covariates, by = c("departed_actor_id", "repo_name", "time_period"), all.x = TRUE)

df_project_departed[
  , `:=`(treated_project = 1 - as.numeric(is.na(last_pre_period)))
][
  , `:=`(treatment = as.numeric(treated_project & time_period >= treatment_period))
][
  , c("final_period", "last_pre_period") := .(as.Date(final_period), as.Date(last_pre_period))
][
  , project_age := 181 + as.numeric(time_period - as.Date(created_at))
][
  , smallest_age := min(project_age, na.rm = TRUE), by = repo_name
][
  , project_age := fifelse(smallest_age < 0, NA_real_, project_age)
][
  , project_id := frank(repo_name, ties.method = "dense")
][
  , row := .I
][
  , treatment_time_index := ifelse(any(treatment == 1), min(time_index[treatment == 1], na.rm = TRUE), NA_real_), by = repo_name
]

all_projects <- unique(df_project_departed$repo_name)
treated_projects <- unique(df_project_departed[treatment == 1, repo_name])
control_projects <- unique(df_project_departed[repo_name %ni% treated_projects, repo_name])

cat("Project Count:", length(all_projects), "\n",
    "Control Projects:", length(control_projects), "\n",
    "Treated Projects:", length(treated_projects), "\n")

org_covars <- c("stars_accumulated", "forks_gained", "truckfactor", "contributors", "org_status", "project_age")
org_structure <- c("min_layer_count", "problem_discussion_higher_layer_work", "coding_higher_layer_work",
                   "total_HHI", "contributors_comments_wt", "comments_cooperation_pct")
contributor_covars <- c("truckfactor_member", "max_rank", "total_share", "comments_share_avg_wt",
                        "comments_hhi_avg_wt", "pct_cooperation_comments")

outcome <- "commits"
norm_outcome <- df_project_departed[time_period <= last_pre_period, mean(get(outcome), na.rm = TRUE)]
df_project_departed[, (outcome) := get(outcome) / norm_outcome]

g_list <- sort(unique(df_project_departed[treatment == 1, time_index]))
t_list <- sort(unique(df_project_departed$time_index))

X_space <- unique(df_project_departed[, .SD, .SDcols = c(org_covars, org_structure, contributor_covars, "time_index", "project_id", "row")])
X_space_mat <- as.matrix(X_space[, !c("time_index", "project_id", "row"), with = FALSE])

t <- 3
g <- 6
num_trees <- 20000

data_obj <- InputData(data = df_project_departed, g = g, t = t, outcome = outcome)
data_gt <- data_obj$data_gt
sample1_ids <- sort(data_obj$sample1_ids)
sample2_ids <- sort(data_obj$sample2_ids)

tree_min_threshold <- 1000

data_gt_x_sample1 <- data_obj$data_gt_x[project_id %in% sample1_ids]
X_sample1 <- as.matrix(data_gt_x_sample1[, !c("time_index", "project_id", "row"), with = FALSE])
Y_sample1 <- as.matrix(data_obj$data_gt_y[project_id %in% sample1_ids, outcome_diff])
D_sample1 <- as.matrix(data_obj$data_gt_d[project_id %in% sample1_ids, D])

data_gt_x_sample2 <- data_obj$data_gt_x[project_id %in% sample2_ids]
X_sample2 <- as.matrix(data_gt_x_sample2[, !c("time_index", "project_id", "row"), with = FALSE])
Y_sample2 <- as.matrix(data_obj$data_gt_y[project_id %in% sample2_ids, outcome_diff])
D_sample2 <- as.matrix(data_obj$data_gt_d[project_id %in% sample2_ids, D])





forest1 <- causal_forest(X = X_sample1, Y = Y_sample1, W = D_sample1, num.trees = num_trees,
                         tune.parameters = c("mtry", "honesty.fraction", "alpha",
                                             "tune.num.trees" = num_trees*0.2))
forest2 <- causal_forest(X = X_sample2, Y = Y_sample2, W = D_sample2, num.trees = num_trees,
                         tune.parameters = c( "mtry", "honesty.fraction", "alpha",
                                              "tune.num.trees"= num_trees*0.2))


ExtractLeafNodes <- function(forest, X_matrix, tree_indices, index = TRUE) {
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  leaf_list <- foreach(i=tree_indices, .packages = "grf") %dopar% {
    tree <- get_tree(forest, i)
    if (!tree$nodes[[1]]$is_leaf) {
      get_leaf_node(tree, X_matrix)
    }
  }
  valid_tree_indices <- tree_indices[!sapply(leaf_list, is.null)]
  leaf_list <- leaf_list[!sapply(leaf_list, is.null)]
  stopCluster(cl)
  if (index) {
    return(list(leaf_list = leaf_list, valid_tree_indices = valid_tree_indices))
  } else {
    return(leaf_list)
  }
}

leaf_nodes_sample1_obj <- ExtractLeafNodes(forest2, X_sample1, 1:num_trees)
leaf_nodes_sample1 <- leaf_nodes_sample1_obj$leaf_list
leaf_nodes_sample2_obj <- ExtractLeafNodes(forest1, X_sample2, 1:num_trees)
leaf_nodes_sample2 <- leaf_nodes_sample2_obj$leaf_list
system.time(forest1_X <- ExtractLeafNodes(forest1, X_space_mat, leaf_nodes_sample2_obj$valid_tree_indices, index = FALSE))
system.time(forest2_X <- ExtractLeafNodes(forest2, X_space_mat, leaf_nodes_sample1_obj$valid_tree_indices, index = FALSE))
forest1_numtrees <- length(forest1_X)
forest2_numtrees <- length(forest2_X)

### MAYBE DO THE ABOVE ITERATIVELY UNTIL I HAVE SOME THRESHOLD NUMBER OF TREES...

gt_obs <- nrow(data_obj$data_gt_x)
x_obs <- nrow(X_space_mat)

row_sample1_ids <- ConvertProjectIDToRow(data_gt, t, sample1_ids)
row_sample2_ids <- ConvertProjectIDToRow(data_gt, t, sample2_ids)
forest1_numtrees <- length(forest1_X)
forest2_numtrees <- length(forest2_X)

cat(sprintf("g:%d t:%d forest2_tree_count:%d forest1_tree_count:%d # trees:%d\n",
            g, t, forest2_numtrees, forest1_numtrees, num_trees))

cl <- makeCluster(num_cores)
registerDoParallel(cl)

batch_size <- 5
total_trees <- max(forest1_numtrees, forest2_numtrees)
batches <- split(1:total_trees, ceiling(seq_along(1:total_trees) / batch_size))

with_progress({
  p <- progressor(along = batches)
  matrix_list_vectorized <- foreach(batch = batches, .packages = "Matrix", .combine = 'c') %dopar% {
    batch_matrices <- lapply(batch, function(i) {
      GenerateTreeWeights(gt_obs, x_obs, i, row_sample1_ids, row_sample2_ids,
                          leaf_nodes_sample1, leaf_nodes_sample2, forest2_X, forest1_X,
                          forest1_numtrees, forest2_numtrees)
    })
    sum_batch_matrices <- Reduce(`+`, batch_matrices)
    p()
    list(sum_batch_matrices)
  }
})

stopCluster(cl)

sum_M <- Reduce(`+`, matrix_list_vectorized)
#col_sums <- colSums(sum_M)
#sum_M@x <- sum_M@x / rep.int(col_sums, diff(sum_M@p))

saveRDS(sum_M, file = file.path(issue_tempdir, sprintf("g%d_t%d_trees%d.rds", g, t, num_trees)))
