library(data.table)
library(Matrix)
library(grf)
library(doParallel)
library(foreach)
library(arrow)
library(progressr)
library(dplyr)

`%ni%` <- Negate(`%in%`)
handlers(global = TRUE)
handlers("txtprogressbar")
num_cores <- parallel::detectCores() - 2


ConvertProjectIDToRow <- function(data_gt, t, sample_ids) {
  project_id_vector <- data_gt[time_index == t, project_id]
  match(sample_ids, project_id_vector)
}

GenerateTreeWeights <- function(i, row_sample1_ids, row_sample2_ids,
                                leaf_nodes1, leaf_nodes2, forest2_X_i, forest1_X_i,
                                forest1_numtrees, forest2_numtrees) {
  rows <- integer(0)
  cols <- integer(0)
  vals <- numeric(0)
  
  if (length(row_sample1_ids) > 0 & i <= forest2_numtrees) {

    forest2_map <- split(seq_along(forest2_X_i), forest2_X_i)
    matched_cols_list <- forest2_map[as.character(leaf_nodes1)]
    valid_matches <- !sapply(matched_cols_list, is.null)
    
    rows_to_set <- rep(row_sample1_ids[valid_matches], lengths(matched_cols_list[valid_matches]))
    cols_to_set <- unlist(matched_cols_list[valid_matches], use.names = FALSE)
    
    vals_to_set <- rep(1 / forest2_numtrees, length(cols_to_set))
    rows <- c(rows, rows_to_set)
    cols <- c(cols, cols_to_set)
    vals <- c(vals, vals_to_set)  
  }
  if (length(row_sample2_ids) > 0 & i <= forest1_numtrees) {    
    forest1_map <- split(seq_along(forest1_X_i), forest1_X_i)
    matched_cols_list <- forest1_map[as.character(leaf_nodes2)]
    valid_matches <- !sapply(matched_cols_list, is.null)
    
    rows_to_set <- rep(row_sample2_ids[valid_matches], lengths(matched_cols_list[valid_matches]))
    cols_to_set <- unlist(matched_cols_list[valid_matches], use.names = FALSE)
    
    vals_to_set <- rep(1 / forest1_numtrees, length(cols_to_set))
    
    rows <- c(rows, rows_to_set)
    cols <- c(cols, cols_to_set)
    vals <- c(vals, vals_to_set)
  }
  
  return(list(rows = rows, cols = cols, vals = vals))
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



ExtractLeafNodes <- function(forest, X_matrix, tree_indices, index = TRUE) {
  cl <- makeCluster(num_cores, outfile = "")
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

MinTreesExtractLeafNodes <- function(train_sample_ids, evaluate_sample_ids, num_trees, tree_min_threshold, X_space_mat) {
  num_valid_trees <- 0
  num_total_trees <- 0
  forest_list <- list()
  
  data_gt_x_sample <- data_obj$data_gt_x[project_id %in% train_sample_ids]
  X_sample <- as.matrix(data_gt_x_sample[, !c("time_index", "project_id", "row"), with = FALSE])
  Y_sample <- as.matrix(data_obj$data_gt_y[project_id %in% train_sample_ids, outcome_diff])
  D_sample <- as.matrix(data_obj$data_gt_d[project_id %in% train_sample_ids, D])
  
  data_gt_x_evaluate <- data_obj$data_gt_x[project_id %in% evaluate_sample_ids]
  X_evaluate <- as.matrix(data_gt_x_evaluate[, !c("time_index", "project_id", "row"), with = FALSE])
  
  while (num_valid_trees < tree_min_threshold) {
    forest_sample <- causal_forest(
      X = X_sample, Y = Y_sample, W = D_sample, num.trees = num_trees, 
      tune.parameters = c("mtry", "honesty.fraction", "alpha", "tune.num.trees" = num_trees * 0.2))
    
    leaf_nodes_eval_obj <- ExtractLeafNodes(forest_sample, X_evaluate, 1:num_trees)
    leaf_nodes_eval <- leaf_nodes_eval_obj$leaf_list
    num_valid_trees <- num_valid_trees + length(leaf_nodes_eval_obj$valid_tree_indices)
    num_total_trees <- num_total_trees + num_trees
    forest_list[[length(forest_list) + 1]] <- forest_sample
  }
  print("Finished training whole forest")
  forests_all <- merge_forests(forest_list)
  leaf_nodes_eval_obj <- ExtractLeafNodes(forests_all, X_evaluate, 1:num_total_trees)
  forests_X <- ExtractLeafNodes(forests_all, X_space_mat, leaf_nodes_eval_obj$valid_tree_indices, index = FALSE)
  return(list(leaf_list = leaf_nodes_eval_obj$leaf_list, forests_X = forests_X, forests_all = forests_all))
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

args = commandArgs(trailingOnly=TRUE)
g <- args[1]
t <- args[2]
num_trees <- 2000
tree_min_threshold <- 100

data_obj <- InputData(data = df_project_departed, g = g, t = t, outcome = outcome)
data_gt <- data_obj$data_gt
sample1_ids <- sort(data_obj$sample1_ids)
sample2_ids <- sort(data_obj$sample2_ids)
gt_obs <- nrow(data_obj$data_gt_x)
x_obs <- nrow(X_space_mat)

system.time(forest1_X_obj <- MinTreesExtractLeafNodes(train_sample_ids = sample1_ids, evaluate_sample_ids = sample2_ids, 
                                                  num_trees = num_trees, tree_min_threshold = tree_min_threshold, 
                                                  X_space_mat = X_space_mat))
forest1_X <- forest1_X_obj$forests_X
leaf_nodes_sample2 <- forest1_X_obj$leaf_list
forest1_cf <- forest1_X_obj$forests_all

system.time(forest2_X_obj <- MinTreesExtractLeafNodes(train_sample_ids = sample2_ids, evaluate_sample_ids = sample1_ids, 
                                                  num_trees = num_trees, tree_min_threshold = tree_min_threshold, 
                                                  X_space_mat = X_space_mat))
forest2_X <- forest2_X_obj$forests_X
leaf_nodes_sample1 <- forest2_X_obj$leaf_list
forest2_cf <- forest2_X_obj$forests_all

row_sample1_ids <- ConvertProjectIDToRow(data_gt, t, sample1_ids)
row_sample2_ids <- ConvertProjectIDToRow(data_gt, t, sample2_ids)
forest1_numtrees <- length(forest1_X)
forest2_numtrees <- length(forest2_X)

cat(sprintf("g:%d t:%d forest2_tree_count:%d forest1_tree_count:%d",
            g, t, forest2_numtrees, forest1_numtrees))

causal_forest_estimation_obj <- list(data_gt = data_gt, forest1_cf = forest1_cf,
                                     forest2_cf = forest2_cf, sample1_ids = sample1_ids, sample2_ids = sample2_ids)
saveRDS(causal_forest_estimation_obj, 
        file = file.path(issue_tempdir, sprintf("g%d_t%d_causal_forest.rds", g, t)))

ComputeSparseMatrixBatched <- function(gt_obs, x_obs, row_sample1_ids, row_sample2_ids,
                                       leaf_nodes_sample1, leaf_nodes_sample2, forest2_X, forest1_X,
                                       forest1_numtrees, forest2_numtrees, batch_size, 
                                       show_progress = TRUE) {
  max_trees <- max(forest1_numtrees, forest2_numtrees)
  if (show_progress) {
    library(progressr)
    p <- progressor(steps = ceiling(max_trees / batch_size))
  } else {
    p <- function(...) NULL
  }
  system.time({M <- sparseMatrix(i = integer(0), j = integer(0), x = numeric(0),
                    dims = c(gt_obs, x_obs))})
  print("sparse matrix created")
  for (start in seq(1, max_trees, by = batch_size)) {
    system.time({
      end <- min(start + batch_size - 1, max_trees)
      print(paste(start, end))
      
      rows_batch <- integer(0)
      cols_batch <- integer(0)
      vals_batch <- numeric(0)
      
      for (i in seq.int(start, end)) {
        forest2_X_i <- leaf_nodes1 <- leaf_nodes2 <- forest1_X_i <- NA
        if (i <= forest2_numtrees) {
          forest2_X_i <- forest2_X[[i]]
          leaf_nodes1 <- leaf_nodes_sample1[[i]]
        }
        if (i <= forest1_numtrees) {
          leaf_nodes2 <- leaf_nodes_sample2[[i]]
          forest1_X_i <- forest1_X[[i]]
        }

        idx_list <- GenerateTreeWeights(i, row_sample1_ids, row_sample2_ids,
                                               leaf_nodes1, leaf_nodes2,
                                               forest2_X_i, forest1_X_i,
                                               forest1_numtrees, forest2_numtrees)
  
        rows_batch <- c(rows_batch, idx_list$rows)
        cols_batch <- c(cols_batch, idx_list$cols)
        vals_batch <- c(vals_batch, idx_list$vals)
      }
      
      partial_M <- sparseMatrix(i = rows_batch, j = cols_batch, x = vals_batch, dims = c(gt_obs, x_obs))
      M <- M + partial_M
      
      rm(partial_M, rows_batch, cols_batch, vals_batch)
      gc()
      p()
    })
  }
  col_sums <- colSums(M)
  M@x <- M@x / rep.int(col_sums, diff(M@p))
  
  return(M)
}

sum_M <- ComputeSparseMatrixBatched(
  gt_obs, x_obs, row_sample1_ids, row_sample2_ids, leaf_nodes_sample1, 
  leaf_nodes_sample2, forest2_X, forest1_X, forest1_numtrees, 
  forest2_numtrees, batch_size = 1, show_progress = TRUE)
  
saveRDS(sum_M, file = file.path(issue_tempdir, sprintf("g%d_t%d_trees%d_f1_%d_f2_%d.rds", g, t, tree_min_threshold,
                                                       forest1_numtrees, forest2_numtrees)))
