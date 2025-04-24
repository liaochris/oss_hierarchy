library(tidyverse)
library(arrow)
library(gridExtra)
library(ggplot2)
library(egg)
library(eventstudyr)
library(SaveData)
library(future)
library(future.apply)
library(dplyr)
library(purrr)
library(stringr)
library(fixest)
library(didimputation)


set.seed(1234)
`%ni%` <- Negate(`%in%`)

NormalizeOutcome <- function(df, outcome, outcome_norm) {
  df_norm <- df
  if (grepl("download", outcome, ignore.case = TRUE)) {
    df_norm[[outcome]] <- df_norm %>% group_by(time_index) %>%  
      mutate(norm_outcome = !!sym(outcome) / sum(!!sym(outcome))) %>% pull(norm_outcome)
  }
  df_norm <- df_norm %>% group_by(repo_name) %>% mutate(mean_outcome = mean(get(outcome), na.rm = TRUE)) %>% ungroup()
  df_norm[[outcome_norm]] <- df_norm[[outcome]] / df_norm$mean_outcome
  df_norm
}

EventStudyAnalysis <- function(df, outcome, post, pre, title, normalize = TRUE, tfe = TRUE) {
  if (normalize) {
    df_norm <- NormalizeOutcome(df, outcome, paste0(outcome, "_norm"))
  } else {
    df_norm <- df
    df_norm[[paste0(outcome, "_norm")]] <- df_norm[[outcome]]
  } 
  results <- eventstudyr::EventStudy(estimator = "OLS",
                                     data = df_norm,
                                     outcomevar = paste0(outcome, "_norm"),
                                     policyvar = "treatment",
                                     idvar = "repo_name",
                                     timevar = "time_index",
                                     controls = c("graph_importance", "contributor_count", 
                                                  "prop_important", "total_important"),
                                     post = post,
                                     pre = pre,
                                     TFE = tfe)
  plot <- EventStudyPlot(estimates = results, ytitle = "Coefficient", xtitle = "Event time") + 
    ggtitle(title) + theme(plot.title = element_text(size = 12))
  list(plot = plot, results = results)
}

EventStudyAnalysisSA <- function(df, outcome, title, normalize = TRUE) {
  if (normalize) {
    df_norm <- NormalizeOutcome(df, outcome, paste0(outcome, "_norm"))
  } else {
    df_norm <- df
    df_norm[[paste0(outcome, "_norm")]] <- df_norm[[outcome]]
  } 
  
  results <- feols(as.formula(paste(paste0(outcome, "_norm"), "sunab(treatment_group, time_index) | repo_name + time_index", sep = "~")), df_norm, vcov = ~repo_name)
  plot <- results |>
    iplot(
      main     = title,
      xlab     = "Time to treatment",
      keep     = "^-[1-5]|[0-5]",  # only –5…5
      drop     = "[[:digit:]]{2}",    # Limit lead and lag periods to -9:9
      ref.line = 1
    )
  list(plot = plot, results = results)
}


EventStudyAnalysisImpute <- function(df, outcome, title, normalize = TRUE) {
  if (normalize) {
    df_norm <- NormalizeOutcome(df, outcome, paste0(outcome, "_norm"))
  } else {
    df_norm <- df
    df_norm[[paste0(outcome, "_norm")]] <- df_norm[[outcome]]
  } 
  
  results <- did2s(df_norm, yname = paste0(outcome, "_norm"), first_stage = ~ graph_importance | repo_name + time_index,
        second_stage = ~ i(time_index - treatment_group), treatment = "treatment",
        cluster_var = "repo_name")
  plot <- results |>
    iplot(
      main     = title,
      xlab     = "Time to treatment",
      keep     = "^-[1-5]|[0-5]",  # only –5…5
      drop     = "[[:digit:]]{2}",    # Limit lead and lag periods to -9:9
      ref.line = 1
    )
  list(plot = plot, results = results)
}

EventStudyAnalysisBJS <- function(df, outcome, normalize = TRUE) {
  if (normalize) {
    df_norm <- NormalizeOutcome(df, outcome, paste0(outcome, "_norm"))
  } else {
    df_norm <- df
    df_norm[[paste0(outcome, "_norm")]] <- df_norm[[outcome]]
  } 
  results <- did_imputation(df_norm, yname = paste0(outcome, "_norm"), gname = "treatment_group", 
                 tname = "time_index", idname = "repo_name", horizon = T, pretrends = -5:-1) %>% head(11)
  results |>
    iplot(
      main     = title,
      xlab     = "Time to treatment",
      keep     = "^-[1-5]|[0-5]",  # only –5…5
      drop     = "[[:digit:]]{2}",    # Limit lead and lag periods to -9:9
      ref.line = 1
    )
}

MakeTitle <- function(title, title_str, df) {
  paste0(str_replace_all(title, "_", " "), "\n", title_str, "\n", 
         nrow(df), " obs, PC: ", length(unique(df$repo_name)),
         " T: ", length(unique(df %>% filter(treatment == 1) %>% pull(repo_name))),
         "\n# important:", length(unique(df %>% filter(time_index < treatment_group & important == 1) %>% pull(repo_name))))
}

DescribeBin <- function(colname, bin_val) {
  pattern <- "^(.*)_([0-9]+p)_back_bin_([a-z]+)$"
  match <- regexec(pattern, colname, perl = TRUE)
  parts <- regmatches(colname, match)[[1]]
  if (length(parts) != 4) stop("colname does not match expected pattern.")
  paste0(parts[2], " (", parts[3], ") ", bin_val, " ", parts[4])
}

AdjustYScaleUniformly <- function(plot_list, num_breaks = 9, normalize = TRUE) {
  const_min <- ifelse(normalize, -10, -300)
  const_max <- ifelse(normalize, 10, 300)
  if (length(plot_list) == 0) return(plot_list)
  yr <- do.call(rbind, lapply(plot_list, function(p) ggplot_build(p)$layout$panel_params[[1]]$y.range))
  y_min <- max(const_min*1.2, min(yr[,1], na.rm = TRUE))
  y_max <- min(max(yr[,2], na.rm = TRUE), const_max*.8)
  y_breaks <- pretty(c(y_min, y_max), n = num_breaks)
  lapply(plot_list, function(p) p + coord_cartesian(ylim = c(y_min, y_max)) + scale_y_continuous(breaks = y_breaks))
}

ExportIndividualPlots <- function(plot_list, outdir_combo, control_sample) {
  # Ensure output directory exists
  dir.create(outdir_indiv <- file.path(outdir_combo, "indiv_plots"), recursive = TRUE, showWarnings = FALSE)
  
  invisible(lapply(plot_list, function(p) {
    # Extract title rows and drop first, penultimate, and last
    rows   <- strsplit(p$labels$title, "\n")[[1]]
    middle <- rows[-c(1, length(rows)-1, length(rows))]
    
    # Build base string
    base <- paste(middle, collapse = "_")
    # Apply cleaning steps without pipes
    clean <- gsub("Up_to_Last_Active_Period", "early_sample",
                  gsub("all_time_periods", "full_sample",
                       sub("_+$", "_",
                           gsub("_+", "_",
                                gsub(" ", "_",
                                     gsub("-", "_",
                                          gsub("[()]", "", base)))))))
    
    # Compose filename and save
    fname <- sprintf("%s_%s.png", control_sample, clean)
    fname_coefs <- sprintf("%s_%s_coefs.csv", control_sample, clean)
    ggsave(file.path(outdir_indiv, fname), p, bg = "white")
    appendlog <- grepl("full_sample", fname, fixed = TRUE)
    SaveData(p$data %>% arrange(label_num), key = "term", file.path(outdir_indiv, fname_coefs), appendlog = appendlog, sortbykey = F)
  }))
}




PlotSpecCombo <- function(split_spec, spec, outcome, event_result, df_cov_panel, df_cov_panel_nyt, spec_covariates, na_keep_cols, 
                          outdir_spec, post, pre, outcomes, plot_flag, tfe = tfe, suffix, control_sample, normalize = normalize) {
  split_vars <- spec_covariates[[spec]]
  if (length(split_vars)>1) {
    split_spec_vars <- c(paste(split_vars[1], split_spec, sep = "_"),
                         paste(split_vars[2:length(split_vars)], str_replace(split_spec, "bin_third","bin_median"), sep = "_"))
    print(split_spec_vars)
  } else if (length(split_vars) == 1) {
    split_spec_vars <- paste(split_vars, split_spec, sep = "_")
  } else{
    return(NULL)
  }
  if (control_sample == "exact") {
    if (!all(split_spec_vars %in% names(df_cov_panel))) {
      message(paste(split_spec_vars, collapse = ", "), " not available")
      return(NULL)
    }
  } else if (control_sample == "restrictive") {
    control_split_spec_vars <- intersect(names(df_cov_panel), split_spec_vars)
    if (length(control_split_spec_vars) == 0) {
      message(paste(split_spec_vars, collapse = ", "), " not available")
      return(NULL) 
    }
  }
  
  # intentionally excludes .5 medians
  na_drop_cols <- names(df_cov_panel_nyt[split_spec_vars])[!(split_vars %in% na_keep_cols)]
  combos <- expand.grid(lapply(df_cov_panel_nyt[split_spec_vars], unique)) %>% 
    drop_na(all_of(na_drop_cols)) %>% arrange(across(everything()))
  plots <- list(event_result$full$plot, event_result$early$plot)
  
  for (i in seq_len(nrow(combos))) {
    combo <- combos[i, , drop = FALSE]
    descs <- sapply(names(combo), function(col) DescribeBin(col, as.numeric(combo[[col]])))
    
    exclude_projects <- unique(df_cov_panel_nyt %>% filter(!if_all(-repo_name, is.na)) %>% pull(repo_name))
    df_control <- df_cov_panel %>% filter(repo_name %ni% exclude_projects)
    if (control_sample == "all") {
      control_obs <- df_control %>% select(repo_name) %>% pull()
      treated_obs <- df_cov_panel_nyt %>% inner_join(combo, by = names(combo)) %>% select(repo_name) %>% pull()
      all_obs <- data.frame(repo_name = unique(c(control_obs, treated_obs)))
      control_descs <- c("Control: All untreated")
    } else if (control_sample == "restrictive") {
      control_obs <- df_control %>% inner_join(combo[control_split_spec_vars], by = control_split_spec_vars) %>% select(repo_name) %>% pull()
      treated_obs <- df_cov_panel_nyt %>% inner_join(combo, by = names(combo)) %>% select(repo_name) %>% pull()
      all_obs <- data.frame(repo_name = c(control_obs, treated_obs))
      restricted_descs <- sapply(control_split_spec_vars, function(col) DescribeBin(col, as.numeric(combo[[col]])))
      control_descs <- c(paste("Control: Untreated, median", restricted_descs))
    } else if (control_sample == "exact") {
      control_obs <- df_control %>% inner_join(combo, by = names(combo)) %>% select(repo_name) %>% pull()
      treated_obs <- df_cov_panel_nyt %>% inner_join(combo, by = names(combo)) %>% select(repo_name)  %>% pull()
      all_obs <- data.frame(repo_name = c(control_obs, treated_obs))
      control_descs <- c(paste("Control: Untreated", descs))
    }
    df_selected <- event_result$df_early %>% inner_join(all_obs, by = "repo_name")
    sample_out <- tryCatch({
      if (outcome == outcomes[1]) {
        fname <- paste0(paste(names(combo), combo, collapse = "_", sep = "_"), "_controls_", control_sample, ".csv")
        SaveData(df_selected %>% select(repo_name, treated) %>% unique(), c("repo_name"), file.path(outdir_spec, fname), appendlog = (i>1 | control_sample != "all"))
      }
      #EventStudyAnalysisImpute(df_selected, outcome,  MakeTitle(paste0(suffix, outcome), paste(descs, collapse = "\n"), df_selected), normalize = normalize)
      EventStudyAnalysis(df_selected, outcome, post, pre, MakeTitle(paste0(suffix, outcome), paste(descs, collapse = "\n"), df_selected), tfe = tfe, normalize = normalize)
    }, error = function(e) {
      message("Error in EventStudyAnalysis for combo: ", paste(combo, collapse = " "), ". Skipping.")
      NULL
    })
    if (!is.null(sample_out)) plots[[length(plots) + 1]] <- sample_out$plot
  }
  Filter(Negate(is.null), plots)
}

AggregateEventStudies <- function(event_result, df_panel, df_panel_sel, outcome, fill_na, normalize, outdir) {
  # 1) Define each event‐study and its label as a formula
  analyses <- list(
    LP_NYT = function() {
      event_result$early$plot$data %>% mutate(label = "Linear Panel, Control = NYT")},
    LP_NT = function() {EventStudyAnalysis(df_panel %>% filter(time_period <= final_period),
                                  outcome, post, pre,"",
                                  tfe = tfe, normalize = normalize)$plot$data %>%
      mutate(label = "Linear Panel, Control = NT", label_num = as.numeric(label_num))},
    SA_NYT = function() {EventStudyAnalysisSA(df_panel_sel %>% filter(time_period <= final_period), 
                                    outcome, "", normalize = normalize)$plot$prms %>%
      rename(label_num = x, conf.low = ci_low, conf.high = ci_high) %>%
      mutate(label = "Sun and Abraham, Control = NYT", label_num = as.numeric(label_num))},
    SA_NT = function() {EventStudyAnalysisSA(df_panel %>% filter(time_period <= final_period) %>%
                                      mutate(treatment_group = coalesce(treatment_group, Inf)), 
                                    outcome, "", normalize = TRUE)$plot$prms %>%
      rename(label_num = x, conf.low = ci_low, conf.high = ci_high) %>%
      mutate(label = "Sun and Abraham, Control = NT", label_num = as.numeric(label_num))},
    BJS_NYT = function() {EventStudyAnalysisBJS(df_panel_sel %>% filter(time_period <= final_period), 
                                      outcome, normalize = normalize) %>%
      rename(label_num = term) %>%
      mutate(label = "BJS, Control = NYT", label_num = as.numeric(label_num))},
    BJS_NT  = function() {EventStudyAnalysisBJS(df_panel %>% filter(time_period <= final_period), 
                                                outcome, normalize = normalize) %>%
      rename(label_num = term) %>%
      mutate(label = "BJS, Control = NT", label_num = as.numeric(label_num))})
  
  combined_df <- map_dfr(analyses, ~ .x()) %>%
    select(estimate, label_num, label, conf.low,conf.high)
  
  # 3) Common plotting settings
  dodge    <- position_dodge(width = 0.4)
  cap_w    <- 0.1
  plot_es  <- function(data, title, show_legend = TRUE) {
    ggplot(data,
           aes(x = factor(label_num),
               y = estimate,
               colour = label,
               group = label)) +
      geom_errorbar(aes(ymin = conf.low, ymax = conf.high),
                    width    = cap_w,
                    position = dodge) +
      geom_point(position = dodge, size = 2) +
      geom_hline(yintercept = 0,
                 linetype   = "dashed",
                 colour     = "green") +
      labs(x      = "Event time",
           y      = "Estimate",
           colour = if (show_legend) "Model" else NULL,
           title  = title) +
      theme_minimal()
  }
  
  # 4) Draw the three plots
  print(plot_es(combined_df, "All Models"))
  ggsave(file.path(outdir, "prs_opened_all_specs.png"), width = 9, height = 9, dpi = 300, bg = "white")
  print(plot_es(filter(combined_df, str_detect(label, "Control = NT")),
                "Control = NT", show_legend = FALSE))
  ggsave(file.path(outdir, "prs_opened_nt_specs.png"), width = 9, height = 9, dpi = 300, bg = "white")
  print(plot_es(filter(combined_df, str_detect(label, "Control = NYT")),
                "Control = NYT", show_legend = FALSE))
  ggsave(file.path(outdir, "prs_opened_nyt_specs.png"), width = 9, height = 9, dpi = 300, bg = "white")
}
  

# NOTE NOT TECHNICALLY EQUIAVLENT BECAUSE 5+, -5

GenerateEventStudies <- function(nyt, df_panel, df_cov_panel, df_cov_panel_nyt, outcomes, spec_covariates, post, pre, outdir,
                                 na_keep_cols, fill_na, plot_flag = FALSE, tfe = TRUE, suffix = "", control_sample, normalize,
                                 rename_dict) {
  treated_projects <- df_panel %>% filter(treated == 1) %>% select(repo_name) %>% unique() %>% pull()
  all_projects <-  df_panel %>% select(repo_name) %>% unique() %>% pull()
  selected_repos <- if (nyt) treated_projects else all_projects
  
  df_panel_sel <- df_panel %>% filter(repo_name %in% selected_repos)
  df_cov_panel_sel <- df_cov_panel %>% filter(repo_name %in% selected_repos)
  df_cov_panel_nyt_sel <- df_cov_panel_nyt %>% filter(repo_name %in% selected_repos)
  specs <- names(spec_covariates)
  plan(multisession, workers = parallel::detectCores() - 1)
  
  future_lapply(specs, function(spec) {
    for (outcome in outcomes) {      
      if (outcome %in% fill_na) df_panel_sel[[outcome]] <- ifelse(is.na(df_panel_sel[[outcome]]), 0,df_panel_sel[[outcome]])
      if (outcome %in% fill_na) df_panel[[outcome]] <- ifelse(is.na(df_panel[[outcome]]), 0,df_panel_sel[[outcome]])
      event_result <- tryCatch({
        full_sample <- EventStudyAnalysis(df_panel_sel, outcome, post, pre, 
                                          MakeTitle(paste0(suffix, outcome), paste0(outcome, " - All Time Periods"), df_panel_sel), 
                                          tfe = tfe, normalize = normalize)
        # full_sample <- EventStudyAnalysisImpute(df_panel_sel, outcome, 
        #                                   MakeTitle(paste0(suffix, outcome), paste0(outcome, " - All Time Periods"), df_panel_sel), 
        #                                   normalize = normalize)
        early_panel <- df_panel_sel %>% filter(time_period <= final_period)
        early_sample <- EventStudyAnalysis(early_panel, outcome, post, pre, 
                                          MakeTitle(paste0(suffix, outcome), paste0(outcome, " - All Time Periods"), df_panel_sel), 
                                          tfe = tfe, normalize = normalize)
        # early_sample <- EventStudyAnalysisImpute(early_panel, outcome, 
        #                                          MakeTitle(paste0(suffix, outcome), paste0(outcome, " - All Time Periods"), df_panel_sel), 
        #                                          normalize = normalize)
        list(full = full_sample, early = early_sample, df_early = early_panel)
      }, error = function(e) {
        message("Error for outcome ", outcome, " with spec ", spec, ". Skipping.")
        NULL
      })
      
      if (is.null(event_result)) next
      #if (outcome == "prs_opened" & spec == "more_imp") AggregateEventStudies(event_result, df_panel, df_panel_sel, outcome, fill_na, normalize, outdir)
      
      outdir_combo <- file.path(outdir, spec, outcome)
      outdir_spec <- file.path(outdir, spec)
      dir.create(outdir_combo, recursive = TRUE, showWarnings = FALSE)
      
      combos <- expand.grid(k = c(2, 3), bin = c("bin_median", "bin_third"), stringsAsFactors = FALSE) %>%
        mutate(split_specs = paste0(k, "p_back_", bin))
      for (split_spec in combos$split_specs) {
        plot_list <- PlotSpecCombo(split_spec, spec, outcome, event_result, df_cov_panel_sel, df_cov_panel_nyt_sel, spec_covariates, na_keep_cols,
                                   outdir_spec, post, pre, outcomes, plot_flag, tfe = tfe, suffix, control_sample = control_sample, normalize = normalize)
        PlotCombinedEventStudy(plot_list, split_spec, rename_dict)
        ggsave(file.path(outdir_combo, sprintf("combined_%s.png", split_spec)), width = 9, height = 9, dpi = 150, bg = "white")
        if (length(plot_list) == 0) next
        plot_list <- AdjustYScaleUniformly(plot_list, normalize = normalize)
        final_plot <- grid.arrange(grobs = plot_list, ncol = 2)
        fname <- file.path(outdir_combo, sprintf("%s_%s_%s.png", control_sample, spec, gsub("_back_bin", "", split_spec)))
        if (plot_flag) ggsave(plot = final_plot, filename = fname, width = 9, height = 4 * (length(plot_list)), limitsize = FALSE, dpi = 150, bg = "white")
        if (plot_flag) ExportIndividualPlots(plot_list, outdir_combo, control_sample)
        message("Saved file: ", fname)
        flush.console()
      }
    }
  })
}

TransformString <- function(raw_statements, split_specs, rename_dict) {
  # this inner function handles one statement + its spec
  single_transform <- function(stmt, split_spec) {
    # 1) split into lines, grab 2nd
    lines <- str_split(stmt, "\n")[[1]]
    if (length(lines) < 2) return(NA_character_)
    core <- lines[-c(1, (length(lines)-1):length(lines))]
    # 2) remove "(...)"  
    core <- str_remove(core, "\\s*\\([^)]*\\)")
    # 3) split into var and code  
    parts <- str_split_fixed(str_squish(core), "\\s+", 3)
    var  <- parts[,1]
    code <- parts[,2]
    # 4) recode based on split_spec
    val <- if (str_detect(split_spec, regex("median", ignore_case = TRUE))) {
      c(`0` = "Below median", `1` = "Above median")[code]
    } else if (str_detect(split_spec, regex("third", ignore_case = TRUE))) {
      c(`1` = "Bottom third", `2` = "Middle third", `3` = "Top third")[code]
    } else {
      code
    }
    # 5) rename or title-case the variable
    var_clean <- map_chr(var, ~ rename_dict[[.x]] %||% str_to_title(str_replace_all(.x, "_", " ")))
    # 6) paste and return
    out <- paste(var_clean, val)
    out <- paste(out, collapse = "+")
    if (is.na(out)) NA_character_ else out
  }
  # vectorize over both inputs
  mapply(single_transform,
         stmt       = raw_statements,
         split_spec = split_specs,
         USE.NAMES  = FALSE,
         SIMPLIFY   = TRUE,
         MoreArgs   = NULL)
}

PlotCombinedEventStudy <- function(plot_list, split_spec, rename_dict) {
  # 1. bind and name data frames
  df_coefs <- plot_list[-1] %>% imap_dfr(~ {data <- .x$data
  data$split <- if (.y == 1) "All observations" else .x$labels$title %||% paste0("plot_", .y)
  data})
  
  # 2. transform and split labels
  rename_vals <- unlist(rename_dict)
  event_df <- df_coefs %>%
    mutate(full_split   = coalesce(TransformString(split, split_spec, rename_dict), split),
           legend_title = map_chr(full_split, function(fs) {
             df <- imap_dfr(rename_vals, function(v, i) {
               loc <- str_locate(fs, fixed(v))
               if (!is.na(loc[1])) tibble(v = v, start = loc[1]) else NULL
             })
             if (nrow(df) == 0) return(NA_character_)
             paste(df$v[order(df$start)], collapse = "+")
           }),
           split_value = map2_chr(full_split, legend_title, ~ {
             if (is.na(.y)) .x else
               str_squish(str_remove_all(.x,
                                         regex(str_c("\\b(", str_replace_all(.y, "\\+", "|"), ")\\b"))))
           }),
           is_all_obs   = str_to_lower(full_split) == "all observations")
  
  # 3. split data & dodge params
  all_df   <- filter(event_df, is_all_obs)
  split_df <- filter(event_df, !is_all_obs)
  n_splits    <- n_distinct(split_df$split_value)
  dodge_total <- .2*n_splits
  bar_width   <- dodge_total / max(n_splits, 1)
  dodge       <- position_dodge(width = dodge_total)
  col_title   <- unique(event_df$legend_title[!event_df$is_all_obs])
  x_labels <- event_df %>%
    distinct(label_num, label) %>%
    arrange(label_num) %>%
    pull(label)
  # 4. plot
  ggplot() +
    # sup-t bands, CIs & points for all obs in grey
    geom_linerange(data = all_df,
                   aes(x = factor(label_num), ymin = suptband_lower, ymax = suptband_upper,
                       linetype = "All observations"),
                   colour = "black", size = 1, position = dodge) +
    geom_errorbar(data = all_df,
                  aes(x = factor(label_num), ymin = conf.low, ymax = conf.high,
                      linetype = "All observations"),
                  colour = "black", width = bar_width, position = dodge) +
    geom_point(data = all_df,
               aes(x = factor(label_num), y = estimate, linetype = "All observations"),
               colour = "black", size = 2, position = dodge) +
    
    # sup-t bands, CIs & points for splits in colour
    geom_linerange(data = split_df,
                   aes(x = factor(label_num), ymin = suptband_lower, ymax = suptband_upper,
                       colour = split_value),
                   size = 1, position = dodge) +
    geom_errorbar(data = split_df,
                  aes(x = factor(label_num), ymin = conf.low, ymax = conf.high,
                      colour = split_value),
                  width = bar_width, position = dodge) +
    geom_point(data = split_df,
               aes(x = factor(label_num), y = estimate, colour = split_value),
               size = 2, position = dodge) +
    
    geom_hline(yintercept = 0, linetype = "dashed", colour = "green") +
    
    # legends: linetype for all obs, colour for splits
    scale_linetype_manual(name = NULL,
                          values = c("All observations" = "solid"),
                          guide = guide_legend(override.aes = list(colour = "black"))) +
    scale_colour_discrete(name = col_title) +
    scale_x_discrete(labels = x_labels) +
    guides(linetype = guide_legend(order = 1), colour = guide_legend(order = 2)) +
    labs(x = "Event time", y = "Coefficient") +
    theme_minimal()
}

drop_na_outcomes <- function(df, sel_outcomes) {
  # Identify repo_name groups for which all columns that start with outcome_prefix are NA
  repos_to_drop <- df %>%
    group_by(repo_name) %>%
    summarise(all_outcomes_na = all(if_all(sel_outcomes, is.na))) %>%
    filter(all_outcomes_na) %>%
    pull(repo_name)
  
  # Print the count of groups dropped
  num_dropped <- length(repos_to_drop)
  cat("Dropped", num_dropped, "repo(s)\n")
  
  # Filter out the dropped groups from the original data frame
  df_filtered <- df %>%
    filter(!repo_name %in% repos_to_drop)
  
  return(df_filtered)
}


rename_dict <- list(
  normalized_degree = "Departed contributor importance",
  total_important = "Important contributor count",
  mean_cluster_overlap = "Project clustering",
  overall_overlap = "Departed contributor overlap",
  imp_to_imp_avg_edge_weight = "Departed imp<->imp communication",
  imp_to_other_avg_edge_weight = "Departed imp<->other communication",
  imp_to_imp_overall = "Project imp<->imp communication",
  imp_to_other_overall = "Project imp<->other communication"
)


sel_outcomes <- c("prs_opened", "prs_merged", "commits", "commits_lt100", "comments","issue_comments", "pr_comments","issues_opened", "issues_closed")

outcomes <- c(
  "prs_opened",
  "prs_merged", "commits", "commits_lt100", "comments", "issue_comments", "pr_comments",
  "issues_opened", "issues_closed",
  "p_prs_merged", "closed_issue",
  "p_prs_merged_30d", "p_prs_merged_60d", "p_prs_merged_90d", "p_prs_merged_180d",
  "p_prs_merged_360d", "closed_in_30_days", "closed_in_60_days", "closed_in_90_days",
  "total_downloads", "total_downloads_one_project", "total_downloads_rel", "total_downloads_one_project_rel",
  "closed_in_180_days", "closed_in_360_days",
  "overall_new_release_count", "major_new_release_count",
  "minor_new_release_count", "patch_new_release_count", "other_new_release_count", "major_minor_release_count",
  "major_minor_patch_release_count", "total_nodes", "forks_gained", "stars_gained",
  "overall_score", "overall_increase",
  "overall_decrease", "overall_stable", "vulnerabilities_score"
)


spec_covariates <- list(
  # imp_contr = c("total_important"),
  # more_imp = c("normalized_degree"),
  project_clus_ov = c("mean_cluster_overlap"),
  indiv_clus = c("overall_overlap"),
  project_clus_node = c("avg_clusters_per_node"),
  project_clus_pct_one = c("pct_nodes_one_cluster"),
  # imp_contr_more_imp = c("total_important", "normalized_degree"),
  # imp_ratio = c("prop_important"),
  # indiv_clus_more_imp = c("overall_overlap", "normalized_degree"),
  # project_clus_ov_more_imp  = c("mean_cluster_overlap","normalized_degree"),
  # project_clus_ov_imp_contr = c("mean_cluster_overlap","total_important"),
  # project_clus_ov_more_imp_imp_contr = c("mean_cluster_overlap","normalized_degree", "total_important"),
  # indiv_cluster_size = c("overall_overlap", "total_important"),
  indiv_cluster_ov_cluster = c("overall_overlap", "mean_cluster_overlap"),
  imp_imp_comm_dept = c("imp_to_imp_avg_edge_weight"),
  imp_other_comm_dept = c("imp_to_other_avg_edge_weight"),
  both_comm_dept = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight"),
  # comm_imp_more_imp_dept = c("normalized_degree", "imp_to_imp_avg_edge_weight"),
  # comm_within_more_imp_dept = c("normalized_degree", "imp_to_other_avg_edge_weight"),
  # both_comm_cluster_dept = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "overall_overlap"),
  # both_comm_ov_cluster_dept = c("imp_to_imp_avg_edge_weight", "imp_to_other_avg_edge_weight", "mean_cluster_overlap"),
  # comm_cluster_dept = c("imp_to_imp_avg_edge_weight", "overall_overlap"),
  # comm_within_cluster_dept = c("imp_to_other_avg_edge_weight", "overall_overlap"),
  imp_imp_comm = c("imp_to_imp_overall"),
  imp_other_comm = c("imp_to_other_overall"),
  both_comm = c("imp_to_imp_overall", "imp_to_other_overall")
  # comm_imp_more_imp = c("normalized_degree", "imp_to_imp_overall"),
  # comm_within_more_imp = c("normalized_degree", "imp_to_other_overall"),
  # project_clus_ov_imp_imp_comm = c("mean_cluster_overlap","imp_to_imp_overall"),
  # project_clus_ov_imp_other_comm = c("mean_cluster_overlap","imp_to_other_overall"),
  # project_clus_ov_imp_imp_comm_more_imp = c("mean_cluster_overlap","imp_to_imp_overall", "normalized_degree"),
  # project_clus_ov_imp_other_comm_more_imp = c("mean_cluster_overlap","imp_to_other_overall", "normalized_degree"),
  # both_comm_cluster = c("imp_to_imp_overall", "imp_to_other_overall", "overall_overlap"),
  # both_comm_ov_cluster = c("imp_to_imp_overall", "imp_to_other_overall", "mean_cluster_overlap"),
  # comm_cluster = c("imp_to_imp_overall", "overall_overlap"),
  # comm_within_cluster = c("imp_to_other_overall", "overall_overlap")
)

Main <- function() {
  params <- list(time_period = 6, rolling_window = 732, criteria_pct = 75,
                 consecutive_periods = 3, post_periods = 2, graph_flag = FALSE)
  
  issue_tempdir <- "issue"
  outdir <- "issue/event_study/graphs"
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  df_panel <- read_parquet(file.path(issue_tempdir, "event_study_panel.parquet"))
  df_panel <- drop_na_outcomes(df_panel %>% mutate(across(sel_outcomes, ~ na_if(., 0))), sel_outcomes)
  
  df_cov_panel <- read_parquet(file.path(issue_tempdir, "covariate_bins.parquet"))
  df_cov_panel_nyt <- read_parquet(file.path(issue_tempdir, "nyt_covariate_bins.parquet"))
  fill_na <- outcomes[!(grepl("downloads", outcomes, ignore.case = TRUE) |
                          grepl("^p_", outcomes) |
                          grepl("^closed_in_", outcomes) |
                          outcomes %in% c("overall_score", "overall_increase", "overall_decrease", "overall_stable", "vulnerabilities_score"))]
  na_keep <- if (params$graph_flag) c() else c() # c("overall_overlap")
  post_period <- 4
  pre_period <- 0
  
  
  # all uses all untreated
  # exact uses the exact covariate bin match if it exists
  # restrictive finds the largest covariate bin subset that matches
  for (nyt in c(TRUE)) {
    nyt_suffix <- ifelse(nyt, "_nyt", "")
    control_sample_list <- if (nyt) c("all") else c("all", "exact", "restrictive")
    for (control_sample in control_sample_list) {
      for (normalize in c(TRUE, FALSE)) {
        outdir_graphs <- ifelse(normalize, outdir, paste("issue/event_study/graphs", "val", sep = "_"))
        outdir_graphs <- paste0(outdir_graphs, nyt_suffix)
        dir.create(outdir_graphs, recursive = TRUE, showWarnings = FALSE)
        GenerateEventStudies(nyt, df_panel, df_cov_panel, df_cov_panel_nyt,
                             outcomes, spec_covariates, post = post_period, pre = pre_period,
                             outdir = outdir_graphs, na_keep_cols = na_keep, fill_na = fill_na, plot_flag = TRUE, control_sample = control_sample,
                             normalize = normalize, rename_dict = rename_dict)
        GenerateEventStudies(nyt, df_panel, df_cov_panel, df_cov_panel_nyt,
                             c(paste("avg", sel_outcomes, sep = '_'),paste("cc", sel_outcomes, sep = '_')), 
                             spec_covariates, post = post_period, pre = pre_period,
                             outdir = outdir_graphs, na_keep_cols = na_keep, fill_na = fill_na, 
                             plot_flag = TRUE, control_sample = control_sample, normalize = normalize, rename_dict = rename_dict)
      }
    }
    
    # for (suffix in paste0("_", c("all","unimp","new", "alltime", "imp"))) {
    #   for (control_sample in control_sample_list) {
    #     for (normalize in c(TRUE, FALSE)) { 
    #       outdir_suffix <- ifelse(normalize, paste0("issue/event_study/graphs",suffix), 
    #                               paste(paste0("issue/event_study/graphs",suffix), "val", sep = "_"))
    #       outdir_suffix <- paste0(outdir_suffix, nyt_suffix)
    #       dir.create(outdir_suffix, recursive = TRUE, showWarnings = FALSE)
    #       df_panel_suffix <- read_parquet(file.path(issue_tempdir, paste0("event_study_panel",suffix,".parquet")))
    #       GenerateEventStudies(nyt, df_panel_suffix, df_cov_panel, df_cov_panel_nyt,
    #                            outcomes, spec_covariates[1:10], post = post_period, pre = pre_period,
    #                            outdir = outdir_suffix, na_keep_cols = na_keep, fill_na = fill_na, plot_flag = TRUE,
    #                            tfe = suffix == "_new", suffix, control_sample = control_sample, normalize = normalize, rename_dict = rename_dict)
    #       GenerateEventStudies(nyt, df_panel_suffix, df_cov_panel, df_cov_panel_nyt,
    #                            c(paste("avg", sel_outcomes, sep = '_'),paste("cc", sel_outcomes, sep = '_')), 
    #                            spec_covariates, post = post_period, pre = pre_period,
    #                            outdir = outdir_suffix, na_keep_cols = na_keep, fill_na = fill_na, plot_flag = TRUE,
    #                            tfe = suffix == "_new", suffix, control_sample = control_sample, normalize = normalize, rename_dict = rename_dict)
    #     }
    #   }
    # }
  }
}

Main()
