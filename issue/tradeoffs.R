library(dplyr)
library(ggplot2)
library(cowplot)
library(fixest)
library(stargazer)
library(modelsummary)
options("modelsummary_format_numeric_latex" = "plain")

#––– 1) Density-plot fn w/ inline FE regression, annotation & legend control –––
PlotPreperiodDensity <- function(
    data,
    outcome_col,
    x_label,
    plot_title    = NULL,
    anno_x        = Inf,
    anno_hjust    = 1.1,
    anno_vjust    = 1.1,
    show_legend   = FALSE,
    legend_pos    = "bottom",
    margin_top    = 2,
    margin_bottom = 5
) {
  df_pre <- data %>%
    mutate(relative_time = time_index - treatment_group) %>%
    filter(relative_time >= -5, relative_time <= -1) %>%
    mutate(
      collab_label = factor(
        ind_key_collab_2bin,
        levels = c(1, 0),
        labels = c("High", "Low")
      )
    )
  
  # FEOLS regression, extract coef & SE
  model    <- feols(as.formula(paste0(outcome_col, " ~ ind_key_collab | time_index")), data = df_pre)
  coef_val <- coef(model)["ind_key_collab"]
  se_val   <- se(model)["ind_key_collab"]
  anno_text <- sprintf("β = %.3f\nSE = %.3f", coef_val, se_val)
  
  p <- ggplot(df_pre, aes_string(x = outcome_col, fill = "collab_label")) +
    geom_density(alpha = 0.5)
  
  # conditional legend customization
  if(show_legend) {
    p <- p +
      guides(fill = guide_legend(
        title = "Departed contributor collaborativeness",
        keywidth = unit(0.7, "lines"),
        keyheight = unit(0.5, "lines"),
        label.theme = element_text(size = 12),
        override.aes = list(alpha = 0.6)
      ))
  }
  
  p +
    annotate(
      "text",
      x     = anno_x, y = Inf,
      label = anno_text,
      hjust = anno_hjust, vjust = anno_vjust,
      size  = 4
    ) +
    labs(
      x     = x_label,
      y     = NULL
    ) +
    theme_minimal() +
    theme(
      plot.title          = element_text(hjust = 0.5, face = "bold", size = 14),
      legend.position     = if (show_legend) legend_pos else "none",
      legend.key.size     = unit(0.8, "cm"),
      legend.spacing.x    = unit(0.1, "cm"),
      axis.title.y        = element_blank(),
      axis.title.x        = element_text(size = 12),
      axis.text.x         = element_text(size = 10),
      axis.text.y         = element_text(size = 10),
      plot.margin         = margin(
        t = margin_top,
        r = 5,
        b = margin_bottom,
        l = 5
      )
    )
}


#––– 2) Assemble 3×3 grid, shared y-axis & main title –––
MakeDensityPanel <- function(main_title = "Pre-Treatment Density Distributions") {
  plots <- list(
    # first row: larger top margin, zero bottom margin to compact spacing
    PlotPreperiodDensity(
      df_panel_nyt,
      "prs_opened",
      "Number of PRs Opened",
      margin_top    = 3,
      margin_bottom = 0
    ),
    PlotPreperiodDensity(
      df_panel_nyt,
      "avg_prs_opened",
      "PRs Opened / Contributor",
      show_legend   = TRUE,
      legend_pos    = "top",
      margin_top    = 3,
      margin_bottom = 0
    ),
    PlotPreperiodDensity(
      df_panel_nyt,
      "contr_per_pr",
      "Average # of Contributions per PR",
      margin_top    = 3,
      margin_bottom = 0
    ),
    # second row: no margins
    PlotPreperiodDensity(
      df_panel_nyt,
      "problem_avg_imp_contr_count",
      "Average Important Contributors per PR",
      margin_top    = 0,
      margin_bottom = 0
    ),
    PlotPreperiodDensity(
      df_panel_nyt,
      "problem_avg_unimp_contr_count",
      "Average Unimportant Contributors per PR",
      margin_top    = 0,
      margin_bottom = 0
    ),
    PlotPreperiodDensity(
      df_panel_nyt,
      "review_count",
      "Average # of Reviews",
      margin_top    = 0,
      margin_bottom = 0
    ),
    PlotPreperiodDensity(
      df_panel_nyt,
      "review_comment_count",
      "Average # of Review Comments",
      margin_top    = 0,
      margin_bottom = 0
    ),
    # third row: no margins, coef annotation top-left, no legend
    PlotPreperiodDensity(
      df_panel_nyt,
      "important_contributions_share",
      "Average Share by Important Contributors",
      plot_title    = NULL,
      anno_x        = -Inf,
      anno_hjust    = 0,
      anno_vjust    = 1,
      margin_top    = 0,
      margin_bottom = 0
    ),
    PlotPreperiodDensity(
      df_panel_nyt,
      "close_time",
      "Average Close Time per PR",
      margin_top    = 0,
      margin_bottom = 0
    )
  )
  
  # create 3×3 grid compactly
  grid <- plot_grid(plotlist = plots, ncol = 3, align = "hv")
  
  # shared y-axis label
  y_label <- ggdraw() + draw_label("Density", angle = 90, fontface = "bold", size = 10)
  
  # overall title
  title_panel <- ggdraw() + draw_label(main_title, fontface = "bold", size = 12)
  
  # layout: y-axis + grid
  mid <- plot_grid(y_label, grid, ncol = 2, rel_widths = c(0.05, 1))
  
  # stack title over mid
  final <- plot_grid(title_panel, mid, ncol = 1, rel_heights = c(0.05, 1))
  
  return(final)
}
df_panel_nyt <- df_panel_nyt %>%
  mutate(avg_cc_prs_opened = prs_opened/cc_prs_opened,
         contr_per_pr = total_contributions/prs_opened,
         problem_avg_imp_contr_count = problem_avg_contr_count-problem_avg_unimp_contr_count)
#––– 3) Render –––
density_panel <- MakeDensityPanel("")
density_panel
# Save to file
ggsave(
  filename = "issue/output/collab_outcomes.png",
  plot     = density_panel,
  width    = 12,
  height   = 9,
  dpi      = 600
)
# 
reg_model <- feols(
  close_time ~ review_count + contr_per_pr + prop_review_count_na +
    ind_key_collab + problem_avg_imp_contr_count + problem_avg_unimp_contr_count + important_contributions_share |
    time_index,
  data    = df_panel_nyt %>% mutate(relative_time = time_index - treatment_group) %>% filter(relative_time >= -5, relative_time <= -1),
  cluster = ~repo_name
)

cov_rename <- c(
  "review_count"                     = "Average # of Reviews",
  "contr_per_pr"                     = "Average # of Contributions per PR",
  "prop_review_count_na"             = "Proportion w/ no Reviews",
  "ind_key_collab"                   = "Departed Contributor Collaborativeness",
  "problem_avg_imp_contr_count"      = "Average # of Important Contributors",
  "problem_avg_unimp_contr_count"    = "Average # of Unimportant Contributors",
  "important_contributions_share"                  = "Average Share by Important Contributors"
)
library(modelsummary)
library(tibble)
library(kableExtra)  # ensure available

options(modelsummary_factory_latex = "kableExtra")  # force plain tabular backend

InsertDashedLines <- function(tex) {
  tex <- sub("(?m)^\\s*Observations\\s*&", "\\\\hdashline\nObservations &", tex, perl = TRUE)
  tex <- sub("(?m)^\\s*\\$R\\^2\\$\\s*&", "\\\\hdashline\n$R^2$ &", tex, perl = TRUE)
  tex <- sub("(?m)^\\s*Time FE\\s*&", "\\\\hdashline\nTime FE &", tex, perl = TRUE)
  tex
}

MakeRegressionFigure <- function(
    reg_model,
    cov_rename,
    caption   = "Determinants of pull request close time",
    label     = "fig:close_time_regression",
    file_path = "issue/output/collab_regression_figure.tex"
) {
  models  <- list("PR closing time" = reg_model)
  
  gof_map <- tribble(
    ~raw,             ~clean,                 ~fmt, ~omit,
    "Num.Obs.",       "Observations",         0,    FALSE,
    "R2",             "$R^2$",                3,    FALSE,
    "Std.Errors",     "SE clustered by repo", 0,    FALSE,
    "FE: time_index", "Time FE",              0,    FALSE
  )
  
  latex_tabular <- modelsummary(
    models,
    coef_map         = cov_rename,
    estimate         = "{estimate}",
    statistic        = "std.error",
    statistic_rename = c("std.error" = "SE"),
    shape            = term ~ model + statistic,   # Estimate | SE columns
    gof_map          = gof_map,                    # keeps only these GOF rows
    gof_omit         = NULL,
    escape           = FALSE,
    output           = "latex_tabular"
  )
  
  latex_tabular <- InsertDashedLines(latex_tabular)
  
  figure_tex <- paste0(
    "\\begin{figure}[!htbp]\n",
    "\\centering\n",
    "{\\small\n",
    latex_tabular, "\n",
    "}\n",
    "\\caption{", caption, "}\n",
    "\\label{", label, "}\n",
    "\\end{figure}\n"
  )
  
  writeLines(figure_tex, file_path)
  invisible(file_path)
}


MakeRegressionFigure(reg_model, cov_rename)

# 
# df_plot_preperiod_density(
#   df_panel_nyt %>% mutate(avg_cc_prs_opened = prs_opened/cc_prs_opened),
#   "avg_cc_prs_opened",
#   "PRs Opened/PR Contributor pre-period",
#   "Number of PRs Opened/PR Contributor"
# )
# feols(avg_cc_prs_opened ~ ind_key_collab | time_index, df_pre)

# df_plot_preperiod_density(
#   df_panel_nyt,
#   "problem_avg_contr_count",
#   "Average Contributors per PR",
#   "Average Contributors per PR"
# )
# feols(problem_avg_contr_count ~ ind_key_collab | time_index, df_pre)
