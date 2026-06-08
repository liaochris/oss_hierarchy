I would like you to check a few things for me about the model-prediction calculations, implement a few improvements to the dianostics, and examine the current predictions + help me come up with improvements I can implement to improve predictive accuracy. 

Each check/action within each of the subcategories of tasks should be a separate commit, if changes are necessary. Please update this markdown with the plan. 
I'd also liek to reorder parts of the plan so that smaller, less substantive analysis/conceptual questions like renaming figures etc are tackled first. 

Also things could be written more clearly than they are now. the english is a little poor and unprofessional, improve. 

You may find ~/Downloads/ad-price-drivers.pdf and ~/Downloads/qjae041.pdf helpful in thiking about predictive models in economics. 

### Checks 
1. Can you ensure that `source/derived/model_prediction` restricts the panel + downstream analysis, for each of `exact1`, `exact2` and `exact1_2`, to the final sample of organizations that have well-defined outcomes across all pre-periods and the sample is equivalent to the event study sample? 
2. For `source/analysis/model_prediction`, when you predict the count of outcomes after the first (like pull requests opened), like reviewed, merged, etc, are you accounting for randomness from the model in the intermediary steps as well? 
3. There’s several things I don’t understand about the decomposition
    - What are the units - are they percentages?
    - Why can values be negative? Shouldn't they all be "contributing" to some positive percent of the overall error? Or are you saying holding things fixed at the truth in some cases worsens error? Which doesn't make sense.... either.... 
4. Does anything in source/analysis/model_prediction not align with model.tex?
4. Why is Q so large for control orgs in pre-period, LOO???

### Implement improvements
1. Instead of calculating the KS statistic vs the standard normal, you should calculate the KS statistic vs. the following distributino because the standard normal doesn't account for zero lower truncation
    - Randomly draw an org, then randomly draw from the model fit on the org's post-period data (we can call this the true model, or our approximation of the true model). Calculate the z score associated with this draw, across all 5 post-periods (as we typically do). Repeat this like num_draws time across each org so that I have a cross-org distribution of what the actual true distributino f z scores hsould look like. You can also do this for the qs. using the same z-scores. 
    - @CLAUDE: can you a) check whether this is a correct way to approximate the correct "comparison" distribution for the ks statistic when i'm doing cross-org comprisons and zero truncation, and b) rewrite this more clearly. You wuld probably do this for every outcome? 
2. You didn't calculate, in `source/analysis/model_prediction`, the version of error where you try to approximate departure-specific prediction error error by sutracting, within treated groups in the post-period, the treated organization pre-period LOO error (average across Zs/Qs)
3. I'm thinking again about why i care about in-sample pre-period fit. Obviously it's a good check, so maybe it's good to have (not sure...) but like under what scenario or ciumstance do we actually care about it/is it useful? The scenario under which Z/Q is not concentrated around standard normal (adjusted) or chis quared (adjusted) is rare r???


### More Improvements
1. I want a single fit statistic that allows me to choose how to pick a) how i'm calculating things (neg bin, poisson) x (per period, pooled). One option is just to use PRs merged fit. But then I need a statistic summarizing this fit (KS statistic)?
2. As you can tell from the distribution of z's and q's, the model is an ~ok~ fit to the real data (see LOO pre-period fits, control organization post-period). Please see `source/paper/model.tex` for the math underlying the model. How can I improve the model such that it is a better prediction of reality? Please think deeply about this and provide suggestions. Here are some ideas I had but these aren't very good ideas... Please help me think of ways to refine my existing model that keeps it in a simple format... The improvements should also use the fit statistic I adopt throughout to systematzie comparisons/picking best predictive model.
    1. Model churn: other departures and new entrants 
    2. Reduce number of parameters by modeling an “other” member, instead of all members, or incorporate regularization somehow
        1. Although regularization messes up aggregate term - maybe regularization determines who goes into the aggregate?
    3. Use less pre period
3. I want to also be able to use the fit statistic or some other set of statistics to compare the post-period fits for treated and control organizations, to separate departure-specific error from modelling post-period error. I also want to be able tor say whether the model systematically is worse at predicting for treated vs. control orgs.. because validating the model-specific predictions in post-period after removing LOO requires pre-period LOO and post-period control to be similar in error distribution. (Actually I realize u can just compare the z score distribution?)
4. Is there a way to use the error distribution for control post-period, which we know is just model prediction error in post-period, to try and separate quantitatively approximte model prediction error vs. approximate departure model prediction error?
5. Can we think about how much of the variation in each outcome my model explains? 

### OTHER improvements
1. `output/analysis/model_prediction/opened_cohort/fitted` isn't a very clear name - `fitted` should be replaced with something that is more clear about how these are the actual parameters of the model. Also, does it make sense to have `fitted` be a separate outer folder, separate from `negative_binomial` and `poisson`? Wouldn't it make more sense for the paraeter files to be in a separate subfolder of say `output/analysis/model_prediction/opened_cohort/negative_binomial`?

Also, `output/analysis/model_prediction/opened_cohort/poisson/predictions` isn't very clear that it's describing predictive statistics which are different from evaluative graphs... 

2. The minus signs in each of the analysis figures in the summary stats should be made more distinctive somehow, the negative should either be bigger, or there should be some visaul marker to distinguish negative hnumbers.... 
3. The truncated bar count overlaps with the last bar - can we fix the visualization so that there isn't overlap? This applies to all figures in `output/analysis/model_prediction/opened_cohort/poisson`, `output/analysis/model_prediction/opened_cohort/negative_binomial`. 
4. I would like a better way of visualizing the distribution of the Z and Q scores. The axis scales should be the same across all figures in each png... but maybe they should differ for each png? For example, current boundaries don't work for `output/analysis/model_prediction/opened_cohort/poisson/evaluation/pooled/important_degree_top3/exact_1_2/nevertreated/panels/z/pre_period_fit_insample.png`  but do work for `output/analysis/model_prediction/opened_cohort/poisson/evaluation/pooled/important_degree_top3/exact_1_2/nevertreated/panels/z/pre_period_fit_leaveoneout.png`
5. Please combine `output/analysis/model_prediction/opened_cohort/poisson/evaluation/pooled/important_degree_top3/exact_1_2/nevertreated/panels/z/post_period_fit_control.png` and  `output/analysis/model_prediction/opened_cohort/poisson/evaluation/pooled/important_degree_top3/exact_1_2/nevertreated/panels/z/post_period_fit_treated.png`. into one figure with two panels. ALso update model.tex accordingly. You should also delete the old separated files. 
6. Integrate source/derived/model_prediction into DATA_APPENDIX.md
7. Am wondering if for `source/paper/model.tex` main figures i should just use one outcome like pulls merged, and have the other 4 + pulls merged be in an appendix figure. Not sure it makes sense to depict pulls reviewed, opened and merged as 'main' figures. 

### Notes for later
Actually add the figures to the draft and interpret them in the draft
