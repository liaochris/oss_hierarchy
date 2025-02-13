// Clear the current Stata session
clear

// Define local macro for covariates
local covlist total_share comments_hhi_avg_wt pct_coop_comments pct_coop_commits_count ///
               min_layer_count problem_disc_hl_contr_ov coding_hl_contr_ov ///
               problem_disc_hl_work coding_hl_work total_HHI problem_discussion_HHI ///
               coding_HHI problem_approval_HHI hhi_comments hhi_commits_count contributors

// Define local macro for bins
local binlist bin_2 //bin_3

// Define local macro for outcomes
local outcomelist prs_opened own_issue_comments helping_issue_comments issues_opened issue_comments pr_comments commits prs_merged closed_issue
//

// Load the dataset
use issue/df_project_departed.dta, clear

// Keep only treated projects
keep if treated_project == 1
// (99,177 observations deleted)

// Encode repository names to generate project IDs
encode repo_name, gen(project_id)

// Change working directory
cd "issue/event_study/did_imp"
// /Volumes/cliao_HD/oss_hierarchy/issue/event_study/did_imp

gen double contributors_temp = cond(treatment == 0 & time_index >= (treatment_group - 4), contributors, .)
bysort repo_name: egen mean_contrib  = mean(contributors_temp)
gen double contributors_norm = contributors / mean_contrib

did_imputation contributors_norm repo_name time_index treatment_group, ///
        autosample horizons(0/2) pretrends(4) ///
        hetby(total_share_bin_2) fe(project_id time_index#total_share_bin_2) minn(0) controls(contributors)
estimates store temp
event_plot temp ., ///
        stub_lag(tau#_0 tau#_1) ///
        default_look ///
        plottype(scatter) ///
        graph_opt(xtitle("Time Periods since Departure") ///
                          ytitle("Qty change in Contributors") ///
                          title("Commits, Split by Total Share") ///
                          xlabel(-4(1)2) ///
                          legend(order(3 "Total Share < mean" 7 "Total Share >= mean")))

						  
/*
* The following block is commented out. To include it in the script, remove the /* and */.

did_imputation commits_100 repo_name time_index treatment_group, ///
        autosample horizons(0/4) pretrends(4) ///
        hetby(total_share_bin_2) fe(project_id time_index#total_share_bin_2) minn(0)
estimates store temp
event_plot temp ., ///
        stub_lag(tau#_0 tau#_1) ///
        default_look ///
        plottype(scatter) ///
        graph_opt(xtitle("Time Periods since Departure") ///
                          ytitle("% change in Commits") ///
                          title("Commits, Split by Total Share") ///
                          xlabel(-4(1)4) ///
                          legend(order(3 "Total Share < mean" 7 "Total Share >= mean")))

did_imputation contributors repo_name time_index treatment_group, ///
        autosample horizons(0/4) pretrends(4) ///
        hetby(total_share_bin_2) fe(project_id time_index#total_share_bin_2) minn(0)
estimates store temp
event_plot temp ., ///
        stub_lag(tau#_0 tau#_1) ///
        default_look ///
        plottype(scatter) ///
        graph_opt(xtitle("Time Periods since Departure") ///
                          ytitle("change in # Contributors") ///
                          title("Contributor Count, Split by Total Share") ///
                          xlabel(-4(1)4) ///
                          legend(order(3 "Total Share < mean" 7 "Total Share >= mean")))

gen double contributors_temp = cond(treatment == 0 & time_index >= (treatment_group - 4), contributors, .)
bysort repo_name: egen mean_contrib = mean(contributors_temp)
bysort repo_name: egen mean_commits_100  = mean(commits_100)

gen double contributors_norm = contributors / mean_contrib
gen double commits_100_norm = commits_100 / mean_commits_100

did_imputation commits_100_norm repo_name time_index treatment_group, ///
        autosample horizons(0/4) pretrends(4) ///
        hetby(total_share_bin_2) fe(project_id time_index#total_share_bin_2) minn(0)
estimates store temp
event_plot temp ., ///
        stub_lag(tau#_0 tau#_1) ///
        default_look ///
        plottype(scatter) ///
        graph_opt(xtitle("Time Periods since Departure") ///
                          ytitle("% change in Commits (project-specific)") ///
                          title("Commits, Split by Total Share") ///
                          xlabel(-4(1)4) ///
                          legend(order(3 "Total Share < mean" 7 "Total Share >= mean")))

did_imputation contributors_norm repo_name time_index treatment_group, ///
        autosample horizons(0/4) pretrends(4) ///
        hetby(total_share_bin_2) fe(project_id time_index#total_share_bin_2) minn(0)
estimates store temp
event_plot temp ., ///
        stub_lag(tau#_0 tau#_1) ///
        default_look ///
        plottype(scatter) ///
        graph_opt(xtitle("Time Periods since Departure") ///
                          ytitle("change in # Contributors (project-specific") ///
                          title("Contributor Count, Split by Total Share") ///
                          xlabel(-4(1)4) ///
                          legend(order(3 "Total Share < mean" 7 "Total Share >= mean")))
*/

// Loop over each outcome in the outcomelist
foreach outcome of local outcomelist {
    
    // Create a directory for the outcome
    //mkdir `outcome'
    
    // Change to the outcome directory
    cd `outcome'
    
    // Generate temporary outcome variable
    gen double `outcome'_temp = cond(treatment == 0 & time_index >= (treatment_group - 4), `outcome', .)
    
    // Calculate mean of the temporary outcome by repo_name
    bysort repo_name: egen mean_`outcome' = mean(`outcome'_temp)
    
    // Normalize the outcome to a percentage
    gen double `outcome'_100 = 100 * `outcome' / mean_`outcome'
    
    // Save the current data to a temporary file
    tempfile og_data
    save `og_data'
    
    // Clear the data from memory
    clear
    
    // Reload the temporary data
    use `og_data'
    
    // Perform Difference-in-Differences imputation
	did_imputation `outcome'_100 repo_name time_index treatment_group, ///
		autosample horizons(0/2) pretrends (4) ///
		fe(project_id time_index) minn(0) controls(contributors)
    
    // Store the estimates
    estimates store standard
    
    // Generate event study plots
    event_plot standard, default_look plottype(scatter) ///
            graph_opt(xtitle("Time Periods since Departure") ///
                      ytitle("% change in `outcome'") ///
                      xlabel(-4(1)2))
    
    // Export the graph
    graph export "`outcome'_es.png", replace
    
    // Loop over each bin in the binlist
    foreach bin of local binlist {
        
        // Create a directory for the bin
        //mkdir `bin'
        
        // Change to the bin directory
        cd `bin'
        
        // Loop over each variable in the covlist
        foreach var of local covlist {
            
            // Clear the data from memory
            clear
            
            // Reload the original data
            use `og_data'
            
            // Drop observations with missing values for the current variable and bin
            drop if missing(`var'_`bin')
            
            // Display a message
            di as text "Now running DID for `var'..."
    
            // Perform Difference-in-Differences imputation with heterogeneity by `var'_`bin'
            if "`var'" != "contributors" {
				did_imputation `outcome'_100 repo_name time_index treatment_group, ///
					autosample horizons(0/2) pretrends(4) ///
					hetby(`var'_`bin') fe(project_id time_index#`var'_`bin') minn(0) controls(contributors)
			} 
            else {
				did_imputation `outcome'_100 repo_name time_index treatment_group, ///
					autosample horizons(0/2) pretrends(4) ///
					hetby(`var'_`bin') fe(project_id time_index#`var'_`bin') minn(0) 
			}
    
				
            // Store the estimates
            estimates store `var'_p
            
            // Generate event study plots
            event_plot `var'_p ., ///
                    stub_lag(tau#_0 tau#_1) ///
                    default_look ///
                    plottype(scatter) ///
                    graph_opt(xtitle("Time Periods since Departure") ///
                              ytitle("% change in `outcome'") ///
                              title("Split by `var'") ///
                              xlabel(-4(1)2) ///
                              legend(order(3 "`var' < mean" 7 "`var' >= mean")))
            
            // Export the graph
            graph export "`var'.pdf", replace
            
            // If the variable is not "total_share", perform additional analyses
            if "`var'" != "total_share" {
                
                // Drop observations with missing total_share for the current bin
                drop if missing(total_share_`bin')
                
                // Sort the data by total_share and the current variable
                sort total_share_`bin' `var'_`bin'
                
                // Create a grouped variable based on total_share and the current variable
                egen `var'_s = group(total_share_`bin' `var'_`bin')
                
                // Save the grouped data to a temporary file
                tempfile dt
                save `dt'
                
                // Contract the data to unique groups
                contract `var'_s total_share_`bin' `var'_`bin'
                
                // List the contracted data (abbreviated)
                list, abbrev(32)
                
                // Clear the data from memory
                clear
                
                // Reload the contracted data
                use `dt'
                
				if "`var'" != "contributors" {
					did_imputation `outcome'_100 repo_name time_index treatment_group, ///
                        autosample horizons(0/2) pretrends(4) ///
                        hetby(`var'_s) fe(project_id time_index#`var'_s) minn(0) controls(contributors)
				} 
				else {
					did_imputation `outcome'_100 repo_name time_index treatment_group, ///
                        autosample horizons(0/2) pretrends(4) ///
                        hetby(`var'_s) fe(project_id time_index#`var'_s) minn(0)
				}
                // Store the estimates
                estimates store `var'_sp
                
                // Generate event study plots for the grouped variable
                event_plot `var'_sp . . ., ///
                        stub_lag(tau#_1 tau#_2 tau#_3 tau#_4) ///
                        default_look ///
                        plottype(scatter) ///
                        graph_opt(xtitle("Time Periods since Departure") ///
                                  ytitle("% change in `outcome'") ///
                                  title("Split by `var'_share") ///
                                  xlabel(-4(1)2) ///
                                  legend(order(3 "split < mean ts<mean" 7 "split >= mean ts<mean" ///
                                               11 "split < mean ts>=mean" 15 "split >= mean ts>=mean")))
                
                // Export the grouped graph
                graph export "`var'_sp.pdf", replace
                
                // Drop the grouped variable to clean up
                drop `var'_s
            }
        }
        
        // Return to the parent directory
        cd ../
    }
    
    // Return to the main directory
    cd ../
}
cd ../../../
