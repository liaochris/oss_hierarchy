#!/usr/bin/env bash
#
# Usage:
#   ./check_weights_and_rerun.sh <g> <half> <mode>
#
# Example:
#   ./check_weights_and_rerun.sh 5 firsthalf print
#     -> Excludes t=4 (since 5-1=4), checks the first 8 t-values,
#        and only prints missing files without running the R script.
#
# Args:
#   <g>    : integer group value (e.g., 5)
#   <half> : "firsthalf" or "secondhalf"
#   <mode> : "print" (just list missing t-values) or "run" (invoke R script)

###############################################################################
# 1) Collect inputs
###############################################################################

g="$1"
half="$2"
mode="$3"

if [ -z "$g" ] || [ -z "$half" ] || [ -z "$mode" ]; then
  echo "Error: Missing one or more arguments."
  echo "Usage: $0 <g> <half> <mode>"
  echo "  <g>    : integer"
  echo "  <half> : 'firsthalf' or 'secondhalf'"
  echo "  <mode> : 'print' or 'run'"
  exit 1
fi

###############################################################################
# 2) Define the full set of t-values, then exclude (g - 1) if in [1..17].
###############################################################################

all_ts=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17)
exclude_t=$(( g - 1 ))

# Build t_values by excluding "exclude_t" if it's in [1..17].
t_values=()
for t in "${all_ts[@]}"; do
  if [ "$t" -ne "$exclude_t" ]; then
    t_values+=("$t")
  fi
done

# After excluding, we expect exactly 16 t-values if (g-1) was within [1..17].
if [ "${#t_values[@]}" -ne 16 ]; then
  echo "Error: After excluding t=$exclude_t, we have ${#t_values[@]} t-values, not 16."
  echo "Cannot split them evenly into firsthalf and secondhalf."
  echo "Check that g is in [2..18] so (g-1) is between 1 and 17."
  exit 1
fi

###############################################################################
# 3) Split into firsthalf and secondhalf (8 each).
###############################################################################

# Sort t_values in ascending order
sorted_ts=($(printf '%s\n' "${t_values[@]}" | sort -n))

# firsthalf = first 8, secondhalf = last 8
first_half=("${sorted_ts[@]:0:8}")
second_half=("${sorted_ts[@]:8:8}")

# Pick which set of t-values we actually want
if [ "$half" == "firsthalf" ]; then
  selected_ts=("${first_half[@]}")
elif [ "$half" == "secondhalf" ]; then
  selected_ts=("${second_half[@]}")
else
  echo "Error: <half> must be 'firsthalf' or 'secondhalf'."
  exit 1
fi

###############################################################################
# 4) Check for missing files in drive/output/analysis/hte/weights
###############################################################################

weights_dir="drive/output/analysis/hte/weights"
r_script="issue/causal_forest_event_study.R"  # <-- Update path as needed

missing_ts=()

for t in "${selected_ts[@]}"; do
  # We look for at least one file matching g<g>_t<t>*
  if ! ls "${weights_dir}/g${g}_t${t}"* 1>/dev/null 2>&1; then
    missing_ts+=( "$t" )
  fi
done

###############################################################################
# 5) Handle missing files
###############################################################################

if [ "${#missing_ts[@]}" -eq 0 ]; then
  echo "[`date`] All files found for g=$g, half=$half. Nothing to do."
  exit 0
fi

case "$mode" in
  print)
    echo "[`date`] Missing t-values for g=$g ($half): ${missing_ts[*]}"
    ;;
  run)
    echo "[`date`] Missing t-values for g=$g ($half): ${missing_ts[*]}"
    for t in "${missing_ts[@]}"; do
      echo "[`date`] --> Running R script for g=$g, t=$t ..."
      Rscript "$r_script" "$g" "$t"
    done
    ;;
  *)
    echo "Error: <mode> must be 'print' or 'run'."
    exit 1
    ;;
esac

