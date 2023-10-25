#!/bin/bash

input=$1
saving_dir=$2

while IFS= read -r line
do
  printf 'Run experiment: %s\n' "$line"
  IFS=' ' read -ra ss <<< "$line"
  exp_name="${ss[0]}"
  query="${ss[1]}"
  protocol="${ss[2]}"
  interval="${ss[3]}"
  scale_factor="${ss[4]}"
  rate="${ss[5]}"
  failure="${ss[6]}"

  ./scripts/run_experiment.sh "$exp_name" "$query" "$protocol" "$interval" "$scale_factor" "$rate" "$saving_dir" \
                              "$failure"

done < "$input"