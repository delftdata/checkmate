#!/bin/bash

experiment=$1
query=$2
protocol=$3
interval=$4
scale_factor=$5
rate=$6
saving_dir=$7
failure=$8
skew=$9

./scripts/deploy_run.sh "$protocol" "$interval" "$scale_factor" "$failure"
sleep 30
if [[ $query == "q1" || $query == "q12-running" ]]; then
    python ./nexmark_queries/"$query"/nexmark-client.py -r "$rate" -bp "$scale_factor" -s "$skew"
elif [[ $query == "q3" || $query == "q8-running" ]]; then
    python ./nexmark_queries/"$query"/nexmark-client.py -r "$rate" -pp "$scale_factor" -ap "$scale_factor" -s "$skew"
fi

echo "generator exited"
mkdir -p "$saving_dir"/"$experiment"/figures
python ./nexmark_queries/"$query"/kafka_input_consumer.py "$saving_dir" "$experiment"
python ./nexmark_queries/"$query"/kafka_output_consumer.py "$saving_dir" "$experiment"
python ./nexmark_queries/"$query"/metrics/calculate_latency.py "$saving_dir" "$experiment"
python ./nexmark_queries/"$query"/metrics/get_99_percentile.py "$saving_dir" "$experiment"
python ./nexmark_queries/"$query"/metrics/get_99_percentile_output.py "$saving_dir" "$experiment"
./scripts/delete_deployment.sh "$experiment" "$saving_dir"