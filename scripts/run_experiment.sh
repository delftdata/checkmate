experiment=$1
query=$2
protocol=$3
interval=$4
scale_factor=$5
rate=$6
saving_dir=$7

./scripts/deploy_run.sh $protocol $interval $scale_factor
sleep 30
if [[ $query == "q1" || $query == "q12-running" ]]; then
    python ./nexmark_queries/$query/nexmark-client.py -r $rate -bp $scale_factor
elif [[ $query == "q3" || $query == "q8-running" ]]; then
    python ./nexmark_queries/$query/nexmark-client.py -r $rate -pp $scale_factor -ap $scale_factor
fi

echo "generator exited"
mkdir -p $saving_dir/$experiment/figures
python ./nexmark_queries/$query/kafka_input_consumer.py $saving_dir $experiment
python ./nexmark_queries/$query/kafka_output_consumer.py $saving_dir $experiment
sleep 10
./scripts/delete_deployment.sh $experiment $saving_dir