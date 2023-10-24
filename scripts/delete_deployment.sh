#!/bin/bash

experiment=$1
saving_dir=$2

experiment_folder=$saving_dir/$experiment

docker cp checkpointing-coordinator-1:/usr/local/universalis/metrics.txt $experiment_folder/$experiment-metrics.txt

docker compose logs &> $experiment_folder/$experiment-styx-logs.log
docker compose -f docker-compose-simple-minio.yml logs &> $experiment_folder/$experiment-minio-logs.log
docker compose -f docker-compose-kafka.yml logs &> $experiment_folder/$experiment-kafka-logs.log

docker compose down --volumes
docker compose -f docker-compose-kafka.yml down --volumes
docker compose -f docker-compose-simple-minio.yml down --volumes