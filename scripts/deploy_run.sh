#!/bin/bash

protocol=$1
interval=$2
scale_factor=$3
failure=$4

docker compose -f docker-compose-kafka.yml up -d
sleep 5
docker compose -f docker-compose-simple-minio.yml up -d
sleep 5
if [[ $failure = "true" || $failure = "True" ]]; then
  echo "Failure is $failure"
  docker compose build --build-arg protocol="$protocol" --build-arg interval="$interval" --build-arg failure="-f" \
                    --build-arg scale_factor="$scale_factor"
else
  docker compose build --build-arg protocol="$protocol" --build-arg interval="$interval" \
                    --build-arg scale_factor="$scale_factor"
fi
docker compose up --scale worker="$scale_factor" -d