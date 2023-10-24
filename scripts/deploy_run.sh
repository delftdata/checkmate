#!/bin/bash

protocol=$1
interval=$2
scale_factor=$3

docker compose -f docker-compose-kafka.yml up -d
sleep 5
docker compose -f docker-compose-simple-minio.yml up -d
sleep 5 
docker compose build --build-arg protocol=$protocol --build-arg interval=$interval
docker compose up --scale worker=$scale_factor -d