#!/bin/bash

./scripts/deploy_run.sh COR 4 4 true
sleep 5
python consistency-checks/consistency-check-count/consistency-demo.py

echo "look at logs"