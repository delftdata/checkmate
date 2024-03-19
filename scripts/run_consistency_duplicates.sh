#!/bin/bash

./scripts/deploy_run.sh COR 4 4 true
sleep 5
python consistency-checks/consistency-check-set-duplicates/consistency-demo-set-duplicate.py

echo "look at logs"