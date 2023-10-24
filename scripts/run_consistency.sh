#!/bin/bash

./scripts/deploy_run.sh COR 4 4
sleep 5
python consistency-check/consistency-demo.py 

echo "look at logs"