#!/bin/bash

set -m

exec python coordinator/coordinator_service.py -cp $1 -ci $2
