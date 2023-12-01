import json
import sys
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

saving_dir = sys.argv[1]
experiment_name = sys.argv[2]

input_msgs = pd.read_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-input.csv')
output_msgs = pd.read_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-output.csv')
experiment_length = 60 # in seconds

joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')
responded = joined.dropna().sort_values('timestamp_x').reset_index(drop=True)
responded = responded[responded['timestamp_x'] > (30000 + responded['timestamp_x'][0])].reset_index(drop=True)

responded = responded.sort_values('timestamp_y').reset_index(drop=True)
start_time = -math.inf

latency_buckets = {}
bucket_id = -1
granularity = 1000  # 1 second (ms) (i.e. bucket size)
num_of_buckets = int((responded['timestamp_y'].iloc[-1] - responded['timestamp_y'].iloc[0])/granularity) + 10
print(num_of_buckets)
for i in range(num_of_buckets):
    latency_buckets[i] = {}
    if i == 0:
        latency_buckets[i]['bound'] = responded['timestamp_y'][0]
    else:
        latency_buckets[i]['bound'] = latency_buckets[i-1]['bound'] + granularity
    latency_buckets[i]['items'] = []    

for idx, t in enumerate(responded['timestamp_y']):
    for i in latency_buckets.keys():
        if t < latency_buckets[i]['bound']:
            latency_buckets[i]['items'].append(responded['timestamp_y'][idx] - responded['timestamp_x'][idx])
            break

# print(latency_buckets)

latency_buckets_99: dict[int, float] = {k*100: np.percentile(v['items'], 50) for k, v in latency_buckets.items() if v['items'] != []}
# latency_buckets_50: dict[int, float] = {k*100: np.percentile(v['items'], 50) for k, v in latency_buckets.items() if v['items'] != []}

with open(f"{saving_dir}/{experiment_name}/{experiment_name}-50p-output.csv", "w") as fp:
    json.dump(latency_buckets_99, fp, indent=4)
