import sys
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt


protocol = sys.argv[1]

input_msgs = pd.read_csv(f'./results/q1/{protocol}-input.csv')
output_msgs = pd.read_csv(f'./results/q1/{protocol}-output.csv')
experiment_length= 60 # in seconds

joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')
# runtime = joined['timestamp_y'] - joined['timestamp_x']

joined_sorted = joined.sort_values('timestamp_x').reset_index()
joined_sorted = joined_sorted[joined_sorted['timestamp_x'] > (30000 + joined_sorted['timestamp_x'][0])].reset_index()
runtime = joined_sorted['timestamp_y'] - joined_sorted['timestamp_x']

runtime_no_nan = runtime.dropna()
print(f'min latency: {min(runtime_no_nan)}ms')
print(f'max latency: {max(runtime_no_nan)}ms')
print(f'average latency: {np.average(runtime_no_nan)}ms')
print(f'99%: {np.percentile(runtime_no_nan, 99)}ms')
print(f'95%: {np.percentile(runtime_no_nan, 95)}ms')
print(f'90%: {np.percentile(runtime_no_nan, 90)}ms')
print(f'75%: {np.percentile(runtime_no_nan, 75)}ms')
print(f'60%: {np.percentile(runtime_no_nan, 60)}ms')
print(f'50%: {np.percentile(runtime_no_nan, 50)}ms')
print(f'25%: {np.percentile(runtime_no_nan, 25)}ms')
print(f'10%: {np.percentile(runtime_no_nan, 10)}ms')
print(np.argmax(runtime_no_nan))
print(np.argmin(runtime_no_nan))

missed = joined[joined['response'].isna()]

if len(missed) > 0:
    print('--------------------')
    print('\nMISSED MESSAGES!\n')
    print('--------------------')
    print(missed)
    print('--------------------')
else:
    print('\nNO MISSED MESSAGES!\n')


start_time = -math.inf
latency_buckets = {}
bucket_id = -1

granularity = 100  # 1 second (ms) (i.e. bucket size)

num_of_buckets = int(experiment_length*1000/granularity) + 1
print(num_of_buckets)
for i in range(num_of_buckets):
    latency_buckets[i] = {}
    if i == 0:
        latency_buckets[i]['bound'] = joined_sorted['timestamp_x'][0]
    else:
        latency_buckets[i]['bound'] = latency_buckets[i-1]['bound'] + granularity
    latency_buckets[i]['items'] = []    

for idx, t in enumerate(joined_sorted['timestamp_x']):
    for i in latency_buckets.keys():
        if t < latency_buckets[i]['bound']:
            latency_buckets[i]['items'].append(joined_sorted['timestamp_y'][idx] - joined_sorted['timestamp_x'][idx])
            break

latency_buckets_99: dict[int, float] = {k*100: np.percentile(v['items'], 99) for k, v in latency_buckets.items() if v['items'] != []}
latency_buckets_50: dict[int, float] = {k*100: np.percentile(v['items'], 50) for k, v in latency_buckets.items() if v['items'] != []}

_, ax = plt.subplots()
ax.plot(latency_buckets_99.keys(), latency_buckets_99.values(), linewidth=2.5, label='99p')
ax.plot(latency_buckets_50.keys(), latency_buckets_50.values(), linewidth=2.5, label='50p')
ax.set_xlabel('Time (ms)')
ax.set_ylabel('Latency (ms)')
handles, labels = plt.gca().get_legend_handles_labels()
order = [1,0]

ax.legend([handles[idx] for idx in order],[labels[idx] for idx in order],bbox_to_anchor=(0.5, -0.2), loc="center", ncol=2)
ax.set_title(f"NexMark Q1 - {protocol}")
plt.tight_layout()
# plt.show()
plt.savefig(f'results/q1/figures/{protocol}')