import json
import sys
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

saving_dir = sys.argv[1]
experiment_name = sys.argv[2]
# protocol = sys.argv[3]

input_msgs = pd.read_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-input.csv')
output_msgs = pd.read_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-output.csv')
experiment_length = 60 # in seconds

joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')
responded = joined.dropna().sort_values('timestamp_x').reset_index()
responded = responded[responded['timestamp_x'] > (30000 + responded['timestamp_x'][0])].reset_index()

runtime = responded['timestamp_y'] - responded['timestamp_x']
print(responded)

print(f'min latency: {min(runtime)}ms')
print(f'max latency: {max(runtime)}ms')
print(f'average latency: {np.average(runtime)}ms')
print(f'99%: {np.percentile(runtime, 99)}ms')
print(f'95%: {np.percentile(runtime, 95)}ms')
print(f'90%: {np.percentile(runtime, 90)}ms')
print(f'75%: {np.percentile(runtime, 75)}ms')
print(f'60%: {np.percentile(runtime, 60)}ms')
print(f'50%: {np.percentile(runtime, 50)}ms')
print(f'25%: {np.percentile(runtime, 25)}ms')
print(f'10%: {np.percentile(runtime, 10)}ms')
print(np.argmax(runtime))
print(np.argmin(runtime))

input_sorted = input_msgs.sort_values("timestamp").reset_index() 
output_sorted = output_msgs.sort_values("timestamp").reset_index() 
total_messages = len(input_msgs.index)
total_time = (output_sorted["timestamp"].iloc[-1] - input_sorted["timestamp"].iloc[0])/1_000
print(f'average_throughput: {total_messages/total_time}')


# missed = joined[joined['response'].isna()]
# print(missed)


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

latency_buckets_99: dict[int, float] = {k: np.percentile(v['items'], 99) for k, v in latency_buckets.items() if v['items'] != []}
latency_buckets_50: dict[int, float] = {k: np.percentile(v['items'], 50) for k, v in latency_buckets.items() if v['items'] != []}

# print(latency_buckets_50)
# print(latency_buckets_99)

_, ax = plt.subplots()
ax.plot(latency_buckets_99.keys(), latency_buckets_99.values(), linewidth=2.5, label='99p')
ax.plot(latency_buckets_50.keys(), latency_buckets_50.values(), linewidth=2.5, label='50p')
ax.set_xlabel('Time (ms)')
ax.set_ylabel('Latency (ms)')
handles, labels = plt.gca().get_legend_handles_labels()
order = [1,0]

ax.legend([handles[idx] for idx in order],[labels[idx] for idx in order],bbox_to_anchor=(0.5, -0.2), loc="center", ncol=2)
ax.set_title(f"Reachability Query - {experiment_name}")
plt.tight_layout()
# plt.show()
plt.savefig(f'{saving_dir}/{experiment_name}/figures/{experiment_name}.pdf')
