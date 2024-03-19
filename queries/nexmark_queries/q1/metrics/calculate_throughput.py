import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt


experiment_length = 60 # in seconds
input_msgs = pd.read_csv('./results/q1/input.csv')
output_msgs = pd.read_csv('./results/q1/output.csv')
output_msgs_sorted = output_msgs.sort_values('timestamp')


start_time = -math.inf
throughput_buckets = {}
bucket_id = -1

granularity = 1000  # 1 second (ms) (i.e. bucket size)

num_of_buckets = int(experiment_length*1000/granularity) + 1
print(num_of_buckets)
for i in range(num_of_buckets):
    throughput_buckets[i] = {}
    if i == 0:
        throughput_buckets[i]['bound'] = output_msgs_sorted['timestamp'][0]
    else:
        throughput_buckets[i]['bound'] = throughput_buckets[i-1]['bound'] + granularity
    throughput_buckets[i]['items'] = 0

for idx, t in enumerate(output_msgs_sorted['timestamp']):
    for i in throughput_buckets.keys():
        if t < throughput_buckets[i]['bound']:
            throughput_buckets[i]['items'] += 1
            break


throughput_values = [v['items'] for v in throughput_buckets.values()]
_, ax = plt.subplots()
ax.plot(throughput_buckets.keys(), throughput_values, linewidth=2.5, label='output_throughput')
ax.set_xlabel('Time (s)')
ax.set_ylabel('Throughput (records/s)')
ax.legend(bbox_to_anchor=(0.5, -0.2), loc="center", ncol=2)
ax.set_title("NexMark Q1 - Uncoordinated")
plt.tight_layout()
plt.show()