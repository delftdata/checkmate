import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt


input_msgs = pd.read_csv('./results/q1/input.csv')
output_msgs = pd.read_csv('./results/q1/output.csv')
output_msgs_sorted = output_msgs.sort_values('timestamp')

start_time = -math.inf
throughput = {}
bucket_id = -1

granularity = 1000  # 1 second (ms) (i.e. bucket size)

for t in output_msgs_sorted['timestamp']:
    if t - start_time > granularity:
        bucket_id += 1
        start_time = t
        throughput[bucket_id] = 1
    else:
        throughput[bucket_id] += 1
print(throughput)

_, ax = plt.subplots()
ax.plot(throughput.keys(), throughput.values(), linewidth=2.5, label='99p')
ax.set_xlabel('Time (s)')
ax.set_ylabel('Throughput (records/s)')
ax.legend(bbox_to_anchor=(0.5, -0.2), loc="center", ncol=2)
ax.set_title("NexMark Q1 - Uncoordinated")
plt.tight_layout()
plt.show()