import json
import sys
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

# plt.rcParams.update({'font.size': 18})

protocol_1 = sys.argv[1]
protocol_2 = sys.argv[2]
protocol_3 = sys.argv[3]
protocol_4 = sys.argv[4]

fp_1 = open(f"./results/q3/{protocol_1}-99p.csv", "r")
fp_2 = open(f"./results/q3/{protocol_2}-99p.csv", "r")
fp_3 = open(f"./results/q3/{protocol_3}-99p.csv", "r")
fp_4 = open(f"./results/q3/{protocol_4}-99p.csv", "r")

p99_1 = json.load(fp_1)
p99_2 = json.load(fp_2)
p99_3 = json.load(fp_3)
p99_4 = json.load(fp_4)

fp_1.close()
fp_2.close()
fp_3.close()
fp_4.close()

p99_1 = {int(k): v for k, v in p99_1.items()}
p99_2 = {int(k): v for k, v in p99_2.items()}
p99_3 = {int(k): v for k, v in p99_3.items()}
p99_4 = {int(k): v for k, v in p99_4.items()}

experiment_length= 60 # in seconds

# joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')
# runtime = joined['timestamp_y'] - joined['timestamp_x']

# joined_sorted = joined.sort_values('timestamp_x').reset_index()
# joined_sorted = joined_sorted[joined_sorted['timestamp_x'] > (30000 + joined_sorted['timestamp_x'][0])].reset_index()
# runtime = joined_sorted['timestamp_y'] - joined_sorted['timestamp_x']

# runtime_no_nan = runtime.dropna()
# print(f'min latency: {min(runtime_no_nan)}ms')
# print(f'max latency: {max(runtime_no_nan)}ms')
# print(f'average latency: {np.average(runtime_no_nan)}ms')
# print(f'99%: {np.percentile(runtime_no_nan, 99)}ms')
# print(f'95%: {np.percentile(runtime_no_nan, 95)}ms')
# print(f'90%: {np.percentile(runtime_no_nan, 90)}ms')
# print(f'75%: {np.percentile(runtime_no_nan, 75)}ms')
# print(f'60%: {np.percentile(runtime_no_nan, 60)}ms')
# print(f'50%: {np.percentile(runtime_no_nan, 50)}ms')
# print(f'25%: {np.percentile(runtime_no_nan, 25)}ms')
# print(f'10%: {np.percentile(runtime_no_nan, 10)}ms')
# print(np.argmax(runtime_no_nan))
# print(np.argmin(runtime_no_nan))

# missed = joined[joined['response'].isna()]

# if len(missed) > 0:
#     print('--------------------')
#     print('\nMISSED MESSAGES!\n')
#     print('--------------------')
#     print(missed)
#     print('--------------------')
# else:
#     print('\nNO MISSED MESSAGES!\n')


# start_time = -math.inf
# latency_buckets = {}
# bucket_id = -1

# granularity = 100  # 1 second (ms) (i.e. bucket size)

# num_of_buckets = int(experiment_length*1000/granularity) + 1
# print(num_of_buckets)
# for i in range(num_of_buckets):
#     latency_buckets[i] = {}
#     if i == 0:
#         latency_buckets[i]['bound'] = joined_sorted['timestamp_x'][0]
#     else:
#         latency_buckets[i]['bound'] = latency_buckets[i-1]['bound'] + granularity
#     latency_buckets[i]['items'] = []    

# for idx, t in enumerate(joined_sorted['timestamp_x']):
#     for i in latency_buckets.keys():
#         if t < latency_buckets[i]['bound']:
#             latency_buckets[i]['items'].append(joined_sorted['timestamp_y'][idx] - joined_sorted['timestamp_x'][idx])
#             break

# latency_buckets_99: dict[int, float] = {k*100: np.percentile(v['items'], 99) for k, v in latency_buckets.items() if v['items'] != []}
# latency_buckets_50: dict[int, float] = {k*100: np.percentile(v['items'], 50) for k, v in latency_buckets.items() if v['items'] != []}

_, ax = plt.subplots()
ax.plot(p99_1.keys(), p99_1.values(), linewidth=1.5, linestyle="--", marker="o", markevery=(0.2,0.2), markersize=4, color="orchid", label=f'{protocol_1.split("-")[0]}')
ax.plot(p99_2.keys(), p99_2.values(), linewidth=1.5, linestyle="-.", marker="v", markevery=(0.2,0.2), markersize=4, color="sandybrown", label=f'{protocol_2.split("-")[0]}')
ax.plot(p99_3.keys(), p99_3.values(), linewidth=1.5, linestyle="-", marker="s", markevery=(0.2,0.2), markersize=4, color="slateblue", label=f'{protocol_3.split("-")[0]}')
ax.plot(p99_4.keys(), p99_4.values(), linewidth=1.5, linestyle=":", marker="D", markevery=(0.2,0.2), markersize=4, color="cornflowerblue", label=f'{protocol_4.split("-")[0]}')

ax.set_xlabel('Time (ms)', fontweight="bold")
ax.set_ylabel('Latency (ms)', fontweight="bold")
# ax.set_xticks([10000, 20000, 30000, 40000, 50000, 60000])
handles, labels = plt.gca().get_legend_handles_labels()
order = [0, 1, 2, 3]

ax.legend([handles[idx] for idx in order],[labels[idx] for idx in order],bbox_to_anchor=(0.5, -0.2), loc="center", ncol=4)
ax.set_title("NexMark Q3 - 99th percentile")
plt.tight_layout()
# plt.show()
plt.savefig('results/q3/figures/combined-99th.pdf')