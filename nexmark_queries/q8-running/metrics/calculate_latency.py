import json
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from universalis.common.networking import NetworkingManager


networking = NetworkingManager()

input_msgs = pd.read_csv('./results/q3/input.csv').sort_values('timestamp').reset_index()
output_msgs = pd.read_csv('./results/q3/output.csv').sort_values('timestamp').reset_index()
experiment_length = 60 # in seconds

# print(input_msgs[['request','timestamp']])
# print(output_msgs['response'].iloc[0])

input_currated = {'request': [], 'timestamp': []}
for index, row in input_msgs[['request','timestamp']].iterrows():
    req_dict = eval(row['request'])
    item_id = req_dict['__MSG__']['__PARAMS__'][0]
    if len(req_dict['__MSG__']['__PARAMS__']) == 10:
        input_currated['request'].append(str(item_id)+'_a')
    else:
        input_currated['request'].append(str(item_id)+'_p')
    input_currated['timestamp'].append(row['timestamp'])

input_currated_df = pd.DataFrame.from_dict(input_currated).sort_values('timestamp').reset_index()


output_currated = {"response": [], "timestamp_start": [], "timestamp_end": []}
for index, row in output_msgs[['response','timestamp']].iterrows():
    resp_list = eval(row['response'])
    if len(resp_list[0]) == 10:
        joined_list = [str(resp_list[0][0])+'_a', str(resp_list[1][0])+'_p']
    else:
        joined_list = [str(resp_list[0][0])+'_p', str(resp_list[1][0])+'_a']
    output_currated['response'].append(joined_list)
    joined_list_df = input_currated_df.loc[input_currated_df['request'].isin(joined_list)].reset_index(drop=True)
    if joined_list_df.iloc[0,2] >= joined_list_df.iloc[1,2]:
        output_currated['timestamp_start'].append(joined_list_df.iloc[0,2])
    else:
        output_currated['timestamp_start'].append(joined_list_df.iloc[1,2])
  
    output_currated['timestamp_end'].append(row['timestamp'])

output_currated_df = pd.DataFrame.from_dict(output_currated).sort_values('timestamp_start').reset_index()
# print(output_currated_df)

latency_buckets = {}
bucket_id = -1
granularity = 100  # 1 second (ms) (i.e. bucket size)
num_of_buckets = int(experiment_length*1000/granularity) + 1
print(num_of_buckets)
for i in range(num_of_buckets):
    latency_buckets[i] = {}
    if i == 0:
        latency_buckets[i]['bound'] = input_currated_df['timestamp'][0]
    else:
        latency_buckets[i]['bound'] = latency_buckets[i-1]['bound'] + granularity
    latency_buckets[i]['items'] = []    

for idx, row in output_currated_df.iterrows():  
    for i in latency_buckets.keys():
        if row['timestamp_start'] < latency_buckets[i]['bound']:
            latency_buckets[i]['items'].append(row['timestamp_end'] - row['timestamp_start'])
            break

# print(latency_buckets)

# joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')
# responded = joined.dropna().sort_values('timestamp_x').reset_index()
runtime = []
for i in latency_buckets.keys():
    runtime.extend(latency_buckets[i]['items'])
# print(responded)

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

# # missed = joined[joined['response'].isna()]
# # print(missed)


latency_buckets_99: dict[int, float] = {k*100: np.percentile(v['items'], 99) for k, v in latency_buckets.items() if v['items'] != []}
latency_buckets_50: dict[int, float] = {k*100: np.percentile(v['items'], 50) for k, v in latency_buckets.items() if v['items'] != []}

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
ax.set_title("NexMark Q8 - No checkpointing")
plt.tight_layout()
plt.show()
