import math

# import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

input_msgs = pd.read_csv('client_requests.csv')
output_msgs = pd.read_csv('output.csv')

joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')
runtime = joined['timestamp_y'] - joined['timestamp_x']

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
throughput = {}
bucket_id = -1

granularity = 1000  # 1 second (ms) (i.e. bucket size)

for t in output_msgs['timestamp']:
    if t - start_time > granularity:
        bucket_id += 1
        start_time = t
        throughput[bucket_id] = 1
    else:
        throughput[bucket_id] += 1

print(throughput)  # HINT: in this example we don't have constant load that's why the spikes

req_ids = output_msgs['request_id']
dup = output_msgs[req_ids.isin(req_ids[req_ids.duplicated()])].sort_values("request_id")

print(f'Number of input messages: {len(input_msgs)}')
print(f'Number of output messages: {len(output_msgs)}')
print(f'Number of duplicate messages: {len(dup)}')
# plt.plot(throughput.keys(), throughput.values())
# plt.show()

if len(dup) > 0:
    print('--------------------')
    print('\nDUPLICATE MESSAGES!\n')
    print('--------------------')
    print(dup)
    print('--------------------')
else:
    print('\nNO DUPLICATE MESSAGES!\n')
