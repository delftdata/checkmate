import json
import sys
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

plt.rcParams.update({'font.size': 18})

saving_dir = sys.argv[1]

experiment_name_1 = sys.argv[2]
experiment_name_2 = sys.argv[3]
experiment_name_3 = sys.argv[4]
experiment_name_4 = sys.argv[5]

fp_1 = open(f"{saving_dir}/{experiment_name_1}/{experiment_name_1}-99p.csv", "r")
fp_2 = open(f"{saving_dir}/{experiment_name_2}/{experiment_name_2}-99p.csv", "r")
fp_3 = open(f"{saving_dir}/{experiment_name_3}/{experiment_name_3}-99p.csv", "r")
fp_4 = open(f"{saving_dir}/{experiment_name_4}/{experiment_name_4}-99p.csv", "r")

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

_, ax = plt.subplots()
ax.plot(p99_1.keys(), p99_1.values(), linewidth=1.5, linestyle="--", marker="o", markevery=(0.2,0.2), markersize=4, color="orchid", label=f'{experiment_name_1.split("-")[0]}')
ax.plot(p99_2.keys(), p99_2.values(), linewidth=1.5, linestyle="-.", marker="v", markevery=(0.2,0.2), markersize=4, color="sandybrown", label=f'{experiment_name_2.split("-")[0]}')
ax.plot(p99_3.keys(), p99_3.values(), linewidth=1.5, linestyle="-", marker="s", markevery=(0.2,0.2), markersize=4, color="slateblue", label=f'{experiment_name_3.split("-")[0]}')
ax.plot(p99_4.keys(), p99_4.values(), linewidth=1.5, linestyle=":", marker="D", markevery=(0.2,0.2), markersize=4, color="cornflowerblue", label=f'{experiment_name_4.split("-")[0]}')

ax.set_xlabel('Time (ms)', fontweight="bold")
ax.set_ylabel('Latency (ms)', fontweight="bold")
# ax.set_xticks([10000, 20000, 30000, 40000, 50000, 60000])
handles, labels = plt.gca().get_legend_handles_labels()
order = [0, 1, 2, 3]

ax.legend([handles[idx] for idx in order],[labels[idx] for idx in order],bbox_to_anchor=(0.5, -0.4), loc="center", ncol=4)
ax.set_title("NexMark Q1 - 99th percentile")
plt.tight_layout()
# plt.show()
plt.savefig(f'{saving_dir}/{experiment_name_1}/figures/combined-99th.pdf')