import json
import sys
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

plt.rcParams.update({'font.size': 18})

saving_dir = sys.argv[1]

protocol_names = ["NOC", "UNC", "COR", "CIC"]
# queries = ["q1", "q3", "q8r", "q12r"]
queries = ["q3", "q8r", "q12r"]
# query_names = ["Q1", "Q3", "Q8", "Q12"]
query_names = ["Q3", "Q8", "Q12"]


# query_settings = {
#     "q1": ["NOC-q1-10w-5si-12000r-1f", "UNC-q1-10w-5si-12000r-1f", "COR-q1-10w-5si-12000r-1f", "CIC-q1-10w-5si-12000r-1f"],
#     "q3": ["NOC-q3-10w-5si-12000r-1f", "UNC-q3-10w-5si-12000r-1f", "COR-q3-10w-5si-12000r-1f", "CIC-q3-10w-5si-12000r-1f"],
#     "q8r": ["NOC-q8-10w-5si-12000r-1f", "UNC-q8-10w-5si-12000r-1f", "COR-q8-10w-5si-12000r-1f", "CIC-q8-10w-5si-12000r-1f"],
#     "q12r": ["NOC-q12-10w-5si-12000r-1f", "UNC-q12-10w-5si-12000r-1f", "COR-q12-10w-5si-12000r-1f", "CIC-q12-10w-5si-12000r-1f"]
# }
# query_settings = {
#     "q1": ["NOC-q1-30w-5si-36000r-1f", "UNC-q1-30w-5si-36000r-1f", "COR-q1-30w-5si-36000r-1f", "CIC-q1-30w-5si-36000r-1f"],
#     "q3": ["NOC-q3-30w-5si-36000r-1f", "UNC-q3-30w-5si-36000r-1f", "COR-q3-30w-5si-36000r-1f", "CIC-q3-30w-5si-36000r-1f"],
#     "q8r": ["NOC-q8-30w-5si-36000r-1f", "UNC-q8-30w-5si-36000r-1f", "COR-q8-30w-5si-36000r-1f", "CIC-q8-30w-5si-36000r-1f"],
#     "q12r": ["NOC-q12-30w-5si-36000r-1f", "UNC-q12-30w-5si-36000r-1f", "COR-q12-30w-5si-36000r-1f", "CIC-q12-30w-5si-36000r-1f"]
# }

# query_settings = {
#     "q1": ["NOC-q1-50w-5si-60000r-1f", "UNC-q1-50w-5si-60000r-1f", "COR-q1-50w-5si-60000r-1f", "CIC-q1-50w-5si-60000r-1f"],
#     "q3": ["NOC-q3-50w-5si-60000r-1f", "UNC-q3-50w-5si-60000r-1f", "COR-q3-50w-5si-60000r-1f", "CIC-q3-50w-5si-60000r-1f"],
#     "q8r": ["NOC-q8-50w-5si-60000r-1f", "UNC-q8-50w-5si-60000r-1f", "COR-q8-50w-5si-60000r-1f", "CIC-q8-50w-5si-60000r-1f"],
#     "q12r": ["NOC-q12-50w-5si-60000r-1f", "UNC-q12-50w-5si-60000r-1f", "COR-q12-50w-5si-60000r-1f", "CIC-q12-50w-5si-60000r-1f"]
# }

# query_settings = {
#     "q1": ["NOC-q1-50w-5si-60000r-1f", "UNC-q1-50w-5si-60000r-1f", "COR-q1-50w-5si-60000r-1f", "CIC-q1-50w-5si-60000r-1f"],
#     "q3": ["NOC-q3-50w-5si-60000r-1f", "UNC-q3-50w-5si-60000r-1f", "COR-q3-50w-5si-60000r-1f", "CIC-q3-50w-5si-60000r-1f"],
#     "q8r": ["NOC-q8-50w-5si-60000r-1f", "UNC-q8-50w-5si-60000r-1f", "COR-q8-50w-5si-60000r-1f", "CIC-q8-50w-5si-60000r-1f"],
#     "q12r": ["NOC-q12-50w-5si-60000r-1f", "UNC-q12-50w-5si-60000r-1f", "COR-q12-50w-5si-60000r-1f", "CIC-q12-50w-5si-60000r-1f"]
# }

query_settings = {
    "q3": ["NOC-q3-10w-5si-4500r-nf-skew", "UNC-q3-10w-5si-4500r-nf-skew", "COR-q3-10w-5si-4500r-nf-skew", "CIC-q3-10w-5si-4500r-nf-skew"],
    "q8r": ["NOC-q8-10w-5si-4500r-nf-skew", "UNC-q8-10w-5si-4500r-nf-skew", "COR-q8-10w-5si-4500r-nf-skew", "CIC-q8-10w-5si-4500r-nf-skew"],
    "q12r": ["NOC-q12-10w-5si-4500r-nf-skew", "UNC-q12-10w-5si-4500r-nf-skew", "COR-q12-10w-5si-4500r-nf-skew", "CIC-q12-10w-5si-4500r-nf-skew"]
}

fig, ax = plt.subplots(1, 3, figsize=(25, 5))

for idx, q in enumerate(queries):
    # fp_1 = open(f"{saving_dir}/{query_settings[q][0]}/{query_settings[q][0]}-99p.csv", "r")
    fp_2 = open(f"{saving_dir}/{query_settings[q][1]}/{query_settings[q][1]}-99p.csv", "r")
    fp_3 = open(f"{saving_dir}/{query_settings[q][2]}/{query_settings[q][2]}-99p.csv", "r")
    fp_4 = open(f"{saving_dir}/{query_settings[q][3]}/{query_settings[q][3]}-99p.csv", "r")

    # p99_1 = json.load(fp_1)
    p99_2 = json.load(fp_2)
    p99_3 = json.load(fp_3)
    p99_4 = json.load(fp_4)

    # fp_1.close()
    fp_2.close()
    fp_3.close()
    fp_4.close()

    # p99_1 = {int(k): v for k, v in p99_1.items()}
    p99_2 = {int(k): v for k, v in p99_2.items()}
    p99_3 = {int(k): v for k, v in p99_3.items()}
    p99_4 = {int(k): v for k, v in p99_4.items()}

    experiment_length = 60  # in seconds
    # ax[idx].set_yscale("log")
    # ax[idx].axvline(x=18000, color="red", linestyle='--', linewidth=3)
    # ax[idx].plot(p99_1.keys(), p99_1.values(), linewidth=1.5, linestyle="--", marker="o", markevery=(0.2,0.2), markersize=4, color="orchid", label="NOC")
    ax[idx].plot(p99_2.keys(), p99_2.values(), linewidth=1.5, linestyle="-.", marker="v", markevery=(0.2,0.2), markersize=4, color="sandybrown", label="UNC")
    ax[idx].plot(p99_3.keys(), p99_3.values(), linewidth=1.5, linestyle="-", marker="s", markevery=(0.2,0.2), markersize=4, color="slateblue", label="COR")
    ax[idx].plot(p99_4.keys(), p99_4.values(), linewidth=1.5, linestyle=":", marker="D", markevery=(0.2,0.2), markersize=4, color="cornflowerblue", label="CIC")

    ax[idx].set_xlabel('Time (ms)', fontweight="bold")
    ax[idx].set_ylabel('Latency (ms)', fontweight="bold")
    # ax.set_xticks([10000, 20000, 30000, 40000, 50000, 60000])

    # if idx == 1:
    #     # handles, labels = plt.gca().get_legend_handles_labels()
    #     # order = [0, 1, 2, 3]
    #     ax[idx].legend(bbox_to_anchor=(1, -0.4), loc="center", ncol=4)
    ax[idx].set_title(f"NexMark {query_names[idx]}")

handles, labels = plt.gca().get_legend_handles_labels()
fig.legend(handles, labels, bbox_to_anchor=(0.5, 0), loc="center", ncol=4)
fig.tight_layout()
# plt.show()

fig.savefig(f'{saving_dir}/{query_settings["q3"][0]}/figures/combined-all-99th.pdf', bbox_inches='tight')