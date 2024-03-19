import json
import sys
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

plt.rcParams.update({'font.size': 18})

saving_dir = sys.argv[1]

cor_color = "#efc506"
unc_color = "darkturquoise"
cic_color = "crimson"
noc_color = "orchid"

protocol_names = ["NOC", "UNC", "COR", "CIC"]
queries = ["q1", "q3", "q8r", "q12r"]
# queries = ["q3", "q8r", "q12r"]

query_names = ["Q1", "Q3", "Q8", "Q12"]
# query_names = ["Q3", "Q8", "Q12"]


# query_settings = {
#     "q1": ["NOC-q1-10w-5si-16000r-1f", "UNC-q1-10w-5si-14400r-1f", "COR-q1-10w-5si-16000r-1f", "CIC-q1-10w-5si-12000r-1f"],
#     "q3": ["NOC-q3-10w-5si-24000r-1f", "UNC-q3-10w-5si-20000r-1f", "COR-q3-10w-5si-21600r-1f", "CIC-q3-10w-5si-12000r-1f"],
#     "q8r": ["NOC-q8-10w-5si-18400r-1f", "UNC-q8-10w-5si-16800r-1f", "COR-q8-10w-5si-18400r-1f", "CIC-q8-10w-5si-12800r-1f"],
#     "q12r": ["NOC-q12-10w-5si-16800r-1f", "UNC-q12-10w-5si-15200r-1f", "COR-q12-10w-5si-16800r-1f", "CIC-q12-10w-5si-12000r-1f"]
# }
# query_settings = {
#     "q1": ["NOC-q1-30w-5si-43200r-1f", "UNC-q1-30w-5si-38400r-1f", "COR-q1-30w-5si-43200r-1f", "CIC-q1-30w-5si-25600r-1f"],
#     "q3": ["NOC-q3-30w-5si-67200r-1f", "UNC-q3-30w-5si-50400r-1f", "COR-q3-30w-5si-57600r-1f", "CIC-q3-30w-5si-25600r-1f"],
#     "q8r": ["NOC-q8-30w-5si-50400r-1f", "UNC-q8-30w-5si-45600r-1f", "COR-q8-30w-5si-50400r-1f", "CIC-q8-30w-5si-28000r-1f"],
#     "q12r": ["NOC-q12-30w-5si-45600r-1f", "UNC-q12-30w-5si-42400r-1f", "COR-q12-30w-5si-45600r-1f", "CIC-q12-30w-5si-25600r-1f"]
# }
#
query_settings = {
    "q1": ["NOC-q1-50w-5si-68000r-1f", "UNC-q1-50w-5si-64000r-1f", "COR-q1-50w-5si-68000r-1f", "CIC-q1-50w-5si-35200r-1f"],
    "q3": ["NOC-q3-50w-5si-88000r-1f", "UNC-q3-50w-5si-63200r-1f", "COR-q3-50w-5si-64000r-1f", "CIC-q3-50w-5si-35200r-1f"],
    "q8r": ["NOC-q8-50w-5si-72000r-1f", "UNC-q8-50w-5si-63200r-1f", "COR-q8-50w-5si-72000r-1f", "CIC-q8-50w-5si-36800r-1f"],
    "q12r": ["NOC-q12-50w-5si-72000r-1f", "UNC-q12-50w-5si-66400r-1f", "COR-q12-50w-5si-72000r-1f", "CIC-q12-50w-5si-33600r-1f"]
}
# query_settings = {
#     "q3": ["NOC-q3-10w-5si-24000r-nf-skew-80mst", "UNC-q3-10w-5si-20000r-nf-skew-80mst", "COR-q3-10w-5si-21600r-nf-skew-80mst", "CIC-q3-10w-5si-12000r-nf-skew-80mst"],
#     "q8r": ["NOC-q8-10w-5si-18400r-nf-skew-80mst", "UNC-q8-10w-5si-16800r-nf-skew-80mst", "COR-q8-10w-5si-18400r-nf-skew-80mst", "CIC-q8-10w-5si-12800r-nf-skew-80mst"],
#     "q12r": ["NOC-q12-10w-5si-16800r-nf-skew-80mst", "UNC-q12-10w-5si-15200r-nf-skew-80mst", "COR-q12-10w-5si-16800r-nf-skew-80mst", "CIC-q12-10w-5si-12000r-nf-skew-80mst"]
# }


fig, ax = plt.subplots(1,4, figsize=(25,4))

for idx, q in enumerate(queries): 
    fp_1 = open(f"{saving_dir}/{query_settings[q][0]}/{query_settings[q][0]}-50p-output.csv", "r")
    fp_2 = open(f"{saving_dir}/{query_settings[q][1]}/{query_settings[q][1]}-50p-output.csv", "r")
    fp_3 = open(f"{saving_dir}/{query_settings[q][2]}/{query_settings[q][2]}-50p-output.csv", "r")
    fp_4 = open(f"{saving_dir}/{query_settings[q][3]}/{query_settings[q][3]}-50p-output.csv", "r")

    p99_1 = json.load(fp_1)
    p99_2 = json.load(fp_2)
    p99_3 = json.load(fp_3)
    p99_4 = json.load(fp_4)

    fp_1.close()
    fp_2.close()
    fp_3.close()
    fp_4.close()

    if q == "q1":
        p99_1 = {int(k): v for k, v in p99_1.items()}
        p99_2 = {int(k): v for k, v in p99_2.items()}
        p99_3 = {int(k): v for k, v in p99_3.items()}
        p99_4 = {int(k): v for k, v in p99_4.items()}
    else:
        p99_1 = {int(k)/100: v for k, v in p99_1.items()}
        p99_2 = {int(k)/100: v for k, v in p99_2.items()}
        p99_3 = {int(k)/100: v for k, v in p99_3.items()}
        p99_4 = {int(k)/100: v for k, v in p99_4.items()}

    experiment_length = 60  # in seconds
    ax[idx].set_yscale('log')
    ax[idx].axvline(x=18, color="black", linestyle='--', linewidth=3)
    ax[idx].plot(p99_1.keys(), p99_1.values(), linewidth=2, linestyle="--", marker="o", markersize=4, markevery=(0.1, 0.1), color=noc_color, label="No checkpoints")
    ax[idx].plot(p99_2.keys(), p99_2.values(), linewidth=2, linestyle="-.", marker="v", markersize=4, markevery=(0.1, 0.1), color=unc_color, label="Uncoordinated")
    ax[idx].plot(p99_3.keys(), p99_3.values(), linewidth=2, linestyle="-", marker="s", markersize=4, markevery=(0.1, 0.1), color=cor_color, label="Coordinated")
    ax[idx].plot(p99_4.keys(), p99_4.values(), linewidth=2, linestyle=":", marker="D", markersize=4, markevery=(0.1, 0.1), color=cic_color, label="Communication-induced")

    ax[idx].set_xlabel('Time (s)', fontweight="bold")
    ax[idx].set_ylabel('Latency (ms)', fontweight="bold")
    ax[idx].set_xticks([10, 20, 30, 40, 50, 60])
    ax[idx].set_xlim(0, 60)

    # if idx == 1:
    #     # handles, labels = plt.gca().get_legend_handles_labels()
    #     # order = [0, 1, 2, 3]
    #     ax[idx].legend(bbox_to_anchor=(1, -0.4), loc="center", ncol=4)
    ax[idx].set_title(f"NexMark {query_names[idx]}")

handles, labels = plt.gca().get_legend_handles_labels()
fig.legend(handles, labels, bbox_to_anchor=(0.5, 0), loc="center", ncols=4)
fig.tight_layout()
# plt.show()

fig.savefig(f'{saving_dir}/{query_settings["q1"][0]}/figures/combined-all-50th-output.pdf', bbox_inches='tight')
