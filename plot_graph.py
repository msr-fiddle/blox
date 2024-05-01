import matplotlib.pyplot as plt
import numpy as np
import json

def get_avg_jct_dict_pollux_author():
    avg_jct_dict = {}
    sd_jct_dict = {}
    period_list = ["30s", "1m", "2m", "4m", "8m"]
    for period in period_list[:3]:
        with open("results-period/pollux-{}/summary.json".format(period)) as f:
            summary = json.load(f)
        average_jct = summary["avgs"]["workload-6"]
        average_jct = average_jct / 3600
        temp = list(summary["jcts"]["workload-6"].values())
        temp = [n / 3600 for n in temp]
        sd_jct = np.std(temp)
        if int(period[0]) == 3:
            avg_jct_dict["30 s"] = average_jct
            sd_jct_dict["30 s"] = sd_jct
        else:
            avg_jct_dict["{} min".format(period[0])] = average_jct
            sd_jct_dict["{} min".format(period[0])] = sd_jct

    for period in period_list[3:]:
        with open("reproduce-period/pollux-{}/summary.json".format(period)) as f:
            summary = json.load(f)
        average_jct = summary["avgs"]["workload-6"]
        average_jct = average_jct / 3600
        # temp = list(summary["jcts"]["workload-6"].values()) / 3600 - average_jct
        # sd_jct = temp * temp / (len(temp) - 1)
        sd_jct = np.std(list(summary["jcts"]["workload-6"].values())) / 3600
        if int(period[0]) == 3:
            avg_jct_dict["30 s"] = average_jct
            sd_jct_dict["30 s"] = sd_jct
        else:
            avg_jct_dict["{} min".format(period[0])] = average_jct
            sd_jct_dict["{} min".format(period[0])] = sd_jct        
    return avg_jct_dict, sd_jct_dict

def get_avg_jct_dict_pollux_blox():
    avg_jct_dict = {}
    sd_jct_dict = {}
    period_list = ["30s", "1min", "2min", "4min", "8min"]
    for period in period_list:
        with open("result-interval/{}_0_159_Pollux_accept_all_load_1.0_job_stats.json".format(period)) as f:
            y = json.loads(f.read())
        count = 0
        total = 0
        for value in y.values():
            total += (value[1] - value[0])
            count += 1
        average_jct = total / count
        average_jct = average_jct / 3600
        temp = [(value[1] - value[0]) / 3600 for value in y.values()] 
        sd_jct = np.std(temp)
        if int(period[0]) == 3:
            avg_jct_dict["30 s"] = average_jct
            sd_jct_dict["30 s"] = sd_jct
        else:
            avg_jct_dict["{} min".format(period[0])] = average_jct
            sd_jct_dict["{} min".format(period[0])] = sd_jct  
    return avg_jct_dict, sd_jct_dict

a, a_sd = get_avg_jct_dict_pollux_author()
b, b_sd = get_avg_jct_dict_pollux_blox()
print(f"a = {a}")
print(f"a_sd = {a_sd}")
print(f"b = {b}")
print(f"b_sd = {b_sd}")

# Extracting keys and values
keys = list(a.keys())
values_a = list(a.values())
values_b = list(b.values())
sd_a = list(a_sd.values())
sd_b = list(b_sd.values())

# Creating bar indexes
indexes = np.arange(len(keys))

# Width of each bar
bar_width = 0.35

# Plotting bars
plt.bar(indexes, values_a, bar_width, label='Pollux-Author Implementation', color='#a50021ff')
plt.bar(indexes + bar_width, values_b, bar_width, label='Pollux-Blox Implementation', color="#4472c4ff")

# # Adding standard deviation (sd) error bar
# plt.errorbar(indexes, values_a, sd_a, fmt='.', color='Black', elinewidth=2, capthick=10, errorevery=1, ms=4, capsize = 2)
# plt.errorbar(indexes + bar_width, values_b, sd_b, fmt='.', color='Black', elinewidth=2, capthick=10, errorevery=1, ms=4, capsize = 2)

# Adding labels
plt.xlabel('Scheduling Interval (mins)')
plt.ylabel('Avg JCT (hours)')
# plt.title('Comparison of a and b')
plt.xticks(indexes + bar_width / 2, keys)
plt.ylim(0.0, 1.0)

# Adding legend
plt.legend()

plt.savefig('pollux.png', dpi=2400)

# Showing plot
plt.tight_layout()
plt.show()