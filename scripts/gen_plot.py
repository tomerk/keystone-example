import seaborn as sns
import pandas as pd
import sys
import glob
import matplotlib.patches as mpatches
import math

constant_arms = sys.argv[2]
oracles = sys.argv[3]
output_file = sys.argv[4]

policy_labels = {'constant:0': 'Convolve via Loops', 'constant:1': 'Convolve via Matrix Multiply',
                 'constant:2': 'Convolve via FFT', 'oracle:min:oracle_data.csv': 'Oracle'}

## Plot the current policy
bandit_rewards = pd.read_csv(sys.argv[1])
df = bandit_rewards
df = df[df['pos_in_partition'] == 0].copy()
df['partition_start_time'] = df['system_nano_start_time']
bandit_rewards = pd.merge(bandit_rewards, df[['partition_id', 'partition_start_time']], on='partition_id')

bandit_rewards['end_time'] = (bandit_rewards['system_nano_end_time'] - bandit_rewards['partition_start_time']) / 1.0e9

max_end_time = bandit_rewards['end_time'].max()

plot_every_x = 25
df = bandit_rewards.sort_values(['pos_in_partition', 'partition_id'], ascending=True)
df = df[df['pos_in_partition'] % plot_every_x == 0]
sns.set_style("whitegrid")
bandit_color = 'slategrey'
ax = sns.boxplot(x="pos_in_partition", y="end_time", color=bandit_color, data=df)

## Plot the constant arms
data_list = []
for csv in glob.glob(constant_arms):
    df = pd.read_csv(csv)
    df['policy'] = csv
    data_list.append(df)

for csv in glob.glob(oracles):
    df = pd.read_csv(csv)
    df['policy'] = csv
    data_list.append(df)

bandit_rewards = pd.concat(data_list)
df = bandit_rewards
df = df[df['pos_in_partition'] == 0].copy()
df['partition_start_time'] = df['system_nano_start_time']
bandit_rewards = pd.merge(bandit_rewards, df[['policy', 'partition_id', 'partition_start_time']],
                          on=['partition_id', 'policy'])

bandit_rewards['end_time'] = (bandit_rewards['system_nano_end_time'] - bandit_rewards['partition_start_time']) / 1.0e9

plot_every_x = 25
df = bandit_rewards.sort_values(['pos_in_partition', 'partition_id'], ascending=True)
df = df[df['pos_in_partition'] % plot_every_x == 0]
df['constant'] = 0
sns.pointplot(x="pos_in_partition", y="end_time", markers='', ci=None, hue='policy', ax=ax, data=df)

## Add some pizazz
for label in ax.get_xticklabels():
    label.set_visible(False)
for label in ax.get_xticklabels()[::2]:
    label.set_visible(True)
ax.set_ylim(0, math.ceil(max_end_time / 25.0) * 25)
ax.set(xlabel='Items Processed by vCPU Core', ylabel='Elapsed Time (s)')

# Set legend correctly
new_handles = []
handles, labels = ax.get_legend_handles_labels()
for handle, label in zip(handles, labels):
    new_handles.append(
        mpatches.Patch(color=handle.get_edgecolors()[0], label=policy_labels[label.split('/')[-1].split('-')[0]]))
new_handles.append(mpatches.Patch(color=bandit_color, label='Bandit Policy'))
ax.legend(handles=new_handles)

# ax.legend_.remove()

sns.plt.savefig(output_file)
