import seaborn as sns
import pandas as pd
import sys

bandit_rewards = pd.read_csv(sys.argv[1])
ideal_rewards = pd.read_csv(sys.argv[2])
output_file = sys.argv[3]

bandit_regret = pd.merge(bandit_rewards, ideal_rewards, on='canonical_tuple_id')
bandit_regret["regret"] = bandit_regret["reward_y"] - bandit_regret["reward_x"]
bandit_regret = bandit_regret.drop('reward_x', 1)
bandit_regret = bandit_regret.drop('reward_y', 1)

plot_every_x = 25
df = bandit_regret.sort_values(['pos_in_partition','partition_id'],ascending=True)
df['cum_partition_regret']=df.groupby(['partition_id'])['regret'].cumsum()
df = df[df['pos_in_partition'] % plot_every_x == 0]
df['constant'] = 0
sns.set_style("whitegrid")
ax = sns.swarmplot(x="pos_in_partition", y="cum_partition_regret", hue='partition_id', data=df)
ax.legend_.remove()

sns.plt.savefig(output_file)
