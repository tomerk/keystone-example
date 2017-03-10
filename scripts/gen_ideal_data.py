import pandas as pd
import sys
import glob

data_glob = sys.argv[1]
output_file = sys.argv[2]

data_list = []
for csv in glob.glob(data_glob):
    df = pd.read_csv(csv)
    data_list.append(df)

df = pd.concat(data_list)
out = df.sort_values("reward", ascending=False).groupby("canonical_tuple_id", as_index=False).first()
del out['pos_in_partition']
del out['partition_id']
del out['system_nano_start_time']
del out['system_nano_end_time']

out.to_csv(output_file)