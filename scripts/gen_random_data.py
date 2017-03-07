import numpy as np

print 'partition_id,pos_in_partition,canonical_tuple_id,reward'
canonical_id = 0
for i in range(64):
    for j in range(1000):
        reward = np.random.normal()
        print '{},{},{},{}'.format(i,j,canonical_id,reward)
        canonical_id += 1 
