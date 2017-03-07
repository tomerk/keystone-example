import numpy as np

print 'canonical_tuple_id,reward'
canonical_id = 0
for i in range(64):
    for j in range(1000):
        reward = np.random.normal()+2
        print '{},{}'.format(canonical_id,reward)
        canonical_id += 1 
