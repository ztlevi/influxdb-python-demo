
import pandas as pd
import numpy as np

# df = pd.read_parquet("data/mrcnn_p4d_1node_gpu_kernels_trimmed.parquet")
# import pdb; pdb.set_trace()

df = pd.read_parquet("data/kernel/mrcnn_p4d_1node_gpu_kernels.parquet")

a = len(set(np.arange(0, df.shape[0], 1)))
print(a)

names = {}
counter = 0
for n in df['name']:
    if n not in names:
        names[n] = counter
        counter += 1

name_column = []
for n in df['name']:
    name_column.append(names[n])
df.drop(['name'], axis=1)
df['name'] = np.array(name_column)
# name_column_series = pd.Series(np.array(name_column).astype(np.int32))

precisions = {}
counter = 0
for n in df['precision']:
    if n not in precisions:
        precisions[n] = counter
        counter +=1
precision_column = []
for p in df['precision']:
    precision_column.append(precisions[p])
df.drop(['precision'], axis=1)
df['precision']= np.array(precision_column)

categories = {}
counter = 0
for n in df['category']:
    if n not in categories:
        categories[n] = counter
        counter +=1
category_column = []
for p in df['category']:
    category_column.append(categories[p])
df.drop(['category'], axis=1)
df['category']= np.array(category_column)

df.drop(['id'], axis=1)
df['id'] = np.arange(0, df.shape[0], 1)

df.to_parquet("data/mrcnn_p4d_1node_gpu_kernels_trimmed.parquet")
