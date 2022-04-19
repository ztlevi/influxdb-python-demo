#!/usr/bin/env python3

import pandas as pd
import time

stime = time.time()
df = pd.read_parquet(
    "/home/ubuntu/dev/influxdb-python-demo/data/mrcnn_p4d_1node_gpu_kernels_gpu0.parquet"
)
df.apply(lambda row: "{0}, {1}, {2}, {3}, {4}, {5}".format(*row), axis=1, raw=True)
# for row in df.itertuples(name=None):
#     s = "%d, %d, %d, %d, %d, %d".format(row[0],row[1],row[2],row[3],row[4],row[5])
etime = time.time()

print(etime - stime)

# print(df.shape)
# stime = time.time()
# df = pd.read_parquet("/home/ubuntu/dev/influxdb-python-demo/data/").to_numpy()
# etime = time.time()
# print("numpy: ", etime-stime)

# stime = time.time()
# for row in df:
#     s = f"{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[6]}"
# etime = time.time()

# print("Serialization Time: ", etime-stime)

# print(df.shape)
