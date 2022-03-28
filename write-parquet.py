#!/usr/bin/env python3

import sys
import os
from pytz import UTC
import pandas as pd
from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

URL = "http://localhost:8086"
DBNAME = "profiler"
ORG = "aws"
BUCKET = "test"
TOKEN = "my-super-secret-auth-token"

def read_parquet_file(filename):
    """ """
    df = pd.read_parquet(filename)

    print("Begin:", df.timestamp.min())
    print("End:  ", df.timestamp.max())

    # Sorting I
    # df.sort_values(by=['timestamp'], inplace=True, ascending=True)

    # Sorting II
    df.set_index(pd.DatetimeIndex(df.timestamp), inplace=True)
    del df["timestamp"]
    df.sort_index(inplace=True, ascending=True)

    return df

def dataframe_to_influxdb(df=None):
    client = InfluxDBClient(URL, token=TOKEN, org=ORG)

    buckets_api = client.buckets_api()
    test_bucket = buckets_api.find_bucket_by_name(BUCKET)
    if test_bucket:
        buckets_api.delete_bucket(bucket=test_bucket)
    buckets_api.create_bucket(bucket_name =BUCKET, org=ORG)

    # delete bucket ###########################################################
    delete_api = client.delete_api()
    start = "1970-01-01T00:00:00Z"
    stop = "2030-01-01T00:00:00Z"
    # delete_api.delete(
    #     start, stop, f'_measurement="h2o_feet"', bucket=BUCKET, org=ORG
    # )

    # write bucket ############################################################
    write_api = client.write_api(
        write_options=WriteOptions(
            batch_size=1,
            flush_interval=10_000,
            jitter_interval=2_000,
            retry_interval=5_000,
            max_retries=5,
            max_retry_delay=30_000,
            exponential_base=2,
        )
    )

    # write_api.write(bucket, org, record = df,
    #                 data_frame_measurement_name=measurement, data_frome_tag_columns=tag_columns)
    write_api.write(
        BUCKET,
        ORG,
        [
            {
                "measurement": "h2o_feet",
                "tags": {"location": "coyote_creek"},
                "fields": {"water_level": 2.0},
                "time": 2,
            },
            {
                "measurement": "h2o_feet",
                "tags": {"location": "coyote_creek"},
                "fields": {"water_level": 3.0},
                "time": 3,
            },
        ],
    )
    """
    Write Line Protocol formatted as byte array
    """
    write_api.write(BUCKET, ORG, "h2o_feet,location=coyote_creek water_level=1.0 1".encode())
    write_api.write(BUCKET, ORG, ["h2o_feet,location=coyote_creek water_level=2.0 2".encode(),
                                                "h2o_feet,location=coyote_creek water_level=3.0 3".encode()])

    """
    Write Dictionary-style object
    """
    write_api.write(BUCKET, ORG, {"measurement": "h2o_feet", "tags": {"location": "coyote_creek"},
                                                "fields": {"water_level": 1.0}, "time": 1})
    import pdb; pdb.set_trace()
    # _now = datetime.now(UTC)
    # _data_frame = pd.DataFrame(
    #     data=[["coyote_creek", 1.0], ["coyote_creek", 2.0]],
    #     index=[_now, _now + timedelta(hours=1)],
    #     columns=["location", "water_level"],
    # )

    # write_api.write(
    #     bucket,
    #     org,
    #     record=_data_frame,
    #     data_frame_measurement_name="h2o_feet",
    #     data_frame_tag_columns=["location"],
    # )
    import pdb

    pdb.set_trace()
    # client.query_api

    client.close()


if __name__ == "__main__":
    # df = read_parquet_file(sys.argv[1])
    # print(df)
    df = None
    # dataframe_to_influxdb(df=df)
