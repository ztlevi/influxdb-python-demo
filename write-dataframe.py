"""
How to ingest large DataFrame by splitting into chunks.
"""
import logging
import random
from datetime import datetime

from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.extras import pd, np
from multiprocessing.pool import Pool

"""
Enable logging for DataFrame serializer
"""
# loggerSerializer = logging.getLogger(
#     "influxdb_client.client.write.dataframe_serializer"
# )
# loggerSerializer.setLevel(level=logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
# loggerSerializer.addHandler(handler)

"""
Configuration
"""
url = "http://localhost:8086"
token = "my-super-secret-auth-token"
org = "aws"

BILLION = 10**9
DEFAULT_BATCH_SIZE = 1_000
current_time = 1648352907 * BILLION


def create_data(dataframe_rows_count):
    """
    Generate Dataframe
    """
    start_time = current_time - BILLION * np.arange(
        0, dataframe_rows_count, 1, dtype=int
    )
    duration = np.random.randint(1, 200, dataframe_rows_count)

    col_data = {
        "time": start_time,
        # "start": start_time,
        # "stop": end_time,
        "duration": duration,
        "tag": np.random.choice(
            ["tag_a", "tag_b", "test_c"], size=(dataframe_rows_count,)
        ),
    }
    # for n in range(2, 10):
    #     col_data[f"col{n}"] = random.randint(1, 10)

    data_frame = pd.DataFrame(data=col_data).set_index("time")
    return data_frame


def write_bucket(data_frame, bucket, batch_size=50_000):
    """
    Ingest DataFrame
    """

    with InfluxDBClient(url=url, token=token, org=org) as client:
        """
        Delete and create new bucket
        """
        buckets_api = client.buckets_api()
        test_bucket = buckets_api.find_bucket_by_name(bucket)
        if test_bucket:
            buckets_api.delete_bucket(bucket=test_bucket)
        buckets_api.create_bucket(bucket_name=bucket, org=org)

        startTime = datetime.now()
        """
        Use batching API
        """
        with client.write_api(
            write_options=WriteOptions(
                batch_size=DEFAULT_BATCH_SIZE,
                flush_interval=10_000,
                jitter_interval=2_000,
                retry_interval=5_000,
                max_retries=5,
                max_retry_delay=30_000,
                exponential_base=2,
            )
        ) as write_api:
            write_api.write(
                bucket=bucket,
                record=data_frame,
                data_frame_tag_columns=["tag"],
                data_frame_measurement_name="gpu_event",
            )
            # print(f"Wait to finishing ingesting DataFrame {data_frame.shape}, batch_size: {batch_size}...")

        print(
            f"Import DataFrame {data_frame.shape}, batch_size: {batch_size} finished in: {datetime.now() - startTime}"
        )


def query_bucket(bucket_name):
    """
    Query: using Table structure
    """
    with InfluxDBClient(url=url, token=token, org=org) as client:
        query_api = client.query_api()
        tables = query_api.query(
            f'from(bucket:"{bucket_name}")'
            " |> range(start: 0)"
            ' |> drop(columns: ["_start", "_stop"])'
            " |> limit(n: 10) "
            # |> filter(fn: (r) => r._measurement == "measurement_name") |> count()
        )

        for table in tables[:1]:
            print(table)
            for record in table.records:
                print(record.values)


def helper(dataframe_rows_count, bucket_name, batch_size=DEFAULT_BATCH_SIZE):
    df = create_data(dataframe_rows_count)
    write_bucket(df, bucket_name, batch_size)


if __name__ == '__main__':
    num_buckets = 70
    num_processes = min(30, num_buckets)
    # helper(1000000, "aws")
    with Pool() as pool:
        pool.starmap(
            helper, [(1_000_000, "b" + str(i), 1 * i) for i in range(1, num_processes + 1)]
        )
    # for i in range(1, num_processes + 1):
    #     query_bucket("b" + str(i))
