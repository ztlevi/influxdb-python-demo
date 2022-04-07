"""
How to ingest large DataFrame by splitting into chunks.
"""
import logging
from rich import print
import s3fs
import random
from datetime import datetime
import time

from influxdb_client import InfluxDBClient, WriteOptions, TaskCreateRequest
from influxdb_client.extras import pd, np
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryOptions
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
import string

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
np.random.seed(42)

BILLION = 10**9
MILLION = 10**6

DEFAULT_BATCH_SIZE = 5_000
INFLUXDB_TIMEOUT = 12_000_000  # 12s

END_TIME = 1648352907 * BILLION
TOTAL_DURATION = 3600 * BILLION  # 1h
TIME_BIN = 3000  # 3000ms for 1200 events

ALPHABET = np.array(list(string.ascii_lowercase + " "))


def create_timeseries_data(dataframe_rows_count):
    start_times = np.linspace(
        END_TIME - TOTAL_DURATION,
        END_TIME,
        dataframe_rows_count,
        dtype=np.int64,
    )
    duration = np.random.randint(0, 10 * MILLION, dataframe_rows_count, dtype=np.int64)

    col_data = {
        "time": start_times,
        "duration": duration,
        "id": np.arange(0, dataframe_rows_count, dtype=np.int64),
        "value": np.random.randint(0, 100, dataframe_rows_count, dtype=np.int64),
    }

    data_frame = pd.DataFrame(data=col_data).set_index("time")
    return data_frame


def create_timeline_data(dataframe_rows_count):
    start_times = np.linspace(
        END_TIME - TOTAL_DURATION, END_TIME, num=dataframe_rows_count, dtype=int
    )
    duration = np.random.randint(0, 10 * MILLION, dataframe_rows_count, dtype=int)
    kernel_names = ["".join(np.random.choice(ALPHABET, size=400)) for _ in range(1000)]

    col_data = {
        "time": start_times,
        "duration": duration,
        "id": np.arange(0, dataframe_rows_count, dtype=int),
        "name": np.random.choice(kernel_names, size=(dataframe_rows_count,)),
        "category": np.random.choice(
            ["py_annotation", "gpu_kernel"], size=(dataframe_rows_count,)
        ),
        "precision": np.random.choice(
            ["FP32", "FP16", "TF32", "BF16"], size=(dataframe_rows_count,)
        ),
    }

    df = pd.DataFrame(data=col_data)
    return df


def write_bucket(data_frame, bucket, batch_size=DEFAULT_BATCH_SIZE, cleanup=False):
    """
    Ingest DataFrame
    """

    with InfluxDBClient(url=url, token=token, org=org) as client:
        """
        Delete and create new bucket
        """
        buckets_api = client.buckets_api()
        cur_bucket = buckets_api.find_bucket_by_name(bucket)
        if cur_bucket and cleanup:
            buckets_api.delete_bucket(bucket=cur_bucket)
        buckets_api.create_bucket(bucket_name=bucket, org=org)

        startTime = datetime.now()
        """
        Use batching API
        """
        with client.write_api(
            write_options=WriteOptions(
                # write_type=SYNCHRONOUS,
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
                data_frame_tag_columns=["category",
                                        "name",
                                        "precision"],
                data_frame_measurement_name="gpu_event",
            )
            # print(f"Wait to finishing ingesting DataFrame {data_frame.shape}, batch_size: {batch_size}...")

        print(
            f"Import DataFrame {data_frame.shape}, batch_size: {batch_size} finished in: {datetime.now() - startTime}"
        )


def query(query_string):
    """
    Query: using Table structure
    """
    with InfluxDBClient(
        url=url, token=token, org=org, timeout=INFLUXDB_TIMEOUT
    ) as client:
        query_api = client.query_api()

        stime = time.time()
        dfs = query_api.query_data_frame(query=query_string)
        if not isinstance(dfs, list):
            dfs = [dfs]

        print(query_string)
        for df in dfs:
            print(df.head())
            print("queried count: {}".format(df.shape))
        etime = time.time()
        print("queried time: ", etime - stime)
        return dfs


def query_bucket(bucket_name, zoom=1):
    """
    Query: using Table structure

    To enable profiler, use:
    import "profiler"
    option profiler.enabledProfilers = ["query", "operator"]
    """
    start_time = END_TIME - (TOTAL_DURATION // zoom)
    bin = TIME_BIN // zoom

    query_string = f"""
// ================= Query {bucket_name} with time bin {bin}ms. =================
from(bucket:"{bucket_name}")
  |> range(start: time(v: {start_time}), stop: time(v: {END_TIME}))
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: {bin}ms, fn: max, column: "duration", createEmpty: false)
    """
    query(query_string)

def downsample_task(bucket_name, zoom=250, run=False, cleanup=False):
    downsampled_bucket_name = "downsampled_" + bucket_name
    with InfluxDBClient(
        url=url, token=token, org=org, timeout=INFLUXDB_TIMEOUT
    ) as client:
        buckets_api = client.buckets_api()
        cur_bucket = buckets_api.find_bucket_by_name(downsampled_bucket_name)
        if cur_bucket and cleanup:
            buckets_api.delete_bucket(bucket=cur_bucket)
        if not cur_bucket or cleanup:
            buckets_api.create_bucket(bucket_name=downsampled_bucket_name, org=org)

    bin = TIME_BIN // zoom

    if run:
        query_max_time_string = f"""
// ================= Query max time from {downsampled_bucket_name} =================
from(bucket:"{downsampled_bucket_name}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> group(columns: ["_measurement"])
  |> max(column: "_time")
        """
        res = query(query_max_time_string)
        start_time = 0
        if not res[0].empty:
            start_time = res[0].at[0, "_time"].timestamp() * BILLION

        query_string = f"""
// =============== Dump downsampled data to {downsampled_bucket_name} =================
from(bucket:"{bucket_name}")
  |> range(start: time(v: {start_time}))
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: {bin}ms, fn: max, column: "duration", createEmpty: false)
  |> to(bucket: "{downsampled_bucket_name}", org: "aws", fieldFn: (r)=>({{"id": r.id, "duration": r.duration}}))
        """
        query(query_string)


def create_task(bucket_name, zoom=200, cleanup=False):
    downsampled_bucket_name = "downsampled_" + bucket_name

    with InfluxDBClient(
        url=url, token=token, org=org, timeout=INFLUXDB_TIMEOUT
    ) as client:
        buckets_api = client.buckets_api()
        cur_bucket = buckets_api.find_bucket_by_name(downsampled_bucket_name)
        if cur_bucket and cleanup:
            buckets_api.delete_bucket(bucket=cur_bucket)
        if not cur_bucket or cleanup:
            buckets_api.create_bucket(bucket_name=downsampled_bucket_name, org=org)

        bin = TIME_BIN // zoom
        tasks_api = client.tasks_api()
        task_name = "task_" + downsampled_bucket_name
        query_string = f"""
option task = {{
  name: "{task_name}",
  every: 20m
}}

from(bucket:"{bucket_name}")
  |> range(start: time(v: 0))
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: {bin}ms, fn: max, column: "duration", createEmpty: false)
  |> to(bucket: "{downsampled_bucket_name}", org: "aws", fieldFn: (r)=>({{"id": r.id, "duration": r.duration}}))
        """
        print(f"// =============== Dump downsampled data to {downsampled_bucket_name} =================")
        task_request = TaskCreateRequest(
            flux=query_string, org=org, description="Task Description", status="active"
        )
        task = tasks_api.create_task(task_create_request=task_request)
        stime = time.time()
        tasks_api.run_manually(task.id)
        etime = time.time()
        print(f"=================== downsampled task takes {etime-stime}s to run ===============")

    count_query_string = f"""
// =============== Print the num of rows in the downsampled bucket ================
from(bucket:"{downsampled_bucket_name}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "gpu_event" and r._field == "duration")
  |> group(columns: ["_measurement"])
  |> count(column: "_value")
    """
    query(count_query_string)


def write_dataframe_helper(
    dataframe_rows_count, bucket_name, batch_size=DEFAULT_BATCH_SIZE
):
    # s3bucket = "s3://zhot-test-data/kernel"
    # s3_parquet_file = f"{s3bucket}/{bucket_name}.parquet"

    df = create_timeline_data(dataframe_rows_count)
    # Remove uploaded files
    # file_system = s3fs.S3FileSystem()
    # file_system.rm(s3_parquet_file)
    # df.to_parquet(s3_parquet_file)

    # df = pd.read_parquet(s3_parquet_file)

    df = df.set_index("time")
    write_bucket(df, bucket_name, batch_size, cleanup=True)
    create_task(bucket_name, zoom=200, cleanup=True)
    # downsample_task(bucket_name, zoom=1500, run=True, cleanup=True)


if __name__ == "__main__":
    num_buckets = 8
    num_processes = int(min(round(cpu_count() * 0.5), num_buckets))

    # Single table test
    # write_dataframe_helper(1000_000, "gpu1")
    # query_bucket("gpu1")

    # Multiprocessing test
    write = 1
    if write == 1:
        with Pool(processes=num_processes) as pool:
            pool.starmap(
                write_dataframe_helper,
                [(2_500_000,  str(i)) for i in range(1, num_buckets + 1)],
            )

    # with Pool(processes=num_processes) as pool:
    #     pool.starmap(
    #         query_bucket, [("gpu" + str(i), 1) for i in range(1, num_buckets + 1)]
    #     )
    # for i in range(1, num_processes + 1):
    #     query_bucket("b" + str(i))
    # task_query()
