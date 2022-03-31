"""
How to ingest large DataFrame by splitting into chunks.
"""
import logging
from rich import print
import random
from datetime import datetime
import time

from influxdb_client import InfluxDBClient, WriteOptions, TaskCreateRequest
from influxdb_client.extras import pd, np
from influxdb_client.client.query_api import QueryOptions
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
MILLION = 10**6
DEFAULT_BATCH_SIZE = 1_000
current_time = 1648352907 * BILLION
total_duration = 3600 * BILLION  # 1h days
time_bin = 3  # 3s for 1200 events
np.random.seed(42)

def create_timeseries_data():
    pass

def create_timeline_data(dataframe_rows_count):
    """
    Generate Dataframe
    """
    start_time = current_time - np.linspace(
        0, total_duration, dataframe_rows_count, dtype=np.int64
    )
    duration = np.random.randint(0, 10 * MILLION, dataframe_rows_count, dtype=np.int64)

    col_data = {
        "time": start_time,
        "duration": duration,
        "id": np.arange(0, dataframe_rows_count, dtype=np.int64),
        "name": np.random.choice(
            [f"kernel{i}" for i in range(1000)], size=(dataframe_rows_count,)
        ),
        "category": np.random.choice(
            ["py_annotation", "gpu_kernel"], size=(dataframe_rows_count,)
        ),
        "precision": np.random.choice(
            ["FP32", "FP16", "TF32", "BF16"], size=(dataframe_rows_count,)
        ),
    }
    # for n in range(2, 10):
    #     col_data[f"col{n}"] = random.randint(1, 10)

    data_frame = pd.DataFrame(data=col_data).set_index("time")
    return data_frame


def write_bucket(data_frame, bucket, batch_size=DEFAULT_BATCH_SIZE):
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
                data_frame_tag_columns=["category", "name", "precision"],
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
    with InfluxDBClient(url=url, token=token, org=org, timeout=600_000) as client:
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


def query_bucket(bucket_name, zoom=1):
    """
    Query: using Table structure
    """
    query_string = f"""
from(bucket:"{bucket_name}")
  |> range(start: {current_time-total_duration//zoom}, stop: {current_time})
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: {time_bin}s, fn: max, column: "duration", createEmpty: false)
    """
    query(query_string)


def create_task(bucket_name):
    dumped_bucket_name = "100x_" + bucket_name
    with InfluxDBClient(url=url, token=token, org=org, timeout=600_000) as client:
        buckets_api = client.buckets_api()
        cur_bucket = buckets_api.find_bucket_by_name(dumped_bucket_name)
        if cur_bucket:
            buckets_api.delete_bucket(bucket=cur_bucket)
        buckets_api.create_bucket(bucket_name=dumped_bucket_name, org=org)

        tasks_api = client.tasks_api()
        task_name = "task_" + dumped_bucket_name
        query_string = f"""
option task = {{
  name: "{task_name}",
  every: 20m
}}

from(bucket:"{bucket_name}")
  |> range(start: {current_time-total_duration}, stop: {current_time})
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: {10*time_bin}ms, fn: max, column: "duration", createEmpty: false)
  |> to(bucket: "{dumped_bucket_name}", org: "aws", fieldFn: (r)=>({{"id": r.id, "duration": r.duration}}))
        """
        task_request = TaskCreateRequest(
            flux=query_string, org=org, description="Task Description", status="active"
        )
        task = tasks_api.create_task(task_create_request=task_request)
        tasks_api.run_manually(task.id)
        print(task)


def task_query():
    bucket_name = "b1"
    dumped_bucket_name = "100x_" + bucket_name
    with InfluxDBClient(url=url, token=token, org=org, timeout=600_000) as client:
        buckets_api = client.buckets_api()
        cur_bucket = buckets_api.find_bucket_by_name(dumped_bucket_name)
        if cur_bucket:
            buckets_api.delete_bucket(bucket=cur_bucket)
        buckets_api.create_bucket(bucket_name=dumped_bucket_name, org=org)

    query_string = f"""
from(bucket:"{bucket_name}")
  |> range(start: {current_time-total_duration}, stop: {current_time})
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: {10*time_bin}ms, fn: max, column: "duration", createEmpty: false)
  |> to(bucket: "{dumped_bucket_name}", org: "aws", fieldFn: (r)=>({{"id": r.id, "duration": r.duration}}))
    """
    query(query_string)

    query_string = f"""
from(bucket:"{dumped_bucket_name}")
  |> range(start: {current_time-total_duration}, stop: {current_time})
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> group(columns: ["_measurement"])
  |> count(column: "_value")
    """
    query(query_string)


def write_dataframe_helper(
    dataframe_rows_count, bucket_name, batch_size=DEFAULT_BATCH_SIZE
):
    df = create_timeline_data(dataframe_rows_count)
    write_bucket(df, bucket_name, batch_size)


if __name__ == "__main__":
    num_buckets = 10
    num_processes = min(30, num_buckets)
    # write_dataframe_helper(100000, "b1")
    write = 1
    if write == 1:
        with Pool(processes=num_processes) as pool:
            pool.starmap(
                write_dataframe_helper,
                [(30_000_000, "gpu" + str(i)) for i in range(1, num_buckets + 1)],
            )
    #     query_string = f"""
    # from(bucket:"100x_b1")
    #   |> range(start: 0)
    #   |> filter(fn: (r) => r._measurement == "gpu_event")
    #   |> drop(columns: ["_start", "_stop"])
    #   |> group(columns: ["_measurement"])
    #   |> max(column: "_time")
    #     """
    #     query_string2 = f"""
    # from(bucket:"b1")
    #   |> range(start: 1648349307000000000, stop: 1648352907000000000)
    #   |> filter(fn: (r) => r._measurement == "gpu_event")
    #   |> drop(columns: ["_start", "_stop"])
    #   |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    #   |> group(columns: ["_measurement", "name"])
    #   |> sum(column: "duration")
    #   |> group(columns: ["_measurement"])
    #   |> top(n:3, columns: ["duration"])
    #     """

    #     query(query_string)

    with Pool(processes=num_processes) as pool:
        pool.starmap(
            query_bucket, [("gpu" + str(i), 10 * i) for i in range(1, num_buckets + 1)]
        )
    # for i in range(1, num_processes + 1):
    #     query_bucket("b" + str(i))
    # query_bucket("b1")
    # task_query()
