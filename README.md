https://docs.influxdata.com/influxdb/v2.1/install/?t=CLI+Setup#configure-influxdb-with-docker

https://hub.docker.com/_/influxdb/

Official influxDB python client examples:
https://github.com/influxdata/influxdb-client-python/tree/v1.27.0/examples

```sh
python3 src/write-dataframe.py
```

## Query total count

```flux
from(bucket:"{bucket_name}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "gpu_event" and r._field == "duration")
  |> group(columns: ["_measurement"])
  |> count(column: "_value")
```

## Show first 10 rows 
``` flux
from(bucket:"{bucket_name}")
  |> range(start: 0)
  |> group(columns: ["_measurement"])
  |> limit(n:10)
```
## Downsampled query

```flux
from(bucket:"{bucket_name}")
  |> range(start: time(v: {start_time}), stop: time(v: {END_TIME}))
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: {bin}ms, fn: max, column: "duration", createEmpty: false)
```

## Others

```flux
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
```
