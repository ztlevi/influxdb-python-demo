
https://docs.influxdata.com/influxdb/v2.1/install/?t=CLI+Setup#configure-influxdb-with-docker

https://hub.docker.com/_/influxdb/

Official influxDB python client examples: https://github.com/influxdata/influxdb-client-python/tree/v1.27.0/examples

``` sh
python3 src/write-dataframe.py
```

Query count
``` flux
from(bucket:"{bucket_name}")
  |> range(start: {current_time-total_duration}, stop: {current_time})
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> group(columns: ["_measurement"])
  |> count(column: "_value")
```

``` flux
from(bucket: "b1")
|> range(start: 0)
|> count(column: "_value")

from(bucket:"b1")
  |> range(start: 1648349307000000000, stop: 1648352907000000000)
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: 3s, fn: max, column: "duration", createEmpty: false)
  |> yield(name: "max")

from(bucket:"b1")
  |> range(start: 1648349307000000000, stop: 1648352907000000000)
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: 3s, fn: max, column: "duration", createEmpty: false)
  |> to(bucket: "100x_b1", org: "aws", fieldFn: (r)=>({"id": r.id, "duration": r.duration}))

  |> pivot(rowKey: ["_time"], columnKey: [], valueColumn: "_value")

import "influxdata/influxdb/schema"
from(bucket:"b1")
  |> range(start: 1648349307000000000, stop: 1648352907000000000)
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: 3s, fn: max, createEmpty: false)
  |> schema.fieldsAsCols()
//   |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")


from(bucket:"b1")
  |> range(start: 1648349307000000000, stop: 1648352907000000000)
  |> filter(fn: (r) => r._measurement == "gpu_event")
  |> drop(columns: ["_start", "_stop"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["_measurement"])
  |> aggregateWindow(every: 3s, fn: max, column: "duration", createEmpty: false)
  |> to(bucket: "100x_b1", org: "aws", fieldFn: (r)=>({"id": r.id, "duration": r.duration}))
```

