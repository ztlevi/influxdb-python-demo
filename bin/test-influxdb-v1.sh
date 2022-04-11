#!/usr/bin/env zsh
# -scale-var 2

pushd ~/dev/influxdb-comparisons
(cd cmd/bulk_data_gen/; go build .)
# ./cmd/bulk_data_gen/bulk_data_gen -cardinality 10  | wc -l
(cd cmd/bulk_load_influx/; go build .)

# V1
# ./cmd/bulk_data_gen/bulk_data_gen -cardinality 10  | \
#     ./cmd/bulk_load_influx/bulk_load_influx \
#     -urls http://localhost:8086

# V1 UDP
./cmd/bulk_data_gen/bulk_data_gen -cardinality 10  | \
    ./cmd/bulk_load_influx/bulk_load_influx \
    -urls http://localhost:8189
popd
