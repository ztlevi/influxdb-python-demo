#!/usr/bin/env zsh
# -scale-var 2

pushd ~/dev/influxdb-comparisons
(cd cmd/bulk_data_gen/; go build .)
# ./cmd/bulk_data_gen/bulk_data_gen -cardinality 10  | wc -l
(cd cmd/bulk_load_influx/; go build .)
./cmd/bulk_data_gen/bulk_data_gen -cardinality 10  | \
    ./cmd/bulk_load_influx/bulk_load_influx -workers 12 -organization aws -token my-super-secret-auth-token -urls http://localhost:8086

popd
