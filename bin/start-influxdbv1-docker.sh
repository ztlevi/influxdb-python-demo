#!/usr/bin/env zsh
# https://github.com/influxdata/influxdb/blob/1.8/services/udp/README.md
# sudo sysctl -w net.core.rmem_max=26214400
# sudo sysctl -w net.core.rmem_default=26214400
# docker run --rm influxdb:1.8 influxd config > influxdb-configv1.conf

mkdir -p /data2/influxdb-docker-data-volume-v1/
sudo rm -rf /data2/influxdb-docker-data-volume-v1/*
docker run --rm \
    --name influxdbv1 \
    -p 8086:8086 \
    -p 8089:8089 \
    -p 8189:8189 \
    -v /data2/influxdb-docker-data-volume-v1:/var/lib/influxdb \
    -v ${0:A:h}/influxdb-configv1.conf:/etc/influxdb/influxdb.conf:ro \
    -e INFLUXDB_DB=aws \
    -e INFLUXDB_ADMIN_USER=smprofiler \
    -e INFLUXDB_ADMIN_PASSWORD=smprofiler \
    influxdb:1.8 -config /etc/influxdb/influxdb.conf

# docker run -p 8086:8086 \
#       -v $PWD/influxdb.conf:/etc/influxdb/influxdb.conf:ro \
#       influxdb:1.8 -config /etc/influxdb/influxdb.conf
