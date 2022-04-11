#!/usr/bin/env zsh

# -v ${0:A:h}/influxdb-configv2.yml:/etc/influxdb2/config.yml \
rm -rf /data2/influxdb-docker-data-volume/*
docker run --rm \
    --name influxdb \
    -p 8086:8086 \
    --volume /data2/influxdb-docker-data-volume:/var/lib/influxdb2 \
    -e DOCKER_INFLUXDB_INIT_MODE=setup \
    -e DOCKER_INFLUXDB_INIT_USERNAME=smprofiler \
    -e DOCKER_INFLUXDB_INIT_PASSWORD=smprofiler \
    -e DOCKER_INFLUXDB_INIT_ORG=aws \
    -e DOCKER_INFLUXDB_INIT_BUCKET=aws \
    -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=my-super-secret-auth-token \
    influxdb:2.1.1


