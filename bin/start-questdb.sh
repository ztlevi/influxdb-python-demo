#!/usr/bin/env bash

mkdir -p /data2/questdb-docker-data-volume/
sudo rm -rf /data2/questdb-docker-data-volume/*

docker run --rm -p 9000:9000 \
 -p 9009:9009 \
 -p 8812:8812 \
 -p 9003:9003 \
 -v "/data2/questdb-docker-data-volume:/root/.questdb/" questdb/questdb
