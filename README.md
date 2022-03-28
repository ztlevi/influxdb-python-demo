
https://docs.influxdata.com/influxdb/v2.1/install/?t=CLI+Setup#configure-influxdb-with-docker

https://hub.docker.com/_/influxdb/

``` sh
python write-parquet.py data/2020-01-ppd42ns.snappy.parquet
```

``` flux
from(bucket: "aws")
|> range(start: 0)
|> count(column: "_value")
|> limit(n: 10)
```

``` sh
curl --get http://localhost:8086/query \
  --header "Authorization: Token my-super-secret-auth-token" \
  --data-urlencode "db=aws" \
  --data-urlencode "q=SELECT COUNT(*) from aws"

  --data-urlencode "q=SELECT * FROM mem WHERE host=host1;SELECT mean(used_percent) FROM mem WHERE host=host1 GROUP BY time(10m)"
```
