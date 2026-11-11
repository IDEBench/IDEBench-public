#!/bin/bash
# Load generated flights CSV files into ClickHouse database idebench_<size>.
# Usage: clickhouse/load.sh <size> <file.csv>...
#   e.g. clickhouse/load.sh 500k dataset.csv
#        clickhouse/load.sh 1b gen/part_*.csv
# A header line (as written by datagen.py) is detected automatically.
# Set CH to change the client command, e.g. CH="clickhouse client --host myhost".
set -e
CH="${CH:-clickhouse client}"
SIZE=$1
shift
sed "s/{size}/$SIZE/g" "$(dirname "$0")/schema.sql" | $CH --multiquery
for f in "$@"; do
    $CH --query "INSERT INTO idebench_$SIZE.tbl_flights FORMAT CSV" < "$f"
    echo "loaded $f"
done
$CH --query "SELECT 'idebench_$SIZE', count() FROM idebench_$SIZE.tbl_flights"
