#!/bin/bash
# Timed IDEBench runs on ClickHouse, one dataset size at a time, with the table prewarmed
# (a full scan of all columns) before each size so that cache state does not depend on run order.
# Execute from the IDEBench root directory after ground truths are computed.
PY=${PY:-python3}
CH="${CH:-clickhouse client --port 39000}"
mkdir -p results/configs

(while true; do echo "$(date +%FT%T) $(cut -d' ' -f1-3 /proc/loadavg)"; sleep 10; done) > results/loadavg.log &
LOADAVG_PID=$!
trap 'kill $LOADAVG_PID' EXIT

prewarm() {
    $CH --time -q "SELECT sum(YEAR_DATE), sum(length(UNIQUE_CARRIER) + length(ORIGIN) + length(ORIGIN_STATE_ABR) + length(DEST) + length(DEST_STATE_ABR)), sum(DEP_DELAY + TAXI_OUT + TAXI_IN + ARR_DELAY + AIR_TIME + DISTANCE) FROM idebench_$1.tbl_flights FORMAT Null"
}

make_config() {  # size, workflows (python list expression), output
    $PY - "$1" "$2" "$3" <<'EOF'
import json, sys
size, workflows, out = sys.argv[1], eval(sys.argv[2]), sys.argv[3]
json.dump({"driver-names": ["clickhouse", "clickhouse_online"], "driver-args": [[]], "settings-datasets": ["flights"],
           "settings-workflows": workflows, "settings-sizes": [size], "settings-thinktimes": [1000],
           "settings-time-requirements": [500, 1000, 3000, 5000, 10000], "settings-confidence-levels": [95],
           "settings-normalized": False}, open(out, "w"), indent=4)
EOF
}

# Main experiment: 10 mixed workflows at each size.
for size in 100m 500m 1b; do
    make_config $size '["mixed_%d" % i for i in range(10)]' results/configs/mixed_$size.json
    prewarm $size
    echo "$(date +%FT%T) start mixed $size"
    $PY idebench.py --run-config results/configs/mixed_$size.json > results/mixed_$size.log 2>&1 || echo "FAILED mixed $size"
done

# Workflow types at 500M: 5 workflows each of independent, sequential, 1:N, N:1.
make_config 500m '["%s_%d" % (t, i) for t in ["independent", "sequential", "1n", "n1"] for i in range(5)]' results/configs/types_500m.json
prewarm 500m
echo "$(date +%FT%T) start types 500m"
$PY idebench.py --run-config results/configs/types_500m.json > results/types_500m.log 2>&1 || echo "FAILED types 500m"
echo "$(date +%FT%T) all done"
