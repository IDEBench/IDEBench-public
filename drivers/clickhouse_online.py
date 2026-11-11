# ClickHouse driver with online (deadline-bounded, approximate) execution; see drivers/clickhouse.py.
# A separate module because IDEBench distinguishes results by driver name, not by driver-args.
from drivers.clickhouse import IDEBenchDriver as ExactDriver


class IDEBenchDriver(ExactDriver):
    mode = "online"
