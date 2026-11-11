import json
import os
import time
import urllib.parse
import urllib.request
import urllib.error
from common import util

# IDEBench driver for ClickHouse (HTTP interface, Python standard library only).
#
# Two drivers share this code:
#   clickhouse:        exact, blocking execution. A query exceeding the time requirement is cancelled
#                      by the server (max_execution_time) and reported as violated, with no result.
#   clickhouse_online: approximate execution. Each query reads at most a budget of rows, sized from
#                      the scan throughput measured at the start of each workflow so that the query
#                      completes within the time requirement; the aggregate over the rows read is
#                      returned, with COUNTs scaled by total_rows / read_rows. This is a valid estimate
#                      because the rows produced by datagen.py are i.i.d. The time requirement is still
#                      enforced as in the exact driver.
#
# driver-args: [] or [url], the URL of the ClickHouse HTTP interface. Without driver-args (e.g. on the
# command line), the URL is taken from the CLICKHOUSE_URL environment variable, or http://localhost:8123/.
# The dataset size selects the database: idebench_<size>, e.g. idebench_500m.tbl_flights.

DEFAULT_URL = "http://localhost:8123/"

# Fraction of the time requirement that the online driver budgets for reading data;
# the rest is headroom for concurrent requests of the same interaction and for the client.
ONLINE_TIME_FRACTION = 0.5
CALIBRATION_ROWS = 20000000
CALIBRATION_RUNS = 3


class IDEBenchDriver:

    mode = "exact"

    def init(self, options, schema, driver_arg):
        self.options = options
        self.schema = schema
        self.url = driver_arg[0] if driver_arg else os.environ.get("CLICKHOUSE_URL", DEFAULT_URL)
        if not self.url.endswith("/"):
            self.url += "/"
        self.rows_per_ms = None

    def database(self, options):
        return "idebench_" + options.settings_size.lower()

    def workflow_start(self):
        if self.mode == "online" and not self.options.groundtruth:
            self.calibrate()

    def workflow_end(self):
        pass

    def query(self, database, sql, settings):
        params = dict(settings)
        params["database"] = database
        params["output_format_json_quote_64bit_integers"] = 0
        params["wait_end_of_query"] = 1
        url = self.url + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, data=(sql + " FORMAT JSONCompact").encode("utf-8"), method="POST")
        try:
            with urllib.request.urlopen(req, timeout=3600) as resp:
                summary = json.loads(resp.headers.get("X-ClickHouse-Summary", "{}"))
                return json.loads(resp.read()), summary, None
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            if "TIMEOUT_EXCEEDED" in body:
                return None, None, "timeout"
            raise Exception("query failed: %s\n%s" % (sql, body))

    def calibrate(self):
        # Measure end-to-end scan throughput of a typical histogram query on the current table.
        sql = "SELECT FLOOR(DEP_DELAY/10.0) AS bin, COUNT(*) FROM %s GROUP BY bin" % self.schema.get_fact_table_name()
        settings = {"max_rows_to_read": CALIBRATION_ROWS, "read_overflow_mode": "break"}
        rates = []
        for _ in range(CALIBRATION_RUNS):
            start = time.monotonic()
            _, summary, _ = self.query(self.database(self.options), sql, settings)
            elapsed_ms = (time.monotonic() - start) * 1000.0
            rates.append(int(summary["read_rows"]) / max(elapsed_ms, 1.0))
        self.rows_per_ms = sorted(rates)[len(rates) // 2]

    def process_request(self, viz_request, options, schema, result_queue):
        viz = viz_request.viz
        sql = viz.get_computed_filter_as_sql(schema)
        tr_ms = float(options.settings_time_requirement)

        settings = {}
        if not options.groundtruth:
            settings["max_execution_time"] = tr_ms / 1000.0
            settings["timeout_overflow_mode"] = "throw"
            if self.mode == "online":
                settings["max_rows_to_read"] = max(1, int(self.rows_per_ms * tr_ms * ONLINE_TIME_FRACTION))
                settings["read_overflow_mode"] = "break"

        viz_request.start_time = util.get_current_ms_time()
        data, summary, err = self.query(self.database(options), sql, settings)
        viz_request.end_time = util.get_current_ms_time()

        results = {}
        progress = 1.0
        if err is None:
            scale = 1.0
            read_rows = int(summary.get("read_rows", 0))
            total_rows = int(summary.get("total_rows_to_read", 0))
            if self.mode == "online" and 0 < read_rows < total_rows:
                scale = total_rows / read_rows
                progress = read_rows / total_rows
            for row in data["data"]:
                keys = []
                for i, bin_desc in enumerate(viz.binning):
                    if "width" in bin_desc:
                        keys.append(str(int(row[i])))
                    else:
                        keys.append(str(row[i]))
                values = []
                for j, agg in enumerate(viz.per_bin_aggregates):
                    v = float(row[len(viz.binning) + j])
                    values.append(v * scale if agg["type"] == "count" else v)
                results[",".join(keys)] = values

        if options.groundtruth:
            viz_request.timedout = False
        else:
            viz_request.timedout = err is not None or (viz_request.end_time - viz_request.start_time) > tr_ms
            if viz_request.timedout:
                results = {}
                progress = 0.0

        viz_request.progress = progress
        viz_request.result = results
        result_queue.put(viz_request)
