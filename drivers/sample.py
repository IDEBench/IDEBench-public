import time
from common import util

class IDEBenchDriver:

    def init(self, options, schema, driver_arg):
        pass

    def workflow_start(self):
        pass

    def workflow_end(self):
        pass

    def process_request(self, viz_request, options, schema, result_queue):
        
        # record start time
        viz_request.start_time = util.get_current_ms_time() 
        
        # print SQL translation of request and simulate query execution
        # print(viz_request.viz.get_computed_filter_as_sql(schema))
        time.sleep(0.1)

        # record end time
        viz_request.end_time = util.get_current_ms_time() 

        # write an empty result to the viz_request
        viz_request.result = {}

        # notify IDEBench that processing is done by writing it to the result buffer
        result_queue.put(viz_request)