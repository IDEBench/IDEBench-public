import pymonetdb
import datetime, time
import itertools
import csv
import json
import os
import multiprocessing
from subprocess import call
from common import util

class IDEBenchDriver:

    def init(self, options, schema, driver_arg):
        pass

    def create_connection(self):
        connection = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", port=50000, database="demo")
        cursor = connection.cursor()
        return connection, cursor

    def process_request(self, viz_request, options, schema, result_queue):
        print("processsing..." + str(viz_request.operation_id))
        viz = viz_request.viz
        sql_statement = viz.get_computed_filter_as_sql(schema)
        connection, cursor = self.create_connection()
        viz_request.start_time = util.get_current_ms_time()                
        cursor.execute(sql_statement)                
        data = cursor.fetchall() 
        viz_request.end_time = util.get_current_ms_time()
        connection.close()

        results = {}
        for row in data:
            keys = []
            for i, bin_desc in enumerate(viz.binning):
                
                if "width" in bin_desc:
                    bin_width = bin_desc["width"]                    
                    keys.append(str(int(row[i])))
                else:
                    keys.append(str(row[i]))

            key = ",".join(keys)
            results[key] = row[len(viz.binning):]
        
        viz_request.result = results
        result_queue.put(viz_request)

    def workflow_start(self):        
        # clear cache here
        pass  

    def workflow_end(self):        
        pass            
