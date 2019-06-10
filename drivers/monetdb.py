import pymonetdb
import datetime, time
import itertools
import csv
import json
import os
import multiprocessing
import logging
from subprocess import call
from common import util

logger = logging.getLogger("idebench")
class IDEBenchDriver:

    def init(self, options, schema, driver_arg):
        self.connection = None
        pass

    def create_connection(self):
        if not self.connection:
            self.connection = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", port=50000, database="crossfilter")
        cursor = self.connection.cursor()
        return self.connection, cursor

    def process_request(self, viz_request, options, schema, result_queue):
        viz = viz_request.viz
        sql_statement = viz.get_computed_filter_as_sql(schema)
        connection, cursor = self.create_connection()
        viz_request.start_time = util.get_current_ms_time()                
        logger.info(sql_statement)
        cursor.execute(sql_statement)                
        data = cursor.fetchall() 
        viz_request.end_time = util.get_current_ms_time()
        
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
