import datetime, time
import itertools
import psycopg2
import decimal
import os
import multiprocessing
from multiprocessing import Queue
from common import util

class IDEBenchDriver:

    def init(self, options, schema, driver_arg):
        pass

    def workflow_start(self):
        print("workflow start")
        pass

    def workflow_end(self):
        os.system("/usr/local/pgsql/bin/pg_ctl stop -D ~/xdb_data")
        os.system('sudo -b bash -c "echo 1 > /proc/sys/vm/drop_caches"')
        os.system("/usr/local/pgsql/bin/pg_ctl start -D ~/xdb_data")   
        pass
        
    def can_execute_online(self, sql_statement):
        return (not " or " in sql_statement.lower()) and (not " AVG(" in sql_statement)

    def create_connection(self, timeout=None):
        ip = "localhost"
        if timeout is None:
            connection = psycopg2.connect("dbname='idebench' port=45001 user='test' host='%s' password='test'" % (ip))
        else:
            connection = psycopg2.connect("dbname='idebench' port=45001 user='test' host='%s' password='test' options='-c statement_timeout=%i'" % (ip, timeout))
        cursor = connection.cursor()
        return connection, cursor

    def process_request(self, viz_request, options, schema, out_q):
        print("processsing..." + str(viz_request.operation_id))
        if viz_request.viz.binning:
            sql_statement = viz_request.viz.get_computed_filter_as_sql(schema)
            sql_statement = sql_statement.replace(schema.get_fact_table_name(), "%s_%s%s" %  (schema.get_fact_table_name(), options.settings_size, "n" if options.settings_normalized else "") )
            if self.can_execute_online(sql_statement):
                sql_statement = sql_statement.replace("SELECT ", "SELECT ONLINE ")
                sql_statement += " WITHTIME %s CONFIDENCE 95" % options.settings_time_requirement
                sql_statement += " REPORTINTERVAL %s;" % options.settings_time_requirement
                connection, cursor = self.create_connection(options.settings_time_requirement + 20)
            else:
                connection, cursor = self.create_connection(options.settings_time_requirement)
            viz_request.start_time = util.get_current_ms_time()
            try:        
                cursor.execute(sql_statement)
            except psycopg2.extensions.QueryCanceledError as qce:
                viz_request.result = {}
                viz_request.margins = {}
                viz_request.timedout = True
                viz_request.end_time = util.get_current_ms_time()
                out_q.put(viz_request)  
                return
                
            data = cursor.fetchall() 
            viz_request.end_time = util.get_current_ms_time()
            connection.close()
            
            results = {}
            margins = {}
            for row in data:
                keys = []

                if row[0] is None:
                    continue

                startindex = 3 if self.can_execute_online(sql_statement) else 0

                for i, bin_desc in enumerate(viz_request.viz.binning):                        
                    if "width" in bin_desc:
                        bin_width = bin_desc["width"]
                        keys.append(str(int(row[i+startindex])))  
                    else:
                        keys.append(str(row[startindex+i]).strip())

                key = ",".join(keys)
                
                row = list(row)
                for i,r in enumerate(row):
                    if isinstance(r, decimal.Decimal):
                        row[i] = float(r)
                if startindex == 3:
                    results[key] = row[len(viz_request.viz.binning)+startindex:-1]
                else:
                    results[key] = row[len(viz_request.viz.binning)+startindex:]

                if self.can_execute_online(sql_statement) and startindex == 3:
                    margins[key] = row[len(row)-1:]

            viz_request.result = results
            viz_request.margins = margins
            out_q.put(viz_request)               
            print("delivering...")
