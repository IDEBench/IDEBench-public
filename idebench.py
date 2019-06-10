import importlib
import json
import csv
import time
import hashlib
import multiprocessing
import statistics
from collections import OrderedDict
import numpy as np
import os
from common.schema import Schema
from common.vizgraph import VizGraph
from common.vizrequest import VizRequest
from common.operation import Operation
from optparse import OptionParser
from scipy import spatial
import glob
from os.path import basename
import logging
from evaluator import Evaluator
from common import util

logging.basicConfig(filename='output.log', level=logging.INFO)
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger = logging.getLogger("idebench")
logger.addHandler(consoleHandler)

class IDEBench:

    result_queue = multiprocessing.Queue()
    def __init__(self):

        parser = OptionParser()
        parser.add_option("--driver-name", dest="driver_name", action="store", help="Driver name")
        parser.add_option("--driver-create-storage", dest="driver_create_storage", action="store_true", help="Calls create_storage on driver", default=False)
        parser.add_option("--driver-clear-storage", dest="driver_clear_storage", action="store_true", help="Calls clear_storage on driver", default=False)
        parser.add_option("--driver-clear-cache", dest="driver_clear_cache", action="store_true", help="Calls clear_cache on driver", default=False)
        parser.add_option("--driver-args", dest="driver_args", action="store", help="Arguments to pass to the driver", default="")

        parser.add_option("--settings-normalized", dest="settings_normalized", action="store_true", help="Whether joins should be used", default=False)
        parser.add_option("--settings-dataset", dest="settings_dataset", action="store", help="Name of the dataset")
        parser.add_option("--settings-size", dest="settings_size", default="", action="store", help="Number of rows in the dataset")
        parser.add_option("--settings-thinktime", dest="settings_thinktime", type="int", action="store", help="Think-time in seconds between two executions", default=1000)
        parser.add_option("--settings-time-requirement", dest="settings_time_requirement", action="store", help="The Time requirement to be used", default=1000)
        parser.add_option("--settings-confidence-level", dest="settings_confidence_level", action="store", help="The confidence level to be used", default=95)
        parser.add_option("--settings-workflow", dest="settings_workflow", action="store", help="The workflow file to be used")
        
        parser.add_option("--evaluate", dest="evaluate", action="store_true", help="Size of the dataset in MB", default=False)
        parser.add_option("--create--full-report", dest="create_report", action="store_true", help="Merges all reports in the reports directory into a single file", default=False)
        parser.add_option("--run", dest="run", action="store_true", help="Flag to run the benchmark without config file", default=False)
        parser.add_option("--run-config", dest="config", action="store", help="Flag to run the benchmark with the specified config file")
        parser.add_option("--groundtruth", dest="groundtruth", action="store_true", help="If set computes the ground-truth for the specified workflow", default=False)

        (self.options, args) = parser.parse_args()
        
        self.workflow_start_time = -1

        self.evaluator = Evaluator(self.get_config_hash())

        if not self.options.config:
            
            if self.options.create_report:
                self.evaluator.create_report()
                return

            if not self.options.driver_name:
                parser.error("No driver name specified.")

            if not self.options.settings_dataset:
                parser.error("No dataset specified.")

            if not self.options.settings_size:
                print("Warning: No dataset size specified.")

            if self.options.groundtruth or self.options.run:
                self.setup()

            if self.options.groundtruth:

                self.options.think_time = 1
                self.options.time_requirement = 999999

                workflow_files = glob.glob("data/" + self.options.settings_dataset + "/workflows/*.json") 

                for workflow_file in workflow_files:
                    self.options.settings_workflow = basename(workflow_file).split(".")[0]
                    self.run()
            
            elif self.options.run:
                
                if not self.options.settings_workflow:
                    parser.error("No workflow specified.")
                
                self.run()
            elif self.options.evaluate:
                pass
                self.evaluator.evaluate(self.get_config_hash())
        else:

            with open(self.options.config) as f:
                config = json.load(f)
                assure_path_exists("./results")
                for d in config["settings-datasets"]:
                    assure_path_exists("./data/%s/groundtruths" % d)

                # TODO: create pairs instead
                for dataset in config["settings-datasets"]:
                    self.options.settings_dataset = dataset
                    
                    for driver_name in config["driver-names"]:
                        for driver_arg in config["driver-args"]:                        

                            self.options.driver_name = driver_name
                            self.setup(driver_arg)                        

                            for size in config["settings-sizes"]:
                                for workflow in config["settings-workflows"]:
                                    for thinktime in config["settings-thinktimes"]:
                                        for time_requirement in config["settings-time-requirements"]:
                                            for confidence_level in config["settings-confidence-levels"]:
                                            
                                                self.options.driver_name = driver_name
                                                
                                                self.options.settings_size = size
                                                self.options.settings_workflow = workflow
                                                self.options.settings_thinktime = thinktime
                                                self.options.settings_time_requirement = time_requirement
                                                self.options.settings_confidence_level = confidence_level
                                                self.options.settings_normalized = config["settings-normalized"]
                                                self.options.groundtruth = config["groundtruth"] if "groundtruth" in config else False
                                                self.options.run = config["run"] if "run" in config else True
                                                self.options.evaluate = config["evaluate"] if "evaluate" in config else True

                                                if self.options.run:
                                                    self.run()

                                                if self.options.evaluate:
                                                    self.evaluator.evaluate(self.get_config_hash())

    def setup(self, driver_arg = None):
        logger.info("loading schema")
        with open(self.get_schema_path()) as f:
            self.schema = Schema(json.load(f), self.options.settings_normalized)

        logger.info("loading driver")
        module = importlib.import_module("drivers." +  self.options.driver_name)
        self.driver = getattr(module, "IDEBenchDriver")()

        logger.info("initializing driver")
        try:
            self.driver.init(self.options, self.schema, driver_arg)
        except AttributeError:
            pass

    def run(self):

        self.vizgraph = VizGraph()
        with open(self.get_workflow_path()) as f:
            json_data = json.load(f)
            for s in json_data["setup"]:
                self.vizgraph.add_viz(s)

            for s in json_data["setup"]:
               self.vizgraph.apply_interaction(Operation(s))

            self.workflow_interactions = json_data["interactions"]

        self.operation_results = OrderedDict({ "args": vars(self.options), "results": OrderedDict() })
        self.current_interaction_index = 0
        self.current_vizrequest_index = 0

        try:
            logger.info("calling 'workflow_start' on driver")
            self.driver.workflow_start()
        except AttributeError:
            pass
        
        interaction_index = 0
        while interaction_index < len(self.workflow_interactions):
            self.process_interaction(interaction_index)
            interaction_index +=1
       
        self.end_run()


    def end_run(self):
        logger.info("done processing interactions")
        try:
            logger.info("calling 'workflow_end' on driver")
            self.driver.workflow_end()
        except AttributeError:
            pass

        path = "results/%s.json" % (self.get_config_hash())
        
        if not self.options.groundtruth:
            logger.info("saving results to %s" % path)
            with open(path, "w") as fp:
                json.dump(self.operation_results, fp, indent=4)

        if self.options.groundtruth:
            path = "data/%s/groundtruths/%s_%s.json" % (self.options.settings_dataset, self.options.settings_size, self.options.settings_workflow)
            with open(path, "w") as fp:
                json.dump(self.operation_results, fp, indent=4)
    
    def process_interaction(self, interaction_index):
        logger.info("interaction %i" % interaction_index)
        interaction = self.workflow_interactions[interaction_index]
        next_interaction = self.workflow_interactions[interaction_index + 1] if interaction_index +1 < len(self.workflow_interactions) else None
        vizs_to_request = self.vizgraph.apply_interaction(Operation(interaction))
        expected_start_time = interaction["time"]
        print(expected_start_time)
        
        viz_requests = []
        for viz in vizs_to_request:
            viz_requests.append(VizRequest(self.current_vizrequest_index, self.current_interaction_index, expected_start_time, viz))
            self.current_vizrequest_index += 1

        # TODO: document this feature
        try:
            self.driver.before_requests(self.options, self.schema, IDEBench.result_queue)
        except AttributeError:
            pass

        procs = []
        nprocs = len(viz_requests)
        if hasattr(self.driver, "use_single_process") and self.driver.use_single_process:
            for viz_request in viz_requests:
                self.driver.process_request(viz_request, self.options, self.schema, IDEBench.result_queue)
        else:
            for viz_request in viz_requests:
                proc = multiprocessing.Process(target=self.driver.process_request, args=(viz_request, self.options, self.schema, IDEBench.result_queue))
                procs.append(proc)
                proc.start()
 
        resultlist = []
        for i in range(nprocs):
            resultlist.append(IDEBench.result_queue.get())           

        for proc in procs:
            proc.join()
        
        if "time" in interaction and next_interaction:
            think_time = next_interaction["time"] - interaction["time"]
        else:
            think_time = self.options.settings_thinktime
        logger.info("interaction delay: %i ms" % think_time)

        if not self.options.groundtruth:
            time.sleep(think_time / 1000)
            
        self.deliver_viz_request(resultlist)
        self.current_interaction_index += 1

    def deliver_viz_request(self, viz_requests):


        if len(self.operation_results["results"]) == 0 :
            self.workflow_start_time = sorted(viz_requests, key=lambda x: x.operation_id)[0].start_time
            
        for viz_request in viz_requests:            
            operation_result = {}
            operation_result["id"] = viz_request.operation_id
            operation_result["sql"] = viz_request.viz.get_computed_filter_as_sql(self.schema)
            operation_result["viz_name"] = viz_request.viz.name
            operation_result["event_id"] = viz_request.parent_operation_id
            operation_result["expected_start_time"] = viz_request.expected_start_time
            operation_result["start_time"] = viz_request.start_time - self.workflow_start_time
            operation_result["end_time"] = viz_request.end_time - self.workflow_start_time
            operation_result["time_violated"] = viz_request.timedout
            #operation_result["t_pause"] = viz_request.t_pause
            #operation_result["t_start"] = viz_request.t_start
            operation_result["progress"] = viz_request.progress
            operation_result["output"] = viz_request.result
            operation_result["margins"] = viz_request.margins
            operation_result["num_binning_dimensions"] = len(viz_request.viz.binning)
            operation_result["num_aggregates_per_bin"] = len(viz_request.viz.per_bin_aggregates)
  
            bin_types = []
            for viz_bin in viz_request.viz.binning:
                if "width" in viz_bin:
                    bin_types.append("quantitative")
                else:
                    bin_types.append("nominal")
            operation_result["binning_type"] = "_".join(sorted(bin_types))

            agg_types = []
            for viz_agg in viz_request.viz.per_bin_aggregates:
                if viz_agg["type"] == "count":
                    agg_types.append("count")
                elif viz_agg["type"] == "avg":
                    agg_types.append("avg")
                else:
                    raise Exception()
            operation_result["aggregate_type"] = "_".join(sorted(agg_types))

            if not viz_request.operation_id in self.operation_results:
                self.operation_results["results"][viz_request.operation_id] = operation_result
            
            viz_request.delivered = True
    
    def get_config_hash(self):
        o = self.options
        h = (o.driver_name, o.settings_dataset, o.settings_workflow, o.settings_size, o.settings_normalized, o.settings_confidence_level, o.settings_thinktime, o.settings_thinktime, o.settings_time_requirement)
        return hashlib.md5(str(h).encode('utf-8')).hexdigest()

    def get_schema_path(self):
        return "data/%s/sample.json" % (self.options.settings_dataset)

    def get_workflow_path(self):
        return "data/%s/workflows/%s.json" % (self.options.settings_dataset, self.options.settings_workflow)   


def assure_path_exists(path):
    d = os.path.dirname(path)
    if not os.path.exists(d):
        os.makedirs(d)

if __name__ == '__main__':  
    IDEBench()

