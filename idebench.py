import importlib
import json
import csv
import time
import hashlib
import multiprocessing
import statistics
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

        if not self.options.config:
            
            if self.options.create_report:
                self.create_report()
                return

            if not self.options.driver_name:
                parser.error("No driver name specified.")

            if not self.options.settings_dataset:
                parser.error("No dataset specified.")

            if not self.options.settings_size:
                print("Warning: No dataset size specified.")

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
                self.evaluate(self.get_config_hash())
        else:

            with open(self.options.config) as f:
                config = json.load(f)
                assure_path_exists("./results")
                for d in config["settings-datasets"]:
                    assure_path_exists("./data/%s/groundtruths" % d)

                # TODO: create pairs instead
                for driver_name in config["driver-names"]:
                    for dataset in config["settings-datasets"]:
                        for size in config["settings-sizes"]:
                            for workflow in config["settings-workflows"]:
                                for thinktime in config["settings-thinktimes"]:
                                    for time_requirement in config["settings-time-requirements"]:
                                        for confidence_level in config["settings-confidence-levels"]:
                                            self.options.driver_name = driver_name
                                            self.options.settings_dataset = dataset
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
                                                self.evaluate(self.get_config_hash())


    def run(self):
        with open(self.get_schema_path()) as f:
            self.schema = Schema(json.load(f), self.options.settings_normalized)
       
        module = importlib.import_module("drivers." +  self.options.driver_name)
        self.driver = getattr(module, "IDEBenchDriver")()

        try:
            self.driver.clear_cache()
        except AttributeError:
            pass
        

        with open(self.get_workflow_path()) as f:
            self.workflow_interactions = json.load(f)["interactions"]

        self.vizgraph = VizGraph()
        self.operation_results = { "args": vars(self.options), "results": {} }
        self.current_interaction_index = 0
        self.current_vizrequest_index = 0
        self.process_interaction(0)

    def end_run(self):

        try:
            self.driver.workflow_end()
        except AttributeError:
            pass

        path = "results/%s.json" % (self.get_config_hash())
        
        if not self.options.groundtruth:
            with open(path, "w") as fp:
                json.dump(self.operation_results, fp)

        if self.options.groundtruth:
            path = "data/%s/groundtruths/%s_%s.json" % (self.options.settings_dataset, self.options.settings_size, self.options.settings_workflow)
            with open(path, "w") as fp:
                json.dump(self.operation_results, fp)
    
    def process_interaction(self, interaction_index):
        print("processing!")
        if interaction_index < 0 or interaction_index >= len(self.workflow_interactions):
            print("reached end of interactions")
            self.end_run()
            return

        print("thinking...")
        time.sleep(self.options.settings_thinktime / 1000)

        interaction = self.workflow_interactions[interaction_index]
        vizs_to_request = self.vizgraph.apply_interaction(Operation(interaction))

        viz_requests = []
        for viz in vizs_to_request:
            viz_requests.append(VizRequest(self.current_vizrequest_index, self.current_interaction_index, viz))
            self.current_vizrequest_index += 1

        #if interaction_index == 0:
        #    self.result_queue = multiprocessing.Queue()

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
            
        self.deliver_viz_request(resultlist)
        self.current_interaction_index += 1
        self.process_interaction(self.current_interaction_index)


    def deliver_viz_request(self, viz_requests):

        for viz_request in viz_requests:
            if len(viz_request.result.keys()) == 0:
                pass
                #print(" ############## No results delivered! #############")
            
            operation_result = {}
            operation_result["id"] = viz_request.operation_id
            operation_result["sql"] = viz_request.viz.get_computed_filter_as_sql(self.schema)
            operation_result["viz_name"] = viz_request.viz.name
            operation_result["parent_operation_id"] = viz_request.parent_operation_id
            operation_result["start_time"] = viz_request.start_time
            operation_result["end_time"] = viz_request.end_time
            operation_result["time_violated"] = viz_request.timedout
            operation_result["t_pause"] = viz_request.t_pause
            operation_result["t_start"] = viz_request.t_start
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

        #self.driver.request_vizs(self.viz_requests)
    
    def get_config_hash(self):
        o = self.options
        h = (o.driver_name, o.settings_dataset, o.settings_workflow, o.settings_size, o.settings_normalized, o.settings_confidence_level, o.settings_thinktime, o.settings_thinktime, o.settings_time_requirement)
        return hashlib.md5(str(h).encode('utf-8')).hexdigest()

    def get_schema_path(self):
        return "data/%s/sample.json" % (self.options.settings_dataset)

    def get_workflow_path(self):
        return "data/%s/workflows/%s.json" % (self.options.settings_dataset, self.options.settings_workflow)

    def compute_viz_similarity(self, viz_gt, viz):

        if len(viz.keys()) == 0 and len(viz_gt.keys()) == 0:
            return 1

        if len(viz_gt.keys()) == 0 and len(viz.keys()) > 0:
            raise Exception()

        if len(viz_gt.keys()) > 0 and len(viz.keys()) == 0:
            return 0

        for gt_key in viz_gt.keys():
            if gt_key not in viz:
                viz[gt_key] = 0

        viz_gt_vals = []
        viz_vals = []
        for gt_key in viz_gt.keys():
            if isinstance(viz_gt[gt_key], list):
                viz_gt_vals.append(viz_gt[gt_key][0])
            else:
                viz_gt_vals.append(viz_gt[gt_key])
            
            if isinstance(viz[gt_key], list):
                viz_vals.append(viz[gt_key][0])
            else:
                viz_vals.append(viz[gt_key])
     
        viz_gt_vals = np.array(viz_gt_vals).astype(float)
        viz_vals = np.array(viz_vals).astype(float)
        


        #viz_gt_vals = self.normalize(viz_gt_vals)
        #viz_vals = self.normalize(viz_vals)

        if np.isnan(viz_gt_vals).any():
            raise Exception()

        if np.isnan(viz_vals).any():
            raise Exception()


        #score = np.dot(viz_gt_vals, viz_vals)/ ( np.sqrt(np.sum(np.square(viz_gt_vals))) * np.sqrt(np.sum(np.square(viz_vals))) )
        np.seterr(all='raise')
        try:
            score = 1 - spatial.distance.cosine(viz_gt_vals, viz_vals)
        except:
            return 0
        return score if not np.isnan(score) else 0
        
    def normalize(self, v):
        norm=np.linalg.norm(v, ord=1)
        if norm==0:
            norm=np.finfo(v.dtype).eps
        return v/norm

    def evaluate(self, config_hash):
        print("evaluate")
        result_json = None
        try:
            with open("results/%s.json" % config_hash, "r") as json_data:
                result_json = json.load(json_data)
        except:
            print("couldn't load file %s" % ("results/%s.json" % config_hash))
            return
        
        workflow = result_json["args"]["settings_workflow"]
        dataset = result_json["args"]["settings_dataset"]
        size = result_json["args"]["settings_size"]
        time_requirement = result_json["args"]["settings_time_requirement"]
        
        with open("data/%s/groundtruths/%s_%s.json" % (dataset, size, workflow), "r") as json_data:
            groundtruths = json.load(json_data)["results"]
        
        with open("reports/%s.csv" % config_hash, 'w') as fp:
            w = csv.DictWriter(fp, [
                                    "operation_id",
                                    "config_hash",
                                    "interaction_id",
                                    "dataset",
                                    "size",
                                    "viz_name",
                                    "interface",
                                    "think_time",
                                    "time_requirement",
                                    "t_start",
                                    "t_pause",
                                    "workflow",
                                    "start_time",
                                    "end_time",
                                    "duration",
                                    "progress",
                                    "time_violated",
                                    "num_binning_dimensions",
                                    "binning_type",                                    
                                    "has_invalid_bins",
                                    "num_bins_out_of_margin",
                                    "num_bins_delivered",
                                    "num_bins_in_gt",
                                    "missing_bins",
                                    "dissimilarity",
                                    "num_aggregates_per_bin",                                    
                                    "aggregate_type",
                                    "bias",
                                    "rel_error_avg",
                                    "rel_error_stdev",
                                    "rel_error_min",
                                    "rel_error_max",
                                    "margin_avg",
                                    "margin_stdev",
                                    "margin_min",
                                    "margin_max",
                                    "margin_ratio"], delimiter=",", lineterminator="\n")
            w.writeheader()

            operations = result_json["results"]


            for op_number in operations.keys():
                
                gt_output = groundtruths[op_number]["output"]
                operation = operations[op_number]

                margins = []
                rel_errors = []
                forecast_values = []
                actual_values = []
                out_of_margin_count = 0

                for gt_bin_identifier, gt_aggregate_results in gt_output.items():                    

                    if gt_bin_identifier in operation["output"]:
           
                        for agg_bin_result_index, agg_bin_result in enumerate(operation["output"][gt_bin_identifier]):
                            rel_error = None
                            op_result = operation["output"][gt_bin_identifier][agg_bin_result_index]
                            gt_result = gt_aggregate_results[agg_bin_result_index]

                            if abs(gt_result) > 0:
                                rel_error = abs(op_result - gt_result)/abs(gt_result)
                                if rel_error > 1e-5:
                                    pass
                                rel_errors.append(rel_error)
                            else:
                                print("ignoring zero in groundtruth")
                                #print(result_json)
                                #os._exit(1)
                           
                            forecast_values.append(op_result)
                            actual_values.append(gt_result)
                            
                            if operation["margins"] and gt_bin_identifier in operation["margins"]:
                                op_margin = float(operation["margins"][gt_bin_identifier][agg_bin_result_index])
          
                                if np.isnan(op_margin) or np.isinf(op_margin) or abs(op_margin) > 1000000:
                                    #print(op_margin)
                                    if os.path.exists("./margin_errors"):
                                        append_write = 'a' # append if already exists
                                    else:
                                        append_write = 'w' # make a new file if not
                                    with open("./margin_errors", append_write) as ffff:
                                        ffff.writelines(self.options.settings_workflow + "\n" + str(operation["margins"][gt_bin_identifier][agg_bin_result_index]) + "\n")

                                elif gt_result + 1e-6 < op_result - abs(op_result * op_margin) or gt_result - 1e-6 > op_result + abs(op_result * op_margin):
                                    out_of_margin_count += 1
                                    margins.append(abs(op_margin))
                                else:
                                    margins.append(abs(op_margin))                                       
                
                     
                    else:                       
                        pass
                        #print("nx")
                         # add error as many times as a bin was expected!
                        #rel_errors.extend( [ 1 for n in range(len(gt_aggregate_results)) ] )
                        
                # invalid bins test
                has_invalid_bins = False
                num_invalid = 0
                inv = []
                
                for kk in operation["output"].keys():
                    if kk not in gt_output:
                        has_invalid_bins = True
                        num_invalid += 1
                        inv.append(kk)
           
                        print(self.options.settings_workflow)
                        print(str(operation["id"]))
                        print("invalid key:" + kk)
                        print(operation["sql"])
                        print(operation["output"])
                        os._exit(0)
                
                args = result_json["args"]

                missing_bins = 1 - len(operation["output"].keys()) / len(gt_output.keys()) if len(gt_output.keys()) > 0  else 0
                op_eval_result = {}
                op_eval_result["operation_id"] = operation["id"]
                op_eval_result["config_hash"] = self.get_config_hash()
                op_eval_result["interaction_id"] = operation["parent_operation_id"]
                op_eval_result["dataset"] = args["settings_dataset"]
                op_eval_result["size"] = args["settings_size"]
                op_eval_result["viz_name"] = operation["viz_name"]
                op_eval_result["think_time"] = args["settings_thinktime"]
                op_eval_result["time_requirement"] = args["settings_time_requirement"]
                op_eval_result["interface"] = args["driver_name"]
                op_eval_result["workflow"] = args["settings_workflow"]
                op_eval_result["start_time"] = operation["start_time"]
                op_eval_result["end_time"] = operation["end_time"]
                op_eval_result["t_pause"] = operation["t_pause"] if "t_pause" in operation else 0
                op_eval_result["t_start"] = operation["t_start"] if "t_start" in operation else 0
                op_eval_result["duration"] = operation["end_time"] - operation["start_time"]
                
                if "time_violated" in operation:
                    op_eval_result["time_violated"] = operation["time_violated"]
                elif "timedout" in operation:
                    op_eval_result["time_violated"] = operation["timedout"]
                else:
                    raise Exception()

                op_eval_result["has_invalid_bins"] = has_invalid_bins
                op_eval_result["binning_type"] = operation["binning_type"]
                op_eval_result["aggregate_type"] = operation["aggregate_type"]
                op_eval_result["num_bins_delivered"] = len(operation["output"].keys())
                op_eval_result["num_bins_in_gt"] = len(gt_output.items())
                op_eval_result["missing_bins"] = "%.5f" %  missing_bins

                op_eval_result["dissimilarity"] = "%.5f" % (1- self.compute_viz_similarity(gt_output, operation["output"]))

                op_eval_result["num_bins_out_of_margin"] = "%i" % out_of_margin_count
                op_eval_result["num_aggregates_per_bin"] = operation["num_aggregates_per_bin"]       
                op_eval_result["num_binning_dimensions"] = operation["num_binning_dimensions"]     
                op_eval_result["progress"] = "%.5f" %  operation["progress"]
                op_eval_result["bias"] = "%.5f" % (sum(forecast_values) / sum(actual_values) - 1)if len(actual_values) > 0  else 0
                op_eval_result["rel_error_stdev"] = "%.5f" % statistics.stdev(rel_errors) if len(rel_errors) > 1 else 0.0
                op_eval_result["rel_error_min"] = "%.5f" % min(rel_errors) if len(rel_errors) > 0  else 0
                op_eval_result["rel_error_max"] = "%.5f" % max(rel_errors) if len(rel_errors) > 0  else 0
                op_eval_result["rel_error_avg"] = "%.5f" % float(sum(rel_errors) / float(len(rel_errors))) if len(rel_errors) > 0  else 0
                op_eval_result["margin_stdev"] = "%.5f" % statistics.stdev(margins) if len(margins) > 1 else 0.0
                op_eval_result["margin_min"] = "%.5f" % min(margins) if len(margins) > 0 else 0.0
                op_eval_result["margin_max"] = "%.5f" % max(margins) if len(margins) > 0 else 0.0
                op_eval_result["margin_avg"] = "%.5f" % float(sum(margins) / float(len(margins))) if len(margins) > 0 else 0.0
                op_eval_result["margin_ratio"] = "%.5f" % float(len(operation["margins"]) / len(operation["output"])) if operation["margins"] and len(operation["output"]) > 0 else  1
                w.writerow(op_eval_result)

    def create_report(self):
        header_saved = False
        interesting_files = glob.glob("reports/*.csv") 
        with open('./full_report.csv','w') as fout:
            for filename in interesting_files:
                print(filename)
                with open(filename) as fin:
                    header = next(fin)
                    if not header_saved:
                        print(header)
                        fout.write(header)
                        header_saved = True
                    for line in fin:
                        fout.write(line)
        print("saved report")


def assure_path_exists(path):
    d = os.path.dirname(path)
    if not os.path.exists(d):
        os.makedirs(d)

if __name__ == '__main__':  
    IDEBench()

