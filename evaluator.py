import json
import csv
import glob
import numpy as np
import os
import statistics
import logging
from scipy import spatial

logger = logging.getLogger("idebench")

class Evaluator: 

    def __init__(self, options):
        self.options = options

    def evaluate(self, config_hash):
        self.config_hash = config_hash
        logger.info("starting evaluation")
        result_json = None
        try:
            with open("results/%s.json" % config_hash, "r") as json_data:
                result_json = json.load(json_data)
        except:
            print("couldn't load file %s" % ("results/%s.json" % config_hash))
            return
        
        args = result_json["args"]
        workflow = args["settings_workflow"]
        dataset = args["settings_dataset"]
        size = args["settings_size"]
        time_requirement =args["settings_time_requirement"]
        
        with open("data/%s/groundtruths/%s_%s.json" % (dataset, size, workflow), "r") as json_data:
            groundtruths = json.load(json_data)["results"]
        
        with open("reports/%s.csv" % config_hash, 'w') as fp:
            w = csv.DictWriter(fp, [
                                    "db_query_id",
                                    "config_hash",
                                    "event_id",
                                    "dataset",
                                    "dataset_size",
                                    "driver",
                                    "viz_name",
                                    "workflow",
                                    "expected_start_time",
                                    "actual_start_time",
                                    "actual_end_time",
                                    "duration",
                                    "think_time",
                                    "time_requirement",
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
                                    "margin_ratio",
                                    "progress",], delimiter=",", lineterminator="\n")
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
                op_eval_result["db_query_id"] = operation["id"]
                op_eval_result["config_hash"] = self.config_hash
                op_eval_result["event_id"] = operation["event_id"]
                op_eval_result["dataset"] = args["settings_dataset"]
                op_eval_result["dataset_size"] = args["settings_size"]
                op_eval_result["viz_name"] = operation["viz_name"]
                op_eval_result["think_time"] = args["settings_thinktime"]
                op_eval_result["time_requirement"] = args["settings_time_requirement"]
                op_eval_result["driver"] = args["driver_name"]
                op_eval_result["workflow"] = args["settings_workflow"]
                op_eval_result["expected_start_time"] = operation["expected_start_time"]
                op_eval_result["actual_start_time"] = operation["start_time"]
                op_eval_result["actual_end_time"] = operation["end_time"]
                #op_eval_result["t_pause"] = operation["t_pause"] if "t_pause" in operation else 0
                #op_eval_result["t_start"] = operation["t_start"] if "t_start" in operation else 0
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
        logger.info("save report")