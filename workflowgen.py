import random
import numpy as np
import pprint
import json
from workflowgen.vizaction import VizAction
from workflowgen.linkaction import LinkAction
from optparse import OptionParser
import pandas as pd
from common.schema import Schema
from common.vizgraph import VizGraph
#from common.storage import Storage
import pandasql


class WorkflowGenerator:

    def __init__(self):

        parser = OptionParser()
        parser.add_option("-r", "--seed", dest="seed", action="store", type=int, help="Random seed", default=25000)
        parser.add_option("-d", "--dataset", dest="data_folder", action="store", help="path to save the file", default="flights")
        parser.add_option("--debug", dest="debug", action="store_true", help="creates a debug file", default=False)
        parser.add_option("-n", "--num-operations", dest="num_operations", action="store", type=int, help="Number of operations to generate", default=20)
        parser.add_option("-c", "--workflow-type", dest="config", action="store", help="path to config file", default="data/flights/workflowtypes/sequential.json")
        parser.add_option("-p", "--output", dest="path", action="store", help="path to save the file", default="workflow.json")
        parser.add_option("-s", "--num-samples", dest="numsamples", action="store", type=int, help="Number of samples to draw from the original dataset", default=10000)
        (options, args) = parser.parse_args()
        self.options = options

        random.seed(options.seed)
        np.random.seed(seed=options.seed)

        print("data/" + options.data_folder + "/" + options.config)
        with open("data/" + options.data_folder + "/workflowtypes/" + options.config, "r") as fp:
            self.config = json.load(fp)

        schema = None
        with open(self.get_schema_path()) as f:
            schema = Schema(json.load(f))

        print("reading csv...")
        # load sample data
        df = pd.read_csv("data/" + options.data_folder + "/sample.csv", nrows=options.numsamples, header=0)
        
        #schema = {"tables": [{ "name": "df", "dimensions": []}]}
        sample_json = None
        with open("data/" + options.data_folder + "/sample.json", "r") as f:
            sample_json = json.load(f)
    #       print(sample_json)
    #        for field in sample_json["tables"]["fact"]["fields"]:
    #          schema["tables"][0]["dimensions"].append({"name": field["field"]})


        #storage = Storage(schema)

        zero_qs_ratio = 100

        tries = -1
        while zero_qs_ratio > 0.15:
            tries += 1
            num_zeros_qs = 0
            num_qs = 0
            VizAction.VIZ_COUNTER = -1  
            LinkAction.FIRST_LINK = None
            LinkAction.LATEST_LINK = None
            LinkAction.LINKS = set()
            
            vizgraph = VizGraph()
            random.seed(options.seed + tries)
            root = VizAction(self.config, df, vizgraph, schema, sample_json)
            current = root
            states = []
            
            num_ops = 0
            
            debug_states = []
            while num_ops < options.num_operations:
                res = current.get_states()
                if res:                 
                    affected_vizs = vizgraph.apply_interaction(res)
                    if options.debug:
                        nodes_dict = vizgraph.get_nodes_dict()
                        states_dict = {}
                        for n in nodes_dict.keys():
                            states_dict[n] = {
                                "name":n,
                                "source" : nodes_dict[n].get_source(),
                                "binning":  nodes_dict[n].binning,
                                "agg": nodes_dict[n].per_bin_aggregates,
                                "selection": nodes_dict[n].get_selection(),
                                "filter": nodes_dict[n].get_filter(),
                                "computed_filter": nodes_dict[n].get_computed_filter_as_sql(schema),
                            }
                        debug_states.append(states_dict)
                    
                    for x in affected_vizs:
                        sql = x.get_computed_filter_as_sql(schema).replace("FLOOR", "ROUND").replace(schema.get_fact_table_name(), "df")
                        r = pandasql.sqldf(sql, locals())
                        num_qs += 1
                        if len(r.index) == 0:
                            num_zeros_qs += 1
                            #print("ZERO QUERY")

                    states.append(res.data)
                    #print(res.data)
                    #if "source" not in res:
                    num_ops += 1

                current = current.get_next()
                if current is None:
                    zero_qs_ratio = num_zeros_qs/num_qs
                    break
            zero_qs_ratio = num_zeros_qs/num_qs
        
        print("zero queries:")
        print( (num_zeros_qs / num_qs))

        with open("data/" + options.data_folder +  "/workflows/" + options.path + ".json", "w") as fp:
            fp.write(json.dumps({"name": "generated", "dataset": options.data_folder, "seed": options.seed, "config": options.config, "interactions": states}))

        print("done.")
        #with open("workflowviewer/public/workflow.json", "w") as fp:
        #    fp.write(json.dumps({"name": "generated", "dataset": options.data_folder, "seed": options.seed, "config": options.config, "interactions": states}))

        #with open("workflowviewer/public/workflow_debug.json", "w") as fp:
        #    fp.write(json.dumps(debug_states))

        #if options.debug:
        #    import webbrowser
        #    url = "http://localhost:3000"
        #    webbrowser.open(url)

    def get_schema_path(self):
        return "data/%s/sample.json" % (self.options.data_folder)

    def get_viz_name(self):
        return "viz_%i" % self.config["viz_counter"]

WorkflowGenerator()