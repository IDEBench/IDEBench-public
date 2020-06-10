import random
from workflowgen.baseaction import BaseAction
from common.operation import Operation
from collections import OrderedDict
import pandasql
import pandas as pd

class VizAction(BaseAction):

    def __init__(self, config, df, vizgraph, storage, sample_json):
        super().__init__(config, df, vizgraph, storage, sample_json)

        self.dim_to_type = {}
        for field in sample_json["tables"]["fact"]["fields"]:
            self.dim_to_type[field["field"]] = field["type"]

    def get_states(self):
        num_bins = self.pick(self.config["numBinDimensionsPerViz"]["values"], self.config["numBinDimensionsPerViz"]["pd"] )
        bins = []
        picks = []

        while len(bins) < num_bins:
            dimensions_p = [dim["p"] for dim in self.config["dimensions"]]
            dimensions_p = [p/sum(dimensions_p) for p in dimensions_p]
            dimension = self.pick(self.config["dimensions"], dimensions_p)
            
            if dimension in picks:
                continue
            
            picks.append(dimension)

            sql_statement = "SELECT * FROM df "
            df = self.df
            df = pandasql.sqldf(sql_statement, locals())

            d_bin = {"dimension": dimension["name"] }
            if self.dim_to_type[dimension["name"]] == "quantitative":
                dim_max_val = df[dimension["name"]].max()
                dim_min_val = df[dimension["name"]].min()
                #d_bin["width"] = round(random.uniform(0.025, 0.1) * (dim_max_val - dim_min_val))
                d_bin["width"] = round(random.uniform(0.025, 0.1) * (dim_max_val - dim_min_val))
            elif self.dim_to_type[dimension["name"]] == "categorical":
                try:
                    pd.to_numeric(df[dimension["name"]])
                    d_bin["width"] = 1
                except:
                    pass

            bins.append(d_bin)
       
        per_bin_aggregate_type = self.pick(self.config["perBinAggregates"]["values"], self.config["perBinAggregates"]["pd"] )
        per_bin_aggregate = {"type": per_bin_aggregate_type}
        if per_bin_aggregate_type == "avg":
            avg_dimension = self.pick([d for d in self.sample_json["tables"]["fact"]["fields"] if (d["type"] == "quantitative")])
            per_bin_aggregate["dimension"] = avg_dimension["field"]
            
        VizAction.VIZ_COUNTER += 1
        self.viz_name = "viz_%s" % VizAction.VIZ_COUNTER
        self.binning = bins
        self.perBinAggregates = [per_bin_aggregate]
        return Operation(OrderedDict({"name": self.viz_name, "binning": self.binning, "perBinAggregates": self.perBinAggregates}))

VizAction.VIZ_COUNTER = -1