import random
import math
import numpy as np
import pandasql
from collections import OrderedDict
from workflowgen.baseaction import BaseAction
from workflowgen.vizaction import VizAction
from common.operation import Operation

class FilterAction(BaseAction):

    def get_states(self):
        
        src_viz_num = random.randint(0, VizAction.VIZ_COUNTER)
        src_viz = list(self.vizgraph.get_nodes())[src_viz_num]
        computed_filter = src_viz.get_computed_filter()
        df = self.df
        sql_statement = "SELECT * FROM df "
        if len(computed_filter) > 0:
            sql_statement += "WHERE " + computed_filter
        
        df_result = pandasql.sqldf(sql_statement, locals())

        if df_result.empty:
            return None

        filter_per_dim = []
        
        for bin_dim in range(len(src_viz.binning)):
            filters = []
            dim = src_viz.binning[bin_dim]["dimension"]
            field = list(filter(lambda x: x["field"] == dim, self.sample_json["tables"]["fact"]["fields"]))[0]     
            if field["type"] == "quantitative":
                bin_width = float(src_viz.binning[bin_dim]["width"])
                min_val = df_result[dim].min()
                max_val = df_result[dim].max()

                min_index = math.floor(min_val / bin_width)
                max_index = math.floor(max_val / bin_width)
                num_bins = 0
                if np.random.rand() < 0.4:
                    num_bins = 1
                else:
                    num_bins = random.randint(1, max_index-min_index) if max_index > min_index else 1
                selected_bins = np.random.choice(np.arange(min_index, max_index + 1), size=num_bins, replace=False)
                
                for selected_bin in selected_bins:
                    range_min = selected_bin * bin_width
                    range_max = (selected_bin + 1) * bin_width
                    filt = "(%s >= %s and %s < %s)" % (dim, '{:.1f}'.format(range_min), dim, '{:.1f}'.format(range_max))
                    filters.append(filt)
            else:
                all_bins = df_result[dim].unique().tolist()
                num_bins = random.randint(1, len(all_bins))
                selected_bins = np.random.choice(all_bins, size=num_bins, replace=False)
                for selected_bin in list(selected_bins):
                    filt =  "(%s = '%s')" % (dim, selected_bin)
                    filters.append(filt)
            filter_per_dim.append(" or ".join(filters))
        filter_per_dim = ["(%s)" % f for f in filter_per_dim]
                            
        return Operation(OrderedDict({"name": ("viz_%s" % src_viz_num), "filter": " and ".join(filter_per_dim)}))