import json
import re

class Viz:

    @staticmethod
    def createFromDict(obj):
        viz = Viz()
        viz.name = "" if "name" not in obj else obj["name"]
        viz.source = "" if "source" not in obj else obj["source"]
        viz.selection = "" if "selection" not in obj else obj["selection"]
        viz.filter = "" if "filter" not in obj else obj["filter"]
        viz.computed_filter = ""
        viz.binning = [] if "binning" not in obj else obj["binning"]
        viz.per_bin_aggregates = [] if "perBinAggregates" not in obj else obj["perBinAggregates"]
        return viz

    def __init__(self):
        self.name = ""
        self.source = ""
        self.selection = ""
        self.filter = ""
        self.computed_filter = ""
        self.binning = []
        self.per_bin_aggregates = []

    def apply_interaction(self, operation):
        self.source = operation.get_source() if operation.has_source() else ""
        self.selection = operation.get_selection() if operation.has_selection() else ""
        self.filter = operation.get_filter() if operation.has_filter() else ""

    def has_filter(self):
        return len(self.filter) > 0

    def get_filter(self):
        return self.filter

    def get_computed_filter(self):
        return self.computed_filter

    def set_computed_filter(self, filter_str):
        self.computed_filter = filter_str

    def get_source(self):
        return self.source

    def has_source(self):
        return True if not self.source == "" else False

    def has_selection(self):
        return True if not self.selection == "" else False

    def get_selection(self):
        return self.selection

    def get_source_vizs(self):
        sources = self.get_source().replace("(", "").replace(")", "").replace("and", "").replace("or", "").split(" ")
        return set([s for s in sources if not s == "" ])

    def get_computed_filter_as_sql(self, schema):
        bins = []
        bin_str = ""
        tables = set()
        tables.add(schema.get_fact_table_name())
        joins = set()
        for bin_desc in self.binning:
            dimension = bin_desc["dimension"]
            bins.append("bin_" + dimension)
            if "width" in bin_desc:
                bin_width = bin_desc["width"]
                bin_str += "FLOOR(%s/%s) AS bin_%s, " % (dimension, bin_width, dimension)
            else:
                dd, tblas, tjoins = schema.translate_field(dimension)
                joins.add(tjoins)
                tables.add(tblas)

                bin_str += "%s AS bin_%s, " % (dd, dimension)

        agg_str = ""
        for per_bin_aggregate_desc in self.per_bin_aggregates:

            if "dimension" in per_bin_aggregate_desc:
                aggregate_dimension = per_bin_aggregate_desc["dimension"]
            if per_bin_aggregate_desc["type"] == "count":
                agg_str += "COUNT(*) as count, "
            elif per_bin_aggregate_desc["type"] == "avg":
                aggregate_dimension = per_bin_aggregate_desc["dimension"]
                aggregate_dimension2 = aggregate_dimension
                
                #if storage.normalized:
                #    aggregate_dimension2 = storage.get_all_tables()[0]["name"] + "." + aggregate_dimension
                    
                agg_str += "AVG(%s) as average_%s, " % (aggregate_dimension2, aggregate_dimension)
                
        agg_str = agg_str.rstrip(", ")
        bins_str = ", ".join(bins)

        sql_statement = "SELECT %s %s " % (bin_str, agg_str)
        if schema.is_normalized:

            computed_filter = self.get_computed_filter()    
            fields = [f["field"] for f in schema.get_fact_table()["fields"]]

            for field in fields:
                if field not in computed_filter:
                    continue

                translation, tblas, tjoins = schema.translate_field(field)
                joins.add(tjoins)
                tables.add(tblas)

                computed_filter = computed_filter.replace(field + " ", translation + " ")

            
            sql_statement += "FROM %s " % ", ".join(tables)
            joins = list(filter(None, joins))
            if len(joins) > 0:
                sql_statement += "WHERE (%s) " % computed_filter + " AND " if computed_filter  else "WHERE "
                sql_statement += "("
                sql_statement += " AND ".join(joins) + ") "
            else:
                sql_statement += " "
                
            sql_statement += "GROUP BY %s" % bins_str
        else:
            sql_statement = "SELECT %s %s " % (bin_str, agg_str)
            sql_statement += "FROM %s " % schema.get_fact_table_name()
            sql_statement += "WHERE (%s) " % self.get_computed_filter() if self.get_computed_filter() else ""
            sql_statement += "GROUP BY %s" % bins_str

        return sql_statement


    def get_computed_filter_as_sql2(self, storage):
        bins = []
        bin_str = ""
        for bin_desc in self.binning:
            dimension = bin_desc["dimension"]
            
            if "width" in bin_desc:
                bin_width = bin_desc["width"]
                bin_str += "FLOOR(%s/%s), " % (dimension, bin_width)
                bins.append("FLOOR(%s/%s)" % (dimension, bin_width))
            else:
                dd = dimension
                bin_str += "%s, " % (dd)
                bins.append("%s" % (dd))

        agg_str = ""
        for per_bin_aggregate_desc in self.per_bin_aggregates:

            if "dimension" in per_bin_aggregate_desc:
                aggregate_dimension = per_bin_aggregate_desc["dimension"]
            if per_bin_aggregate_desc["type"] == "count":
                agg_str += "COUNT(*) as count_, relative_error(count_),"
            elif per_bin_aggregate_desc["type"] == "avg":
                aggregate_dimension = per_bin_aggregate_desc["dimension"]
                agg_str += "AVG(%s) as average_%s, relative_error(average_%s)" % (aggregate_dimension, aggregate_dimension, aggregate_dimension)
                
        agg_str = agg_str.rstrip(", ")
        bins_str = ", ".join(bins)
        sql_statement = "SELECT %s %s " % (bin_str, agg_str)
        sql_statement += "FROM %s " % storage.get_fact_table_name()
        sql_statement += "WHERE (%s) " % self.get_computed_filter() if self.get_computed_filter() else ""
        sql_statement += "GROUP BY %s" % bins_str
        return sql_statement + " WITH ERROR 0.1 BEHAVIOR 'do_nothing'"