import os
import logging
logger = logging.getLogger("idebench")

class Schema:
    
    def __init__(self, schema_json, is_normalized=False):
        self.is_normalized = is_normalized
        self.schema_json = schema_json
    
    def get_fact_table(self):
        return self.schema_json["tables"]["fact"]

    def get_fact_table_name(self):
        return self.schema_json["tables"]["fact"]["name"]

    def translate_field(self, field_name):

        if not self.is_normalized:
            return field_name, None, None
        
 
        for dim_tbl in self.schema_json["tables"]["dimension"]:
            for m_idx, mapping in enumerate(dim_tbl["mapping"]):
                for f_idx, field in enumerate(mapping["fromFields"]):
                    if field == field_name:
                        tbl_alias = "%s%s" % (dim_tbl["name"], m_idx)
                        tbl_join = "%s.ID = %s.%s" % (tbl_alias, self.get_fact_table_name(), mapping["fk"])
                        tbl_as = "%s AS %s" % (dim_tbl["name"], tbl_alias)
                        return ("%s.%s" % (tbl_alias, dim_tbl["columns"][f_idx])), tbl_as, tbl_join
        return field_name, self.get_fact_table_name(), None

    def get_tables_for(self, field_name):
        if not self.is_normalized:
            return ""

        for dim_tbl in self.schema_json["tables"]["dimension"]:
            for m_idx, mapping in enumerate(dim_tbl["mapping"]):
                for f_idx, field in enumerate(mapping["fromFields"]):
                    if field == field_name:
                        tbl_alias = "%s%s" % (dim_tbl["name"], m_idx)
                        tbl_as = "%s AS %s" % (dim_tbl["name"], tbl_alias)
                        if dim_tbl["name"] == "tbl_carriers":
                            logger.info("exiting")
                            os._exit()
                        return ("%s.%s" % (tbl_alias, dim_tbl["columns"][f_idx])), tbl_as

