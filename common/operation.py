import json 

class Operation:

    def __init__(self, data):
        self.data = data

    def get_viz_name(self):
        if "name" in self.data:
            return self.data["name"]
        return None

    def get_source(self):
        return self.data["source"]

    def has_source(self):
        return "source" in self.data

    def has_selection(self):
        return "selection" in self.data and len(self.data["selection"]) > 0

    def get_selection(self):
        return self.data["selection"]

    def has_filter(self):
        return "filter" in self.data and len(self.data["filter"]) > 0

    def get_filter(self):
        return self.data["filter"]

    def get_source_vizs(self):
        sources = self.get_source().replace("(", "").replace(")", "").replace("and", "").replace("or", "").split(" ")
        return set([s for s in sources if not s == "" ])