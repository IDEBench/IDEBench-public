import random
import importlib
import numpy as np

class BaseAction:

    def __init__(self, config, df, vizgraph, storage, sample_json):
        self.config = config
        self.df = df
        self.vizgraph = vizgraph
        self.storage = storage
        self.sample_json = sample_json

    def get_next(self):
        pick = self.pick(self.config["nextAction"]["values"], self.config["nextAction"]["pd"])
        pick_split = pick.split(".")
        module = importlib.import_module(pick_split[0] + "." + pick_split[1])
        return getattr(module, pick_split[2])(self.config, self.df, self.vizgraph, self.storage, self.sample_json) 

    def get_states(self):
        return []
    
    def pick(self, choices, pd=None):
        if pd is None:
            return random.choice(choices)
            
        total = sum(pd)
        r = random.uniform(0, total)
        upto = 0
        for i, c in enumerate(choices):
            if upto + pd[i] >= r:
                return c
            upto += pd[i]
        assert False, "Shouldn't get here"

    def pick_range(self, val_min, val_max):
        delta = val_max - val_min
        selectionrange = max(0, min(np.random.normal(loc=0.5, scale=0.25),1))
        selectionstart = random.uniform(0, 1 - selectionrange)
        return val_min + delta * selectionstart, (1 - selectionrange) * delta
