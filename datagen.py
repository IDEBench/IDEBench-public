import json
import math
import random
import itertools
import numpy as np
import pandas as pd
import scipy as sp
import collections
import threading
from scipy.linalg import eigh, cholesky
from scipy.stats import norm
import scipy.interpolate as interpolate
from optparse import OptionParser
import time
import os
from collections import OrderedDict
import logging

current_milli_time = lambda: int(round(time.time() * 1000))
logger = logging.getLogger("idebench")

class DataGen:

    def __init__(self):
        
        logger.info("wowow")
        parser = OptionParser()
        parser.add_option("-r", "--random-seed", dest="seed", action="store", type=int, help="Random seed", default=41001)
        parser.add_option("--normalize", dest="normalize", action="store", help="Size of the dataset in MB")
        parser.add_option("--prevent-zero", dest="prevent_zero", action="store_true", help="Size of the dataset in MB", default=True)
        parser.add_option("-s", "--size", dest="size", action="store", type=int, help="Number of samples to generate", default=50000)
        parser.add_option("-b", "--batchsize", dest="batchsize", action="store", type=int, help="Number of samples to process in a single batch", default=100000)
        parser.add_option("-x", "--sample-file", dest="samplefile", action="store", help="Path to the sample file", default="data/flights/sample.csv")
        parser.add_option("-y", "--sample-descriptor", dest="samplejsonfile", action="store", help="Path to the sample file", default="data/flights/sample.json")
        parser.add_option("-n","--num-samples", dest="numsamples", action="store", type=int, help="Number of samples to draw from the original dataset", default=5000000)
        parser.add_option("-o", "--output-file", dest="output", action="store", help="The name of the output file", default="dataset.csv")
        #parser.add_option("--output-normalized-file", dest="output_normalized", action="store", help="The name of the output file", default="datagen/output_normalized.csv")
        (options, args) = parser.parse_args()

        self.options = options

        # read sample json
        self.sample_json = None
        with open(self.options.samplejsonfile, "r") as f:
            self.sample_json = json.load(f)

        if self.options.normalize:
            self.normalize()
        else:
            self.generate_data()

    def normalize(self):

        if "tables" not in self.sample_json:
            raise Exception("no tables defined in sample json")

        tables = self.sample_json["tables"]["dimension"]
        table_dfs = {}
        for tbl in tables:
            table_dfs[tbl["name"]] = pd.DataFrame(columns=tbl["columns"])

        for chunk_id, chunk in enumerate(pd.read_csv(self.options.normalize, chunksize=100000, header=0)):
            all_from_fields = []

            for tbl in tables:
                
                table_df = table_dfs[tbl["name"]]

                for mapping in tbl["mapping"]:
                    xx = chunk[mapping["fromFields"]]
                    xx.columns = tbl["columns"]
                    table_df = table_df.append(xx)
                    table_df = table_df.drop_duplicates(subset=tbl["columns"])
                              
            
                table_df = table_df.reset_index(drop=True)
                table_df.index.name = "ID"
                table_dfs[tbl["name"]] = table_df  
                if "tmp_ID" in table_df.columns:
                    del table_df["tmp_ID"]
                table_df.to_csv(tbl["name"], index=True, mode="w")
                

                table_df["tmp_ID"] = table_df.index

                count = 0
                for mapping in tbl["mapping"]:
                    all_from_fields.extend(mapping["fromFields"])

                    boo = len(chunk)
                    beforechunk = chunk
                    chunk = pd.merge(chunk, table_df, how="left", left_on=mapping["fromFields"], right_on=tbl["columns"])

                    if len(chunk) > boo:
                        print("EEORR")
                        print(boo)
                        print(len(chunk))
                        print(mapping["fromFields"])
                        print(tbl["columns"])
                        print(table_df)
                        print(beforechunk)
                        print(chunk)
                        os._exit(0)

       
                    
                    chunk = chunk.rename(columns={'tmp_ID': mapping["fk"]})

                    for c in tbl["columns"]:

                        del chunk[c]

                
            
            for c in all_from_fields:
                del chunk[c]

            if chunk_id == 0:       
                chunk.to_csv(self.options.output, index=False, mode="w")
            else:
                chunk.to_csv(self.options.output, index=False, header=False, mode="a")



    def generate_data(self):
         # load sample data
        self.df = pd.read_csv(self.options.samplefile, nrows=self.options.numsamples, header=0)

        if self.options.prevent_zero:
            self.quant_col_names = [ col["field"] for col in self.sample_json["tables"]["fact"]["fields"] if col["type"] == "quantitative" ]
            for quant_col_name in self.quant_col_names:
                self.df[quant_col_name] = self.df[quant_col_name] - self.df[quant_col_name].min()

        self.cat_col_names = [ col["field"] for col in self.sample_json["tables"]["fact"]["fields"] if col["type"] == "categorical" ]
        for cat_col_name in self.cat_col_names:
            self.df[cat_col_name] = self.df[cat_col_name].astype("category")

        self.derived_cols = [ col for col in self.sample_json["tables"]["fact"]["fields"] if "deriveFrom" in col ]
        self.derivates = {}
        for derived_col in self.derived_cols:
            kk = self.df.groupby(derived_col["deriveFrom"])[derived_col["field"]].first().to_dict()
            self.derivates[derived_col["field"]] = kk

   
        self.orgdf = self.df.copy()
        self.cat_cols = list(self.orgdf.select_dtypes(include=["category"]).columns)
        self.cat_hists = {}
        self.cat_hists_keys = {}
        self.cat_hists_values = {}

        for cat_col in self.cat_cols:           
            self.cat_hists[cat_col] = self.df[cat_col].value_counts(normalize=True).to_dict()
            self.cat_hists[cat_col] = OrderedDict(sorted(self.cat_hists[cat_col].items(), key=lambda x:x[0]))
            self.cat_hists_keys[cat_col] = list(self.cat_hists[cat_col].keys())
            self.cat_hists_values[cat_col]  = list(self.cat_hists[cat_col].values())   
            del self.df[cat_col]            

        self.means = self.df.mean()
        self.stdevs = self.df.std()
        np.set_printoptions(suppress=True)

        # z-normalize all data
        for idx, col in enumerate(self.df.columns):
            self.df[col] = (self.df[col] - self.means[col])/self.stdevs[col]
        
        self.inv_cdfs = self.get_inverse_cdfs(self.orgdf, self.df)
        
        # apply a gaussian copula
        covariance = self.df.cov()
        self.decomposition = cholesky(covariance, lower=True)

        # calculate how many batches we need to compute     
        num_batches = int(math.ceil(self.options.size / self.options.batchsize))

        st = current_milli_time()
        # process all batches
        for batch_i in range(num_batches):
            logger.info(" %i/%i batches processed." % (batch_i, num_batches))
            self.process_batch(batch_i)

        logger.info("done. took %i " %  (current_milli_time() - st))

    def process_batch(self, batch_number):

        # Calculate how many samples we need to generate for this batch
        if (batch_number+1) * self.options.batchsize > self.options.size:
            num_samples_to_generate = self.options.size - (batch_number) * self.options.batchsize 
        else:
            num_samples_to_generate = self.options.batchsize
        
        # adjust the random seed based on the batch number
        np.random.seed(seed=self.options.seed + batch_number)

        data_rnormal = sp.stats.norm.rvs(size=(len(self.df.columns), num_samples_to_generate))
        data_rnormal_correlated = np.dot(self.decomposition, data_rnormal)

        # convert each value to the its corresponding value of the normal CDF. We could find the CDF empirically,
        # but since our samples follow a normal distribution we know their exact CDF.
        stdnormcdf = sp.stats.norm.cdf(data_rnormal_correlated)

        global cc
        cc = 0
        def apply_inverse(xx):
            global cc        
            res = self.inv_cdfs[cc](xx)
            cc += 1
            return res

        # apply the inverse (empirical) CDF to the correlated random data
        res = np.apply_along_axis(apply_inverse, 1, stdnormcdf)

        # de-normalize
        for i, col in enumerate(self.df.columns):
            res[i,:] = res[i,:] * self.stdevs[col] + self.means[col]
        
        # reconstruct categorical values
        result_frame = pd.DataFrame(res.T)  
        result_frame.columns = self.df.columns 

        for cat_col in self.cat_cols:               
            ix = self.orgdf.columns.get_loc(cat_col)
            keys = self.cat_hists_keys[cat_col]
            values = self.cat_hists_values[cat_col]
            xx = np.random.choice(keys, num_samples_to_generate, p=values)
            result_frame.insert(self.orgdf.columns.get_loc(cat_col), cat_col, xx)

        for quant_col in self.quant_col_names:
            result_frame[quant_col] = result_frame[quant_col].round(decimals=1)

        cast_cols = [ col for col in self.sample_json["tables"]["fact"]["fields"] if "cast" in col ]
        for cast_col in cast_cols:
            if cast_col["cast"] == "int":
                result_frame[cast_col["field"]] = result_frame[cast_col["field"]].astype(int)
            else:
                raise Exception("unsupported cast")

        for derived_col in self.derived_cols:
            new_col = result_frame[derived_col["deriveFrom"]].map(self.derivates[derived_col["field"]])
            result_frame[derived_col["field"]] = new_col



        if batch_number == 0:       
            result_frame.to_csv(self.options.output, index=False, mode="w")
        else:
            result_frame.to_csv(self.options.output, index=False, header=False, mode="a")

    def get_inverse_cdfs(self, orgdf, df):
        cdfs = []        
        for col in df.columns:
            num_bins = 1000
            hist, bin_edges = np.histogram(df[col], bins=num_bins, density=True)
            cdf = np.zeros(bin_edges.shape)            
            cdf[1:] = np.cumsum(hist*np.diff(bin_edges))
            inv_cdf = interpolate.interp1d(cdf, bin_edges)
            cdfs.append(inv_cdf)
        return cdfs

DataGen()