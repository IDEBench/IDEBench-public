import csv
import json
import itertools

from optparse import OptionParser

parser = OptionParser()
parser.add_option("--views-file", dest="views_file", action="store", help="the views.json file")
parser.add_option("--brush-file", dest="brush_file", action="store", help="the brush.csv file")
parser.add_option("--output", dest="output", action="store", help="the file the stored the generate workflow")
(options, args) = parser.parse_args()

if not options.views_file:
    parser.error("No views file specified.")

if not options.brush_file:
    parser.error("No brush file specified.")

if not options.output:
    parser.error("No output file specified.")

def is_reset(row):
    return row[0] == "" and row[1] == "" and row[2] == ""

def is_brush_start(row):
    return row[2] == "brushStart"

def is_brush(row):
    return (not row[0] == "") and (not row[1] == "") and (row[2] == "brushStart" or row[2] == "brushEnd" or row[2] == "brush")

def get_range(row):
    return min(row[1], row[0]), max(row[1], row[0])

def get_dimension(row):
    return row[6]

def get_timestamp(row):
    return int(row[5])

def create_interaction(viz, time, selection, brush_id):
    return { "name": viz, "time": time, "selection": selection, "metadata": { "brush_id" : brush_id}}

def brush_to_selection(row):
    dimension = get_dimension(row)
    value_range = get_range(row)
    return "%s >= %s AND %s < %s" % (dimension, value_range[0], dimension, value_range[1])

vizs = []
with open(options.views_file) as view_file:
    views = json.load(view_file)
    names = []
    for view in views:
        dimension = view["name"]
        names.append(view["name"])
        binning = [signal for signal in view["spec"]["signals"] if signal["name"] == "bin"][0]["value"]
        bin_width = binning["step"]
        reference = binning["start"]
        vizs.append( { "name": dimension, 
        "binning": [{
            "dimension": dimension,
            "width": bin_width,
            "reference": reference
            }],
        "perBinAggregates": [
            {
                "type": "count"
            }
        ]
         })
    
    for viz in vizs:
        viz["source"] = " and ".join([name for name in names if name != viz["name"]])

interactions = []
with open(options.brush_file) as brush_file:
    reader = csv.reader(brush_file, delimiter=",")
    start_time = 0
    brush_id = 0
    for row in reader:
        if is_brush_start(row):
            brush_id += 1
        if is_brush(row):
            if len(interactions) == 0:
                start_time = get_timestamp(row)

            dimension = get_dimension(row)
            selection = brush_to_selection(row)
            interactions.append(create_interaction(dimension, get_timestamp(row) - start_time, selection, brush_id))

with open(options.output, "w") as fp:
    json.dump({"setup": vizs, "interactions": interactions}, fp, indent=4)