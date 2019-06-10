### Using Crossfilter Brush Logs in IDEBench

1. Convert the brush logs to IDEBench workflows (there is an example in `data/crossfilter/logconverter`)
```
cd data/crossfilter/logconverter
python3 convert.py --views-file data/views.json --brush-file data/brush.csv --output workflow.json
```

2. Put the generated `workflow.json` in the `data/crossfilter/workflows` directory, and the compute the groundtruth and run IDEBench as usual

```
python3 idebench.py --driver-name monetdb --settings-dataset crossfilter --settings-size 1GB --settings-workflow workflow --groudtruth
```
```
python3 idebench.py --driver-name monetdb --settings-dataset crossfilter --settings-size 1GB --settings-workflow workflow --run
```
