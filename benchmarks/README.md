# How to benchmark


## Install dependencies


From a [Python 2.7 virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/), install the project dependencies and [pytest-benchmark](https://pypi.python.org/pypi/pytest-benchmark).
```bash
pip install -r ../requirements.txt
pip install pytest-benchmark
```

## Run it!

```bash
pytest .
```

Output should look like:
```
-------------------------------------------- benchmark: 1 tests --------------------------------------------
Name (time in ms)        Min     Max    Mean  StdDev  Median     IQR  Outliers       OPS  Rounds  Iterations
------------------------------------------------------------------------------------------------------------
test_import_claim     2.1760  4.9939  3.3138  0.6188  3.2985  1.0920     118;0  301.7653     304           1
------------------------------------------------------------------------------------------------------------
```

## Plotting

Plotting is also possible from pytest-benchmark, just install dependencies and use `--benchmark-histogram`. A svg image will be generated with the tests results. You can also gather past results (before/after some optimization, for instance) to generate a comparison. For more on plotting, [read this guide](http://pytest-benchmark.readthedocs.io/en/stable/comparing.html#plotting).
```bash
pip install pytest-benchmark[histogram]
pytest . --benchmark-histogram
```
