# Usage

PYTHONPATH='.' luigi --module example_pipeline PrepareDataTask --local-scheduler

PYTHONPATH='.' luigi --module example_pipeline RollupTask

PYTHONPATH='.' luigi --module example_pipeline RollupTask --date-param 2022-01-15

# Run scheduler (drop `--local-scheduler`)
luigid

* Access dashboard at <http://localhost:8082>

