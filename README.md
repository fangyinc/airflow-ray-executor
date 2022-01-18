# Airflow Ray Executor

Airflow executor implemented using [ray](https://github.com/ray-project/ray)

[**中文**](README_ZH.md)

## Usage

```shell
$ pip install airflow-ray-executor
```

Edit your ``airflow.cfg`` to set your executor to class: `airflow_ray_executor.RayExecutor` and add ray client address to this file, example:

```pycon
executor = airflow_ray_executor.RayExecutor

[ray]
# ray client address to connect to ray cluster
# Ray Executor will start Ray on a single machine if not provided
client = ray://127.0.0.1:10001
```

Please note:
Airflow not support sqlite database when executor neither DebugExecutor nor SequentialExecutor
