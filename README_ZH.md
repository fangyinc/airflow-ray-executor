# Airflow Ray Executor
使用[ray](https://github.com/ray-project/ray) 实现的airflow executor

## 使用

```shell
$ pip install airflow-ray-executor
```

Edit your ``airflow.cfg`` to set your executor to class: `airflow_ray_executor.RayExecutor` and add ray client address to this file, example:

编辑你的``airflow.cfg``, 设置executor为类: `airflow_ray_executor.RayExecutor`, 并添加ray客户端地址到该文件, 例如:

```pycon
executor = airflow_ray_executor.RayExecutor

[ray]
# 连接到Ray的客户端地址
# Ray Executor will start Ray on a single machine if not provided
# 如果没有提供该地址，ray executor将会启动一个单机的ray
client = ray://127.0.0.1:10001
```

注意:

使用这个此executor将不支持```sqlite```数据库, 在airflow中只有```DebugExecutor```和```SequentialExecutor```才支持sqlite数据库。