#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
from airflow.utils.state import State
from airflow.utils import timezone
from airflow_ray_executor import RayExecutor

TEST_SUCCESS_COMMANDS = 5
DEFAULT_DATE = timezone.datetime(2022, 1, 1)

def test_execution_parallelism_subprocess(parallelism=0):
    # With default dag task
    SUCCESS_COMMAND = ['airflow', 'tasks', 'run', 'example_bash_operator', 'runme_0', str(DEFAULT_DATE)]
    FAIL_COMMAND = ['airflow', 'tasks', 'run', 'false']
    _test_execute(parallelism, SUCCESS_COMMAND, FAIL_COMMAND)


def _test_execute(parallelism, success_command, fail_command):
    executor = RayExecutor(parallelism=parallelism)
    executor.start()

    success_key = 'success {}'
    assert executor.result_queue.empty()

    execution_date = datetime.datetime.now()
    for i in range(TEST_SUCCESS_COMMANDS):
        key_id, command = success_key.format(i), success_command
        key = key_id, 'fake_ti', execution_date, 0
        executor.running.add(key)
        executor.execute_async(key=key, command=command)

    fail_key = 'fail', 'fake_ti', execution_date, 0
    executor.running.add(fail_key)
    executor.execute_async(key=fail_key, command=fail_command)

    executor.end()
    # By that time Queues are already shutdown so we cannot check if they are empty
    assert len(executor.running) == 0

    for i in range(TEST_SUCCESS_COMMANDS):
        key_id = success_key.format(i)
        key = key_id, 'fake_ti', execution_date, 0
        assert executor.event_buffer[key][0] == State.SUCCESS
    assert executor.event_buffer[fail_key][0] == State.FAILED

    expected = TEST_SUCCESS_COMMANDS + 1 if parallelism == 0 else parallelism
    assert executor.workers_used == expected
