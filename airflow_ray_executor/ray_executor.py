#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import subprocess
from abc import abstractmethod
from multiprocessing import Manager, Process
from multiprocessing.managers import SyncManager
from queue import Empty, Queue
from typing import Any, List, Optional, Tuple, Union

from setproctitle import getproctitle, setproctitle
import ray
from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import NOT_STARTED_MESSAGE, PARALLELISM, BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKey, TaskInstanceStateType
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow.configuration import conf

ExecutorWorkType = Tuple[Optional[TaskInstanceKey], Optional[CommandType]]

class RayJobActor:

    def __init__(self):
        pass

    def run(self, command):
        try:
            print(f"Begin to run task with command: {command}")
            subprocess.check_call(command, close_fds=True)
            return State.SUCCESS
        except subprocess.CalledProcessError as e:
            print(f"Failed to execute task %s{str(e)}")
            return State.FAILED
        except Exception as e:
            print(f'Failed to execute task with unexpected exception:')


class RayWorkerBase(Process, LoggingMixin):
    """
    LocalWorkerBase implementation to run airflow commands. Executes the given
    command and puts the result into a result queue when done, terminating execution.

    :param result_queue: the queue to store result state
    """

    def __init__(self, result_queue: 'Queue[TaskInstanceStateType]'):
        super().__init__(target=self.do_work)
        self.daemon: bool = True
        self.result_queue: 'Queue[TaskInstanceStateType]' = result_queue

    def run(self):
        # We know we've just started a new process, so lets disconnect from the metadata db now
        settings.engine.pool.dispose()
        settings.engine.dispose()
        setproctitle("airflow worker -- RayExecutor")
        return super().run()

    def execute_work(self, key: TaskInstanceKey, command: CommandType) -> None:
        """
        Executes command received and stores result state in queue.

        :param key: the key to identify the task instance
        :param command: the command to execute
        """
        if key is None:
            return

        self.log.info("%s running %s", self.__class__.__name__, command)
        setproctitle(f"airflow worker -- RayExecutor: {command}")
        state = self._execute_work_in_ray(command)
        self.result_queue.put((key, state))
        # Remove the command since the worker is done executing the task
        setproctitle("airflow worker -- RayExecutor")

    def _execute_work_in_ray(self, command) -> str:
        ray_address = conf.get('ray', 'client')
        self.log.info(f"ray_address: {ray_address}")
        if ray_address:
            if not ray.is_initialized():
                # Init ray
                self.log.info(f"Connect to ray cluster with client: {ray_address}")
                ray.init(address=ray_address)
        ray_job_actor = ray.remote(RayJobActor).remote()
        return ray.get(ray_job_actor.run.remote(command))

    @abstractmethod
    def do_work(self):
        """Called in the subprocess and should then execute tasks"""
        raise NotImplementedError()


class RayWorker(RayWorkerBase):
    """
    Local worker that executes the task.

    :param result_queue: queue where results of the tasks are put.
    :param key: key identifying task instance
    :param command: Command to execute
    """

    def __init__(
        self, result_queue: 'Queue[TaskInstanceStateType]', key: TaskInstanceKey, command: CommandType
    ):
        super().__init__(result_queue)
        self.key: TaskInstanceKey = key
        self.command: CommandType = command

    def do_work(self) -> None:
        self.execute_work(key=self.key, command=self.command)


class QueuedRayWorker(RayWorkerBase):
    """
    LocalWorker implementation that is waiting for tasks from a queue and will
    continue executing commands as they become available in the queue.
    It will terminate execution once the poison token is found.

    :param task_queue: queue from which worker reads tasks
    :param result_queue: queue where worker puts results after finishing tasks
    """

    def __init__(self, task_queue: 'Queue[ExecutorWorkType]', result_queue: 'Queue[TaskInstanceStateType]'):
        super().__init__(result_queue=result_queue)
        self.task_queue = task_queue

    def do_work(self) -> None:
        while True:
            try:
                key, command = self.task_queue.get()
            except EOFError:
                self.log.info(
                    "Failed to read tasks from the task queue because the other "
                    "end has closed the connection. Terminating worker %s.",
                    self.name,
                )
                break
            try:
                if key is None or command is None:
                    # Received poison pill, no more tasks to run
                    break
                self.execute_work(key=key, command=command)
            finally:
                self.task_queue.task_done()


class RayExecutor(BaseExecutor):

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)
        self.manager: Optional[SyncManager] = None
        self.result_queue: Optional['Queue[TaskInstanceStateType]'] = None
        self.workers: List[QueuedRayWorker] = []
        self.workers_used: int = 0
        self.workers_active: int = 0
        self.impl: Optional[
            Union['RayExecutor.UnlimitedParallelism', 'RayExecutor.LimitedParallelism']
        ] = None

    class UnlimitedParallelism:
        """
        Implements LocalExecutor with unlimited parallelism, starting one process
        per each command to execute.

        :param executor: the executor instance to implement.
        """

        def __init__(self, executor: 'RayExecutor'):
            self.executor: 'RayExecutor' = executor

        def start(self) -> None:
            """Starts the executor."""
            self.executor.workers_used = 0
            self.executor.workers_active = 0

        def execute_async(
            self,
            key: TaskInstanceKey,
            command: CommandType,
            queue: Optional[str] = None,
            executor_config: Optional[Any] = None,
        ) -> None:
            """
            Executes task asynchronously.

            :param key: the key to identify the task instance
            :param command: the command to execute
            :param queue: Name of the queue
            :param executor_config: configuration for the executor
            """
            if not self.executor.result_queue:
                raise AirflowException(NOT_STARTED_MESSAGE)
            local_worker = RayWorker(self.executor.result_queue, key=key, command=command)
            self.executor.workers_used += 1
            self.executor.workers_active += 1
            local_worker.start()

        def sync(self) -> None:
            """Sync will get called periodically by the heartbeat method."""
            if not self.executor.result_queue:
                raise AirflowException("Executor should be started first")
            while not self.executor.result_queue.empty():
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)
                self.executor.workers_active -= 1

        def end(self) -> None:
            """
            This method is called when the caller is done submitting job and
            wants to wait synchronously for the job submitted previously to be
            all done.
            """
            while self.executor.workers_active > 0:
                self.executor.sync()

    class LimitedParallelism:
        """
        Implements LocalExecutor with limited parallelism using a task queue to
        coordinate work distribution.

        :param executor: the executor instance to implement.
        """

        def __init__(self, executor: 'RayExecutor'):
            self.executor: 'RayExecutor' = executor
            self.queue: Optional['Queue[ExecutorWorkType]'] = None

        def start(self) -> None:
            """Starts limited parallelism implementation."""
            if not self.executor.manager:
                raise AirflowException(NOT_STARTED_MESSAGE)
            self.queue = self.executor.manager.Queue()
            if not self.executor.result_queue:
                raise AirflowException(NOT_STARTED_MESSAGE)
            self.executor.workers = [
                QueuedRayWorker(self.queue, self.executor.result_queue)
                for _ in range(self.executor.parallelism)
            ]

            self.executor.workers_used = len(self.executor.workers)

            for worker in self.executor.workers:
                worker.start()

        def execute_async(
            self,
            key: TaskInstanceKey,
            command: CommandType,
            queue: Optional[str] = None,
            executor_config: Optional[Any] = None,
        ) -> None:
            """
            Executes task asynchronously.

            :param key: the key to identify the task instance
            :param command: the command to execute
            :param queue: name of the queue
            :param executor_config: configuration for the executor
            """
            if not self.queue:
                raise AirflowException(NOT_STARTED_MESSAGE)
            self.queue.put((key, command))

        def sync(self):
            """Sync will get called periodically by the heartbeat method."""
            while True:
                try:
                    results = self.executor.result_queue.get_nowait()
                    try:
                        self.executor.change_state(*results)
                    finally:
                        self.executor.result_queue.task_done()
                except Empty:
                    break

        def end(self):
            """Ends the executor. Sends the poison pill to all workers."""
            for _ in self.executor.workers:
                self.queue.put((None, None))

            # Wait for commands to finish
            self.queue.join()
            self.executor.sync()

    def start(self) -> None:
        """Starts the executor"""
        old_proctitle = getproctitle()
        setproctitle("airflow executor -- LocalExecutor")
        self.manager = Manager()
        setproctitle(old_proctitle)
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = (
            RayExecutor.UnlimitedParallelism(self)
            if self.parallelism == 0
            else RayExecutor.LimitedParallelism(self)
        )

        self.impl.start()

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: Optional[str] = None,
        executor_config: Optional[Any] = None,
    ) -> None:
        """Execute asynchronously."""
        if not self.impl:
            raise AirflowException(NOT_STARTED_MESSAGE)

        self.validate_command(command)

        self.impl.execute_async(key=key, command=command, queue=queue, executor_config=executor_config)

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        if not self.impl:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.impl.sync()

    def end(self) -> None:
        """
        Ends the executor.
        :return:
        """
        if not self.impl:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.manager:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.log.info(
            "Shutting down LocalExecutor"
            "; waiting for running tasks to finish.  Signal again if you don't want to wait."
        )
        self.impl.end()
        self.manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
