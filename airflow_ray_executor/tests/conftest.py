#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import pytest
from contextlib import ExitStack, suppress

@pytest.fixture(scope="module")
def dagbag():
    from airflow.models.dagbag import DagBag

    # Ensure the DAGs we are looking at from the DB are up-to-date
    non_serialized_dagbag = DagBag(read_dags_from_db=False, include_examples=False)
    non_serialized_dagbag.sync_to_db()
    return DagBag(read_dags_from_db=True)


@pytest.fixture
def dag_maker(request):
    """
    The dag_maker helps us to create DAG, DagModel, and SerializedDAG automatically.

    You have to use the dag_maker as a context manager and it takes
    the same argument as DAG::

        with dag_maker(dag_id="mydag") as dag:
            task1 = DummyOperator(task_id='mytask')
            task2 = DummyOperator(task_id='mytask2')

    If the DagModel you want to use needs different parameters than the one
    automatically created by the dag_maker, you have to update the DagModel as below::

        dag_maker.dag_model.is_active = False
        session.merge(dag_maker.dag_model)
        session.commit()

    For any test you use the dag_maker, make sure to create a DagRun::

        dag_maker.create_dagrun()

    The dag_maker.create_dagrun takes the same arguments as dag.create_dagrun

    If you want to operate on serialized DAGs, then either pass ``serialized=True` to the ``dag_maker()``
    call, or you can mark your test/class/file with ``@pytest.mark.need_serialized_dag(True)``. In both of
    these cases the ``dag`` returned by the context manager will be a lazily-evaluated proxy object to the
    SerializedDAG.
    """
    import lazy_object_proxy

    # IMPORTANT: Delay _all_ imports from `airflow.*` to _inside a method_.
    # This fixture is "called" early on in the pytest collection process, and
    # if we import airflow.* here the wrong (non-test) config will be loaded
    # and "baked" in to various constants

    want_serialized = False

    # Allow changing default serialized behaviour with `@pytest.mark.need_serialized_dag` or
    # `@pytest.mark.need_serialized_dag(False)`
    serialized_marker = request.node.get_closest_marker("need_serialized_dag")
    if serialized_marker:
        (want_serialized,) = serialized_marker.args or (True,)

    from airflow.utils.log.logging_mixin import LoggingMixin

    class DagFactory(LoggingMixin):
        _own_session = False

        def __init__(self):
            from airflow.models import DagBag

            # Keep all the serialized dags we've created in this test
            self.dagbag = DagBag(os.devnull, include_examples=False, read_dags_from_db=False)

        def __enter__(self):
            self.dag.__enter__()
            if self.want_serialized:
                return lazy_object_proxy.Proxy(self._serialized_dag)
            return self.dag

        def _serialized_dag(self):
            return self.serialized_model.dag

        def get_serialized_data(self):
            try:
                data = self.serialized_model.data
            except AttributeError:
                raise RuntimeError("DAG serialization not requested")
            if isinstance(data, str):
                return json.loads(data)
            return data

        def __exit__(self, type, value, traceback):
            from airflow.models import DagModel
            from airflow.models.serialized_dag import SerializedDagModel

            dag = self.dag
            dag.__exit__(type, value, traceback)
            if type is not None:
                return

            dag.clear(session=self.session)
            dag.sync_to_db(self.session)
            self.dag_model = self.session.query(DagModel).get(dag.dag_id)

            if self.want_serialized:
                self.serialized_model = SerializedDagModel(dag)
                self.session.merge(self.serialized_model)
                serialized_dag = self._serialized_dag()
                self.dagbag.bag_dag(serialized_dag, root_dag=serialized_dag)
                self.session.flush()
            else:
                self.dagbag.bag_dag(self.dag, self.dag)

        def create_dagrun(self, **kwargs):
            from airflow.utils import timezone
            from airflow.utils.state import State
            from airflow.utils.types import DagRunType

            dag = self.dag
            kwargs = {
                "state": State.RUNNING,
                "start_date": self.start_date,
                "session": self.session,
                **kwargs,
            }
            # Need to provide run_id if the user does not either provide one
            # explicitly, or pass run_type for inference in dag.create_dagrun().
            if "run_id" not in kwargs and "run_type" not in kwargs:
                kwargs["run_id"] = "test"

            if "run_type" not in kwargs:
                kwargs["run_type"] = DagRunType.from_run_id(kwargs["run_id"])
            if kwargs.get("execution_date") is None:
                if kwargs["run_type"] == DagRunType.MANUAL:
                    kwargs["execution_date"] = self.start_date
                else:
                    kwargs["execution_date"] = dag.next_dagrun_info(None).logical_date
            if "data_interval" not in kwargs:
                logical_date = timezone.coerce_datetime(kwargs["execution_date"])
                if kwargs["run_type"] == DagRunType.MANUAL:
                    data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)
                else:
                    data_interval = dag.infer_automated_data_interval(logical_date)
                kwargs["data_interval"] = data_interval

            self.dag_run = dag.create_dagrun(**kwargs)
            for ti in self.dag_run.task_instances:
                ti.refresh_from_task(dag.get_task(ti.task_id))
            return self.dag_run

        def create_dagrun_after(self, dagrun, **kwargs):
            next_info = self.dag.next_dagrun_info(self.dag.get_run_data_interval(dagrun))
            if next_info is None:
                raise ValueError(f"cannot create run after {dagrun}")
            return self.create_dagrun(
                execution_date=next_info.logical_date,
                data_interval=next_info.data_interval,
                **kwargs,
            )

        def __call__(
            self, dag_id='test_dag', serialized=want_serialized, fileloc=None, session=None, **kwargs
        ):
            from airflow import settings
            from airflow.models import DAG
            from airflow.utils import timezone

            if session is None:
                self._own_session = True
                session = settings.Session()

            self.kwargs = kwargs
            self.session = session
            self.start_date = self.kwargs.get('start_date', None)
            default_args = kwargs.get('default_args', None)
            if default_args and not self.start_date:
                if 'start_date' in default_args:
                    self.start_date = default_args.get('start_date')
            if not self.start_date:

                if hasattr(request.module, 'DEFAULT_DATE'):
                    self.start_date = getattr(request.module, 'DEFAULT_DATE')
                else:
                    DEFAULT_DATE = timezone.datetime(2016, 1, 1)
                    self.start_date = DEFAULT_DATE
            self.kwargs['start_date'] = self.start_date
            self.dag = DAG(dag_id, **self.kwargs)
            self.dag.fileloc = fileloc or request.module.__file__
            self.want_serialized = serialized

            return self

        def cleanup(self):
            from airflow.models import DagModel, DagRun, TaskInstance, XCom
            from airflow.models.serialized_dag import SerializedDagModel
            # from airflow.models.taskmap import TaskMap
            from airflow.utils.retries import run_with_db_retries

            for attempt in run_with_db_retries(logger=self.log):
                with attempt:
                    dag_ids = list(self.dagbag.dag_ids)
                    if not dag_ids:
                        return
                    # To isolate problems here with problems from elsewhere on the session object
                    self.session.flush()

                    self.session.query(SerializedDagModel).filter(
                        SerializedDagModel.dag_id.in_(dag_ids)
                    ).delete(synchronize_session=False)
                    self.session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.query(TaskInstance).filter(TaskInstance.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.query(XCom).filter(XCom.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    self.session.query(DagModel).filter(DagModel.dag_id.in_(dag_ids)).delete(
                        synchronize_session=False,
                    )
                    # self.session.query(TaskMap).filter(TaskMap.dag_id.in_(dag_ids)).delete(
                    #     synchronize_session=False,
                    # )
                    self.session.commit()
                    if self._own_session:
                        self.session.expunge_all()

    factory = DagFactory()

    try:
        yield factory
    finally:
        factory.cleanup()
        with suppress(AttributeError):
            del factory.session

