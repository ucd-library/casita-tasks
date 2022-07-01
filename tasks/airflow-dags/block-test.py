from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
import json

with DAG(
  dag_id='block-test',
  catchup=False,
  start_date=datetime(2015, 12, 1),
  dagrun_timeout=timedelta(minutes=2),
  tags=['block', 'composite-image', 'test'],
  max_active_runs=5,
  default_args={
    'depends_on_past': False
  },
  user_defined_filters={},
  params={}
) as dag:

  start = DummyOperator(
    task_id='start',
    depends_on_past=False
  )

  run = BashOperator(
    task_id=f'run',
    bash_command="echo {{ dag_run.conf['files'] }}",
    depends_on_past=False
  )

  start >> run


if __name__ == "__main__":
    dag.cli()