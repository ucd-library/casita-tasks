"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import datetime, timedelta

from airflow import DAG
import json
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator


def getBandFile(**kwargs):
  files = kwargs['files']
  if isinstance(files, str):
    files = json.loads(files)
  return files.get(str(kwargs['band']), "")

def hasFile(**kwargs):
  if kwargs.get('file') is None or kwargs.get('file') == "":
    return f"end_{kwargs.get('index')}"
  return f"composite_{kwargs.get('index')}"

with DAG(
  dag_id='block-composite-images',
  catchup=False,
  start_date=datetime(2015, 12, 1),
  dagrun_timeout=timedelta(minutes=2),
  tags=['block', 'composite-image'],
  max_active_runs=20,
  default_args={
    'depends_on_past': False
  },
  user_defined_filters={
    'fromjson': lambda s: json.loads(s),
    'tojson': lambda s: json.dumps(s)
  },
  params={
    "bands" : [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
  }
) as dag:
    start = DummyOperator(
        task_id='start',
        depends_on_past=False
    )

    for idx in dag.params['bands']:
      group = {};

      group[f'get_file_{idx}'] = PythonOperator(
        task_id=f'get_file_{idx}',
        python_callable=getBandFile,
        do_xcom_push=True,
        op_kwargs={
          "band": idx,
          "files": "{{ dag_run.conf['files'] | tojson }}"
        },
        dag=dag,
        depends_on_past=False
      )

      group[f'branch_{idx}'] = BranchPythonOperator(
        task_id=f'branch_{idx}',
        python_callable=hasFile,
        op_kwargs={
          'index' : idx,
          'file' : "{{ task_instance.xcom_pull(task_ids='get_file_"+str(idx)+"') }}"
        },
        dag=dag,
        depends_on_past=False
      )

      group[f'composite_{idx}'] = BashOperator(
        task_id=f'composite_{idx}',
        bash_command="casita image jp2-to-png -m --metadata-file={{ task_instance.xcom_pull(task_ids='get_file_"+str(idx)+"') }}",
        do_xcom_push=True,
        dag=dag,
        depends_on_past=False
      )

      group[f'web_{idx}'] = BashOperator(
        task_id=f'web_{idx}',
        bash_command='casita image png8-to-web -m -k=png-block-ready --file=$file',
        env={
          'file' : "{{ task_instance.xcom_pull(task_ids='compress_"+str(idx)+"') }}"
        },
        dag=dag,
        depends_on_past=False
      )
      
      group[f'end_{idx}'] = DummyOperator(
        task_id=f'end_{idx}',
        dag=dag,
        depends_on_past=False
      )

      start >> group[f'get_file_{idx}'] >> group[f'branch_{idx}']
      group[f'branch_{idx}'] >> group[f'composite_{idx}'] >> group[f'web_{idx}'] >> group[f'end_{idx}']
      group[f'branch_{idx}'] >> group[f'end_{idx}']

if __name__ == "__main__":
    dag.cli()