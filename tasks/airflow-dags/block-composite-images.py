"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='block-composite-images',
    catchup=False,
    start_date=datetime(2015, 12, 1),
    dagrun_timeout=timedelta(minutes=2),
    tags=['block', 'composite-image'],
    max_active_runs=100,
    params={"example_key": "example_value"},
) as dag:
    start = DummyOperator(
        task_id='start',
    )

    # [START howto_operator_bash]
    block_composite = BashOperator(
        task_id='block_composite',
        bash_command='casita image jp2-to-png --quiet -m -k=png-block-ready --directory {{ params.path }}',
    )
    # [END howto_operator_bash]

    another = DummyOperator(
        task_id='another',
    )

    end = DummyOperator(
        task_id='endly',
    )

    start >> block_composite >> another >> end


if __name__ == "__main__":
    dag.cli()