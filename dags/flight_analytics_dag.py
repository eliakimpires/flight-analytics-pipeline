from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="flight_analytics_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,  # Alterado para 'None' para rodar apenas manualmente
    catchup=False,
    tags=["data_engineering", "flights"],
) as dag:
    
    ingest_data = BashOperator(
        task_id="ingest_data",
        bash_command="python /opt/airflow/app/ingest_data.py",
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=(
            "rm -f /tmp/flights.db && "
            
            "dbt run "
            "--project-dir /opt/airflow/app/dbt_project "
            "--profiles-dir /root/.dbt"
        ),
    )

    # Define a ordem de execução
    ingest_data >> transform_data