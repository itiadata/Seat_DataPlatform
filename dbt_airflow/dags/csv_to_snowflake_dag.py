from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from csvtosnowflake import createstage


# Define the DAG function a set of parameters
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    dag_id="CSV_TO_SNOWFLAKE_DAG",
    default_args=default_args,  # Include default_args here
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)


dummy_task_start = DummyOperator(
    task_id="start", retries=3, execution_timeout=timedelta(minutes=10)
)  # Set execution timeout)


CreateStageJob = PythonOperator(
    task_id="CREATE_AND_COPY_DATA",
    python_callable=createstage,
    op_kwargs={},  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)


dummy_task_start >> CreateStageJob
