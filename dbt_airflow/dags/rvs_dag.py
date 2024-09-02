from airflow.models import DAG
from airflow.models.baseoperator import chain
from pendulum import datetime
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import shutil
from rvscode import createstage
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor



# Define the DAG function a set of parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    dag_id="rvs_dag",
    default_args=default_args,  # Include default_args here
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)


    
dummy_task_start = DummyOperator(task_id='start', retries=3,     execution_timeout=timedelta(minutes=10)) # Set execution timeout)


azure_prefix_sensor = WasbPrefixSensor(
    task_id='azure_prefix_sensor',
    container_name='hvr-prueba-csv',
    prefix='hola_',  # Prefijo general para detectar cualquier blob
    azure_blob_storage_conn_id='blob_id',
    timeout=600,
    poke_interval=60,
    dag=dag,
)

CreateStageJob = PythonOperator(
    task_id='CREATE_AND_COPY_DATA',
    python_callable=createstage,
    op_kwargs={},  # Pass additional variables as keyword arguments
    provide_context=True,
    dag=dag,
)


dummy_task_start>> azure_prefix_sensor>>CreateStageJob