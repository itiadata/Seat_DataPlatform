import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": datetime(2021,3,22,17,15)}

dag = DAG(
    dag_id="snowflake_connector3", default_args=args, schedule_interval=None
)

query1 = [
    """select 1;""",
    """show tables in database ft_db;""",
]

def count1(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_id")
    result = dwh_hook.get_first("select current_date ")
    logging.info("Number of rows in `abcd_db.public.test3`  - %s", result[0])


with dag:
    query1_exec = SnowflakeOperator(
        task_id="snowfalke_task1",
        sql=query1,
        snowflake_conn_id="snowflake_id",
    )

    count_query = PythonOperator(task_id="count_query", python_callable=count1)
query1_exec >> count_query