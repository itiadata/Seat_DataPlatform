from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator
from datetime import  timedelta
from airflow.sensors.external_task import ExternalTaskSensor




dbt_project_path = "/usr/local/airflow/dags/dbt/ETL_quality"
dbt_venv_path = "/usr/local/airflow/dbt_venv"





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,8,1),
    'retries': 0
}


with DAG('ETL_PROCESS_DBT', default_args=default_args, schedule_interval='@once') as dag:
    task_1 = BashOperator(
        task_id='bronze',
        bash_command=f'source {dbt_venv_path}/bin/activate && cd {dbt_project_path} && dbt run --models bronze --profiles-dir .',
        dag=dag
    )

    task_2 = BashOperator(
        task_id='silver',
        bash_command=f'source {dbt_venv_path}/bin/activate && cd {dbt_project_path} && dbt run --models silver --profiles-dir .',
        dag=dag
    )

    task_3 = BashOperator(
        task_id='gold',
        bash_command=f'source {dbt_venv_path}/bin/activate && cd {dbt_project_path} && dbt run --models gold --profiles-dir .',
        dag=dag
    )
    

    wait_for_other_dag = ExternalTaskSensor(
            task_id='WAIT',
            external_dag_id='calidad_carpot_dag',  # Reemplaza con el ID del DAG del cual quieres esperar
            external_task_id='end',  # Reemplaza con el ID de la tarea específica en el otro DAG
            execution_delta=timedelta(minutes=0),  # Tiempo de espera antes de ejecutar la verificación
            timeout=600,  # Tiempo máximo de espera en segundos
            allowed_states=['success'],  # Estados permitidos para continuar
            failed_states=['failed', 'skipped'],  # Estados en los que se considera fallida la espera
        )
    
    task_1 >> task_2 >> task_3




