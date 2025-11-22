import os
import sys
import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from tasks.extractor import extractor
from tasks.transformer import transformer
from tasks.loader import loader

# Add the path to the project code to the environment variable
# so that it is accessible to the python process.
path = '/opt/airflow/'
os.environ['PROJECT_PATH'] = path

# Add the path to the project code in $PATH to import the functions.
sys.path.insert(0, path)

# DAG configuration 
args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 12, 31),
    'catchup': False,
    'max_active_runs': 1
}

# Tsks configuration
with DAG(
    dag_id='excel_processing_dag',
    schedule_interval=None,
    default_args=args,
) as dag:

    # Data extraction task
    extractor_task = PythonOperator(
        task_id='extract_data',
        python_callable=extractor,
        op_kwargs={"file_path": "{{ dag_run.conf['file_path'] }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Data transformation task
    transformer_task = PythonOperator(
        task_id='transform_data',
        python_callable=transformer,
        op_kwargs={"file_path": "{{ dag_run.conf['file_path'] }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Uploading data to the database task
    loader_task = PythonOperator(
        task_id='load_data_into_db',
        python_callable=loader,
        op_kwargs={"file_path": "{{ dag_run.conf['file_path'] }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Deleting a temporary file
    def cleanup_temp_file(**context):
        '''
        Func deletes a temporary file
        '''
        file_path = context['dag_run'].conf.get('file_path')
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            print(f"Удалён временный файл: {file_path}")

    # Deleting a temporary file task
    cleanup_task = PythonOperator(
        task_id='cleanup_temp_file',
        python_callable=cleanup_temp_file,
        provide_context=True,
        dag=dag,
    )

    # Task execution order
    extractor_task >> transformer_task >> loader_task >> cleanup_task
