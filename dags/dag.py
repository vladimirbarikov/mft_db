import os
import sys
import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from tasks.extractor import extractor
from tasks.transformer import transformer
from tasks.loader import loader

# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
path = '/opt/airflow/'
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 12, 31),
    'catchup': False,
    'max_active_runs': 1
}

with DAG(
    dag_id='excel_processing_dag',
    schedule_interval=None,  # Никаких регулярных запусков, только ручное исполнение
    default_args=args,
) as dag:

    # Извлечение данных
    extractor_task = PythonOperator(
        task_id='extract_data',
        python_callable=extractor,
        op_kwargs={"file_path": "{{ dag_run.conf['file_path'] }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Трансформация данных
    transformer_task = PythonOperator(
        task_id='transform_data',
        python_callable=transformer,
        op_kwargs={"file_path": "{{ dag_run.conf['file_path'] }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Загрузка данных в базу
    loader_task = PythonOperator(
        task_id='load_data_into_db',
        python_callable=loader,
        op_kwargs={"file_path": "{{ dag_run.conf['file_path'] }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Удаление временного файла
    def cleanup_temp_file(**context):
        file_path = context['dag_run'].conf.get('file_path')
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            print(f"Удалён временный файл: {file_path}")

    cleanup_task = PythonOperator(
        task_id='cleanup_temp_file',
        python_callable=cleanup_temp_file,
        provide_context=True,
        dag=dag,
    )

    # Порядок выполнения задач
    extractor_task >> transformer_task >> loader_task >> cleanup_task
