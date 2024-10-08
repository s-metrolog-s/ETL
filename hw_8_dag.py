# 1. Скачайте файлы boking.csv, client.csv и hotel.csv;
# 2. Создайте новый dag;
# 3. Создайте три оператора для получения данных и загрузите файлы. Передайте дата фреймы в оператор трансформации;
# 4. Создайте оператор который будет трансформировать данные:
# — Объедините все таблицы в одну;
# — Приведите даты к одному виду;
# — Удалите невалидные колонки;
# — Приведите все валюты к одной;
# 5. Создайте оператор загрузки в базу данных;
# 6. Запустите dag.

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2024, month=6, day=1).in_timezone('Europe/Moscow'),
    'email': ['sergey@sergey.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag1 = DAG(
    'Lesson_8',
    default_args=default_args,
    description="task",
    catchup=False,
    schedule_interval='0 6 * * *')

task1 = BashOperator(
    task_id='pyspark',
    bash_command='python3 /spark/files/Lesson_8/homework_8.py',
    dag=dag1)
