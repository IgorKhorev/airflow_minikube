from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#
       
#
default_args = {
'owner': 'airflow',
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2024,8,25),
}

#dag = DAG(
#'khorev_test',
#default_args=default_args,
#description='A simple DAG to send an email',
#shedule_interval=timedelta(days = 1),
#)
#
# Определение DAG
dag = DAG(
    'khorev_test',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
)
#
def print_hello():
    print("Hello, world")
    # Генерируем треугольник Паскаля с 10 уровнями
    #triangle = generate_pascals_triangle(10)
    # Печатаем треугольник Паскаля
    #print_pascals_triangle(triangle)
       
python_task = PythonOperator(
   task_id = 'print_hello',
   python_callable = print_hello,
   dag = dag,
)

 
