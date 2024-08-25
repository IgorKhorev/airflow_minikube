from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperotor
from airflow.operators.bash_operator import BashOperotor
from airflow.operators.python_operator import PythonOperotor
#
       
#
defualt_args = {
'owner': 'airflow',
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2024,8,25),
}

dag = DAG(
'khorev_test',
default_args=default_args,
description='A simple DAG to send an email',
shedule_interval = timedelta(days = 1),
#shedule_interval = '44 11 * * *',
cachup=False,
)

start = DummyOperator(
   task_id = 'start',
   dag=dag,
)

end = DummyOperator(
   task_id = 'end',
   dag = dag,
)

bash_task = BashOperator(
   task_id = 'print_date',
   bash_command = 'date',
   dag = dag,
)

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

start >> bash_task >> python_task >> end
