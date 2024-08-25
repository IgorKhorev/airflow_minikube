from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#
def generate_pascals_triangle(levels):
    # Начинаем с первого уровня треугольника
    triangle = [[1]]

    # Строим треугольник уровень за уровнем
    for i in range(1, levels):
        row = [1]  # каждый уровень начинается с 1
        for j in range(1, i):
            # Каждый элемент в строке является суммой двух элементов сверху
            row.append(triangle[i - 1][j - 1] + triangle[i - 1][j])
        row.append(1)  # каждый уровень заканчивается на 1
        triangle.append(row)

    return triangle


def print_pascals_triangle(triangle):
    max_width = len('   '.join(map(str, triangle[-1])))  # находим максимальную ширину строки
    for row in triangle:
        # Преобразуем все элементы строки в строковые представления и объединяем их через пробелы
        row_str = '   '.join(map(str, row))
        # Центрируем строку согласно максимальной ширине треугольника
        print(row_str.center(max_width))
  
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
    #schedule_interval=timedelta(days=1),
    shedule_interval = '5 7 * * * ',   
)
#
def print_hello():
    #print("Hello, world")
    # Генерируем треугольник Паскаля с 10 уровнями
    triangle = generate_pascals_triangle(10)
    # Печатаем треугольник Паскаля
    print_pascals_triangle(triangle)
       
python_task = PythonOperator(
   task_id = 'print_hello',
   python_callable = print_hello,
   dag = dag,
)

 
