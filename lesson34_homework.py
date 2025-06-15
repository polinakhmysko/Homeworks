from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import pandas as pd
import os

default_args = {
    'owner': 'polina',
    'start_date': datetime(2025, 6, 1),
}

base_path = '/opt/airflow/files'
os.makedirs(base_path, exist_ok=True)


def write_start_time(**kwargs):
    start_time = datetime.now().isoformat()
    with open(os.path.join(base_path, 'start_time.txt'), 'w') as f:
        f.write(f"Start Time: {start_time}")


def generate_users_file(**kwargs):
    first_names = ['Сергей', 'Татьяна', 'Евгений', 'Мария']
    last_names = ['Смирнов', 'Иванов', 'Петрова', 'Сидоров']
    cities = ['Минск', 'Москва', 'Гродно', 'Санкт-Петербург']

    data = []
    for i in range(random.randint(10, 20)):
        row = {
            'Имя': random.choice(first_names),
            'Фамилия': random.choice(last_names),
            'Город': random.choice(cities),
        }
        data.append(row)

    df = pd.DataFrame(data)
    file_path = os.path.join(base_path, 'users.xlsx')
    df.to_excel(file_path, index=False)


def generate_sales_file(**kwargs):
    items = ['Бананы', 'Яблоки', 'Апельсины', 'Манго']
    lines = []

    for i in range(random.randint(30, 80)):
        line = f"{random.choice(items)},{random.randint(1, 10)}шт,{random.randint(50, 300)}р"
        lines.append(line)

    with open(os.path.join(base_path, 'sales.txt'), 'w') as f:
        f.write('\n'.join(lines))


def rows_ammount(**kwargs):
    users_path = os.path.join(base_path, 'users.xlsx')
    sales_path = os.path.join(base_path, 'sales.txt')

    users_df = pd.read_excel(users_path)
    with open(sales_path, 'r') as f:
        sales_lines = f.readlines()

    print(f"Количество строк в файле users.xlsx: {len(users_df)}")
    print(f"Количество строк в файле sales.txt: {len(sales_lines)}")


with DAG(
    'generate_files_dag',
    default_args=default_args,
    description='Генерация Excel и TXT файлов с выводом количества строк в каждом файле',
    schedule_interval=None,
    catchup=False,
) as dag:

    start_task = PythonOperator(
        task_id='start_generation',
        python_callable=write_start_time
    )

    generate_users_task = PythonOperator(
        task_id='generate_users_file',
        python_callable=generate_users_file
    )

    generate_sales_task = PythonOperator(
        task_id='generate_sales_file',
        python_callable=generate_sales_file
    )

    final_task = PythonOperator(
        task_id='rows_ammount',
        python_callable=rows_ammount
    )

start_task >> [generate_users_task, generate_sales_task] >> final_task
