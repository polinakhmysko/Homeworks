from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import random
import pandas as pd
from python_scripts.telegram_notification import on_success_callback, on_failure_callback


DATA_DIR = '/opt/airflow/dags/PRODUCT_input/'
os.makedirs(DATA_DIR, exist_ok=True)

products_name = [
    'Кроссовки',
    'Футболка',
    'Худи',
    'Ветровка',
    'Бейсболка',
    'Рюкзак'
]

def generate_data():
    orders = []

    for _ in range(random.randint(5, 10)):
        order_id = f'O-{random.randint(1000, 9999)}'
        product_name = random.choice(products_name)
        quantity = random.randint(1, 3)
        price_per_unit = round(random.uniform(50, 300), 2)
        total_price = round(quantity * price_per_unit, 2)

        orders.append({
            'order_id': order_id,
            'product_name': product_name,
            'quantity': quantity,
            'price_per_unit': price_per_unit,
            'total_price': total_price
        })

    time_now = datetime.now().strftime('%Y%m%d%H%M%S')

    df_orders = pd.DataFrame(orders)

    df_orders.to_csv(os.path.join(DATA_DIR, f'orders_{time_now}.csv'), index=False)


with DAG(
    'PRODUCT_generate_data',
    description = 'Генерация данных',
    schedule_interval = '*/5 * * * *',
    start_date = datetime(2025, 7, 13),
    catchup=False,
    tags = ['raw'],
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback
) as dag:
    
    generate_data_task = PythonOperator(
        task_id = 'generate_data_task',
        python_callable=generate_data
    )

    generate_data_task