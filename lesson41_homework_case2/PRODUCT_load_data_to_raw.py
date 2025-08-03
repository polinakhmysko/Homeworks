from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os
import glob
from python_scripts.telegram_notification import on_success_callback, on_failure_callback


DATA_DIR = '/opt/airflow/dags/PRODUCT_input/'
PROCESSED_DIR = '/opt/airflow/dags/PRODUCT_processed/'

os.makedirs(PROCESSED_DIR, exist_ok=True)

class FileSensorWithXCom(FileSensor):
    def poke(self, context):
        files = glob.glob(self.filepath)
        if files:
            context['ti'].xcom_push(key='file_path', value=files[0])
            return True
        return False
    

def load_data_to_raw(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='wait_for_orders')

    df = pd.read_csv(file_path)

    pg_hook = PostgresHook(postgres_conn_id = 'my_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    df.to_sql(
        'orders_new',
        engine,
        schema='raw',
        if_exists='append',
        index=False
    )

    processed_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
    os.rename(file_path, processed_path)


with DAG(
    'PRODUCT_load_data_to_raw',
    description = 'Вставка данных в raw-слой из файлов',
    schedule_interval = '*/5 * * * *',
    start_date=datetime(2025, 7, 13),
    catchup=False,
    tags = ['raw'],
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback
) as dag:

    wait_for_orders = FileSensorWithXCom(
        task_id='wait_for_orders',
        fs_conn_id = 'fs_product',
        filepath = f'{DATA_DIR}orders_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    create_table_orders_new = PostgresOperator(
        task_id='create_table_orders_new',
        postgres_conn_id = 'my_postgres_conn',
        sql = """
            CREATE TABLE IF NOT EXISTS raw.orders_new (
                order_id TEXT,
                product_name TEXT,
                quantity INTEGER,
                price_per_unit NUMERIC,
                total_price NUMERIC
            );
        """
    )

    load_orders_task = PythonOperator(
        task_id = 'load_orders_task',
        python_callable = load_data_to_raw,
        provide_context=True
    )

    wait_for_orders >> create_table_orders_new >> load_orders_task