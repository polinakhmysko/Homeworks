from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
from python_scripts.telegram_notification import on_success_callback, on_failure_callback

def delete_from_table():
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    hook.run("DELETE FROM data_mart.product_popularity", autocommit=True)

with DAG(
    'PRODUCT_dm_product_popularity',
    description = 'Анализ популярности товаров',
    schedule_interval = '*/5 * * * *',
    start_date=datetime(2025, 7, 13),
    catchup=False,
    tags = ['data_mart'],
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback
) as dag:
    
    create_dm_product_popularit_table = PostgresOperator(
        task_id='create_dm_product_popularit_table',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE table if not exists data_mart.product_popularity(
                product_id INTEGER primary key,
                product_name TEXT,
                total_quantity_sold INTEGER,
                total_revenue NUMERIC,
                order_count INTEGER,
                avg_price NUMERIC
            );
        """
    )

    delete_from_table_task = PythonOperator(
        task_id = 'delete_from_table_task',
        python_callable = delete_from_table
    )

    dm_product_popularity_task = PostgresOperator(
        task_id="dm_product_popularity",
        postgres_conn_id="my_postgres_conn",
        sql="""
        INSERT INTO data_mart.product_popularity (product_id, product_name, total_quantity_sold, total_revenue, order_count, avg_price)
        SELECT
            product_id,
            product_name,
            sum(quantity) as total_quantity_sold,
            sum(total_price) as total_revenue,
            count(order_id) as order_count,
            round((sum(total_price)/sum(quantity)), 2) as avg_price
        FROM dds.orders_new
        GROUP BY product_id, product_name
        ;
        """,
    )

    create_dm_product_popularit_table >> delete_from_table_task >> dm_product_popularity_task