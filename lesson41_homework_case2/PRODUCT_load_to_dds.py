from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import logging
from PRODUCT_models import Product, Order
from python_scripts.telegram_notification import on_success_callback, on_failure_callback

logger = logging.getLogger(__name__)

@dag(
    dag_id='PRODUCT_load_to_dds',
    description='Даг, который складывает данные из raw в dds слой',
    start_date=datetime(2025, 7, 13),
    schedule_interval='*/5 * * * *',
    tags=['dds'],
    catchup=False,
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback
)
def build_dds():
    @task()
    def extract_raw_data():
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()

        df = pd.read_sql("select * from raw.orders_new", conn)

        return df

    @task
    def transform_and_load_products(data_df):
        products_df = data_df[['product_name']].drop_duplicates().copy()

        valid_products = []
        for _, record in products_df.iterrows():
            try:
                product = Product(**record)
                valid_products.append(product.dict())
            except Exception as e:
                logger.error(f"Невалидная запись продукта {record}", e)

        products_valid_df = pd.DataFrame(valid_products)

        engine = PostgresHook(postgres_conn_id='my_postgres_conn').get_sqlalchemy_engine()

        if not products_valid_df.empty:
            existing_products = pd.read_sql('select product_name from dds.products_new', engine)
            existing_products_ids = set(existing_products['product_name'])
            products_valid_df = products_valid_df[~products_valid_df['product_name'].isin(existing_products_ids)]

        products_valid_df.to_sql('products_new', engine, schema='dds', if_exists='append', index=False)

        logger.info(f'кол-во строчек вставлено - {products_valid_df.shape[0]}')
        
    @task
    def transform_and_load_orders(data_df):         
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()
        p_id_df = pd.read_sql('select * from dds.products_new', conn)

        merged_df = data_df.merge(p_id_df, on='product_name', how='left')

        orders_df = merged_df[['order_id', 'product_name', 'quantity', 'price_per_unit', 'total_price', 'product_id']].drop_duplicates().copy()

        valid_orders = []
        for _, record in orders_df.iterrows():
            try:
                order = Order(**record)
                valid_orders.append(order.dict())
            except Exception as e:
                logger.error(f"Невалидная запись заказа {record}", e)

        orders_valid_df = pd.DataFrame(valid_orders)

        engine = PostgresHook(postgres_conn_id='my_postgres_conn').get_sqlalchemy_engine()

        if not orders_valid_df.empty:
            existing_orders = pd.read_sql('select order_id from dds.orders', engine)
            existing_orders_ids = set(existing_orders['order_id'])
            orders_valid_df = orders_valid_df[~orders_valid_df['order_id'].isin(existing_orders_ids)]

        orders_valid_df.to_sql('orders_new', engine, schema='dds', if_exists='append', index=False)

        logger.info(f'кол-во строчек вставлено - {orders_valid_df.shape[0]}')

    create_products_table = PostgresOperator(
        task_id='create_products_table',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS dds.products_new(
                product_id SERIAL PRIMARY KEY,
                product_name TEXT UNIQUE
            );
        """
    )

    create_orders_table = PostgresOperator(
        task_id='create_orders_table',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE table if not exists dds.orders_new(
                order_id TEXT primary key,
                product_name TEXT,
                quantity INTEGER,
                price_per_unit NUMERIC,
                total_price NUMERIC,
                product_id INTEGER REFERENCES dds.products_new(product_id)
            );
        """
    )

    df_orders = extract_raw_data()
    create_products_table >> create_orders_table >> df_orders >> transform_and_load_products(df_orders) >> transform_and_load_orders(df_orders)

build_dds()